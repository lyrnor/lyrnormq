//! Compatibility and buffer-ownership regressions for the private RPC envelope.
use super::*;
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct LegacyEnvelope {
    #[prost(bytes = "vec", tag = "1")]
    data: Vec<u8>,
}

#[test]
fn protobuf_and_json_bytes_remain_identical_to_legacy_envelope() {
    let value = serde_json::json!({"nested": [0, 1, 255], "text": "你好"});
    let body = serde_json::to_vec(&value).unwrap();
    let envelope = encode(&value).unwrap();
    assert_eq!(&envelope.data[..], body.as_slice());
    let legacy = LegacyEnvelope { data: body.clone() };
    assert_eq!(envelope.encode_to_vec(), legacy.encode_to_vec());
    let current = wire::Envelope::decode(legacy.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decode::<serde_json::Value>(current).unwrap(), value);
    let old = LegacyEnvelope::decode(envelope.encode_to_vec().as_slice()).unwrap();
    assert_eq!(old.data, body);
    assert_eq!(wire::Envelope::default().encode_to_vec(), Vec::<u8>::new());
}

#[test]
fn cloned_envelopes_share_immutable_body_allocation() {
    let envelope = encode(&vec![42u16; 2048]).unwrap();
    let cloned = envelope.clone();
    assert_eq!(cloned.data.as_ptr(), envelope.data.as_ptr());
    drop(envelope);
    assert_eq!(decode::<Vec<u16>>(cloned).unwrap(), vec![42; 2048]);
}

#[test]
fn decoding_bytes_envelope_keeps_the_received_body_slice() {
    let body = serde_json::to_vec(&vec![42u16; 2048]).unwrap();
    let encoded: prost::bytes::Bytes = LegacyEnvelope { data: body.clone() }.encode_to_vec().into();
    let expected = encoded.as_ptr().wrapping_add(encoded.len() - body.len());
    let envelope = wire::Envelope::decode(encoded.clone()).unwrap();
    assert_eq!(envelope.data.as_ptr(), expected);
    drop(encoded);
    assert_eq!(&envelope.data[..], body.as_slice());
}

#[test]
fn envelope_limits_and_invalid_json_are_preserved() {
    let exact = "x".repeat(MAX_WIRE - 2);
    let envelope = encode(&exact).unwrap();
    assert_eq!(envelope.data.len(), MAX_WIRE);
    assert_eq!(decode::<String>(envelope).unwrap(), exact);
    assert_eq!(
        encode(&"x".repeat(MAX_WIRE - 1)).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let oversized = wire::Envelope {
        data: vec![b'x'; MAX_WIRE + 1].into(),
    };
    assert_eq!(
        decode::<String>(oversized).unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    let malformed = wire::Envelope {
        data: vec![0xff].into(),
    };
    assert_eq!(
        decode::<String>(malformed).unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
}

async fn fixture() -> io::Result<(ClientRpc, std::path::PathBuf)> {
    let mut random = [0; 16];
    getrandom::getrandom(&mut random).map_err(io::Error::other)?;
    let name: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
    let path = std::env::temp_dir().join(format!("rmqtt-rpc-buffer-{name}"));
    tokio::fs::create_dir(&path).await?;
    tokio::fs::write(path.join("token"), "a".repeat(64)).await?;
    let config: Config = serde_json::from_value(serde_json::json!({
        "role": "broker", "cluster_id": "rpc-buffer-cluster", "node_id": 101,
        "listen": "127.0.0.1:7101", "data_dir": path, "token_file": path.join("token"),
        "controllers": [{"id": 1, "address": "http://127.0.0.1:7001"},
                        {"id": 2, "address": "http://127.0.0.1:7002"},
                        {"id": 3, "address": "http://127.0.0.1:7003"}]
    }))
    .map_err(io::Error::other)?;
    let rpc = ClientRpc::new(Arc::new(config)).await?;
    Ok((rpc, path))
}

#[tokio::test]
async fn authentication_metadata_is_shared_and_requests_stay_independent() -> io::Result<()> {
    let (rpc, path) = fixture().await?;
    let first = rpc.authorize(())?;
    let mut second = rpc.authorize(())?;
    for key in ["authorization", "x-rmqtt-cluster"] {
        assert_eq!(
            first.metadata().get(key).unwrap().as_bytes().as_ptr(),
            second.metadata().get(key).unwrap().as_bytes().as_ptr(),
            "constant metadata should share its prevalidated allocation"
        );
    }
    rpc.check(&first).unwrap();
    rpc.check(&second).unwrap();
    second
        .metadata_mut()
        .insert("authorization", "invalid".parse().unwrap());
    assert_eq!(
        rpc.check(&second).unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    rpc.check(&first).unwrap();
    rpc.check(&rpc.authorize(())?).unwrap();
    tokio::fs::remove_dir_all(path).await
}

#[tokio::test]
async fn authentication_rejects_changed_lengths_edges_and_cluster() -> io::Result<()> {
    let (rpc, path) = fixture().await?;
    let correct = format!("Bearer {}", "a".repeat(64));
    let mut wrong_first = correct.clone().into_bytes();
    wrong_first[0] = b'b';
    let mut wrong_last = correct.clone().into_bytes();
    *wrong_last.last_mut().unwrap() = b'b';
    for value in [
        String::from_utf8(wrong_first).unwrap(),
        String::from_utf8(wrong_last).unwrap(),
        correct[..correct.len() - 1].to_owned(),
        format!("{correct}a"),
        String::new(),
    ] {
        let mut request = rpc.authorize(())?;
        request
            .metadata_mut()
            .insert("authorization", value.parse().unwrap());
        assert_eq!(
            rpc.check(&request).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }
    let mut missing = rpc.authorize(())?;
    missing.metadata_mut().remove("authorization");
    assert_eq!(
        rpc.check(&missing).unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    let mut foreign = rpc.authorize(())?;
    foreign
        .metadata_mut()
        .insert("x-rmqtt-cluster", "other".parse().unwrap());
    assert_eq!(
        rpc.check(&foreign).unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    let mut missing_cluster = rpc.authorize(())?;
    missing_cluster.metadata_mut().remove("x-rmqtt-cluster");
    assert_eq!(
        rpc.check(&missing_cluster).unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    tokio::fs::remove_dir_all(path).await
}
