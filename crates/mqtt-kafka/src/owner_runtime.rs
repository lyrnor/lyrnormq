use super::*;
use krafka::{
    Kafka, KrafkaError,
    auth::{AuthConfig, TlsConfig},
    producer::{Acks, Producer},
};
use std::collections::BTreeMap;
use tokio::task::JoinSet;

fn classify(e: KrafkaError) -> SendError {
    let code = match e {
        KrafkaError::Auth { .. } => "kafka_auth",
        KrafkaError::Config { .. } => "kafka_config",
        KrafkaError::Broker { code, .. } => match code {
            krafka::error::ErrorCode::TopicAuthorizationFailed
            | krafka::error::ErrorCode::ClusterAuthorizationFailed
            | krafka::error::ErrorCode::SaslAuthenticationFailed => "kafka_auth",
            krafka::error::ErrorCode::InvalidTopic => "kafka_topic",
            krafka::error::ErrorCode::MessageTooLarge
            | krafka::error::ErrorCode::InvalidMessageSize => "request_too_large",
            _ => "kafka_delivery",
        },
        _ => "kafka_delivery",
    };
    SendError {
        kind: if code == "kafka_delivery" {
            FailureKind::Retryable
        } else {
            FailureKind::Permanent
        },
        code: code.into(),
    }
}
async fn build(c: &KafkaConfig, id: &str, timeout: Duration) -> Result<Producer, SendError> {
    let s = &c.security;
    let u = s.username.as_deref().unwrap_or_default();
    let p = s.password.as_deref().unwrap_or_default();
    let mut auth = match s.sasl_mechanism.as_deref() {
        Some("PLAIN") => AuthConfig::sasl_plain(u, p),
        Some("SCRAM-SHA-256") => AuthConfig::sasl_scram_sha256(u, p),
        Some("SCRAM-SHA-512") => AuthConfig::sasl_scram_sha512(u, p),
        _ => AuthConfig::default(),
    };
    if s.protocol.tls() {
        let mut tls = TlsConfig::new();
        if let Some(ca) = &s.ca_pem {
            tls = tls.with_ca_cert_pem(ca.as_bytes().to_vec());
        } else {
            tls = tls.with_native_roots();
        }
        if let (Some(cert), Some(key)) = (&s.certificate_pem, &s.private_key_pem) {
            tls = tls.with_client_cert_pem(cert.as_bytes().to_vec(), key.as_bytes().to_vec());
        }
        if let Some(p) = &s.private_key_password {
            tls = tls.with_client_key_password(p);
        }
        auth = auth.with_tls(tls);
    }
    let kafka = Kafka::builder(c.brokers.join(","))
        .client_id(format!("rmqtt-{id}"))
        .security(auth)
        .connect_timeout((timeout / 2).min(Duration::from_secs(10)))
        .request_timeout((timeout / 2).min(Duration::from_secs(10)))
        .max_connections(Some(64))
        .max_response_size(8 * 1024 * 1024)
        .connect()
        .await
        .map_err(classify)?;
    let compression = match c.compression {
        crate::Compression::None => krafka::Compression::None,
        crate::Compression::Lz4 => krafka::Compression::Lz4,
        crate::Compression::Zstd => krafka::Compression::Zstd,
    };
    kafka
        .producer()
        .acks(Acks::All)
        .idempotent(true)
        .buffer_memory(8 * 1024 * 1024)
        .max_request_size(1024 * 1024)
        .batch_size(c.batch_bytes)
        .linger(Duration::from_millis(c.linger_ms))
        .delivery_timeout(timeout)
        .max_block(timeout)
        .compression(compression)
        .metrics_push(false)
        .build()
        .await
        .map_err(classify)
}
pub(super) async fn run(
    receiver: &mut mpsc::Receiver<Command>,
    closing: &mut watch::Receiver<bool>,
    c: &KafkaConfig,
    id: &str,
    timeout: Duration,
) -> Vec<Reply> {
    let first = tokio::select! {biased;_=closing.changed()=>return vec![],cmd=receiver.recv()=>match cmd{Some(cmd)=>cmd,None=>return vec![]}};
    if first.deadline <= Instant::now() {
        let _ = first.reply.sender.send(Err(retry("kafka_delivery")));
        return vec![];
    }
    let producer = tokio::select! {biased;_=closing.changed()=>return vec![first.reply],result=tokio::time::timeout_at(first.deadline.into(),build(c,id,timeout))=>match result{
     Ok(Ok(p))=>Arc::new(p),Ok(Err(e))=>{let mut reply=first.reply;reply.error=Some(e);return vec![reply]},Err(_)=>return vec![first.reply]
    }};
    let mut pending = BTreeMap::new();
    let mut jobs = JoinSet::new();
    let mut deadlines = BTreeMap::new();
    let mut next = 0u64;
    let mut command = Some(first);
    loop {
        if let Some(cmd) = command.take() {
            if cmd.deadline <= Instant::now() {
                let _ = cmd.reply.sender.send(Err(retry("kafka_delivery")));
                continue;
            }
            let key = next;
            next = next.wrapping_add(1);
            deadlines.insert(key, cmd.deadline);
            pending.insert(key, cmd.reply);
            let p = producer.clone();
            jobs.spawn(async move {
                let result = match p.enqueue(cmd.record).await {
                    Ok(handle) => handle.await.map(|m| m.is_persisted()),
                    Err(e) => Err(e),
                };
                (key, result)
            });
        }
        let deadline = deadlines
            .values()
            .copied()
            .min()
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(86400));
        tokio::select! {biased;
            _=closing.changed()=>break,
            _=tokio::time::sleep_until(deadline.into()),if !pending.is_empty()=>break,
            result=jobs.join_next(),if !jobs.is_empty()=>{
             match result{Some(Ok((key,result)))=>{
              if let Err(e)=&result && (e.is_fatal() || matches!(e,KrafkaError::DeliveryTimeout{..})) {
          if let Some(reply)=pending.get_mut(&key) { reply.error=Some(classify(e.clone())); }
          break;
        }
              deadlines.remove(&key);
              if let Some(reply)=pending.remove(&key){let answer=match result{Ok(true)=>Ok(()),Ok(false)=>Err(retry("kafka_delivery")),Err(e)=>Err(classify(e))};let _=reply.sender.send(answer);}
             },_=>break}
            },
            cmd=receiver.recv(),if pending.len()<512=>match cmd{Some(cmd)=>command=Some(cmd),None=>break}
           }
    }
    pending.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn authentication_is_permanent_without_secret_details() {
        let e = classify(KrafkaError::Auth {
            message: "secret".into(),
            source: None,
        });
        assert_eq!(e.kind, FailureKind::Permanent);
        assert_eq!(e.code, "kafka_auth");
    }
}
