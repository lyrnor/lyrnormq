//! Quorum register RPC; fences return values from the same successful quorum.
use super::*;
use crate::diagnosis::{self, Failure};
use crate::replica::{Journal, Record, Request};
use std::sync::Arc;
pub(super) mod batch;
mod driver;

pub(super) struct PreparedRequest {
    pub request: Request,
    pub record_canonical: Option<Arc<[u8]>>,
    /// Canonical JSON of `request`. Write records are encoded once and spliced.
    pub wire: Arc<[u8]>,
}

pub(super) fn prepare_request(request: Request) -> io::Result<PreparedRequest> {
    match &request {
        Request::Write { key, record } => {
            let record_canonical: Arc<[u8]> =
                serde_json::to_vec(record).map_err(io::Error::other)?.into();
            if record_canonical.len() > crate::replica::MAX_RECORD {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "replica record limit",
                ));
            }
            let key_json = serde_json::to_vec(key).map_err(io::Error::other)?;
            let mut wire = Vec::with_capacity(record_canonical.len() + key_json.len() + 32);
            wire.extend_from_slice(br#"{"Write":{"key":"#);
            wire.extend_from_slice(&key_json);
            wire.extend_from_slice(br#","record":"#);
            wire.extend_from_slice(&record_canonical);
            wire.extend_from_slice(br#"}}"#);
            Ok(PreparedRequest {
                request,
                record_canonical: Some(record_canonical),
                wire: wire.into(),
            })
        }
        _ => {
            let wire: Arc<[u8]> = serde_json::to_vec(&request)
                .map_err(io::Error::other)?
                .into();
            Ok(PreparedRequest {
                request,
                record_canonical: None,
                wire,
            })
        }
    }
}

#[cfg(test)]
#[derive(serde::Serialize)]
struct BorrowedReplica<'a> {
    generation: u64,
    operation: &'a Request,
}

#[cfg(test)]
fn encode_remote(generation: u64, operation: &Request) -> io::Result<wire::Envelope> {
    let _timer = crate::profile::Timer::start(crate::profile::Metric::QuorumRemoteEncode);
    let envelope = rpc::encode(&BorrowedReplica {
        generation,
        operation,
    })?;
    crate::profile::observe(
        crate::profile::Metric::QuorumRemoteBytes,
        envelope.data.len() as u64,
    );
    Ok(envelope)
}

pub(super) async fn request(
    runtime: &BrokerRuntime,
    journal: &Journal,
    request: Request,
) -> io::Result<Option<Record>> {
    let started = tokio::time::Instant::now();
    if !runtime.ready() {
        return Err(diagnosis::reject(Failure::QuorumNotReady));
    }
    let peers = &runtime
        .config
        .durable
        .as_ref()
        .ok_or_else(unavailable)?
        .replicas;
    let batcher = runtime.replica_batcher().await?.clone();
    let generation = runtime
        .metadata
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .data_generation;
    let prepared = prepare_request(request)?;
    let mut jobs = Vec::with_capacity(peers.len());
    for peer in peers {
        let request = prepared.request.clone();
        let job: driver::PeerFuture = if peer.id == runtime.config.node_id {
            let journal = journal.clone();
            let canonical = prepared.record_canonical.clone();
            Box::pin(async move {
                journal
                    .execute_prepared(generation, request, canonical)
                    .await
            })
        } else {
            let batcher = batcher.clone();
            let address = peer.address.clone();
            let wire = Arc::clone(&prepared.wire);
            Box::pin(async move {
                batcher
                    .submit_prepared(&address, generation, request, wire)
                    .await
            })
        };
        jobs.push(job);
    }
    driver::start(jobs, started)
        .await
        .map_err(|_| unavailable())?
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn borrowed_remote_encoding_preserves_wire_bytes_and_shares_clones() {
        let mut record = Record::empty(17);
        record.snapshot = Some(vec![23; 4096]);
        let operation = Request::Write {
            key: "c:device".into(),
            record,
        };
        let actual = encode_remote(9, &operation).unwrap();
        let expected = rpc::encode(&ReplicaMessage {
            generation: 9,
            operation: operation.clone(),
        })
        .unwrap();
        let prepared = prepare_request(operation.clone()).unwrap();
        assert_eq!(
            prepared.wire.as_ref(),
            serde_json::to_vec(&operation).unwrap()
        );
        assert_eq!(actual.data, expected.data);
        let other = actual.clone();
        assert_eq!(actual.data.as_ptr(), other.data.as_ptr());
        let decoded: ReplicaMessage = rpc::decode(other).unwrap();
        assert_eq!(decoded.generation, 9);
        assert!(matches!(decoded.operation, Request::Write { record, .. } if record.epoch==17));
    }
}
