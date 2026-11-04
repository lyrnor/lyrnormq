//! Quorum register RPC; fences return values from the same successful quorum.
use super::*;
use crate::diagnosis::{self, Failure};
use crate::profile::{self, Metric, Timer};
use crate::replica::{Journal, Record, Reply, Request};
mod driver;

#[derive(serde::Serialize)]
struct BorrowedReplica<'a> {
    generation: u64,
    operation: &'a Request,
}

fn encode_remote(generation: u64, operation: &Request) -> io::Result<wire::Envelope> {
    let _timer = Timer::start(Metric::QuorumRemoteEncode);
    let envelope = rpc::encode(&BorrowedReplica {
        generation,
        operation,
    })?;
    profile::observe(Metric::QuorumRemoteBytes, envelope.data.len() as u64);
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
    let rpc = runtime.rpc().await?.clone();
    let generation = runtime
        .metadata
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .data_generation;
    // Encode before moving the original request into the local store future.
    // Bytes-backed Envelope clones share this immutable wire body across remotes.
    let encoded = encode_remote(generation, &request)?;
    let mut local_request = Some(request);
    let mut jobs = Vec::with_capacity(peers.len());
    for peer in peers {
        let job: driver::PeerFuture = if peer.id == runtime.config.node_id {
            let request = local_request.take().ok_or_else(unavailable)?;
            let journal = journal.clone();
            Box::pin(async move { journal.execute_at(generation, request).await })
        } else {
            let rpc = rpc.clone();
            let address = peer.address.clone();
            let envelope = encoded.clone();
            Box::pin(async move {
                let mut client = rpc.client(&address).await?;
                let reply = client
                    .replica(rpc.authorize(envelope)?)
                    .await
                    .map_err(rpc::io_error)?;
                rpc::decode::<Reply>(reply.into_inner()).map_err(rpc::io_error)
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
        assert_eq!(actual.data, expected.data);
        let other = actual.clone();
        assert_eq!(actual.data.as_ptr(), other.data.as_ptr());
        let decoded: ReplicaMessage = rpc::decode(other).unwrap();
        assert_eq!(decoded.generation, 9);
        assert!(matches!(decoded.operation, Request::Write { record, .. } if record.epoch==17));
    }
}
