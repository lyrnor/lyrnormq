//! Quorum register RPC; fences return values from the same successful quorum.
use super::*;
use crate::replica::{Journal, Record, Reply, Request};
use tokio::task::JoinSet;

pub(super) async fn request(
    runtime: &BrokerRuntime,
    journal: &Journal,
    request: Request,
) -> io::Result<Option<Record>> {
    if !runtime.ready() {
        return Err(unavailable());
    }
    let peers = &runtime
        .config
        .durable
        .as_ref()
        .ok_or_else(unavailable)?
        .replicas;
    let rpc = runtime.rpc().await?.clone();
    let mut jobs = JoinSet::new();
    for peer in peers {
        let peer = peer.clone();
        let rpc = rpc.clone();
        let request = request.clone();
        let journal = journal.clone();
        let local = runtime.config.node_id;
        jobs.spawn(async move {
            if peer.id == local {
                journal.execute(request).await
            } else {
                let mut client = rpc.client(&peer.address).await?;
                let reply = client
                    .replica(rpc.authorize(rpc::encode(&request)?)?)
                    .await
                    .map_err(rpc::io_error)?;
                rpc::decode::<Reply>(reply.into_inner()).map_err(rpc::io_error)
            }
        });
    }
    let mut responses = 0;
    let mut chosen: Option<Record> = None;
    while let Some(result) = jobs.join_next().await {
        if let Ok(Ok(reply)) = result {
            responses += 1;
            if let Some(record) = reply.record {
                if let Some(old) = &chosen {
                    if old.version() == record.version()
                        && serde_json::to_vec(old).map_err(io::Error::other)?
                            != serde_json::to_vec(&record).map_err(io::Error::other)?
                    {
                        return Err(unavailable());
                    }
                    if old.version() >= record.version() {
                        if responses >= 2 {
                            break;
                        } else {
                            continue;
                        }
                    }
                }
                chosen = Some(record);
            }
            if responses >= 2 {
                break;
            }
        }
    }
    if responses >= 2 {
        Ok(chosen)
    } else {
        Err(unavailable())
    }
}
