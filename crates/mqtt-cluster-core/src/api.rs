use crate::{ClientSubscriptions, ClusterMessage};
use std::{fmt::Debug, future::Future, io, pin::Pin, sync::Arc};

pub type ClusterFuture<'a, T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send + 'a>>;

/// Trusted authenticated cluster ingress; implementations never reflect deliveries.
pub trait LocalBroker: Debug + Send + Sync {
    fn deliver(&self, message: ClusterMessage) -> ClusterFuture<'_, ()>;
    /// Close exactly this local connection generation; a newer one is unaffected.
    fn fence(&self, client_id: String, generation: u64) -> ClusterFuture<'_, ()>;
    fn fence_all(&self) -> ClusterFuture<'_, ()>;
    fn subscriptions(&self) -> ClusterFuture<'_, Vec<ClientSubscriptions>>;
}
/// Runtime-neutral metadata and forwarding bridge. Every awaited operation must
/// have a finite deadline; forwarding/mutation admission must be bounded.
pub trait ClusterBridge: Debug + Send + Sync {
    fn ready(&self) -> bool;
    fn open(&self, local: Arc<dyn LocalBroker>) -> ClusterFuture<'_, ()>;
    fn claim(&self, client_id: String, generation: u64) -> ClusterFuture<'_, ()>;
    fn release(&self, client_id: String, generation: u64) -> ClusterFuture<'_, ()>;
    fn subscriptions(&self, client_id: String, filters: Vec<String>) -> ClusterFuture<'_, ()>;
    /// Route using the installed local metadata, exclude local ordinary recipients;
    /// local ordinary dispatch remains transport-owned. Shared selection is deferred.
    fn forward(&self, message: ClusterMessage) -> ClusterFuture<'_, ()>;
    fn shutdown(&self) -> ClusterFuture<'_, ()>;
}
