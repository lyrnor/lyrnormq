use crate::{
    ClientSubscriptions, ClusterMessage, DeliveryId, SessionCommit, SessionLease, SessionMutation,
};
use std::{fmt::Debug, future::Future, io, pin::Pin, sync::Arc};

pub type ClusterFuture<'a, T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send + 'a>>;

/// Trusted authenticated cluster ingress; implementations never reflect deliveries.
pub trait LocalBroker: Debug + Send + Sync {
    /// Install an authoritative offline partition without a synthetic CONNECT.
    fn activate_session(&self, _lease: SessionLease) -> ClusterFuture<'_, ()> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "durable cluster unavailable",
            ))
        })
    }
    /// Receipt and recipient state must share one replicated transaction.
    fn deliver_to(
        &self,
        _client_id: String,
        _id: DeliveryId,
        _message: ClusterMessage,
    ) -> ClusterFuture<'_, ()> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "durable cluster unavailable",
            ))
        })
    }
    fn deliver(&self, message: ClusterMessage) -> ClusterFuture<'_, ()>;
    /// Close exactly this local connection generation; a newer one is unaffected.
    fn fence(&self, client_id: String, generation: u64) -> ClusterFuture<'_, ()>;
    fn fence_all(&self) -> ClusterFuture<'_, ()>;
    fn subscriptions(&self) -> ClusterFuture<'_, Vec<ClientSubscriptions>>;
}
/// Runtime-neutral metadata and forwarding bridge. Every awaited operation must
/// have a finite deadline; forwarding/mutation admission must be bounded.
pub trait ClusterBridge: Debug + Send + Sync {
    /// Historical name for the replicated session/outbox transaction path.
    /// This flag does not promise disk persistence: memory replicas acknowledge
    /// live quorum acceptance and lose their local data when the process dies.
    fn durable(&self) -> bool {
        false
    }
    fn acquire_session(
        &self,
        _client_id: String,
        _generation: u64,
        _expected_epoch: u64,
    ) -> ClusterFuture<'_, SessionLease> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "durable cluster unavailable",
            ))
        })
    }
    /// Quorum-read the opaque state before ownership mutation. Epoch zero means
    /// no directory owner exists; it remains a CAS precondition at acquisition.
    fn inspect_session(&self, _client_id: String) -> ClusterFuture<'_, SessionLease> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "durable cluster unavailable",
            ))
        })
    }
    fn commit_session(&self, _mutation: SessionMutation) -> ClusterFuture<'_, SessionCommit> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "durable cluster unavailable",
            ))
        })
    }
    /// Finite startup-only deadline, independent of forwarding/commit deadlines.
    /// Memory replacement may wait for explicit catch-up before admitting clients.
    fn startup_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(5)
    }
    fn ready(&self) -> bool;
    fn open(&self, local: Arc<dyn LocalBroker>) -> ClusterFuture<'_, ()>;
    fn claim(&self, client_id: String, generation: u64) -> ClusterFuture<'_, ()>;
    fn release(&self, client_id: String, generation: u64) -> ClusterFuture<'_, ()>;
    fn subscriptions(
        &self,
        client_id: String,
        generation: u64,
        filters: Vec<String>,
    ) -> ClusterFuture<'_, ()>;
    /// Legacy mode routes remote ordinary recipients using local metadata.
    /// Replicated mode accepts host/system publications into the shared outbox
    /// for all recipients; transport must suppress its separate local fanout.
    /// Shared selection remains deferred.
    fn forward(&self, message: ClusterMessage) -> ClusterFuture<'_, ()>;
    /// Best-effort ordinary delivery. Replicated mode must not admit this into an outbox.
    fn forward_ephemeral(&self, message: ClusterMessage) -> ClusterFuture<'_, ()> {
        self.forward(message)
    }
    fn shutdown(&self) -> ClusterFuture<'_, ()>;
}
