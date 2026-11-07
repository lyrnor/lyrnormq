use crate::{
    ClientSubscriptions, ClusterMessage, DeliveryId, ManagementCounts, ManagementPage,
    ManagementQuery, ManagementRetained, ManagementSession, ManagementSubscription, SessionCommit,
    SessionLease, SessionMutation, TopologyView,
};
use std::{fmt::Debug, future::Future, io, pin::Pin, sync::Arc};

pub type ClusterFuture<'a, T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send + 'a>>;

/// Administrator configuration replicated through Controller Raft.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SharedConfigKind {
    Rules,
    Access,
}

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
    /// Client ids with a live local connection. Defaults to subscription owners.
    fn online_client_ids(&self) -> ClusterFuture<'_, Vec<String>> {
        Box::pin(async move {
            Ok(self
                .subscriptions()
                .await?
                .into_iter()
                .map(|row| row.client_id)
                .collect())
        })
    }
    /// Apply or clear one retained topic on this Broker's local store.
    fn apply_retained(&self, _message: ClusterMessage) -> ClusterFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
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
    /// for ordinary recipients and one selected shared member per matching group.
    /// Transport must suppress its separate local fanout.
    fn forward(&self, message: ClusterMessage) -> ClusterFuture<'_, ()>;
    /// Best-effort ordinary delivery. Replicated mode must not admit this into an outbox.
    fn forward_ephemeral(&self, message: ClusterMessage) -> ClusterFuture<'_, ()> {
        self.forward(message)
    }
    /// Quorum-write one retained topic (`r:{topic}`). Empty/expired payload clears it.
    fn commit_retained(&self, _message: ClusterMessage) -> ClusterFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    /// Replica retained records matching ordinary (non-shared) filters.
    fn matching_retained(&self, _filters: Vec<String>) -> ClusterFuture<'_, Vec<ClusterMessage>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    /// Commit one validated administrator config document. Returns the new generation.
    fn put_shared_config(
        &self,
        _kind: SharedConfigKind,
        _expected_generation: u64,
        _document: String,
    ) -> ClusterFuture<'_, u64> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "cluster shared config unavailable",
            ))
        })
    }
    /// Latest committed generation known locally, if any document exists.
    fn shared_config_generation(&self, _kind: SharedConfigKind) -> Option<u64> {
        None
    }
    /// Install local apply hooks for committed rules/access documents.
    fn set_shared_config_sink(&self, _sink: Arc<dyn SharedConfigSink>) {}
    /// Read-only topology for management; never blocks the MQTT hot path.
    fn topology(&self) -> ClusterFuture<'_, TopologyView> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "cluster topology unavailable",
            ))
        })
    }
    /// Cluster-directory session/subscription/retained totals for overview cards.
    fn management_counts(&self) -> ClusterFuture<'_, ManagementCounts> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "cluster management unavailable",
            ))
        })
    }
    /// Page sessions from Controller directory metadata (claims + durable `c:`).
    fn management_sessions(
        &self,
        _query: ManagementQuery,
    ) -> ClusterFuture<'_, ManagementPage<ManagementSession>> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "cluster management unavailable",
            ))
        })
    }
    /// Expand directory filters into subscription rows (QoS options unavailable).
    fn management_subscriptions(
        &self,
        _query: ManagementQuery,
    ) -> ClusterFuture<'_, ManagementPage<ManagementSubscription>> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "cluster management unavailable",
            ))
        })
    }
    /// Page retained topics from durable `r:` ownership; enrich bodies when possible.
    fn management_retained(
        &self,
        _query: ManagementQuery,
    ) -> ClusterFuture<'_, ManagementPage<ManagementRetained>> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "cluster management unavailable",
            ))
        })
    }
    /// One directory session by client id, if present.
    fn management_session(
        &self,
        _client_id: String,
    ) -> ClusterFuture<'_, Option<ManagementSession>> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "cluster management unavailable",
            ))
        })
    }
    fn shutdown(&self) -> ClusterFuture<'_, ()>;
}

/// Broker-side application of a committed shared configuration document.
pub trait SharedConfigSink: Debug + Send + Sync {
    fn apply_rules(&self, generation: u64, document: String) -> ClusterFuture<'_, ()>;
    fn apply_access(&self, generation: u64, document: String) -> ClusterFuture<'_, ()>;
}
