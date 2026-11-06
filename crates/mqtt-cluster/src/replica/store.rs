//! Backend-independent transaction boundary; a future disk backend implements this trait.
use super::{memory::MemoryStore, model::*};
use std::{future::Future, io, path::Path, pin::Pin, sync::Arc};

pub(super) type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send + 'a>>;

/// Every operation is linearizable; Fence returns the value under the same fence.
/// Acknowledgement means backend acceptance, whose durability is backend-specific.
pub(super) trait ReplicaStore: Send + Sync {
    fn disk_id(&self) -> &str;
    fn voting_ready(&self, generation: u64) -> bool;
    fn execute_at(&self, generation: u64, request: Request) -> StoreFuture<'_, Reply>;
    /// `canonical` is the Record JSON already produced for this write, when the caller has one.
    fn execute_prepared(
        &self,
        generation: u64,
        request: Request,
        canonical: Option<Arc<[u8]>>,
    ) -> StoreFuture<'_, Reply> {
        let _ = canonical;
        self.execute_at(generation, request)
    }
    /// Runs `requests` in order under one lock. One item's rejection does not roll back the others.
    fn execute_many_at(
        &self,
        generation: u64,
        requests: Vec<Request>,
    ) -> StoreFuture<'_, Vec<io::Result<Reply>>>;
    fn status(&self) -> StoreFuture<'_, RepairStoreStatus>;
    fn freeze(&self, token: RepairToken) -> StoreFuture<'_, RepairStoreStatus>;
    fn export_page(
        &self,
        token: RepairToken,
        after: Option<String>,
        limit: usize,
    ) -> StoreFuture<'_, RepairPage>;
    fn begin_import(
        &self,
        token: RepairToken,
        manifest: RepairManifest,
    ) -> StoreFuture<'_, RepairStoreStatus>;
    fn append_page(
        &self,
        token: RepairToken,
        sequence: u64,
        entries: Vec<RepairEntry>,
    ) -> StoreFuture<'_, RepairStoreStatus>;
    fn finish_import(&self, token: RepairToken) -> StoreFuture<'_, RepairStoreStatus>;
    fn resume(
        &self,
        token: RepairToken,
        allow_unfrozen_advance: bool,
    ) -> StoreFuture<'_, RepairStoreStatus>;
}

#[derive(Clone)]
pub(crate) struct Journal(Arc<dyn ReplicaStore>);
impl Journal {
    /// Performs bounded identity-only filesystem IO; call on startup's blocking pool.
    pub fn open(path: &Path, identity: (&str, u64), members: &[(u64, String)]) -> io::Result<Self> {
        Ok(Self(Arc::new(MemoryStore::open(path, identity, members)?)))
    }
    pub fn disk_id(&self) -> &str {
        self.0.disk_id()
    }
    pub fn voting_ready(&self, generation: u64) -> bool {
        self.0.voting_ready(generation)
    }
    #[cfg(test)]
    pub async fn execute(&self, request: Request) -> io::Result<Reply> {
        self.execute_at(0, request).await
    }
    pub async fn execute_at(&self, generation: u64, request: Request) -> io::Result<Reply> {
        self.0.execute_at(generation, request).await
    }
    pub async fn execute_prepared(
        &self,
        generation: u64,
        request: Request,
        canonical: Option<Arc<[u8]>>,
    ) -> io::Result<Reply> {
        self.0
            .execute_prepared(generation, request, canonical)
            .await
    }
    pub async fn execute_many(
        &self,
        generation: u64,
        requests: Vec<Request>,
    ) -> io::Result<Vec<io::Result<Reply>>> {
        self.0.execute_many_at(generation, requests).await
    }
    pub async fn status(&self) -> io::Result<RepairStoreStatus> {
        self.0.status().await
    }
    pub async fn freeze(&self, token: RepairToken) -> io::Result<RepairStoreStatus> {
        self.0.freeze(token).await
    }
    pub async fn export_page(
        &self,
        token: RepairToken,
        after: Option<String>,
        limit: usize,
    ) -> io::Result<RepairPage> {
        self.0.export_page(token, after, limit).await
    }
    pub async fn begin_import(
        &self,
        token: RepairToken,
        manifest: RepairManifest,
    ) -> io::Result<RepairStoreStatus> {
        self.0.begin_import(token, manifest).await
    }
    pub async fn append_page(
        &self,
        token: RepairToken,
        sequence: u64,
        entries: Vec<RepairEntry>,
    ) -> io::Result<RepairStoreStatus> {
        self.0.append_page(token, sequence, entries).await
    }
    pub async fn finish_import(&self, token: RepairToken) -> io::Result<RepairStoreStatus> {
        self.0.finish_import(token).await
    }
    pub async fn resume(
        &self,
        token: RepairToken,
        allow_unfrozen_advance: bool,
    ) -> io::Result<RepairStoreStatus> {
        self.0.resume(token, allow_unfrozen_advance).await
    }
}
