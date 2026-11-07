//! Backend-independent transaction boundary. Memory and RocksDB both implement it.
use super::{memory::MemoryStore, model::*, rocks::RocksStore};
use std::{future::Future, io, path::Path, pin::Pin, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StoreKind {
    Memory,
    Rocks,
}

pub(super) type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send + 'a>>;

pub(super) fn claim_directory(
    path: &Path,
    identity: (&str, u64),
    members: &[(u64, String)],
    marker: &str,
    blocked: &[(&str, &'static str)],
    quota: &'static str,
) -> io::Result<std::fs::File> {
    use std::io::{Read, Write};
    std::fs::create_dir_all(path)?;
    for (name, message) in blocked {
        match std::fs::symlink_metadata(path.join(name)) {
            Ok(_) => return Err(io::Error::new(io::ErrorKind::InvalidInput, *message)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path.join("data.lock"))?;
    lock.try_lock().map_err(|_| {
        io::Error::new(
            io::ErrorKind::WouldBlock,
            "replica directory is already in use",
        )
    })?;
    let mut members = members.to_vec();
    members.sort();
    let encoded = serde_json::to_vec(&(identity, members))
        .map_err(|error| io::Error::other(error.to_string()))?;
    if identity.0.is_empty() || identity.0.len() > 128 || identity.1 == 0 || encoded.len() > 65536 {
        return Err(io::Error::new(io::ErrorKind::WouldBlock, quota));
    }
    let manifest = path.join(marker);
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&manifest)
    {
        Ok(mut file) => {
            file.write_all(&encoded)?;
            file.sync_data()?;
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let metadata = std::fs::symlink_metadata(&manifest)?;
            if !metadata.is_file() || metadata.len() > 65536 {
                return Err(io::Error::new(io::ErrorKind::WouldBlock, quota));
            }
            let mut bytes = Vec::new();
            std::fs::File::open(&manifest)?
                .take(65537)
                .read_to_end(&mut bytes)?;
            if bytes != encoded {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "replica identity or membership mismatch",
                ));
            }
        }
        Err(error) => return Err(error),
    }
    Ok(lock)
}

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
        Self::open_with(path, identity, members, StoreKind::Memory)
    }
    pub(crate) fn open_with(
        path: &Path,
        identity: (&str, u64),
        members: &[(u64, String)],
        kind: StoreKind,
    ) -> io::Result<Self> {
        let store: Arc<dyn ReplicaStore> = match kind {
            StoreKind::Memory => Arc::new(MemoryStore::open(path, identity, members)?),
            StoreKind::Rocks => Arc::new(RocksStore::open(path, identity, members)?),
        };
        Ok(Self(store))
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
