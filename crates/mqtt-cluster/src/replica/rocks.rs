//! Durable replica records. Acknowledgement follows a synced RocksDB batch.
use super::{
    memory::{MemoryStore, PersistOp, RepairDelta, decode_entry, state_from_loaded},
    model::*,
    store::{ReplicaStore, StoreFuture, claim_directory},
};
use rocksdb::{DB, Options, WriteBatch, WriteOptions};
use std::{
    collections::BTreeMap,
    io,
    path::Path,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    thread::JoinHandle,
};

const QUEUE: usize = 64;

enum Command {
    Execute {
        generation: u64,
        request: Request,
        canonical: Option<Arc<[u8]>>,
        reply: tokio::sync::oneshot::Sender<io::Result<Reply>>,
    },
    ExecuteMany {
        generation: u64,
        requests: Vec<Request>,
        reply: tokio::sync::oneshot::Sender<Vec<io::Result<Reply>>>,
    },
    Status {
        reply: tokio::sync::oneshot::Sender<RepairStoreStatus>,
    },
    Freeze {
        token: RepairToken,
        reply: tokio::sync::oneshot::Sender<io::Result<RepairStoreStatus>>,
    },
    Export {
        token: RepairToken,
        after: Option<String>,
        limit: usize,
        reply: tokio::sync::oneshot::Sender<io::Result<RepairPage>>,
    },
    Begin {
        token: RepairToken,
        manifest: RepairManifest,
        reply: tokio::sync::oneshot::Sender<io::Result<RepairStoreStatus>>,
    },
    Append {
        token: RepairToken,
        sequence: u64,
        entries: Vec<RepairEntry>,
        reply: tokio::sync::oneshot::Sender<io::Result<RepairStoreStatus>>,
    },
    Finish {
        token: RepairToken,
        reply: tokio::sync::oneshot::Sender<io::Result<RepairStoreStatus>>,
    },
    Resume {
        token: RepairToken,
        allow: bool,
        reply: tokio::sync::oneshot::Sender<io::Result<RepairStoreStatus>>,
    },
}

pub(super) struct RocksStore {
    disk_id: String,
    generation: Arc<AtomicU64>,
    frozen: Arc<AtomicBool>,
    tx: SyncSender<Command>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl RocksStore {
    pub(super) fn open(
        path: &Path,
        identity: (&str, u64),
        members: &[(u64, String)],
    ) -> io::Result<Self> {
        let lock = claim_directory(
            path,
            identity,
            members,
            "rocks.identity",
            &[
                (
                    "data.sqlite",
                    "legacy data.sqlite cannot be opened as rocksdb replicas; use a new cluster and separate directories",
                ),
                (
                    "memory.identity",
                    "memory replica directory cannot be opened as rocksdb; use a new directory",
                ),
            ],
            "rocksdb replica unavailable or quota exceeded",
        )?;
        let directory = path.join("rocks");
        if std::fs::symlink_metadata(&directory).is_ok_and(|meta| meta.file_type().is_symlink()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rocksdb directory must not be a symlink",
            ));
        }
        std::fs::create_dir_all(&directory)?;
        let db = open_db(&directory)?;
        let disk_id = ensure_identity(&db)?;
        let state = load_state(&db)?;
        let (ready_generation, ready_frozen) = state.readiness();
        let (generation, frozen) = (
            Arc::new(AtomicU64::new(ready_generation)),
            Arc::new(AtomicBool::new(ready_frozen)),
        );
        let db = Arc::new(db);
        let writer = Arc::clone(&db);
        let persist = Arc::new(move |op: &PersistOp| write_op(&writer, op));
        let store = MemoryStore::from_loaded(lock, disk_id.clone(), state, Some(persist));
        let (tx, rx) = sync_channel(QUEUE);
        let ready_generation = Arc::clone(&generation);
        let ready_frozen = Arc::clone(&frozen);
        let worker = std::thread::Builder::new()
            .name("lyrnormq-rocks".into())
            .spawn(move || serve(store, rx, ready_generation, ready_frozen))
            .map_err(io::Error::other)?;
        Ok(Self {
            disk_id,
            generation,
            frozen,
            tx,
            worker: Mutex::new(Some(worker)),
        })
    }
}

impl Drop for RocksStore {
    fn drop(&mut self) {
        let (closed, rx) = sync_channel(1);
        drop(rx);
        let tx = std::mem::replace(&mut self.tx, closed);
        drop(tx);
        if let Some(worker) = self
            .worker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            let _ = worker.join();
        }
    }
}

fn serve(
    store: MemoryStore,
    rx: Receiver<Command>,
    generation: Arc<AtomicU64>,
    frozen: Arc<AtomicBool>,
) {
    let publish = |store: &MemoryStore| {
        let (current, is_frozen) = store.ready_bits();
        generation.store(current, Ordering::Release);
        frozen.store(is_frozen, Ordering::Release);
    };
    publish(&store);
    while let Ok(command) = rx.recv() {
        match command {
            Command::Execute {
                generation,
                request,
                canonical,
                reply,
            } => {
                let result = store.execute_now(generation, request, canonical);
                publish(&store);
                let _ = reply.send(result);
            }
            Command::ExecuteMany {
                generation,
                requests,
                reply,
            } => {
                let result = store.execute_many_now(generation, requests);
                publish(&store);
                let _ = reply.send(result);
            }
            Command::Status { reply } => {
                let _ = reply.send(store.status_now());
            }
            Command::Freeze { token, reply } => {
                let result = store.freeze_sync(token);
                publish(&store);
                let _ = reply.send(result);
            }
            Command::Export {
                token,
                after,
                limit,
                reply,
            } => {
                let _ = reply.send(store.export_sync(token, after, limit));
            }
            Command::Begin {
                token,
                manifest,
                reply,
            } => {
                let result = store.begin_sync(token, manifest);
                publish(&store);
                let _ = reply.send(result);
            }
            Command::Append {
                token,
                sequence,
                entries,
                reply,
            } => {
                let result = store.append_sync(token, sequence, entries);
                publish(&store);
                let _ = reply.send(result);
            }
            Command::Finish { token, reply } => {
                let result = store.finish_sync(token);
                publish(&store);
                let _ = reply.send(result);
            }
            Command::Resume {
                token,
                allow,
                reply,
            } => {
                let result = store.resume_sync(token, allow);
                publish(&store);
                let _ = reply.send(result);
            }
        }
    }
}

impl ReplicaStore for RocksStore {
    fn disk_id(&self) -> &str {
        &self.disk_id
    }
    fn voting_ready(&self, generation: u64) -> bool {
        !self.frozen.load(Ordering::Acquire)
            && self.generation.load(Ordering::Acquire) == generation
    }
    fn execute_at(&self, generation: u64, request: Request) -> StoreFuture<'_, Reply> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply, rx) = tokio::sync::oneshot::channel();
            tx.try_send(Command::Execute {
                generation,
                request,
                canonical: None,
                reply,
            })
            .map_err(|_| busy())?;
            rx.await.map_err(|_| stopped())?
        })
    }
    fn execute_prepared(
        &self,
        generation: u64,
        request: Request,
        canonical: Option<Arc<[u8]>>,
    ) -> StoreFuture<'_, Reply> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply, rx) = tokio::sync::oneshot::channel();
            tx.try_send(Command::Execute {
                generation,
                request,
                canonical,
                reply,
            })
            .map_err(|_| busy())?;
            rx.await.map_err(|_| stopped())?
        })
    }
    fn execute_many_at(
        &self,
        generation: u64,
        requests: Vec<Request>,
    ) -> StoreFuture<'_, Vec<io::Result<Reply>>> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply, rx) = tokio::sync::oneshot::channel();
            tx.try_send(Command::ExecuteMany {
                generation,
                requests,
                reply,
            })
            .map_err(|_| busy())?;
            rx.await.map_err(|_| stopped())
        })
    }
    fn status(&self) -> StoreFuture<'_, RepairStoreStatus> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply, rx) = tokio::sync::oneshot::channel();
            tx.try_send(Command::Status { reply }).map_err(|_| busy())?;
            rx.await.map_err(|_| stopped())
        })
    }
    fn freeze(&self, token: RepairToken) -> StoreFuture<'_, RepairStoreStatus> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply, rx) = tokio::sync::oneshot::channel();
            tx.try_send(Command::Freeze { token, reply })
                .map_err(|_| busy())?;
            rx.await.map_err(|_| stopped())?
        })
    }
    fn export_page(
        &self,
        token: RepairToken,
        after: Option<String>,
        limit: usize,
    ) -> StoreFuture<'_, RepairPage> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply, rx) = tokio::sync::oneshot::channel();
            tx.try_send(Command::Export {
                token,
                after,
                limit,
                reply,
            })
            .map_err(|_| busy())?;
            rx.await.map_err(|_| stopped())?
        })
    }
    fn begin_import(
        &self,
        token: RepairToken,
        manifest: RepairManifest,
    ) -> StoreFuture<'_, RepairStoreStatus> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply, rx) = tokio::sync::oneshot::channel();
            tx.try_send(Command::Begin {
                token,
                manifest,
                reply,
            })
            .map_err(|_| busy())?;
            rx.await.map_err(|_| stopped())?
        })
    }
    fn append_page(
        &self,
        token: RepairToken,
        sequence: u64,
        entries: Vec<RepairEntry>,
    ) -> StoreFuture<'_, RepairStoreStatus> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply, rx) = tokio::sync::oneshot::channel();
            tx.try_send(Command::Append {
                token,
                sequence,
                entries,
                reply,
            })
            .map_err(|_| busy())?;
            rx.await.map_err(|_| stopped())?
        })
    }
    fn finish_import(&self, token: RepairToken) -> StoreFuture<'_, RepairStoreStatus> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply, rx) = tokio::sync::oneshot::channel();
            tx.try_send(Command::Finish { token, reply })
                .map_err(|_| busy())?;
            rx.await.map_err(|_| stopped())?
        })
    }
    fn resume(&self, token: RepairToken, allow: bool) -> StoreFuture<'_, RepairStoreStatus> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply, rx) = tokio::sync::oneshot::channel();
            tx.try_send(Command::Resume {
                token,
                allow,
                reply,
            })
            .map_err(|_| busy())?;
            rx.await.map_err(|_| stopped())?
        })
    }
}

fn busy() -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, "rocksdb replica is busy")
}
fn stopped() -> io::Error {
    io::Error::other("rocksdb replica worker stopped")
}
fn storage(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("rocksdb replica storage failed: {error}"))
}
fn rejected() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "rocksdb replica state is invalid",
    )
}

fn open_db(path: &Path) -> io::Result<DB> {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);
    options.set_max_background_jobs(2);
    options.set_write_buffer_size(8 * 1024 * 1024);
    options.set_max_write_buffer_number(2);
    options.set_keep_log_file_num(4);
    options.set_max_log_file_size(1024 * 1024);
    DB::open(&options, path).map_err(storage)
}

fn sync_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options
}

fn meta(name: &str) -> Vec<u8> {
    let mut key = vec![0];
    key.extend_from_slice(name.as_bytes());
    key
}
fn record_key(key: &str) -> Vec<u8> {
    let mut encoded = vec![1];
    encoded.extend_from_slice(key.as_bytes());
    encoded
}
fn staging_key(key: &str) -> Vec<u8> {
    let mut encoded = vec![2];
    encoded.extend_from_slice(key.as_bytes());
    encoded
}

fn write_op(db: &DB, op: &PersistOp) -> io::Result<()> {
    let mut batch = WriteBatch::default();
    match op {
        PersistOp::Record { key, value } => put_optional(&mut batch, &record_key(key), value),
        PersistOp::Repair(delta) => write_repair(&mut batch, delta),
    }
    db.write_opt(batch, &sync_options()).map_err(storage)
}

fn put_optional(batch: &mut WriteBatch, key: &[u8], value: &Option<Vec<u8>>) {
    match value {
        Some(bytes) => batch.put(key, bytes),
        None => batch.delete(key),
    }
}

fn write_repair(batch: &mut WriteBatch, delta: &RepairDelta) {
    batch.put(meta("repair"), &delta.repair_json);
    for (key, value) in &delta.records {
        put_optional(batch, &record_key(key), value);
    }
    for (key, value) in &delta.staging {
        put_optional(batch, &staging_key(key), value);
    }
}

fn ensure_identity(db: &DB) -> io::Result<String> {
    match db.get(meta("format")).map_err(storage)? {
        Some(version) if version == b"1" => {
            let bytes = db
                .get(meta("disk"))
                .map_err(storage)?
                .ok_or_else(rejected)?;
            let disk_id = String::from_utf8(bytes).map_err(|_| rejected())?;
            if disk_id.len() == 32 && disk_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                Ok(disk_id)
            } else {
                Err(rejected())
            }
        }
        Some(_) => Err(rejected()),
        None => {
            if prefixed(db, 1)?.next().is_some() || prefixed(db, 2)?.next().is_some() {
                return Err(rejected());
            }
            let mut random = [0u8; 16];
            getrandom::getrandom(&mut random)
                .map_err(|error| io::Error::other(error.to_string()))?;
            let disk_id = random
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let repair = encode_empty_repair()?;
            let mut batch = WriteBatch::default();
            batch.put(meta("format"), b"1");
            batch.put(meta("disk"), disk_id.as_bytes());
            batch.put(meta("repair"), repair);
            db.write_opt(batch, &sync_options()).map_err(storage)?;
            Ok(disk_id)
        }
    }
}

fn encode_empty_repair() -> io::Result<Vec<u8>> {
    Ok(br#"{"generation":0,"frozen":null,"last_token":null,"manifest":null,"next_page":0,"last_key":null,"stream_bytes":0,"records":0,"importing":false,"installed":false,"pages":{}}"#.to_vec())
}

fn load_state(db: &DB) -> io::Result<super::memory::State> {
    let repair = db.get(meta("repair")).map_err(storage)?;
    let mut records = BTreeMap::new();
    for item in prefixed(db, 1)? {
        let (key, value) = item?;
        records.insert(key, decode_entry(&value)?);
    }
    let mut staging = BTreeMap::new();
    for item in prefixed(db, 2)? {
        let (key, value) = item?;
        staging.insert(key, decode_entry(&value)?);
    }
    state_from_loaded(records, staging, repair.as_deref())
}

fn prefixed(db: &DB, prefix: u8) -> io::Result<Prefixed<'_>> {
    Ok(Prefixed {
        prefix,
        iterator: db.iterator(rocksdb::IteratorMode::From(
            &[prefix],
            rocksdb::Direction::Forward,
        )),
    })
}

struct Prefixed<'a> {
    prefix: u8,
    iterator: rocksdb::DBIterator<'a>,
}

impl Iterator for Prefixed<'_> {
    type Item = io::Result<(String, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = match self.iterator.next()? {
            Ok(item) => item,
            Err(error) => return Some(Err(storage(error))),
        };
        if key.first() != Some(&self.prefix) {
            return None;
        }
        let text = match std::str::from_utf8(&key[1..]) {
            Ok(text) if !text.is_empty() && text.len() <= 65538 => text,
            _ => return Some(Err(rejected())),
        };
        Some(Ok((text.to_owned(), value.to_vec())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica::{Journal, StoreKind};

    fn path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "lyrnormq-rocks-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ))
    }
    fn open(path: &Path) -> Journal {
        Journal::open_with(
            path,
            ("rocks-test", 101),
            &[(101, "http://127.0.0.1:7101".into())],
            StoreKind::Rocks,
        )
        .expect("open rocks")
    }
    fn record(epoch: u64, payload: &[u8]) -> Record {
        let mut record = Record::empty(epoch);
        record.snapshot = Some(payload.to_vec());
        record
    }

    #[tokio::test]
    async fn restart_keeps_identity_record_and_fence() {
        let directory = path("restart");
        let journal = open(&directory);
        let disk_id = journal.disk_id().to_owned();
        journal
            .execute(Request::Write {
                key: "client".into(),
                record: record(3, b"accepted"),
            })
            .await
            .expect("write");
        let fenced = journal
            .execute(Request::Fence {
                key: "client".into(),
                epoch: 9,
            })
            .await
            .expect("fence")
            .record
            .expect("fenced record");
        assert_eq!(fenced.snapshot.as_deref(), Some(b"accepted".as_slice()));
        drop(journal);
        let journal = open(&directory);
        assert_eq!(journal.disk_id(), disk_id);
        assert!(journal.voting_ready(0));
        let restored = journal
            .execute(Request::Read {
                key: "client".into(),
            })
            .await
            .expect("read")
            .record
            .expect("restored");
        assert_eq!(restored.snapshot.as_deref(), Some(b"accepted".as_slice()));
        assert!(
            journal
                .execute(Request::Write {
                    key: "client".into(),
                    record: record(8, b"stale"),
                })
                .await
                .is_err()
        );
        journal
            .execute(Request::Write {
                key: "client".into(),
                record: record(9, b"next"),
            })
            .await
            .expect("replacement");
        drop(journal);
        std::fs::remove_dir_all(directory).expect("cleanup");
    }

    #[tokio::test]
    async fn frozen_generation_survives_restart() {
        let directory = path("freeze");
        let journal = open(&directory);
        let disk_id = journal.disk_id().to_owned();
        journal
            .execute(Request::Write {
                key: "client".into(),
                record: record(1, b"body"),
            })
            .await
            .expect("write");
        let token = RepairToken {
            id: "repair-1".into(),
            generation: 1,
        };
        journal.freeze(token.clone()).await.expect("freeze");
        assert!(!journal.voting_ready(0));
        journal.resume(token, false).await.expect("resume");
        assert!(journal.voting_ready(1));
        drop(journal);
        let journal = open(&directory);
        assert_eq!(journal.disk_id(), disk_id);
        assert!(journal.voting_ready(1));
        assert!(!journal.voting_ready(0));
        let restored = journal
            .execute_at(
                1,
                Request::Read {
                    key: "client".into(),
                },
            )
            .await
            .expect("read")
            .record
            .expect("record");
        assert_eq!(restored.snapshot.as_deref(), Some(b"body".as_slice()));
        drop(journal);
        std::fs::remove_dir_all(directory).expect("cleanup");
    }

    #[test]
    fn foreign_directories_are_refused_without_modification() {
        let sqlite = path("sqlite");
        std::fs::create_dir_all(&sqlite).expect("dir");
        std::fs::write(sqlite.join("data.sqlite"), b"preserved").expect("sqlite");
        assert!(Journal::open_with(&sqlite, ("rocks-test", 101), &[], StoreKind::Rocks).is_err());
        assert_eq!(
            std::fs::read(sqlite.join("data.sqlite")).expect("read"),
            b"preserved"
        );
        std::fs::remove_dir_all(&sqlite).expect("cleanup");

        let memory = path("memory");
        let opened = Journal::open(
            &memory,
            ("rocks-test", 101),
            &[(101, "http://127.0.0.1:7101".into())],
        )
        .expect("memory");
        drop(opened);
        let marker = std::fs::read(memory.join("memory.identity")).expect("marker");
        assert!(Journal::open_with(&memory, ("rocks-test", 101), &[], StoreKind::Rocks).is_err());
        assert_eq!(
            std::fs::read(memory.join("memory.identity")).expect("unchanged"),
            marker
        );
        std::fs::remove_dir_all(&memory).expect("cleanup");

        let rocks = path("owned");
        let opened = open(&rocks);
        drop(opened);
        let marker = std::fs::read(rocks.join("rocks.identity")).expect("rocks marker");
        assert!(Journal::open(&rocks, ("rocks-test", 101), &[]).is_err());
        assert_eq!(
            std::fs::read(rocks.join("rocks.identity")).expect("unchanged"),
            marker
        );
        std::fs::remove_dir_all(rocks).expect("cleanup");
    }
}
