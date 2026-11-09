//! Maintenance repair orchestration. Learners never join ordinary data quorums.
use super::*;
use crate::repair_state::{RepairOutcome, RepairProof, RepairSeal, RepairState, RepairTerminal};
use crate::replica::{RepairEntry, RepairManifest, RepairPage, RepairStoreStatus, RepairToken};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum Request {
    Identity,
    Status,
    Freeze {
        token: RepairToken,
    },
    Export {
        token: RepairToken,
        after: Option<String>,
        limit: usize,
    },
    BeginImport {
        token: RepairToken,
        manifest: RepairManifest,
    },
    AppendPage {
        token: RepairToken,
        sequence: u64,
        entries: Vec<RepairEntry>,
    },
    FinishImport {
        token: RepairToken,
    },
    Resume {
        token: RepairToken,
    },
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum Reply {
    Status(RepairStoreStatus),
    Page(RepairPage),
}
#[derive(Clone, Debug, Serialize)]
pub struct RepairReport {
    pub id: String,
    pub target: u64,
    pub generation: u64,
    pub state: String,
    pub records: u64,
    pub bytes: u64,
    pub digest: Option<String>,
}
impl RepairReport {
    fn new(state: &RepairState, outcome: &str) -> Self {
        Self {
            id: state.id.clone(),
            target: state.target,
            generation: state.generation,
            state: outcome.into(),
            records: state.manifest.as_ref().map_or(0, |m| m.records),
            bytes: state.manifest.as_ref().map_or(0, |m| m.bytes),
            digest: state.manifest.as_ref().map(|m| m.digest.clone()),
        }
    }
}
fn token(state: &RepairState) -> RepairToken {
    RepairToken {
        id: state.id.clone(),
        generation: state.generation,
    }
}
fn manifest(seal: &RepairSeal) -> RepairManifest {
    RepairManifest {
        records: seal.records,
        bytes: seal.bytes,
        digest: seal.digest.clone(),
    }
}
fn seal(value: &RepairManifest) -> RepairSeal {
    RepairSeal {
        records: value.records,
        bytes: value.bytes,
        digest: value.digest.clone(),
    }
}
fn address(metadata: &Metadata, node: u64) -> io::Result<String> {
    metadata
        .data_mode
        .as_ref()
        .and_then(DataMode::peers)
        .and_then(|peers| {
            peers
                .iter()
                .find(|(id, _)| *id == node)
                .map(|(_, address)| address.clone())
        })
        .ok_or_else(unavailable)
}
async fn call(rpc: &ClientRpc, address: &str, request: Request) -> io::Result<Reply> {
    let mut client = rpc.repair_client(address).await?;
    let reply = client
        .repair(rpc.authorize(rpc::encode(&request)?)?)
        .await
        .map_err(rpc::io_error)?;
    rpc::decode(reply.into_inner()).map_err(rpc::io_error)
}
async fn status(rpc: &ClientRpc, address: &str) -> io::Result<RepairStoreStatus> {
    match call(rpc, address, Request::Status).await? {
        Reply::Status(s) => Ok(s),
        _ => Err(unavailable()),
    }
}
fn donor(state: &RepairState, node: u64, disk: &str) -> bool {
    state.donors.iter().any(|(id, d)| *id == node && d == disk)
}
fn active<'a>(metadata: &'a Metadata, requested: &RepairToken) -> io::Result<&'a RepairState> {
    metadata
        .repair
        .as_ref()
        .filter(|r| r.id == requested.id && r.generation == requested.generation)
        .ok_or_else(unavailable)
}
fn resume_authorized(
    metadata: &Metadata,
    node: u64,
    status: &RepairStoreStatus,
    requested: &RepairToken,
) -> io::Result<bool> {
    if metadata.repair.is_some() {
        return Err(unavailable());
    }
    let terminal = metadata
        .last_repair
        .as_ref()
        .filter(|t| t.repair.id == requested.id && t.repair.generation == requested.generation)
        .ok_or_else(unavailable)?;
    let state = &terminal.repair;
    if donor(state, node, &status.disk_id) && metadata.disk_ids.get(&node) == Some(&status.disk_id)
    {
        return Ok(terminal.outcome == RepairOutcome::Aborted);
    }
    if terminal.outcome == RepairOutcome::Committed
        && node == state.target
        && status.disk_id == state.new_disk
        && metadata.disk_ids.get(&node) == Some(&status.disk_id)
        && !metadata.excluded.contains(&node)
        && status.installed
        && status.manifest.as_ref() == state.manifest.as_ref().map(manifest).as_ref()
    {
        return Ok(false);
    }
    Err(unavailable())
}
impl BrokerRuntime {
    pub(crate) async fn repair_request(&self, request: Request) -> io::Result<Reply> {
        let journal = self.journal.as_ref().ok_or_else(unavailable)?;
        if matches!(&request, Request::Identity | Request::Status) {
            return Ok(Reply::Status(journal.status().await?));
        }
        let metadata = self.rpc().await?.metadata().await?;
        let node = self.config.node_id;
        let disk = journal.disk_id();
        let reply = match request {
            Request::Freeze { token } => {
                let state = active(&metadata, &token)?;
                if !donor(state, node, disk) {
                    return Err(unavailable());
                }
                Reply::Status(journal.freeze(token).await?)
            }
            Request::Export {
                token,
                after,
                limit,
            } => {
                let state = active(&metadata, &token)?;
                if !donor(state, node, disk) {
                    return Err(unavailable());
                }
                Reply::Page(journal.export_page(token, after, limit).await?)
            }
            Request::BeginImport {
                token,
                manifest: expected,
            } => {
                let state = active(&metadata, &token)?;
                if node != state.target
                    || disk != state.new_disk
                    || state.manifest.as_ref() != Some(&seal(&expected))
                {
                    return Err(unavailable());
                }
                Reply::Status(journal.begin_import(token, expected).await?)
            }
            Request::AppendPage {
                token,
                sequence,
                entries,
            } => {
                let state = active(&metadata, &token)?;
                if node != state.target || disk != state.new_disk || state.manifest.is_none() {
                    return Err(unavailable());
                }
                Reply::Status(journal.append_page(token, sequence, entries).await?)
            }
            Request::FinishImport { token } => {
                let state = active(&metadata, &token)?;
                if node != state.target || disk != state.new_disk || state.manifest.is_none() {
                    return Err(unavailable());
                }
                Reply::Status(journal.finish_import(token).await?)
            }
            Request::Resume { token } => {
                let state = journal.status().await?;
                let allow = resume_authorized(&metadata, node, &state, &token)?;
                Reply::Status(journal.resume(token, allow).await?)
            }
            Request::Identity | Request::Status => return Err(unavailable()),
        };
        Ok(reply)
    }
    pub(crate) async fn reconcile_repair(&self) -> io::Result<()> {
        let Some(journal) = &self.journal else {
            return Ok(());
        };
        let metadata = {
            let metadata = self.metadata.read().unwrap_or_else(PoisonError::into_inner);
            if metadata.repair.is_some() || journal.voting_ready(metadata.data_generation) {
                return Ok(());
            }
            metadata.clone()
        };
        let Some(terminal) = &metadata.last_repair else {
            return Ok(());
        };
        let current = journal.status().await?;
        let token = token(&terminal.repair);
        if let Ok(allow) = resume_authorized(&metadata, self.config.node_id, &current, &token) {
            journal.resume(token, allow).await?;
        }
        Ok(())
    }
}

pub(crate) async fn verify_commit(
    rpc: &ClientRpc,
    metadata: &Metadata,
    id: &str,
    generation: u64,
) -> io::Result<Vec<RepairProof>> {
    let state = metadata
        .repair
        .as_ref()
        .filter(|r| r.id == id && r.generation == generation)
        .ok_or_else(unavailable)?;
    let expected = state.manifest.as_ref().ok_or_else(unavailable)?;
    let mut proofs = Vec::new();
    for (node, disk) in state
        .donors
        .iter()
        .chain(std::iter::once(&(state.target, state.new_disk.clone())))
    {
        let observed = status(rpc, &address(metadata, *node)?).await?;
        if observed.disk_id != *disk || observed.frozen.as_ref() != Some(&token(state)) {
            return Err(unavailable());
        }
        if *node == state.target
            && (!observed.installed || observed.manifest.as_ref() != Some(&manifest(expected)))
        {
            return Err(unavailable());
        }
        proofs.push(RepairProof {
            node: *node,
            disk_id: observed.disk_id,
            id: id.to_owned(),
            generation,
            manifest: observed.manifest.as_ref().map(seal),
            installed: observed.installed,
        });
    }
    Ok(proofs)
}

pub async fn serve_repair_learner(
    config: Config,
    _shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    config.validate()?;
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "memory replicas cannot survive learner process replacement; start the normal Broker, leave it waiting, then run cluster-repair against that live process",
    ))
}

pub async fn repair(config: &Config, target: u64) -> io::Result<RepairReport> {
    config.validate()?;
    let rpc = ClientRpc::new(Arc::new(config.clone())).await?;
    tokio::time::timeout(Duration::from_secs(600), repair_inner(&rpc, target))
        .await
        .map_err(|_| unavailable())?
}
pub async fn abort_repair(config: &Config) -> io::Result<RepairReport> {
    config.validate()?;
    let rpc = ClientRpc::new(Arc::new(config.clone())).await?;
    let mut metadata = rpc.metadata().await?;
    let terminal = if let Some(active) = &metadata.repair {
        rpc.repair_mutate(Command::RepairAbort {
            id: active.id.clone(),
            generation: active.generation,
        })
        .await?;
        metadata = rpc.metadata().await?;
        metadata.last_repair.clone().ok_or_else(unavailable)?
    } else {
        metadata
            .last_repair
            .clone()
            .filter(|t| t.outcome == RepairOutcome::Aborted)
            .ok_or_else(unavailable)?
    };
    resume_nodes(&rpc, &metadata, &terminal).await?;
    Ok(RepairReport::new(&terminal.repair, "aborted"))
}
async fn resume_nodes(
    rpc: &ClientRpc,
    metadata: &Metadata,
    terminal: &RepairTerminal,
) -> io::Result<()> {
    for (node, _) in &terminal.repair.donors {
        call(
            rpc,
            &address(metadata, *node)?,
            Request::Resume {
                token: token(&terminal.repair),
            },
        )
        .await?;
    }
    if terminal.outcome == RepairOutcome::Committed {
        call(
            rpc,
            &address(metadata, terminal.repair.target)?,
            Request::Resume {
                token: token(&terminal.repair),
            },
        )
        .await?;
    }
    Ok(())
}
async fn repair_inner(rpc: &ClientRpc, target: u64) -> io::Result<RepairReport> {
    let mut metadata = rpc.metadata().await?;
    let target_address = address(&metadata, target)?;
    let identity = status(rpc, &target_address).await?;
    if metadata.repair.is_none()
        && let Some(last) = &metadata.last_repair
        && last.outcome == RepairOutcome::Committed
        && last.repair.target == target
        && last.repair.new_disk == identity.disk_id
    {
        resume_nodes(rpc, &metadata, last).await?;
        return Ok(RepairReport::new(&last.repair, "committed"));
    }
    if metadata.repair.is_none() {
        let old_disk = metadata
            .disk_ids
            .get(&target)
            .cloned()
            .ok_or_else(unavailable)?;
        rpc.repair_mutate(Command::RepairBegin {
            target,
            old_disk,
            new_disk: identity.disk_id.clone(),
            expected_generation: metadata.data_generation,
        })
        .await?;
        metadata = rpc.metadata().await?;
    }
    let state = metadata
        .repair
        .clone()
        .filter(|r| r.target == target && r.new_disk == identity.disk_id)
        .ok_or_else(unavailable)?;
    for (node, _) in &state.donors {
        call(
            rpc,
            &address(&metadata, *node)?,
            Request::Freeze {
                token: token(&state),
            },
        )
        .await?;
    }
    let mut merge = Merge::new(rpc, &metadata, &state)?;
    let mut hash = Sha256::new();
    let mut count = 0u64;
    let mut bytes = 0u64;
    while let Some(entry) = merge.next().await? {
        let data = canonical(&entry)?;
        bytes = bytes
            .checked_add(data.len() as u64)
            .ok_or_else(unavailable)?;
        count += 1;
        if bytes > 768 * 1024 * 1024 || count > 100_000 {
            return Err(unavailable());
        }
        hash.update(data);
    }
    let expected = RepairManifest {
        records: count,
        bytes,
        digest: format!("{:x}", hash.finalize()),
    };
    rpc.repair_mutate(Command::RepairSeal {
        id: state.id.clone(),
        generation: state.generation,
        manifest: seal(&expected),
    })
    .await?;
    call(
        rpc,
        &target_address,
        Request::BeginImport {
            token: token(&state),
            manifest: expected.clone(),
        },
    )
    .await?;
    let mut merge = Merge::new(rpc, &metadata, &state)?;
    let mut sequence = 0u64;
    let mut entries = Vec::new();
    let mut size = 0usize;
    while let Some(entry) = merge.next().await? {
        let length = canonical(&entry)?.len();
        if !entries.is_empty()
            && (entries.len() >= 64 || size.saturating_add(length) > 12 * 1024 * 1024)
        {
            call(
                rpc,
                &target_address,
                Request::AppendPage {
                    token: token(&state),
                    sequence,
                    entries: std::mem::take(&mut entries),
                },
            )
            .await?;
            sequence += 1;
            size = 0;
        }
        size = size.saturating_add(length);
        entries.push(entry);
    }
    if !entries.is_empty() {
        call(
            rpc,
            &target_address,
            Request::AppendPage {
                token: token(&state),
                sequence,
                entries,
            },
        )
        .await?;
    }
    call(
        rpc,
        &target_address,
        Request::FinishImport {
            token: token(&state),
        },
    )
    .await?;
    rpc.repair_mutate(Command::RepairCommit {
        id: state.id.clone(),
        generation: state.generation,
        proofs: Vec::new(),
    })
    .await?;
    metadata = rpc.metadata().await?;
    let terminal = metadata
        .last_repair
        .clone()
        .filter(|t| t.repair.id == state.id && t.outcome == RepairOutcome::Committed)
        .ok_or_else(unavailable)?;
    resume_nodes(rpc, &metadata, &terminal).await?;
    Ok(RepairReport::new(&terminal.repair, "committed"))
}
fn canonical(entry: &RepairEntry) -> io::Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(entry).map_err(io::Error::other)?;
    bytes.push(b'\n');
    Ok(bytes)
}
struct Cursor<'a> {
    rpc: &'a ClientRpc,
    address: String,
    token: RepairToken,
    after: Option<String>,
    buffer: VecDeque<RepairEntry>,
    done: bool,
}
impl Cursor<'_> {
    async fn peek(&mut self) -> io::Result<Option<&RepairEntry>> {
        if self.buffer.is_empty() && !self.done {
            let result = call(
                self.rpc,
                &self.address,
                Request::Export {
                    token: self.token.clone(),
                    after: self.after.clone(),
                    limit: 64,
                },
            )
            .await?;
            let page = match result {
                Reply::Page(page) => page,
                _ => return Err(unavailable()),
            };
            if page.entries.len() > 64 || (!page.done && page.entries.is_empty()) {
                return Err(unavailable());
            }
            let mut previous = self.after.as_ref();
            for entry in &page.entries {
                if previous.is_some_and(|p| p >= &entry.key) {
                    return Err(unavailable());
                }
                previous = Some(&entry.key);
            }
            if !page.done && page.next.as_ref() != page.entries.last().map(|e| &e.key) {
                return Err(unavailable());
            }
            self.after = page.next;
            self.done = page.done;
            self.buffer = page.entries.into();
        }
        Ok(self.buffer.front())
    }
}
struct Merge<'a> {
    left: Cursor<'a>,
    right: Cursor<'a>,
}
impl<'a> Merge<'a> {
    fn new(rpc: &'a ClientRpc, metadata: &Metadata, state: &RepairState) -> io::Result<Self> {
        let make = |node| -> io::Result<Cursor<'a>> {
            Ok(Cursor {
                rpc,
                address: address(metadata, node)?,
                token: token(state),
                after: None,
                buffer: VecDeque::new(),
                done: false,
            })
        };
        Ok(Self {
            left: make(state.donors.first().ok_or_else(unavailable)?.0)?,
            right: make(state.donors.get(1).ok_or_else(unavailable)?.0)?,
        })
    }
    async fn next(&mut self) -> io::Result<Option<RepairEntry>> {
        let left = self.left.peek().await?;
        let right = self.right.peek().await?;
        let pick = match (left, right) {
            (None, None) => return Ok(None),
            (Some(_), None) => 0,
            (None, Some(_)) => 1,
            (Some(a), Some(b)) => match a.key.cmp(&b.key) {
                std::cmp::Ordering::Less => 0,
                std::cmp::Ordering::Greater => 1,
                std::cmp::Ordering::Equal => 2,
            },
        };
        if pick == 0 {
            return Ok(self.left.buffer.pop_front());
        }
        if pick == 1 {
            return Ok(self.right.buffer.pop_front());
        }
        let mut a = self.left.buffer.pop_front().ok_or_else(unavailable)?;
        let b = self.right.buffer.pop_front().ok_or_else(unavailable)?;
        a.min_epoch = a.min_epoch.max(b.min_epoch);
        match (&a.record, b.record) {
            (Some(old), Some(next)) => {
                if old.version() == next.version()
                    && serde_json::to_vec(old).map_err(io::Error::other)?
                        != serde_json::to_vec(&next).map_err(io::Error::other)?
                {
                    return Err(unavailable());
                }
                if next.version() > old.version() {
                    a.record = Some(next);
                }
            }
            (None, Some(next)) => a.record = Some(next),
            _ => {}
        }
        Ok(Some(a))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn frozen_donor_keeps_control_plane_and_abort_rejects_old_generation() -> io::Result<()> {
        let directory = std::env::temp_dir().join(format!(
            "lyrnormq-repair-runtime-{}-{}",
            std::process::id(),
            controller::now()
        ));
        let config:Config=serde_json::from_value(serde_json::json!({
            "role":"broker","cluster_id":"test","node_id":101,"listen":"127.0.0.1:7101","data_dir":directory,"token_file":"unused",
            "controllers":[{"id":1,"address":"http://127.0.0.1:7001"},{"id":2,"address":"http://127.0.0.1:7002"},{"id":3,"address":"http://127.0.0.1:7003"}],
            "durable":{"replicas":[{"id":101,"address":"http://127.0.0.1:7101"},{"id":102,"address":"http://127.0.0.1:7102"},{"id":103,"address":"http://127.0.0.1:7103"}]}
        })).map_err(io::Error::other)?;
        let runtime = BrokerRuntime::new(config)?;
        let journal = runtime.journal.as_ref().ok_or_else(unavailable)?;
        let state = RepairState {
            id: "repair-test".into(),
            target: 103,
            old_disk: "1".repeat(32),
            new_disk: "2".repeat(32),
            donors: vec![(101, journal.disk_id().to_owned()), (102, "3".repeat(32))],
            generation: 1,
            manifest: None,
        };
        journal.freeze(token(&state)).await?;
        {
            let mut metadata = runtime
                .metadata
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            metadata.data_mode = Some(DataMode::Memory(
                runtime
                    .config
                    .durable
                    .as_ref()
                    .expect("group")
                    .replicas
                    .iter()
                    .map(|p| (p.id, p.address.clone()))
                    .collect(),
            ));
            metadata.nodes.insert(
                101,
                Node {
                    id: 101,
                    incarnation: runtime.incarnation.clone(),
                    address: runtime.config.address(),
                    expires_at: 100,
                    max_lease_ms: 3000,
                    applied: 0,
                },
            );
            metadata.disk_ids.insert(101, journal.disk_id().to_owned());
            metadata.data_generation = 1;
            metadata.repair = Some(state.clone());
            metadata.excluded.insert(103);
        }
        runtime.initialized.store(true, Ordering::Release);
        *runtime
            .deadline
            .lock()
            .unwrap_or_else(PoisonError::into_inner) =
            tokio::time::Instant::now() + Duration::from_secs(30);
        assert!(runtime.control_ready());
        assert!(!runtime.ready());
        {
            let mut metadata = runtime
                .metadata
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            metadata.repair = None;
            metadata.last_repair = Some(RepairTerminal {
                repair: state,
                outcome: RepairOutcome::Aborted,
            });
        }
        runtime.reconcile_repair().await?;
        assert!(runtime.ready());
        assert!(
            journal
                .execute_at(
                    0,
                    crate::replica::Request::Read {
                        key: "c:client".into()
                    }
                )
                .await
                .is_err()
        );
        assert!(
            journal
                .execute_at(
                    1,
                    crate::replica::Request::Read {
                        key: "c:client".into()
                    }
                )
                .await?
                .record
                .is_none()
        );
        drop(runtime);
        std::fs::remove_dir_all(directory)?;
        Ok(())
    }
}
