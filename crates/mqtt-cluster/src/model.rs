//! Deterministic bounded controller metadata state machine.
use crate::repair_state::{RepairProof, RepairSeal, RepairState, RepairSummary, RepairTerminal};
use mqtt_cluster_core::ClusterMessage;
use openraft::{BasicNode, Entry, LogId, StoredMembership};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
#[path = "model/incremental.rs"]
mod incremental;
pub type NodeId = u64;
fn legacy_max_lease() -> u64 {
    30000
}
openraft::declare_raft_types!(pub Types: D=Command,R=Answer,NodeId=NodeId,Node=BasicNode);
pub type Raft = openraft::Raft<Types>;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub id: u64,
    pub incarnation: String,
    pub address: String,
    pub expires_at: u64,
    #[serde(default = "legacy_max_lease")]
    pub max_lease_ms: u64,
    pub applied: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Client {
    pub node: u64,
    pub incarnation: String,
    pub generation: u64,
    pub filters: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableOwner {
    pub node: u64,
    pub incarnation: String,
    pub generation: u64,
    pub epoch: u64,
    pub filters: Vec<String>,
    #[serde(default)]
    pub logical_generation: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataMode {
    Legacy,
    /// Historical SQLite mode, retained for replay and explicit upgrade refusal.
    Durable(Vec<(u64, String)>),
    /// Two live RAM replicas acknowledge mutations; no disk persistence.
    Memory(Vec<(u64, String)>),
}
impl DataMode {
    pub(crate) fn storage_name(&self) -> &'static str {
        match self {
            Self::Legacy => "none",
            Self::Durable(_) => "legacy_sqlite",
            Self::Memory(_) => "memory",
        }
    }
}
fn legacy_mode() -> Option<DataMode> {
    Some(DataMode::Legacy)
}
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Metadata {
    #[serde(skip)]
    pub(crate) derived: incremental::Derived,
    pub revision: u64,
    #[serde(default = "legacy_mode")]
    pub data_mode: Option<DataMode>,
    #[serde(default)]
    pub disk_ids: BTreeMap<u64, String>,
    #[serde(default)]
    pub data_generation: u64,
    #[serde(default)]
    pub excluded: BTreeSet<u64>,
    #[serde(default)]
    pub repair: Option<RepairState>,
    #[serde(default)]
    pub last_repair: Option<RepairTerminal>,
    pub nodes: BTreeMap<u64, Node>,
    pub clients: BTreeMap<String, Client>,
    pub routes: BTreeMap<String, BTreeSet<u64>>,
    #[serde(default)]
    pub durable: BTreeMap<String, DurableOwner>,
}
#[cfg(test)]
thread_local! { static METADATA_CLONES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
impl Clone for Metadata {
    fn clone(&self) -> Self {
        #[cfg(test)]
        METADATA_CLONES.with(|count| count.set(count.get() + 1));
        Self {
            derived: self.derived.clone(),
            revision: self.revision,
            data_mode: self.data_mode.clone(),
            disk_ids: self.disk_ids.clone(),
            data_generation: self.data_generation,
            excluded: self.excluded.clone(),
            repair: self.repair.clone(),
            last_repair: self.last_repair.clone(),
            nodes: self.nodes.clone(),
            clients: self.clients.clone(),
            routes: self.routes.clone(),
            durable: self.durable.clone(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    RepairBegin {
        target: u64,
        old_disk: String,
        new_disk: String,
        expected_generation: u64,
    },
    RepairSeal {
        id: String,
        generation: u64,
        manifest: RepairSeal,
    },
    RepairCommit {
        id: String,
        generation: u64,
        proofs: Vec<RepairProof>,
    },
    RepairAbort {
        id: String,
        generation: u64,
    },
    Acquire {
        key: String,
        node: u64,
        incarnation: String,
        generation: u64,
        expected_epoch: u64,
    },
    DurableFilters {
        key: String,
        node: u64,
        incarnation: String,
        epoch: u64,
        filters: Vec<String>,
        #[serde(default)]
        logical_generation: u64,
    },
    Register {
        node: u64,
        incarnation: String,
        address: String,
        expires_at: u64,
        #[serde(default = "legacy_max_lease")]
        lease_ms: u64,
        #[serde(default)]
        replicas: Option<Vec<(u64, String)>>,
        #[serde(default)]
        memory: bool,
        #[serde(default)]
        disk_id: Option<String>,
    },
    Heartbeat {
        node: u64,
        incarnation: String,
        applied: u64,
        expires_at: u64,
        #[serde(default = "legacy_max_lease")]
        lease_ms: u64,
    },
    Claim {
        node: u64,
        incarnation: String,
        client: String,
        generation: u64,
    },
    Release {
        node: u64,
        incarnation: String,
        client: String,
        generation: u64,
    },
    Filters {
        generation: u64,
        node: u64,
        incarnation: String,
        client: String,
        filters: Vec<String>,
    },
    Expire {
        node: u64,
        incarnation: String,
        expires_at: u64,
    },
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Answer {
    pub revision: u64,
    pub error: Option<String>,
    #[serde(default)]
    pub lease_ms: u64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct State {
    pub last_applied: Option<LogId<u64>>,
    pub membership: StoredMembership<u64, BasicNode>,
    pub metadata: Metadata,
}
impl Metadata {
    pub fn apply_tracked(&mut self, command: Command) -> (Answer, Option<MetadataUpdate>) {
        self.apply_incremental(command)
    }
    #[allow(dead_code)] // Compatibility API; Raft storage consumes tracked results.
    pub fn apply(&mut self, command: Command) -> Answer {
        self.apply_tracked(command).0
    }
    fn valid_node(&self, node: u64, incarnation: &str) -> Result<(), String> {
        if self
            .nodes
            .get(&node)
            .is_some_and(|n| n.incarnation == incarnation)
        {
            Ok(())
        } else {
            Err("stale node incarnation".into())
        }
    }
    fn mutate(&mut self, command: Command) -> Result<(), String> {
        if self.repair.is_some()
            && matches!(
                &command,
                Command::Acquire { .. } | Command::DurableFilters { .. }
            )
        {
            return Err("durable maintenance in progress".into());
        }
        match command {
            Command::RepairBegin {
                target,
                old_disk,
                new_disk,
                expected_generation,
            } => self.begin_repair(target, old_disk, new_disk, expected_generation)?,
            Command::RepairSeal {
                id,
                generation,
                manifest,
            } => self.seal_repair(&id, generation, manifest)?,
            Command::RepairCommit {
                id,
                generation,
                proofs,
            } => self.commit_repair(&id, generation, proofs)?,
            Command::RepairAbort { id, generation } => self.abort_repair(&id, generation)?,
            Command::Acquire {
                key,
                node,
                incarnation,
                generation,
                expected_epoch,
            } => {
                self.valid_node(node, &incarnation)?;
                if self.excluded.contains(&node) {
                    return Err("replica excluded pending repair".into());
                }
                if !matches!(&self.data_mode,Some(DataMode::Durable(m) | DataMode::Memory(m)) if m.iter().any(|(id,_)|*id==node))
                {
                    return Err("durable membership required".into());
                }
                if self.durable.get(&key).map_or(0, |o| o.epoch) != expected_epoch {
                    return Err("stale acquisition epoch".into());
                }
                if generation == 0
                    && self.durable.get(&key).is_some_and(|owner| {
                        (owner.node != node || owner.incarnation != incarnation)
                            && self
                                .nodes
                                .get(&owner.node)
                                .is_some_and(|n| n.incarnation == owner.incarnation)
                    })
                {
                    return Err("automatic recovery requires abandoned owner".into());
                }
                if self.durable.len() >= 100_000 && !self.durable.contains_key(&key) {
                    return Err("durable directory capacity".into());
                }
                let filters = self
                    .durable
                    .get(&key)
                    .map(|o| o.filters.clone())
                    .unwrap_or_default();
                let logical_generation = self
                    .durable
                    .get(&key)
                    .map_or(0, |owner| owner.logical_generation);
                self.durable.insert(
                    key,
                    DurableOwner {
                        node,
                        incarnation,
                        generation,
                        epoch: self.revision.checked_add(1).ok_or("epoch exhausted")?,
                        filters,
                        logical_generation,
                    },
                );
            }
            Command::DurableFilters {
                key,
                node,
                incarnation,
                epoch,
                filters,
                logical_generation,
            } => {
                self.valid_node(node, &incarnation)?;
                let owner = self.durable.get_mut(&key).ok_or("unknown durable owner")?;
                if owner.node != node || owner.incarnation != incarnation || owner.epoch != epoch {
                    return Err("stale durable owner".into());
                }
                if logical_generation < owner.logical_generation {
                    return Err("logical session generation rollback".into());
                }
                owner.logical_generation = logical_generation;
                owner.filters = filters;
            }
            Command::Register {
                node,
                incarnation,
                address,
                expires_at,
                lease_ms,
                replicas,
                memory,
                disk_id,
            } => {
                if self.excluded.contains(&node)
                    || self.repair.as_ref().is_some_and(|r| r.target == node)
                {
                    return Err("replica excluded pending repair".into());
                }
                let mode = match replicas {
                    Some(mut peers) => {
                        peers.sort();
                        if peers.len() != 3
                            || !peers.iter().any(|(id, a)| *id == node && a == &address)
                            || peers.windows(2).any(|p| p[0].0 == p[1].0)
                        {
                            return Err("invalid data membership".into());
                        }
                        if memory {
                            DataMode::Memory(peers)
                        } else {
                            DataMode::Durable(peers)
                        }
                    }
                    None if !memory => DataMode::Legacy,
                    None => return Err("memory storage requires replica membership".into()),
                };
                if let Some(frozen) = &self.data_mode {
                    if frozen != &mode {
                        return Err("data membership or mode mismatch".into());
                    }
                } else {
                    self.data_mode = Some(mode);
                }
                match (&self.data_mode, disk_id) {
                    (Some(DataMode::Durable(_) | DataMode::Memory(_)), Some(id))
                        if id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()) =>
                    {
                        if self.disk_ids.get(&node).is_some_and(|old| old != &id) {
                            return Err("data disk replacement requires repair".into());
                        }
                        self.disk_ids.insert(node, id);
                    }
                    (Some(DataMode::Legacy), None) => {}
                    _ => return Err("data disk identity required".into()),
                }
                if self.nodes.len() >= 1024 && !self.nodes.contains_key(&node) {
                    return Err("node capacity".into());
                }
                let (applied, max_lease_ms) = if let Some(old) = self.nodes.get(&node) {
                    if old.incarnation != incarnation {
                        return Err("previous node incarnation still registered".into());
                    }
                    if old.address != address {
                        return Err("node address cannot change within an incarnation".into());
                    }
                    (old.applied, old.max_lease_ms.max(lease_ms))
                } else {
                    (0, lease_ms)
                };
                self.nodes.insert(
                    node,
                    Node {
                        id: node,
                        incarnation,
                        address,
                        expires_at,
                        max_lease_ms,
                        applied,
                    },
                );
            }
            Command::Heartbeat {
                node,
                incarnation,
                applied,
                expires_at,
                lease_ms,
            } => {
                self.valid_node(node, &incarnation)?;
                if let Some(n) = self.nodes.get_mut(&node) {
                    n.applied = n.applied.max(applied);
                    n.max_lease_ms = n.max_lease_ms.max(lease_ms);
                    n.expires_at = n.expires_at.max(expires_at);
                }
            }
            Command::Claim {
                node,
                incarnation,
                client,
                generation,
            } => {
                self.valid_node(node, &incarnation)?;
                if self.clients.len() >= 100_000 && !self.clients.contains_key(&client) {
                    return Err("client capacity".into());
                }
                if let Some(old) = self.clients.get(&client)
                    && old.node == node
                    && old.incarnation == incarnation
                {
                    if old.generation == generation {
                        return Ok(());
                    }
                    if old.generation > generation {
                        return Err("stale client generation".into());
                    }
                }
                self.clients.insert(
                    client,
                    Client {
                        node,
                        incarnation,
                        generation,
                        filters: Vec::new(),
                    },
                );
            }
            Command::Release {
                node,
                incarnation,
                client,
                generation,
            } => {
                if self.clients.get(&client).is_some_and(|c| {
                    c.node == node && c.incarnation == incarnation && c.generation == generation
                }) {
                    self.clients.remove(&client);
                }
            }
            Command::Filters {
                generation,
                node,
                incarnation,
                client,
                filters,
            } => {
                self.valid_node(node, &incarnation)?;
                let Some(c) = self.clients.get_mut(&client) else {
                    return Err("unknown client".into());
                };
                if c.node != node || c.incarnation != incarnation {
                    return Err("stale owner".into());
                }
                if filters.len() > 256 {
                    return Err("filter capacity".into());
                }
                if c.generation != generation {
                    return Err("stale client generation".into());
                }
                c.filters = filters;
            }
            Command::Expire {
                node,
                incarnation,
                expires_at,
            } => {
                if self
                    .nodes
                    .get(&node)
                    .is_some_and(|n| n.incarnation == incarnation && n.expires_at == expires_at)
                {
                    self.nodes.remove(&node);
                    let keys = self
                        .derived
                        .clients_by_node
                        .get(&node)
                        .cloned()
                        .unwrap_or_default();
                    for key in keys {
                        self.clients.remove(&key);
                    }
                }
            }
        }
        Ok(())
    }
    fn rebuild(&mut self) {
        self.derived = incremental::Derived::default();
        self.routes.clear();
        for c in self.clients.values() {
            for filter in &c.filters {
                self.routes
                    .entry(filter.clone())
                    .or_default()
                    .insert(c.node);
            }
        }
        for owner in self.durable.values() {
            for filter in &owner.filters {
                self.routes
                    .entry(filter.clone())
                    .or_default()
                    .insert(owner.node);
            }
        }
    }
}
impl State {
    pub fn apply_tracked(&mut self, entry: Entry<Types>) -> (Answer, Option<MetadataUpdate>) {
        self.last_applied = Some(entry.log_id);
        match entry.payload {
            openraft::EntryPayload::Blank => (
                Answer {
                    revision: self.metadata.revision,
                    error: None,
                    lease_ms: 0,
                },
                None,
            ),
            openraft::EntryPayload::Membership(m) => {
                self.membership = StoredMembership::new(Some(entry.log_id), m);
                (
                    Answer {
                        revision: self.metadata.revision,
                        error: None,
                        lease_ms: 0,
                    },
                    None,
                )
            }
            openraft::EntryPayload::Normal(c) => self.metadata.apply_tracked(c),
        }
    }
    pub fn apply(&mut self, entry: Entry<Types>) -> Answer {
        self.apply_tracked(entry).0
    }
}
#[derive(Serialize, Deserialize)]
pub enum RaftRequest {
    Append(openraft::raft::AppendEntriesRequest<Types>),
    Vote(openraft::raft::VoteRequest<u64>),
    Snapshot(openraft::raft::InstallSnapshotRequest<Types>),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Forward {
    pub node: u64,
    pub incarnation: String,
    pub sequence: u64,
    pub message: ClusterMessage,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterStatus {
    /// memory acknowledges live replicas, not disk persistence.
    #[serde(default)]
    pub data_storage: String,
    pub leader: Option<u64>,
    pub term: u64,
    pub last_applied: u64,
    pub nodes: usize,
    pub clients: usize,
    pub routes: usize,
    #[serde(default)]
    pub data_generation: u64,
    #[serde(default)]
    pub excluded: Vec<u64>,
    #[serde(default)]
    pub repair: Option<RepairSummary>,
    #[serde(default)]
    pub last_repair: Option<RepairSummary>,
}

/// A reconnect starts with a snapshot; subsequent coalesced deltas require an exact base.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MetadataUpdate {
    Progress {
        revision: u64,
    },
    Snapshot(Metadata),
    Delta {
        base: u64,
        revision: u64,
        data_mode: Option<DataMode>,
        disk_ids: BTreeMap<u64, String>,
        data_generation: u64,
        excluded: BTreeSet<u64>,
        repair: Option<RepairState>,
        last_repair: Option<RepairTerminal>,
        nodes: BTreeMap<u64, Option<Node>>,
        clients: BTreeMap<String, Option<Client>>,
        durable: BTreeMap<String, Option<DurableOwner>>,
        routes: BTreeMap<String, Option<BTreeSet<u64>>>,
    },
}
fn diff<K: Ord + Clone, V: Clone + PartialEq>(
    old: &BTreeMap<K, V>,
    next: &BTreeMap<K, V>,
) -> BTreeMap<K, Option<V>> {
    old.keys()
        .chain(next.keys())
        .filter(|key| old.get(*key) != next.get(*key))
        .map(|key| (key.clone(), next.get(key).cloned()))
        .collect()
}
fn patch<K: Ord, V>(map: &mut BTreeMap<K, V>, changes: BTreeMap<K, Option<V>>) {
    for (key, value) in changes {
        match value {
            Some(value) => {
                map.insert(key, value);
            }
            None => {
                map.remove(&key);
            }
        }
    }
}
impl MetadataUpdate {
    pub fn merge_in_place(self, target: &mut Metadata) -> Result<(), &'static str> {
        match self {
            Self::Progress { revision } => {
                if revision != target.revision {
                    return Err("metadata revision gap");
                }
            }
            Self::Snapshot(next) => {
                *target = next;
                target.derived = incremental::Derived::default();
            }
            Self::Delta {
                base,
                revision,
                data_mode,
                disk_ids,
                data_generation,
                excluded,
                repair,
                last_repair,
                nodes,
                clients,
                durable,
                routes,
            } => {
                if base != target.revision || revision <= base {
                    return Err("metadata revision gap");
                }
                target.data_mode = data_mode;
                target.disk_ids = disk_ids;
                target.data_generation = data_generation;
                target.excluded = excluded;
                target.repair = repair;
                target.last_repair = last_repair;
                patch(&mut target.nodes, nodes);
                patch(&mut target.clients, clients);
                patch(&mut target.durable, durable);
                patch(&mut target.routes, routes);
                target.revision = revision;
                target.derived = incremental::Derived::default();
            }
        }
        Ok(())
    }
    pub fn between(old: &Metadata, next: &Metadata) -> Self {
        Self::Delta {
            base: old.revision,
            revision: next.revision,
            data_mode: next.data_mode.clone(),
            disk_ids: next.disk_ids.clone(),
            data_generation: next.data_generation,
            excluded: next.excluded.clone(),
            repair: next.repair.clone(),
            last_repair: next.last_repair.clone(),
            nodes: diff(&old.nodes, &next.nodes),
            clients: diff(&old.clients, &next.clients),
            durable: diff(&old.durable, &next.durable),
            routes: diff(&old.routes, &next.routes),
        }
    }
    #[allow(dead_code)] // Immutable compatibility API; normal watchers patch in place.
    pub fn merge(self, old: &Metadata) -> Result<Metadata, &'static str> {
        let mut next = old.clone();
        self.merge_in_place(&mut next)?;
        Ok(next)
    }
}

#[cfg(test)]
#[path = "model/incremental_tests.rs"]
mod incremental_tests;
