//! Exact sparse transactions for routine directory operations.
use super::*;
use serde::Serialize;

/// 64MiB message/snapshot bounds, reserving 64KiB for wrappers.
pub(super) const MAX_BYTES: usize = 64 * 1024 * 1024 - 64 * 1024;
#[derive(Clone, Debug, Default)]
pub(crate) struct Derived {
    valid: bool,
    bytes: usize,
    references: BTreeMap<String, BTreeMap<u64, usize>>,
    pub(super) pending_nodes: BTreeSet<u64>,
    pub(super) clients_by_node: BTreeMap<u64, BTreeSet<String>>,
}
#[derive(Serialize)]
struct Scalars<'a> {
    revision: u64,
    data_mode: &'a Option<DataMode>,
    disk_ids: &'a BTreeMap<u64, String>,
    data_generation: u64,
    excluded: &'a BTreeSet<u64>,
    repair: &'a Option<RepairState>,
    last_repair: &'a Option<RepairTerminal>,
    nodes: BTreeMap<u64, Node>,
    clients: BTreeMap<String, Client>,
    routes: BTreeMap<String, BTreeSet<u64>>,
    durable: BTreeMap<String, DurableOwner>,
}
fn length(value: &impl Serialize) -> Result<usize, String> {
    serde_json::to_vec(value)
        .map(|b| b.len())
        .map_err(|_| "metadata encoding failed".to_owned())
}
fn scalar_length(m: &Metadata) -> Result<usize, String> {
    length(&Scalars {
        revision: m.revision,
        data_mode: &m.data_mode,
        disk_ids: &m.disk_ids,
        data_generation: m.data_generation,
        excluded: &m.excluded,
        repair: &m.repair,
        last_repair: &m.last_repair,
        nodes: BTreeMap::new(),
        clients: BTreeMap::new(),
        routes: BTreeMap::new(),
        durable: BTreeMap::new(),
    })
}
fn entry_length<K: Serialize + Ord, V: Serialize>(key: &K, value: &V) -> Result<usize, String> {
    // Serialize as a map so numeric map keys get their required JSON quotes.
    length(&BTreeMap::from([(key, value)])).map(|n| n - 2)
}
fn map_adjustment<K: Serialize + Ord, V: Serialize>(
    before: &BTreeMap<K, Option<V>>,
    old_len: usize,
    current: &BTreeMap<K, V>,
) -> Result<i128, String> {
    let mut delta = current.len().saturating_sub(1) as i128 - old_len.saturating_sub(1) as i128;
    for (key, old) in before {
        if let Some(old) = old {
            delta -= entry_length(key, old)? as i128;
        }
        if let Some(new) = current.get(key) {
            delta += entry_length(key, new)? as i128;
        }
    }
    Ok(delta)
}
fn restore<K: Ord, V>(map: &mut BTreeMap<K, V>, before: BTreeMap<K, Option<V>>) {
    for (key, value) in before {
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
#[derive(Default)]
struct Undo {
    nodes: BTreeMap<u64, Option<Node>>,
    clients: BTreeMap<String, Option<Client>>,
    durable: BTreeMap<String, Option<DurableOwner>>,
    routes: BTreeMap<String, Option<BTreeSet<u64>>>,
    lengths: [usize; 4],
    data_mode: Option<DataMode>,
    disk_ids: BTreeMap<u64, String>,
    revision: u64,
    bytes: usize,
    scalar: usize,
}
impl Undo {
    fn capture(m: &Metadata, command: &Command) -> Result<Self, String> {
        let mut undo = Self {
            lengths: [
                m.nodes.len(),
                m.clients.len(),
                m.durable.len(),
                m.routes.len(),
            ],
            data_mode: m.data_mode.clone(),
            disk_ids: m.disk_ids.clone(),
            revision: m.revision,
            bytes: m.derived.bytes,
            scalar: scalar_length(m)?,
            ..Self::default()
        };
        match command {
            Command::Register { node, .. }
            | Command::Heartbeat { node, .. }
            | Command::Expire { node, .. } => {
                undo.nodes.insert(*node, m.nodes.get(node).cloned());
            }
            Command::Claim { client, .. }
            | Command::Release { client, .. }
            | Command::Filters { client, .. } => {
                undo.clients
                    .insert(client.clone(), m.clients.get(client).cloned());
            }
            Command::Acquire { key, .. }
            | Command::AcquireOnce { key, .. }
            | Command::DurableFilters { key, .. } => {
                undo.durable
                    .insert(key.clone(), m.durable.get(key).cloned());
            }
            _ => {}
        }
        if let Command::Expire {
            node,
            incarnation,
            expires_at,
        } = command
            && m.nodes
                .get(node)
                .is_some_and(|n| n.incarnation == *incarnation && n.expires_at == *expires_at)
        {
            for client in m.derived.clients_by_node.get(node).into_iter().flatten() {
                undo.clients
                    .insert(client.clone(), m.clients.get(client).cloned());
            }
        }
        Ok(undo)
    }
    fn byte_count(&self, m: &Metadata) -> Result<usize, String> {
        let mut n = self.bytes as i128 + scalar_length(m)? as i128 - self.scalar as i128;
        n += map_adjustment(&self.nodes, self.lengths[0], &m.nodes)?;
        n += map_adjustment(&self.clients, self.lengths[1], &m.clients)?;
        n += map_adjustment(&self.durable, self.lengths[2], &m.durable)?;
        n += map_adjustment(&self.routes, self.lengths[3], &m.routes)?;
        usize::try_from(n).map_err(|_| "metadata byte accounting overflow".to_owned())
    }
    fn primary_restore(self, m: &mut Metadata) {
        restore(&mut m.nodes, self.nodes);
        restore(&mut m.clients, self.clients);
        restore(&mut m.durable, self.durable);
        restore(&mut m.routes, self.routes);
        m.data_mode = self.data_mode;
        m.disk_ids = self.disk_ids;
        m.revision = self.revision;
        m.derived.bytes = self.bytes;
    }
}
fn references(d: &mut Derived, node: u64, filters: &[String], add: bool) {
    for filter in filters {
        if add {
            *d.references
                .entry(filter.clone())
                .or_default()
                .entry(node)
                .or_default() += 1;
        } else if let Some(nodes) = d.references.get_mut(filter) {
            if let Some(n) = nodes.get_mut(&node) {
                if *n <= 1 {
                    nodes.remove(&node);
                } else {
                    *n -= 1;
                }
            }
            if nodes.is_empty() {
                d.references.remove(filter);
            }
        }
    }
}
fn client_index(d: &mut Derived, key: &str, client: &Client, add: bool) {
    if add {
        d.clients_by_node
            .entry(client.node)
            .or_default()
            .insert(key.to_owned());
    } else if let Some(clients) = d.clients_by_node.get_mut(&client.node) {
        clients.remove(key);
        if clients.is_empty() {
            d.clients_by_node.remove(&client.node);
        }
    }
}
impl Metadata {
    fn ensure_derived(&mut self) -> Result<(), String> {
        if self.derived.valid {
            return Ok(());
        }
        let mut d = Derived {
            valid: true,
            bytes: length(self)?,
            ..Derived::default()
        };
        for (key, c) in &self.clients {
            references(&mut d, c.node, &c.filters, true);
            client_index(&mut d, key, c, true);
        }
        for o in self.durable.values() {
            references(&mut d, o.node, &o.filters, true);
        }
        self.derived = d;
        Ok(())
    }
    fn update_references(&mut self, undo: &Undo, forward: bool) {
        for (key, old) in &undo.clients {
            let new = self.clients.get(key);
            let (remove, add) = if forward {
                (old.as_ref(), new)
            } else {
                (new, old.as_ref())
            };
            if let Some(c) = remove {
                references(&mut self.derived, c.node, &c.filters, false);
                client_index(&mut self.derived, key, c, false);
            }
            if let Some(c) = add {
                references(&mut self.derived, c.node, &c.filters, true);
                client_index(&mut self.derived, key, c, true);
            }
        }
        for (key, old) in &undo.durable {
            let new = self.durable.get(key);
            let (remove, add) = if forward {
                (old.as_ref(), new)
            } else {
                (new, old.as_ref())
            };
            if let Some(o) = remove {
                references(&mut self.derived, o.node, &o.filters, false);
            }
            if let Some(o) = add {
                references(&mut self.derived, o.node, &o.filters, true);
            }
        }
    }
    fn patch_routes(&mut self, undo: &mut Undo) {
        let mut touched = BTreeSet::new();
        for (key, old) in &undo.clients {
            for c in old.iter().chain(self.clients.get(key)) {
                touched.extend(c.filters.iter().cloned());
            }
        }
        for (key, old) in &undo.durable {
            for o in old.iter().chain(self.durable.get(key)) {
                touched.extend(o.filters.iter().cloned());
            }
        }
        for key in touched {
            undo.routes
                .insert(key.clone(), self.routes.get(&key).cloned());
            if let Some(nodes) = self.derived.references.get(&key) {
                self.routes.insert(key, nodes.keys().copied().collect());
            } else {
                self.routes.remove(&key);
            }
        }
    }
    pub(super) fn apply_incremental(
        &mut self,
        command: Command,
    ) -> (Answer, Option<MetadataUpdate>) {
        if let Err(error) = self.ensure_derived() {
            return (
                Answer {
                    revision: self.revision,
                    error: Some(error),
                    lease_ms: 0,
                    results: Vec::new(),
                },
                None,
            );
        }
        if let Command::AcquireOnce {
            key,
            node,
            incarnation,
            generation,
            expected_epoch,
        } = &command
            && let Some(owner) = self.durable.get(key)
            && owner.node == *node
            && owner.incarnation == *incarnation
            && owner.generation == *generation
            && owner.acquired_from == Some(*expected_epoch)
        {
            let result = self.validate_acquirer(*node, incarnation);
            return (
                Answer {
                    revision: if result.is_ok() {
                        owner.epoch
                    } else {
                        self.revision
                    },
                    error: result.err(),
                    lease_ms: 0,
                    results: Vec::new(),
                },
                None,
            );
        }
        if self.revision == u64::MAX && !matches!(command, Command::Heartbeat { .. }) {
            return (
                Answer {
                    revision: self.revision,
                    error: Some("metadata revision exhausted".into()),
                    lease_ms: 0,
                    results: Vec::new(),
                },
                None,
            );
        }
        if matches!(
            command,
            Command::RepairBegin { .. }
                | Command::RepairSeal { .. }
                | Command::RepairCommit { .. }
                | Command::RepairAbort { .. }
        ) {
            // Maintenance changes global authority; retain bounded full validation there.
            return self.apply_maintenance(command);
        }
        let heartbeat_node = match &command {
            Command::Heartbeat { node, .. } => Some(*node),
            _ => None,
        };
        let heartbeat = heartbeat_node.is_some();
        let mut undo = match Undo::capture(self, &command) {
            Ok(undo) => undo,
            Err(error) => {
                return (
                    Answer {
                        revision: self.revision,
                        error: Some(error),
                        lease_ms: 0,
                        results: Vec::new(),
                    },
                    None,
                );
            }
        };
        if let Err(error) = self.mutate(command) {
            undo.primary_restore(self);
            return (
                Answer {
                    revision: self.revision,
                    error: Some(error),
                    lease_ms: 0,
                    results: Vec::new(),
                },
                None,
            );
        }
        self.update_references(&undo, true);
        self.patch_routes(&mut undo);
        if !heartbeat {
            self.revision = self.revision.saturating_add(1);
        }
        let bytes = undo.byte_count(self);
        match bytes {
            Ok(n) if n <= MAX_BYTES => self.derived.bytes = n,
            _ => {
                self.update_references(&undo, false);
                undo.primary_restore(self);
                return (
                    Answer {
                        revision: self.revision,
                        error: Some("metadata byte capacity".into()),
                        lease_ms: 0,
                        results: Vec::new(),
                    },
                    None,
                );
            }
        }
        if let Some(node) = heartbeat_node {
            self.derived.pending_nodes.insert(node);
        }
        let answer = Answer {
            revision: self.revision,
            error: None,
            lease_ms: 0,
            results: Vec::new(),
        };
        let delta = (!heartbeat && self.revision != undo.revision).then(|| MetadataUpdate::Delta {
            base: undo.revision,
            revision: self.revision,
            data_mode: self.data_mode.clone(),
            disk_ids: self.disk_ids.clone(),
            data_generation: self.data_generation,
            excluded: self.excluded.clone(),
            repair: self.repair.clone(),
            last_repair: self.last_repair.clone(),
            nodes: {
                let mut changes = changed(&undo.nodes, &self.nodes);
                for node in &self.derived.pending_nodes {
                    changes.insert(*node, self.nodes.get(node).cloned());
                }
                changes
            },
            clients: changed(&undo.clients, &self.clients),
            durable: changed(&undo.durable, &self.durable),
            routes: changed(&undo.routes, &self.routes),
        });
        if delta.is_some() {
            self.derived.pending_nodes.clear();
        }
        (answer, delta)
    }
    fn apply_maintenance(&mut self, command: Command) -> (Answer, Option<MetadataUpdate>) {
        let mut next = self.clone();
        let result = next.mutate(command).and_then(|()| {
            next.rebuild();
            next.revision = self.revision.saturating_add(1);
            let n = length(&next)?;
            if n > MAX_BYTES {
                return Err("metadata byte capacity".into());
            }
            next.derived = Derived::default();
            Ok(())
        });
        if let Err(error) = result {
            return (
                Answer {
                    revision: self.revision,
                    error: Some(error),
                    lease_ms: 0,
                    results: Vec::new(),
                },
                None,
            );
        }
        let mut delta = MetadataUpdate::between(self, &next);
        if let MetadataUpdate::Delta { nodes, .. } = &mut delta {
            for node in &self.derived.pending_nodes {
                nodes.insert(*node, next.nodes.get(node).cloned());
            }
        }
        *self = next;
        (
            Answer {
                revision: self.revision,
                error: None,
                lease_ms: 0,
                results: Vec::new(),
            },
            Some(delta),
        )
    }
    #[cfg(test)]
    pub(super) fn accounted_bytes(&mut self) -> usize {
        self.ensure_derived().unwrap();
        self.derived.bytes
    }
}
fn changed<K: Ord + Clone, V: Clone + PartialEq>(
    before: &BTreeMap<K, Option<V>>,
    current: &BTreeMap<K, V>,
) -> BTreeMap<K, Option<V>> {
    before
        .iter()
        .filter(|(key, old)| old.as_ref() != current.get(*key))
        .map(|(key, _)| (key.clone(), current.get(key).cloned()))
        .collect()
}
