//! Indexed subscriptions and maximum matching QoS.
use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::filter::{valid_topic, validate_filter};

fn merge_qos(target: &mut BTreeMap<u64, u8>, source: &BTreeMap<u64, u8>) {
    for (&client, &qos) in source {
        target
            .entry(client)
            .and_modify(|granted| *granted = (*granted).max(qos))
            .or_insert(qos);
    }
}

#[derive(Default)]
struct Node {
    children: BTreeMap<String, usize>,
    clients: BTreeMap<u64, u8>,
    descendants: BTreeMap<u64, u8>,
}
impl Node {
    fn is_empty(&self) -> bool {
        self.children.is_empty() && self.clients.is_empty() && self.descendants.is_empty()
    }
}

/// Topic-level trie with a reverse client index for bounded disconnect cleanup.
/// Flat node ownership avoids recursion even for adversarial, deeply nested topics.
pub struct Router {
    nodes: Vec<Node>,
    free: Vec<usize>,
    subscriptions: HashMap<u64, BTreeSet<String>>,
}
impl Default for Router {
    fn default() -> Self {
        Self {
            nodes: vec![Node::default()],
            free: Vec::new(),
            subscriptions: HashMap::new(),
        }
    }
}
impl Router {
    /// Insert or replace a subscription with QoS 0.
    /// Use [`Self::subscribe_qos`] to grant a different maximum delivery QoS.
    pub fn subscribe(&mut self, client: u64, filter: String) -> bool {
        self.subscribe_qos(client, filter, 0)
    }

    /// Insert or replace the granted maximum QoS of an exact subscription.
    /// Returns false for invalid filters or QoS outside 0..=2, without mutation.
    /// Replacement changes QoS but does not consume another subscription slot.
    pub fn subscribe_qos(&mut self, client: u64, filter: String, qos: u8) -> bool {
        if qos > 2 || !validate_filter(&filter) {
            return false;
        }
        let mut node = 0;
        for level in filter.split('/') {
            if level == "#" {
                self.nodes[node].descendants.insert(client, qos);
                self.subscriptions.entry(client).or_default().insert(filter);
                return true;
            }
            node = match self.nodes[node].children.get(level).copied() {
                Some(index) => index,
                None => {
                    let index = match self.free.pop() {
                        Some(index) => index,
                        None => {
                            self.nodes.push(Node::default());
                            self.nodes.len() - 1
                        }
                    };
                    self.nodes[node].children.insert(level.to_owned(), index);
                    index
                }
            };
        }
        self.nodes[node].clients.insert(client, qos);
        self.subscriptions.entry(client).or_default().insert(filter);
        true
    }

    /// Remove an exact filter. Removing absent filters is idempotent.
    pub fn unsubscribe(&mut self, client: u64, filter: &str) {
        let Some(filters) = self.subscriptions.get_mut(&client) else {
            return;
        };
        if !filters.remove(filter) {
            return;
        }
        if filters.is_empty() {
            self.subscriptions.remove(&client);
        }
        let mut node = 0;
        let mut path = Vec::new();
        let mut hash = false;
        for level in filter.split('/') {
            if level == "#" {
                hash = true;
                break;
            }
            let Some(next) = self.nodes[node].children.get(level).copied() else {
                return;
            };
            path.push((node, level, next));
            node = next;
        }
        if hash {
            self.nodes[node].descendants.remove(&client);
        } else {
            self.nodes[node].clients.remove(&client);
        }
        for (parent, level, child) in path.into_iter().rev() {
            if !self.nodes[child].is_empty() {
                break;
            }
            self.nodes[parent].children.remove(level);
            self.free.push(child);
        }
    }

    /// Remove all subscriptions for a disconnected connection generation.
    pub fn remove_client(&mut self, client: u64) {
        let Some(filters) = self.subscriptions.get(&client).cloned() else {
            return;
        };
        for filter in filters {
            self.unsubscribe(client, &filter);
        }
    }

    /// Return unique matching connection generations in ascending order.
    pub fn matching_clients(&self, topic: &str) -> Vec<u64> {
        self.matching_qos(topic)
            .into_iter()
            .map(|(client, _)| client)
            .collect()
    }

    /// Return unique clients in ascending order with the maximum QoS granted by
    /// any matching subscription. The caller caps this by the published QoS.
    /// Matching visits only applicable trie branches, never the reverse index.
    pub fn matching_qos(&self, topic: &str) -> Vec<(u64, u8)> {
        if !valid_topic(topic) {
            return Vec::new();
        }
        let levels: Vec<_> = topic.split('/').collect();
        let system = topic.starts_with('$');
        let mut pending = vec![(0, 0)];
        let mut found = BTreeMap::new();
        while let Some((node, depth)) = pending.pop() {
            let current = &self.nodes[node];
            if depth != 0 || !system {
                merge_qos(&mut found, &current.descendants);
            }
            if depth == levels.len() {
                merge_qos(&mut found, &current.clients);
                continue;
            }
            if let Some(&child) = current.children.get(levels[depth]) {
                pending.push((child, depth + 1));
            }
            if (depth != 0 || !system)
                && let Some(&child) = current.children.get("+")
            {
                pending.push((child, depth + 1));
            }
        }
        found.into_iter().collect()
    }

    /// Number of distinct filters held by a connection generation.
    pub fn subscription_count(&self, client: u64) -> usize {
        self.subscriptions.get(&client).map_or(0, BTreeSet::len)
    }
}

#[cfg(test)]
mod tests;
