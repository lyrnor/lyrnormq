//! Broker-wide subscription index shared by per-session actors.
//!
//! Each client's filter set is replaced as a whole after its session commits, so
//! readers see either the old or the new set. The lock is synchronous and is
//! never held by callers across asynchronous work.
use std::collections::{BTreeSet, HashMap};
use std::sync::{PoisonError, RwLock};

use crate::filter::{split_shared_filter, validate_filter};
use crate::trie::Router;

/// One matched shared group: the full `$share/{group}/{filter}` key and its
/// members rotated to start after the last selected member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedMatch {
    pub group: String,
    pub members: Vec<String>,
}

/// Clients with at least one matching ordinary subscription, in client order,
/// and every matching shared group.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexMatches {
    pub ordinary: Vec<String>,
    pub shared: Vec<SharedMatch>,
}

#[derive(Default)]
struct Ids {
    by_name: HashMap<String, u64>,
    names: HashMap<u64, String>,
    next: u64,
}
impl Ids {
    fn get_or_insert(&mut self, name: &str) -> u64 {
        if let Some(id) = self.by_name.get(name) {
            return *id;
        }
        let id = self.next;
        self.next = self.next.wrapping_add(1);
        self.by_name.insert(name.to_owned(), id);
        self.names.insert(id, name.to_owned());
        id
    }
    fn remove(&mut self, name: &str) -> Option<u64> {
        let id = self.by_name.remove(name)?;
        self.names.remove(&id);
        Some(id)
    }
}

#[derive(Default)]
struct State {
    ordinary: Router,
    clients: Ids,
    groups_router: Router,
    groups: Ids,
    members: HashMap<String, BTreeSet<String>>,
    cursors: HashMap<String, String>,
    filters: HashMap<String, BTreeSet<String>>,
}

impl State {
    fn add(&mut self, client: &str, filter: &str) {
        if let Some((_, inner)) = split_shared_filter(filter) {
            let members = self.members.entry(filter.to_owned()).or_default();
            if members.is_empty() {
                let id = self.groups.get_or_insert(filter);
                self.groups_router
                    .subscribe_topic_filter_qos(id, inner.to_owned(), 0);
            }
            members.insert(client.to_owned());
        } else if validate_filter(filter) {
            let id = self.clients.get_or_insert(client);
            self.ordinary.subscribe(id, filter.to_owned());
        }
    }
    fn drop_filter(&mut self, client: &str, filter: &str) {
        if split_shared_filter(filter).is_some() {
            let empty = match self.members.get_mut(filter) {
                Some(members) => {
                    members.remove(client);
                    members.is_empty()
                }
                None => false,
            };
            if empty {
                self.members.remove(filter);
                self.cursors.remove(filter);
                if let Some(id) = self.groups.remove(filter) {
                    self.groups_router.remove_client(id);
                }
            }
        } else if let Some(id) = self.clients.by_name.get(client) {
            self.ordinary.unsubscribe(*id, filter);
        }
    }
}

/// Concurrent ordinary and shared subscription index keyed by client identifier.
#[derive(Default)]
pub struct SubscriptionIndex {
    state: RwLock<State>,
}

impl SubscriptionIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace one client's complete filter set, including `$share` keys.
    pub fn replace(&self, client: &str, filters: &BTreeSet<String>) {
        let mut state = self.state.write().unwrap_or_else(PoisonError::into_inner);
        let previous = state.filters.remove(client).unwrap_or_default();
        for filter in previous.difference(filters) {
            state.drop_filter(client, filter);
        }
        for filter in filters.difference(&previous) {
            state.add(client, filter);
        }
        if filters
            .iter()
            .all(|filter| split_shared_filter(filter).is_some())
            && let Some(id) = state.clients.remove(client)
        {
            state.ordinary.remove_client(id);
        }
        if !filters.is_empty() {
            state.filters.insert(client.to_owned(), filters.clone());
        }
    }

    pub fn remove(&self, client: &str) {
        self.replace(client, &BTreeSet::new());
    }

    pub fn matches(&self, topic: &str) -> IndexMatches {
        let state = self.state.read().unwrap_or_else(PoisonError::into_inner);
        let mut ordinary: Vec<String> = state
            .ordinary
            .matching_clients(topic)
            .into_iter()
            .filter_map(|id| state.clients.names.get(&id).cloned())
            .collect();
        ordinary.sort();
        let mut shared = Vec::new();
        for id in state.groups_router.matching_clients(topic) {
            let Some(group) = state.groups.names.get(&id) else {
                continue;
            };
            let Some(members) = state.members.get(group) else {
                continue;
            };
            let members: Vec<String> = members.iter().cloned().collect();
            let start = state.cursors.get(group).map_or(0, |last| {
                members.iter().position(|member| member > last).unwrap_or(0)
            });
            let mut rotated = members[start..].to_vec();
            rotated.extend_from_slice(&members[..start]);
            shared.push(SharedMatch {
                group: group.clone(),
                members: rotated,
            });
        }
        shared.sort_by(|left, right| left.group.cmp(&right.group));
        IndexMatches { ordinary, shared }
    }

    /// Record the member that accepted a shared copy; the next match starts
    /// after it.
    pub fn select(&self, group: &str, client: &str) {
        let mut state = self.state.write().unwrap_or_else(PoisonError::into_inner);
        if state.members.contains_key(group) {
            state.cursors.insert(group.to_owned(), client.to_owned());
        }
    }

    pub fn filters(&self, client: &str) -> BTreeSet<String> {
        let state = self.state.read().unwrap_or_else(PoisonError::into_inner);
        state.filters.get(client).cloned().unwrap_or_default()
    }

    /// Clients holding at least one subscription.
    pub fn client_count(&self) -> usize {
        let state = self.state.read().unwrap_or_else(PoisonError::into_inner);
        state.filters.len()
    }
}

#[cfg(test)]
#[path = "index_tests.rs"]
mod tests;
