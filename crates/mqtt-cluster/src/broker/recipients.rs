//! Volatile topic index, published under the runtime metadata lock.
use crate::{
    model::{DurableOwner, Metadata},
    replica::Recipient,
};
use mqtt_router::Router;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::PoisonError;

struct Entry {
    slot: usize,
    filters: Vec<String>,
}

struct SharedGroup {
    id: u64,
    members: BTreeSet<String>,
}

/// One selected shared copy: MQTT share name, underlying filter, recipient.
pub(super) struct SharedRecipient {
    pub group: String,
    pub filter: String,
    pub recipient: Recipient,
}

/// Contains only subscribed client owners. Numeric identities are local reusable
/// slots, never durable epochs or externally observable connection identifiers.
#[derive(Default)]
pub(super) struct RecipientIndex {
    router: Router,
    entries: BTreeMap<String, Entry>,
    slots: Vec<Option<Recipient>>,
    free: Vec<usize>,
    groups_router: Router,
    groups: BTreeMap<String, SharedGroup>,
    group_ids: BTreeMap<u64, String>,
    next_group: u64,
    cursors: BTreeMap<String, String>,
}

fn indexable(filters: &[String]) -> bool {
    filters.iter().any(|filter| {
        mqtt_router::split_shared_filter(filter).is_some()
            || mqtt_router::validate_topic_filter(filter)
    })
}

fn ordinary_filter(filter: &str) -> bool {
    mqtt_router::split_shared_filter(filter).is_none() && mqtt_router::validate_topic_filter(filter)
}

impl RecipientIndex {
    pub(super) fn from_metadata(metadata: &Metadata) -> Self {
        let mut index = Self::default();
        for (key, owner) in &metadata.durable {
            index.replace(key, Some(owner));
        }
        index
    }

    pub(super) fn replace(&mut self, key: &str, owner: Option<&DurableOwner>) {
        let Some(client) = key.strip_prefix("c:") else {
            return;
        };
        let owner = owner.filter(|owner| indexable(&owner.filters));
        if let (Some(entry), Some(owner)) = (self.entries.get(key), owner)
            && entry.filters == owner.filters
        {
            if let Some(recipient) = &mut self.slots[entry.slot] {
                recipient.generation = owner.logical_generation;
            }
            return;
        }
        if let Some(previous) = self.entries.remove(key) {
            self.router.remove_client(previous.slot as u64);
            self.drop_shared(client, &previous.filters);
            self.slots[previous.slot] = None;
            self.free.push(previous.slot);
        }
        let Some(owner) = owner else {
            return;
        };
        let slot = self.free.pop().unwrap_or_else(|| {
            self.slots.push(None);
            self.slots.len() - 1
        });
        for filter in &owner.filters {
            if let Some((_, inner)) = mqtt_router::split_shared_filter(filter) {
                self.add_member(client, filter, inner);
            } else if ordinary_filter(filter) {
                self.router
                    .subscribe_topic_filter_qos(slot as u64, filter.clone(), 0);
            }
        }
        self.slots[slot] = Some(Recipient {
            client_id: client.to_owned(),
            generation: owner.logical_generation,
        });
        self.entries.insert(
            key.to_owned(),
            Entry {
                slot,
                filters: owner.filters.clone(),
            },
        );
    }

    fn add_member(&mut self, client: &str, group: &str, inner: &str) {
        if !self.groups.contains_key(group) {
            let id = self.next_group;
            self.next_group = self.next_group.wrapping_add(1);
            self.groups_router
                .subscribe_topic_filter_qos(id, inner.to_owned(), 0);
            self.group_ids.insert(id, group.to_owned());
            self.groups.insert(
                group.to_owned(),
                SharedGroup {
                    id,
                    members: BTreeSet::new(),
                },
            );
        }
        if let Some(group) = self.groups.get_mut(group) {
            group.members.insert(client.to_owned());
        }
    }

    fn drop_shared(&mut self, client: &str, filters: &[String]) {
        for filter in filters {
            if mqtt_router::split_shared_filter(filter).is_some() {
                self.remove_member(client, filter);
            }
        }
    }

    fn remove_member(&mut self, client: &str, group: &str) {
        let Some(entry) = self.groups.get_mut(group) else {
            return;
        };
        entry.members.remove(client);
        if !entry.members.is_empty() {
            return;
        }
        let id = entry.id;
        self.groups.remove(group);
        self.cursors.remove(group);
        self.group_ids.remove(&id);
        self.groups_router.remove_client(id);
    }

    fn recipient(&self, client: &str) -> Option<Recipient> {
        let entry = self.entries.get(&format!("c:{client}"))?;
        self.slots.get(entry.slot).and_then(Option::as_ref).cloned()
    }

    /// Remote node addresses for an indexed match. Local owners are omitted;
    /// the caller already delivered those from its own subscription index.
    pub(super) fn remote_addresses(
        metadata: &Metadata,
        matched: &[Recipient],
        self_node: u64,
        self_incarnation: &str,
    ) -> Vec<String> {
        let mut addresses = BTreeSet::new();
        for recipient in matched {
            let Some(owner) = metadata.durable.get(&format!("c:{}", recipient.client_id)) else {
                continue;
            };
            if owner.node == self_node && owner.incarnation == self_incarnation {
                continue;
            }
            if let Some(node) = metadata.nodes.get(&owner.node) {
                addresses.insert(node.address.clone());
            }
        }
        addresses.into_iter().collect()
    }

    pub(super) fn matching(&self, topic: &str) -> Vec<Recipient> {
        if self.entries.is_empty() {
            return Vec::new();
        }
        let mut recipients: Vec<_> = self
            .router
            .matching_clients(topic)
            .into_iter()
            .filter_map(|slot| {
                self.slots
                    .get(slot as usize)
                    .and_then(Option::as_ref)
                    .cloned()
            })
            .collect();
        recipients.sort_unstable_by(|a, b| a.client_id.cmp(&b.client_id));
        recipients
    }

    /// One member per matching shared group. Advances the in-memory cursor.
    pub(super) fn select_shared(&mut self, topic: &str) -> Vec<SharedRecipient> {
        if self.groups.is_empty() {
            return Vec::new();
        }
        let mut keys: Vec<String> = self
            .groups_router
            .matching_clients(topic)
            .into_iter()
            .filter_map(|id| self.group_ids.get(&id).cloned())
            .collect();
        keys.sort();
        keys.dedup();
        let mut selected = Vec::new();
        let mut cursors = Vec::new();
        for key in keys {
            let Some((group, inner)) = mqtt_router::split_shared_filter(&key) else {
                continue;
            };
            let (group, inner) = (group.to_owned(), inner.to_owned());
            let Some(members) = self
                .groups
                .get(&key)
                .map(|entry| entry.members.iter().cloned().collect::<Vec<_>>())
            else {
                continue;
            };
            let start = self.cursors.get(&key).map_or(0, |last| {
                members.iter().position(|member| member > last).unwrap_or(0)
            });
            let mut chosen = None;
            for offset in 0..members.len() {
                let client = &members[(start + offset) % members.len()];
                if let Some(recipient) = self.recipient(client) {
                    chosen = Some((client.clone(), recipient));
                    break;
                }
            }
            let Some((client, recipient)) = chosen else {
                continue;
            };
            cursors.push((key, client));
            selected.push(SharedRecipient {
                group,
                filter: inner,
                recipient,
            });
        }
        for (key, client) in cursors {
            self.cursors.insert(key, client);
        }
        selected
    }
}

impl super::BrokerRuntime {
    pub(super) fn durable_recipients(&self, topic: &str) -> Vec<Recipient> {
        // Publication takes the same outer guard while updating metadata and
        // index. A lookup cannot observe half of an installation.
        let _metadata = self.metadata.read().unwrap_or_else(PoisonError::into_inner);
        self.recipients
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .matching(topic)
    }

    pub(super) fn select_shared_recipients(&self, topic: &str) -> Vec<SharedRecipient> {
        let _metadata = self.metadata.read().unwrap_or_else(PoisonError::into_inner);
        self.recipients
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .select_shared(topic)
    }
}

#[cfg(test)]
mod tests;
