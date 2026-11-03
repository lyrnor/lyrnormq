//! Volatile topic index, published under the runtime metadata lock.
use crate::{
    model::{DurableOwner, Metadata},
    replica::Recipient,
};
use mqtt_router::Router;
use std::collections::BTreeMap;
use std::sync::PoisonError;

struct Entry {
    slot: usize,
    filters: Vec<String>,
}

/// Contains only subscribed client owners. Numeric identities are local reusable
/// slots, never durable epochs or externally observable connection identifiers.
#[derive(Default)]
pub(super) struct RecipientIndex {
    router: Router,
    entries: BTreeMap<String, Entry>,
    slots: Vec<Option<Recipient>>,
    free: Vec<usize>,
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
        let owner = owner.filter(|owner| {
            owner
                .filters
                .iter()
                .any(|f| mqtt_router::validate_topic_filter(f))
        });
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
            // Preserve literal $share-prefixed matching from the old scan. Group
            // scheduling and per-session delivery options are applied elsewhere.
            self.router
                .subscribe_topic_filter_qos(slot as u64, filter.clone(), 0);
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
}

#[cfg(test)]
mod tests;
