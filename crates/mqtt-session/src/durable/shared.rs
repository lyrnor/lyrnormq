//! Bounded shared group selection. Selected copies use the existing session outbox.
use super::*;

type Copies = Vec<(usize, Message)>;
type CursorUpdates = BTreeMap<String, String>;

/// Only Subscribe resources are unwrapped, never publication Topic Names.
pub(super) fn acl_filter(filter: &str) -> &str {
    split_shared_filter(filter).map_or(filter, |(_, inner)| inner)
}

impl SessionEngine {
    pub(super) fn ensure_shared_epoch(&mut self, group: &str) -> Result<(), EngineError> {
        if !self.shared_epochs.contains_key(group) {
            self.shared_epoch_sequence = self
                .shared_epoch_sequence
                .checked_add(1)
                .ok_or(EngineError::Capacity)?;
            self.shared_epochs
                .insert(group.to_owned(), self.shared_epoch_sequence);
        }
        Ok(())
    }
    pub(super) fn active_shared_groups(&self) -> BTreeSet<String> {
        self.sessions
            .values()
            .flat_map(|session| session.subscriptions.keys())
            .filter(|filter| split_shared_filter(filter).is_some())
            .cloned()
            .collect()
    }

    /// A group's unstarted backlog ends with its last membership. Already-started
    /// exchanges remain their selected session's protocol obligations.
    pub(super) fn prune_shared_state(&mut self) -> bool {
        let groups = self.active_shared_groups();
        self.shared_cursors
            .retain(|group, _| groups.contains(group));
        self.shared_epochs.retain(|group, _| groups.contains(group));
        let mut changed = false;
        for session in self.sessions.values_mut() {
            let before = session.queued.len();
            session.queued.retain(|message| {
                message
                    .shared_subscription
                    .as_ref()
                    .is_none_or(|key| groups.contains(key))
            });
            changed |= before != session.queued.len();
            let stale: Vec<_> = session
                .outgoing_shared
                .iter()
                .filter(|(id, group)| {
                    !groups.contains(*group) && !session.outgoing_started.contains(id)
                })
                .map(|(id, _)| *id)
                .collect();
            for id in stale {
                session.retire_outgoing(id);
                changed = true;
            }
        }
        changed
    }

    /// All copies and cursor changes are planned before any new outbox append.
    pub(super) fn plan_shared_copies(
        &mut self,
        message: &Message,
        clients: &[String],
        ordinary: &[(usize, Message)],
        now: u64,
        only_group: Option<&str>,
    ) -> Result<(Copies, CursorUpdates), EngineError> {
        let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for (index, client) in clients.iter().enumerate() {
            let session = self
                .sessions
                .get(client)
                .ok_or(EngineError::UnknownSession)?;
            if !self
                .access
                .authorize(&session.owner, Action::Connect, client)
            {
                continue;
            }
            for filter in session.subscriptions.keys() {
                if only_group.is_some_and(|group| group != filter) {
                    continue;
                }
                if let Some((_, inner)) = split_shared_filter(filter)
                    && self
                        .access
                        .authorize(&session.owner, Action::Subscribe, inner)
                {
                    groups.entry(filter.clone()).or_default().push(index);
                }
            }
        }
        let mut router = Router::default();
        let group_keys: Vec<_> = groups.keys().cloned().collect();
        for (index, key) in group_keys.iter().enumerate() {
            let (_, inner) =
                split_shared_filter(key).ok_or_else(|| invalid_record("invalid shared group"))?;
            if !router.subscribe_topic_filter_qos(index as u64, inner.into(), 0) {
                return Err(invalid_record("invalid underlying shared pattern"));
            }
        }
        let mut planned = Vec::new();
        let mut counts: BTreeMap<usize, usize> = BTreeMap::new();
        for (index, _) in ordinary {
            *counts.entry(*index).or_default() += 1;
        }
        let mut cursors = BTreeMap::new();
        for (group_index, _) in router.matching_qos(&message.topic) {
            let key = group_keys
                .get(group_index as usize)
                .ok_or_else(|| invalid_record("shared router index"))?;
            let members = groups
                .get(key)
                .ok_or_else(|| invalid_record("absent shared membership"))?;
            let start = self.shared_cursors.get(key).map_or(0, |last| {
                members
                    .iter()
                    .position(|index| clients[*index] > *last)
                    .unwrap_or(0)
            });
            let mut selected = None;
            let mut blocked = false;
            // Prefer eligible online members, then persistent offline QoS1/2.
            for online in [true, false] {
                for offset in 0..members.len() {
                    let index = members[(start + offset) % members.len()];
                    let client = clients
                        .get(index)
                        .ok_or_else(|| invalid_record("shared client index"))?;
                    let session = self
                        .sessions
                        .get_mut(client)
                        .ok_or(EngineError::UnknownSession)?;
                    if session.online != online
                        || (!online && !session.persistent)
                        || !self
                            .access
                            .authorize(&session.owner, Action::Receive, &message.topic)
                    {
                        continue;
                    }
                    let subscription = session
                        .subscriptions
                        .get(key)
                        .ok_or_else(|| invalid_record("absent shared subscription"))?;
                    let qos = message.qos.min(subscription.qos);
                    if !online && qos == 0 {
                        continue;
                    }
                    let mut copy = Message {
                        topic: message.topic.clone(),
                        payload: message.payload.clone(),
                        qos,
                        retain: message.retain && subscription.retain_as_published,
                        retained_replay: false,
                        shared_subscription: Some(key.clone()),
                        expires_at: message.expires_at,
                        properties: message.properties.clone(),
                    };
                    if let Some(id) = subscription.identifier {
                        copy.properties.push(Property::SubscriptionIdentifier(id));
                    }
                    let packet = copy.packet((qos != 0).then_some(1), false);
                    match mqtt5::check_delivery_size(
                        self.config.max_packet_size,
                        session.version,
                        &packet,
                        &copy.properties,
                    ) {
                        Ok(()) => {}
                        Err(EngineError::Capacity) => {
                            blocked = true;
                            continue;
                        }
                        Err(error) => return Err(error),
                    }
                    mqtt5::discard_oversized(session, now, self.config.max_packet_size)?;
                    if mqtt5::exceeds_peer_limit(
                        session,
                        &packet,
                        &copy.properties,
                        copy.expires_at,
                        now,
                    )? {
                        continue;
                    }
                    let reserved = counts.get(&index).copied().unwrap_or(0);
                    if session.queued.len().saturating_add(reserved) >= self.config.max_queued {
                        blocked = true;
                        continue;
                    }
                    selected = Some((index, copy));
                    break;
                }
                if selected.is_some() {
                    break;
                }
            }
            if let Some((index, copy)) = selected {
                cursors.insert(key.clone(), clients[index].clone());
                *counts.entry(index).or_default() += 1;
                planned.push((index, copy));
            } else if blocked {
                return Err(EngineError::Capacity);
            }
        }
        Ok((planned, cursors))
    }
}
