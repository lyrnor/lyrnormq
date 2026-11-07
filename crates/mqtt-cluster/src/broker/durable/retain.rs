//! Topic-keyed retained replica records (`r:{topic}`).
use super::*;
use mqtt_cluster_core::ClusterMessage;

pub(super) fn retain_key(topic: &str) -> String {
    format!("r:{topic}")
}

impl Durable {
    pub async fn commit_retained(self: &Arc<Self>, message: ClusterMessage) -> io::Result<()> {
        if message.qos > 2 || message.payload.len() > MAX_PAYLOAD {
            return Err(unavailable());
        }
        let key = retain_key(&message.topic);
        let (reply, rx) = oneshot::channel();
        self.send(&key, Operation::Retain { message, reply })
            .await?;
        self.await_reply(rx).await
    }

    pub async fn matching_retained(
        self: &Arc<Self>,
        filters: Vec<String>,
    ) -> io::Result<Vec<ClusterMessage>> {
        let runtime = self.runtime.upgrade().ok_or_else(unavailable)?;
        if !runtime.ready() {
            return Err(unavailable());
        }
        if filters.is_empty() {
            return Ok(Vec::new());
        }
        let topics: Vec<String> = {
            let metadata = runtime
                .metadata
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            metadata
                .durable
                .iter()
                .filter_map(|(key, owner)| {
                    if owner.filters.is_empty() {
                        return None;
                    }
                    let topic = key.strip_prefix("r:")?;
                    filters
                        .iter()
                        .any(|filter| mqtt_router::matches(filter, topic))
                        .then(|| topic.to_owned())
                })
                .collect()
        };
        let mut matched = Vec::new();
        let now = controller::now();
        for topic in topics {
            let record = quorum::request(
                &runtime,
                &self.journal,
                Request::Read {
                    key: retain_key(&topic),
                },
            )
            .await?;
            let Some(snapshot) = record.and_then(|record| record.snapshot) else {
                continue;
            };
            let message: ClusterMessage =
                serde_json::from_slice(&snapshot).map_err(|_| unavailable())?;
            if message.expires_at.is_some_and(|deadline| deadline <= now) {
                continue;
            }
            if message.payload.is_empty() {
                continue;
            }
            matched.push(message);
        }
        Ok(matched)
    }

    pub(super) async fn put_retain(
        &self,
        runtime: &BrokerRuntime,
        key: &str,
        message: ClusterMessage,
        cell: &mut Cell,
    ) -> io::Result<()> {
        if (cell.pending.is_some()
            || cell.poisoned
            || cell
                .record
                .as_ref()
                .is_none_or(|record| !Self::authoritative(runtime, key, record.epoch)))
            && let Err(error) = self.recover_cell(runtime, key, cell).await
        {
            return Err(error);
        }
        let current = cell.record.clone().ok_or_else(unavailable)?;
        let clear = message.payload.is_empty()
            || message
                .expires_at
                .is_some_and(|deadline| deadline <= controller::now());
        let mut next = current;
        next.revision = next.revision.checked_add(1).ok_or_else(unavailable)?;
        next.logical_generation = next
            .logical_generation
            .checked_add(1)
            .ok_or_else(unavailable)?;
        next.intents.clear();
        next.snapshot = if clear {
            None
        } else {
            Some(serde_json::to_vec(&message).map_err(|_| unavailable())?)
        };
        next.filters = if clear {
            Vec::new()
        } else {
            vec![message.topic.clone()]
        };
        self.store(runtime, key, next, cell).await?;
        let record = cell.record.as_ref().ok_or_else(unavailable)?;
        if runtime
            .command(
                Command::DurableFilters {
                    key: key.to_owned(),
                    node: runtime.config.node_id,
                    incarnation: runtime.incarnation.clone(),
                    epoch: record.epoch,
                    filters: record.filters.clone(),
                    logical_generation: record.logical_generation,
                },
                true,
            )
            .await
            .is_err()
        {
            cell.poisoned = true;
            return Err(unavailable());
        }
        if let Ok(local) = runtime.local() {
            let applied = if clear {
                ClusterMessage {
                    payload: Vec::new(),
                    ..message
                }
            } else {
                message
            };
            let _ = local.apply_retained(applied).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::retain_key;

    #[test]
    fn retain_key_uses_r_prefix() {
        assert_eq!(retain_key("cluster/retained"), "r:cluster/retained");
    }
}
