//! Sequential child transactions with one sparse final watch update.
use super::*;
pub(crate) const MAX_COMMANDS: usize = 256;
pub(crate) const MAX_BYTES: usize = 512 * 1024;
pub(crate) const EMPTY_BYTES: usize = b"{\"Batch\":{\"commands\":[]}}".len();
#[derive(Serialize)]
enum Borrowed<'a> {
    Batch { commands: &'a [Command] },
}
pub(crate) fn encoded_len(commands: &[Command]) -> Result<usize, serde_json::Error> {
    serde_json::to_vec(&Borrowed::Batch { commands }).map(|bytes| bytes.len())
}
impl Metadata {
    pub(super) fn apply_batch(
        &mut self,
        commands: Vec<Command>,
    ) -> (Answer, Option<MetadataUpdate>) {
        if commands.is_empty()
            || commands.len() > MAX_COMMANDS
            || commands
                .iter()
                .any(|command| matches!(command, Command::Batch { .. }))
            || !matches!(encoded_len(&commands), Ok(size) if size<=MAX_BYTES)
        {
            return (
                Answer {
                    revision: self.revision,
                    error: Some("invalid metadata batch".into()),
                    ..Answer::default()
                },
                None,
            );
        }
        let base = self.revision;
        let (mut nodes, mut clients, mut durable, mut routes) = (
            std::collections::BTreeSet::new(),
            std::collections::BTreeSet::new(),
            std::collections::BTreeSet::new(),
            std::collections::BTreeSet::new(),
        );
        let mut results = Vec::with_capacity(commands.len());
        for command in commands {
            if !valid_command(&command) {
                results.push(Answer {
                    revision: self.revision,
                    error: Some("invalid metadata command".into()),
                    ..Answer::default()
                });
                continue;
            }
            let (answer, delta) = self.apply_incremental(command);
            results.push(answer);
            // The incremental state machine emits Delta or None, never a full snapshot.
            if let Some(MetadataUpdate::Delta {
                nodes: n,
                clients: c,
                durable: d,
                routes: r,
                ..
            }) = delta
            {
                nodes.extend(n.into_keys());
                clients.extend(c.into_keys());
                durable.extend(d.into_keys());
                routes.extend(r.into_keys());
            }
        }
        let delta = if self.revision != base {
            // Include trailing heartbeats in the final touched values, without changing
            // their legacy revision semantics or losing pending progress from prior calls.
            nodes.append(&mut self.derived.pending_nodes);
            Some(MetadataUpdate::Delta {
                base,
                revision: self.revision,
                data_mode: self.data_mode.clone(),
                disk_ids: self.disk_ids.clone(),
                data_generation: self.data_generation,
                excluded: self.excluded.clone(),
                repair: self.repair.clone(),
                last_repair: self.last_repair.clone(),
                nodes: nodes
                    .into_iter()
                    .map(|key| {
                        let value = self.nodes.get(&key).cloned();
                        (key, value)
                    })
                    .collect(),
                clients: clients
                    .into_iter()
                    .map(|key| {
                        let value = self.clients.get(&key).cloned();
                        (key, value)
                    })
                    .collect(),
                durable: durable
                    .into_iter()
                    .map(|key| {
                        let value = self.durable.get(&key).cloned();
                        (key, value)
                    })
                    .collect(),
                routes: routes
                    .into_iter()
                    .map(|key| {
                        let value = self.routes.get(&key).cloned();
                        (key, value)
                    })
                    .collect(),
                rules_config: self.rules_config.clone(),
                access_config: self.access_config.clone(),
            })
        } else {
            None
        };
        (
            Answer {
                revision: self.revision,
                error: None,
                lease_ms: 0,
                results,
            },
            delta,
        )
    }
}
