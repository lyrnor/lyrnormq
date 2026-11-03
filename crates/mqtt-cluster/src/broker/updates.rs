//! Sparse watch installation: fence touched prior owners before publishing changes.
use super::*;

struct Effects {
    fences: BTreeSet<(String, u64)>,
    discard: Vec<String>,
    owners: Vec<String>,
    fence_all: bool,
    reconcile: bool,
}
impl Effects {
    fn prepare(runtime: &BrokerRuntime, update: &MetadataUpdate) -> io::Result<Self> {
        let MetadataUpdate::Delta {
            base,
            revision,
            nodes,
            clients,
            durable,
            data_generation,
            data_mode,
            disk_ids,
            excluded,
            repair,
            last_repair,
            ..
        } = update
        else {
            return Err(unavailable());
        };
        let old = runtime
            .metadata
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        if *base != old.revision || *revision <= *base {
            return Err(unavailable());
        }
        let mut effects = Self {
            fences: BTreeSet::new(),
            discard: Vec::new(),
            owners: durable.keys().cloned().collect(),
            fence_all: false,
            reconcile: false,
        };
        for (client, next) in clients {
            if let Some(previous) = old.clients.get(client)
                && previous.node == runtime.config.node_id
                && previous.incarnation == runtime.incarnation
                && !next.as_ref().is_some_and(|c| {
                    c.node == previous.node
                        && c.incarnation == previous.incarnation
                        && c.generation == previous.generation
                })
            {
                effects.fences.insert((client.clone(), previous.generation));
            }
        }
        for (key, next) in durable {
            if let Some(previous) = old.durable.get(key)
                && previous.node == runtime.config.node_id
                && previous.incarnation == runtime.incarnation
            {
                if next.as_ref().is_none_or(|n| {
                    n.node != previous.node || n.incarnation != previous.incarnation
                }) {
                    effects.discard.push(key.clone());
                }
                if next.as_ref().is_none_or(|n| {
                    n.epoch != previous.epoch
                        || n.node != previous.node
                        || n.incarnation != previous.incarnation
                }) && let Some(client) = key.strip_prefix("c:")
                {
                    effects
                        .fences
                        .insert((client.to_owned(), previous.generation));
                }
            }
        }
        effects.fence_all = nodes.get(&runtime.config.node_id).is_some_and(|next| {
            next.as_ref()
                .is_none_or(|n| n.incarnation != runtime.incarnation)
        });
        effects.reconcile = nodes.iter().any(|(id, next)| {
            old.nodes.get(id).map(|n| &n.incarnation) != next.as_ref().map(|n| &n.incarnation)
        }) || old.data_generation != *data_generation
            || old.data_mode != *data_mode
            || old.disk_ids != *disk_ids
            || old.excluded != *excluded
            || old.repair != *repair
            || old.last_repair != *last_repair;
        Ok(effects)
    }
}
impl BrokerRuntime {
    pub(super) async fn install_update(&self, update: MetadataUpdate) -> io::Result<()> {
        match update {
            MetadataUpdate::Progress { revision } => {
                if self
                    .metadata
                    .read()
                    .unwrap_or_else(PoisonError::into_inner)
                    .revision
                    != revision
                {
                    return Err(unavailable());
                }
                return Ok(());
            }
            MetadataUpdate::Snapshot(metadata) => return self.install(metadata, true).await,
            _ => {}
        }
        let effects = Effects::prepare(self, &update)?;
        let local = self.local()?;
        for (client, generation) in effects.fences {
            local.fence(client, generation).await?;
        }
        if effects.fence_all {
            local.fence_all().await?;
            self.initialized.store(false, Ordering::Release);
        }
        {
            let mut metadata = self
                .metadata
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            update
                .merge_in_place(&mut metadata)
                .map_err(|_| unavailable())?;
            let mut recipients = self
                .recipients
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            for key in &effects.owners {
                recipients.replace(key, metadata.durable.get(key));
            }
        }
        self.reconcile_repair().await?;
        self.initialized.store(true, Ordering::Release);
        self.announce_applied();
        if let Some(data) = &self.data {
            for key in effects.discard {
                data.forget(&key);
            }
            if effects.reconcile {
                data.ready_again(self);
            } else {
                data.owner_events(self, &effects.owners);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn applied_delta_gap_is_rejected_before_fencing_or_publication() {
        let mut old = Metadata {
            revision: 3,
            ..Metadata::default()
        };
        let mut next = old.clone();
        next.revision = 4;
        let delta = MetadataUpdate::between(&old, &next);
        old.revision = 5;
        assert!(delta.merge_in_place(&mut old).is_err());
        assert_eq!(old.revision, 5);
    }
}
