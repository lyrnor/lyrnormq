//! Volatile installation progress. ReadIndex, not this cache, establishes authority.
use super::*;
use std::sync::Mutex;

#[derive(Default)]
struct Cache {
    term: u64,
    nodes: BTreeMap<u64, (String, u64)>,
}
pub(super) struct Progress {
    cache: Mutex<Cache>,
    changed: watch::Sender<u64>,
}
impl Default for Progress {
    fn default() -> Self {
        Self {
            cache: Mutex::new(Cache::default()),
            changed: watch::channel(0).0,
        }
    }
}
impl Progress {
    fn acknowledge(
        &self,
        metadata: &Metadata,
        term: u64,
        node: u64,
        incarnation: &str,
        revision: u64,
    ) -> Result<(), Status> {
        if revision > metadata.revision
            || metadata.excluded.contains(&node)
            || !metadata
                .nodes
                .get(&node)
                .is_some_and(|n| n.incarnation == incarnation)
        {
            return Err(Status::failed_precondition("stale applied acknowledgement"));
        }
        let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        if cache.term > term {
            return Err(Status::unavailable("stale applied term"));
        }
        if cache.term != term {
            cache.term = term;
            cache.nodes.clear();
        }
        if cache.nodes.len() >= 1024 && !cache.nodes.contains_key(&node) {
            cache.nodes.retain(|id, (incarnation, _)| {
                metadata
                    .nodes
                    .get(id)
                    .is_some_and(|n| n.incarnation == *incarnation)
                    && !metadata.excluded.contains(id)
            });
            if cache.nodes.len() >= 1024 {
                return Err(Status::resource_exhausted("applied cache capacity"));
            }
        }
        let entry = cache
            .nodes
            .entry(node)
            .or_insert_with(|| (incarnation.to_owned(), revision));
        if entry.0 != incarnation {
            *entry = (incarnation.to_owned(), revision);
        } else {
            entry.1 = entry.1.max(revision);
        }
        drop(cache);
        self.changed.send_replace(revision);
        Ok(())
    }
    fn ready(&self, metadata: &Metadata, term: u64, revision: u64) -> bool {
        if metadata.revision < revision || metadata.nodes.is_empty() {
            return false;
        }
        let cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        metadata.nodes.iter().all(|(id, node)| {
            node.applied >= revision
                || (cache.term == term
                    && !metadata.excluded.contains(id)
                    && cache.nodes.get(id).is_some_and(|(incarnation, installed)| {
                        *incarnation == node.incarnation && *installed >= revision
                    }))
        })
    }
}
impl Control {
    pub(super) fn acknowledge(
        &self,
        node: u64,
        incarnation: &str,
        revision: u64,
    ) -> Result<u64, Status> {
        let term = {
            let metrics = self.raft.metrics();
            let metrics = metrics.borrow();
            if metrics.current_leader != Some(self.rpc.config_node()) {
                return Err(Status::unavailable("not current leader"));
            }
            metrics.current_term
        };
        self.progress.acknowledge(
            &self
                .store
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .metadata,
            term,
            node,
            incarnation,
            revision,
        )?;
        Ok(revision)
    }
    pub(super) async fn applied(&self, revision: u64) -> Result<u64, Status> {
        tokio::time::timeout(Duration::from_secs(4), async {
            let mut changes = self.store.changed.subscribe();
            let mut acknowledgements = self.progress.changed.subscribe();
            let mut leadership = self.raft.metrics();
            self.leader().await?;
            let term = leadership.borrow().current_term;
            loop {
                let authority = { let metrics = leadership.borrow(); metrics.current_leader == Some(self.rpc.config_node()) && metrics.current_term == term };
                if !authority { return Err(Status::unavailable("applied leader changed")); }
                let ready = { let state = self.store.state.lock().unwrap_or_else(PoisonError::into_inner); self.progress.ready(&state.metadata, term, revision) };
                if ready {
                    self.leader().await?;
                    let unchanged = { let metrics = leadership.borrow(); metrics.current_leader == Some(self.rpc.config_node()) && metrics.current_term == term };
                    if !unchanged { return Err(Status::unavailable("applied leader changed")); }
                    let ready = { let state = self.store.state.lock().unwrap_or_else(PoisonError::into_inner); self.progress.ready(&state.metadata, term, revision) };
                    if ready { return Ok(revision); }
                }
                tokio::select! {
                    result = changes.changed() => { result.map_err(|_| Status::unavailable("applied watch closed"))?; },
                    result = acknowledgements.changed() => { result.map_err(|_| Status::unavailable("applied cache closed"))?; },
                    result = leadership.changed() => { result.map_err(|_| Status::unavailable("leadership watch closed"))?; },
                }
            }
        }).await.map_err(|_| Status::unavailable("applied deadline"))?
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cache_is_monotonic_incarnation_and_term_bound_without_mutating_state() {
        let mut metadata = Metadata {
            revision: 9,
            ..Metadata::default()
        };
        metadata.nodes.insert(
            101,
            Node {
                id: 101,
                incarnation: "current".into(),
                address: "http://127.0.0.1:1".into(),
                expires_at: 0,
                max_lease_ms: 3000,
                applied: 1,
            },
        );
        let progress = Progress::default();
        assert!(!progress.ready(&metadata, 3, 8));
        progress
            .acknowledge(&metadata, 3, 101, "current", 8)
            .unwrap();
        progress
            .acknowledge(&metadata, 3, 101, "current", 4)
            .unwrap();
        assert!(progress.ready(&metadata, 3, 8));
        assert!(!progress.ready(&metadata, 4, 8));
        assert!(
            progress
                .acknowledge(&metadata, 2, 101, "current", 8)
                .is_err()
        );
        assert_eq!(
            metadata.nodes[&101].applied, 1,
            "volatile cache must not modify durable node state"
        );
        assert!(progress.acknowledge(&metadata, 3, 101, "old", 8).is_err());
        assert!(
            progress
                .acknowledge(&metadata, 3, 999, "current", 8)
                .is_err()
        );
        assert!(
            progress
                .acknowledge(&metadata, 3, 101, "current", 10)
                .is_err()
        );
        metadata.excluded.insert(101);
        assert!(
            progress
                .acknowledge(&metadata, 3, 101, "current", 8)
                .is_err()
        );
        assert!(!progress.ready(&metadata, 3, 8));
        metadata.excluded.clear();
        metadata.nodes.get_mut(&101).unwrap().incarnation = "replacement".into();
        assert!(!progress.ready(&metadata, 3, 8));
    }
    #[test]
    fn stale_nodes_are_pruned_with_a_fixed_cache_capacity() {
        let mut metadata = Metadata {
            revision: 1,
            ..Metadata::default()
        };
        for id in 0..1024 {
            metadata.nodes.insert(
                id,
                Node {
                    id,
                    incarnation: "i".into(),
                    address: "http://127.0.0.1:1".into(),
                    expires_at: 0,
                    max_lease_ms: 3000,
                    applied: 0,
                },
            );
        }
        let progress = Progress::default();
        for id in 0..1024 {
            progress.acknowledge(&metadata, 1, id, "i", 1).unwrap();
        }
        metadata.nodes.remove(&0);
        metadata.nodes.insert(
            2048,
            Node {
                id: 2048,
                incarnation: "new".into(),
                address: "http://127.0.0.1:1".into(),
                expires_at: 0,
                max_lease_ms: 3000,
                applied: 0,
            },
        );
        progress.acknowledge(&metadata, 1, 2048, "new", 1).unwrap();
        let cache = progress.cache.lock().unwrap();
        assert_eq!(cache.nodes.len(), 1024);
        assert!(!cache.nodes.contains_key(&0));
    }
}
