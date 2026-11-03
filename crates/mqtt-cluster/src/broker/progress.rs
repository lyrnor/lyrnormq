//! One coalescing sender for installed-version acknowledgements; no consensus writes.
use super::*;
impl BrokerRuntime {
    pub(super) fn announce_applied(&self) {
        if !self.initialized.load(Ordering::Acquire) {
            return;
        }
        let metadata = self.metadata.read().unwrap_or_else(PoisonError::into_inner);
        if metadata
            .nodes
            .get(&self.config.node_id)
            .is_some_and(|n| n.incarnation == self.incarnation)
        {
            self.installed.send_replace(metadata.revision);
        }
    }
    pub(super) async fn applied_ack_loop(self: Arc<Self>) {
        let mut changes = self.installed.subscribe();
        let mut failures = 0u32;
        loop {
            if self.stopped.load(Ordering::Acquire) {
                return;
            }
            let revision = *changes.borrow_and_update();
            let registered = self
                .metadata
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .nodes
                .get(&self.config.node_id)
                .is_some_and(|n| n.incarnation == self.incarnation);
            let result = if registered {
                match self.rpc().await {
                    Ok(rpc) => rpc
                        .acknowledge(self.config.node_id, self.incarnation.clone(), revision)
                        .await
                        .map(|_| ()),
                    Err(error) => Err(error),
                }
            } else {
                Ok(())
            };
            if result.is_err() {
                failures = failures.saturating_add(1);
                tokio::time::sleep(Duration::from_millis(
                    100u64 << failures.min(4).saturating_sub(1),
                ))
                .await;
                continue;
            }
            failures = 0;
            if changes.changed().await.is_err() {
                return;
            }
        }
    }
}
