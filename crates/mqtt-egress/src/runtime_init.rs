//! Config ownership stays cancellation-safe while asynchronous startup is pending.
use mqtt_storage::configuration::ConfigStore;
use std::{
    ops::Deref,
    sync::{Mutex, PoisonError},
};
use tokio::runtime::Handle;

pub(super) struct ConfigOwner {
    store: Mutex<Option<ConfigStore>>,
    cleanup: Handle,
}
impl ConfigOwner {
    pub fn new(store: ConfigStore, cleanup: Handle) -> Self {
        Self {
            store: Mutex::new(Some(store)),
            cleanup,
        }
    }
    pub async fn close(&self) {
        let store = self
            .store
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(store) = store {
            let _ = self.cleanup.spawn_blocking(move || drop(store)).await;
        }
    }
}
impl Deref for ConfigOwner {
    type Target = Mutex<Option<ConfigStore>>;
    fn deref(&self) -> &Self::Target {
        &self.store
    }
}
impl Drop for ConfigOwner {
    fn drop(&mut self) {
        let store = self
            .store
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(store) = store {
            self.cleanup.spawn_blocking(move || drop(store));
        }
    }
}
