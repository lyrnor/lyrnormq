//! Bounded typed replica storage. Memory is volatile; RocksDB replays the same records.
mod memory;
mod model;
mod rocks;
mod store;

pub(crate) use memory::MAX_RECORD;
pub(crate) use model::{
    Intent, Recipient, Record, RepairEntry, RepairManifest, RepairPage, RepairStoreStatus,
    RepairToken, Reply, Request,
};
pub(crate) use store::{Journal, StoreKind};

// Shared only by tests that fork/exec while opening controller/directory locks.
#[cfg(test)]
pub(crate) static TEST_PROCESS_START_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
