//! Group-committing writer thread for [`RowStore`].
//!
//! Submissions never block: a full queue is reported to the caller. Every job
//! waiting when the thread wakes is applied in one transaction, then each
//! completion runs on the storage thread. After the first failed commit every
//! later job fails immediately; the store must be reopened to recover.
use std::{
    fmt,
    sync::{
        Arc,
        mpsc::{Receiver, SyncSender, TrySendError, sync_channel},
    },
    thread::{self, JoinHandle},
};

use super::{DurableError, RowChange, RowStore};

/// Called exactly once with the outcome of the transaction containing the job.
pub type Completion = Box<dyn FnOnce(Result<(), Arc<DurableError>>) + Send>;

struct Job {
    changes: Vec<RowChange>,
    done: Completion,
}

#[derive(Debug)]
pub enum SubmitError {
    Full,
    Closed,
    TooLarge,
}
impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Full => "storage queue is full",
            Self::Closed => "storage service has stopped",
            Self::TooLarge => "storage row exceeds configured byte limit",
        })
    }
}
impl std::error::Error for SubmitError {}

pub struct StorageService {
    sender: Option<SyncSender<Job>>,
    thread: Option<JoinHandle<()>>,
    max_row_bytes: usize,
}

impl StorageService {
    /// `capacity` bounds queued jobs and `max_batch` bounds jobs per transaction.
    pub fn start(store: RowStore, capacity: usize, max_batch: usize) -> Result<Self, DurableError> {
        if capacity == 0 || max_batch == 0 {
            return Err(DurableError::InvalidLimit);
        }
        if store.is_legacy() {
            return Err(DurableError::Schema(
                "container1 must be migrated first".into(),
            ));
        }
        let max_row_bytes = store.limits.max_row_bytes;
        let (sender, receiver) = sync_channel(capacity);
        let thread = thread::Builder::new()
            .name("rmqtt-storage".into())
            .spawn(move || run(store, receiver, max_batch))?;
        Ok(Self {
            sender: Some(sender),
            thread: Some(thread),
            max_row_bytes,
        })
    }

    pub fn try_submit(&self, changes: Vec<RowChange>, done: Completion) -> Result<(), SubmitError> {
        if changes
            .iter()
            .any(|change| change.bytes() > self.max_row_bytes)
        {
            return Err(SubmitError::TooLarge);
        }
        let sender = self.sender.as_ref().ok_or(SubmitError::Closed)?;
        sender
            .try_send(Job { changes, done })
            .map_err(|error| match error {
                TrySendError::Full(_) => SubmitError::Full,
                TrySendError::Disconnected(_) => SubmitError::Closed,
            })
    }

    /// Stop accepting jobs, finish queued ones and join the thread. Blocking.
    pub fn shutdown(mut self) {
        self.sender = None;
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for StorageService {
    /// Dropping detaches the thread; it drains queued jobs and exits.
    fn drop(&mut self) {
        self.sender = None;
    }
}

fn run(mut store: RowStore, receiver: Receiver<Job>, max_batch: usize) {
    let mut failure: Option<Arc<DurableError>> = None;
    while let Ok(first) = receiver.recv() {
        let mut jobs = vec![first];
        while jobs.len() < max_batch {
            match receiver.try_recv() {
                Ok(job) => jobs.push(job),
                Err(_) => break,
            }
        }
        let outcome = match &failure {
            Some(error) => Err(error.clone()),
            None => {
                let changes: Vec<RowChange> = jobs
                    .iter_mut()
                    .flat_map(|job| std::mem::take(&mut job.changes))
                    .collect();
                store.apply(&changes).map_err(Arc::new)
            }
        };
        if let Err(error) = &outcome {
            failure.get_or_insert_with(|| error.clone());
        }
        for job in jobs {
            (job.done)(outcome.clone());
        }
    }
}

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
