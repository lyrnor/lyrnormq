//! Fixed-cardinality diagnostics. Never retain or emit message or identity data.
use std::{
    fmt, io,
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub(crate) enum Failure {
    JournalFrozen,
    JournalGeneration,
    JournalEpoch,
    JournalOlderVersion,
    JournalConflictingValue,
    CommitNoRecord,
    CommitPoisoned,
    CommitEpoch,
    CommitAuthority,
    CommitRevision,
    StoreAuthority,
    QuorumNotReady,
    QuorumPeer,
    QuorumJoin,
    QuorumInsufficient,
    QuorumConflict,
    BackgroundReacquire,
    ActorClosed,
    TakeAuthority,
    StoreAfterWriteAuthority,
    BackgroundWorkReply,
}
const COUNT: usize = Failure::BackgroundWorkReply as usize + 1;
static COUNTERS: [AtomicU64; COUNT] = [const { AtomicU64::new(0) }; COUNT];

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Failure {}

pub(crate) fn observe(failure: Failure) {
    let count = COUNTERS[failure as usize]
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1);
    if should_log(count) {
        tracing::warn!(event = "cluster_data_rejection", category = %failure, count, "cluster data-path diagnostic");
    }
}
fn should_log(count: u64) -> bool {
    count.is_power_of_two()
}
pub(crate) fn reject(failure: Failure) -> io::Error {
    observe(failure);
    io::Error::new(io::ErrorKind::WouldBlock, failure)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_categories_preserve_retry_kind_without_sensitive_context() {
        let error = reject(Failure::CommitRevision);
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(error.to_string(), "CommitRevision");
        assert!(error.get_ref().is_some_and(|e| e.is::<Failure>()));
    }
    #[test]
    fn logarithmic_sampling_has_finite_volume_per_fixed_category() {
        assert_eq!((1..=1024).filter(|n| should_log(*n)).count(), 11);
        assert!(!should_log(0));
    }
}
