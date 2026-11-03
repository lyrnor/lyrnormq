//! Finite keyed dirty state. Notifications carry wakeups, never the work itself.
use super::*;
use tokio::time::Instant;
const LIMIT: usize = 100_000;
struct Entry {
    dirty: bool,
    due: Instant,
    failures: u32,
}
#[derive(Default)]
struct State {
    keys: BTreeMap<String, Entry>,
    timers: BTreeSet<(Instant, String)>,
    overflow: bool,
}
#[derive(Default)]
pub(super) struct Scheduler {
    state: Mutex<State>,
    pub(super) wake: Notify,
}
impl Scheduler {
    pub(super) fn schedule(&self, key: &str) {
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(entry) = s.keys.get_mut(key) {
            entry.dirty = true;
        } else if s.keys.len() < LIMIT {
            let due = Instant::now();
            s.keys.insert(
                key.to_owned(),
                Entry {
                    dirty: true,
                    due,
                    failures: 0,
                },
            );
            s.timers.insert((due, key.to_owned()));
        } else {
            // A finite reconciliation marker retains overflow until a slot frees.
            // No committed notification relies on a fallible transient channel.
            s.overflow = true;
        }
        drop(s);
        self.wake.notify_one();
    }
    pub(super) fn take(&self) -> Option<String> {
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (due, key) = s.timers.first()?.clone();
        if due > Instant::now() {
            return None;
        }
        s.timers.pop_first();
        let entry = s.keys.get_mut(&key)?;
        entry.dirty = false;
        Some(key)
    }
    pub(super) fn deadline(&self) -> Option<Instant> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .timers
            .first()
            .map(|(at, _)| *at)
    }
    pub(super) fn finish(&self, key: String, success: bool) -> bool {
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(mut entry) = s.keys.remove(&key) else {
            return false;
        };
        if !success || entry.dirty {
            entry.failures = if success {
                0
            } else {
                entry.failures.saturating_add(1)
            };
            let delay = if success {
                Duration::ZERO
            } else {
                Duration::from_millis(
                    100u64.saturating_mul(1u64 << entry.failures.min(4).saturating_sub(1)),
                )
            };
            entry.due = Instant::now() + delay;
            s.timers.insert((entry.due, key.clone()));
            s.keys.insert(key, entry);
            false
        } else {
            std::mem::take(&mut s.overflow)
        }
    }
    pub(super) fn wake(&self) {
        self.wake.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn coalesces_without_losing_event_during_worker() {
        let s = Scheduler::default();
        for _ in 0..100 {
            s.schedule("a");
        }
        assert_eq!(s.take().as_deref(), Some("a"));
        assert!(s.take().is_none());
        s.schedule("a");
        s.finish("a".into(), true);
        assert_eq!(s.take().as_deref(), Some("a"));
        s.finish("a".into(), true);
        assert!(s.deadline().is_none());
        assert!(s.state.lock().unwrap().keys.is_empty());
    }
    #[tokio::test]
    async fn new_events_do_not_bypass_retry_backoff() {
        let s = Scheduler::default();
        s.schedule("a");
        s.take();
        s.finish("a".into(), false);
        let due = s.deadline().unwrap();
        s.schedule("a");
        s.schedule("b");
        assert_eq!(s.take().as_deref(), Some("b"));
        assert!(s.take().is_none());
        assert_eq!(s.deadline(), Some(due));
        tokio::time::sleep_until(due).await;
        assert_eq!(s.take().as_deref(), Some("a"));
    }
    #[tokio::test]
    async fn overflow_is_retained_until_idle_slot_is_freed() {
        let s = Scheduler::default();
        for n in 0..LIMIT {
            s.schedule(&n.to_string());
        }
        s.schedule("overflow");
        assert_eq!(s.state.lock().unwrap().keys.len(), LIMIT);
        let key = s.take().unwrap();
        assert!(s.finish(key, true));
        s.schedule("overflow");
        assert!(s.state.lock().unwrap().keys.contains_key("overflow"));
    }
}
