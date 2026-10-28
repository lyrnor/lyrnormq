//! Wall-clock deadlines are persisted only while a session is offline.
use super::{EngineError, SessionEngine};
use std::{
    fmt::Debug,
    time::{SystemTime, UNIX_EPOCH},
};
/// Clock injection keeps expiry state-machine tests independent of sleeps.
pub trait Clock: Debug + Send + Sync {
    fn now_millis(&self) -> u64;
}
#[derive(Debug)]
pub struct SystemClock;
impl Clock for SystemClock {
    fn now_millis(&self) -> u64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(value) => u64::try_from(value.as_millis()).unwrap_or(u64::MAX),
            Err(_) => 0,
        }
    }
}
impl SessionEngine {
    /// Earliest known offline deadline; active sessions never have one.
    pub fn next_expiry_deadline(&self) -> Option<u64> {
        self.sessions
            .values()
            .filter(|s| !s.online)
            .filter_map(|s| s.expires_at)
            .chain(self.next_will_deadline())
            .min()
    }
    /// Retained values and pending Will intents are independent of session expiry.
    pub fn expire_sessions(&mut self) -> Result<bool, EngineError> {
        let now = self.clock.now_millis();
        let before = self.sessions.len();
        self.sessions.retain(|_, session| {
            session.online || !super::temporal::is_expired(session.expires_at, now)
        });
        let changed = before != self.sessions.len();
        if changed {
            self.ensure_bounds()?;
        }
        Ok(changed)
    }
}
