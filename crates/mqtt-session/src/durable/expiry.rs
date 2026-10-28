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
    /// Observe wall time without allowing a committed proposal to rewind it.
    /// The floor belongs to this model clone and is persisted in its snapshot.
    /// Observation alone is not protocol progress: empty polls and timer retries
    /// stay quiet. Commit the observed floor with real mutations or transmissions.
    pub fn effective_now(&self) -> u64 {
        self.clock_floor.observe(self.clock.now_millis())
    }

    /// Earliest actionable deadline, excluding already-started protocol state.
    pub fn next_expiry_deadline(&self) -> Option<u64> {
        self.sessions
            .values()
            .filter(|s| !s.online)
            .filter_map(|s| s.expires_at)
            .chain(
                self.sessions
                    .values()
                    .flat_map(|s| s.queued.iter().filter_map(|m| m.expires_at)),
            )
            .chain(self.retained_expiry.values().copied())
            .chain(self.sessions.values().flat_map(|s| {
                s.outgoing_expiry
                    .iter()
                    .filter(|(id, _)| !s.outgoing_started.contains(id))
                    .map(|(_, deadline)| *deadline)
            }))
            .chain(self.next_will_deadline())
            .min()
    }
    pub fn expire_messages(&mut self) -> Result<bool, EngineError> {
        let now = self.effective_now();
        let mut changed = false;
        for session in self.sessions.values_mut() {
            let before = session.queued.len();
            session
                .queued
                .retain(|m| !super::temporal::is_expired(m.expires_at, now));
            changed |= before != session.queued.len();
            let expired: Vec<_> = session
                .outgoing_expiry
                .iter()
                .filter(|(id, deadline)| {
                    **deadline <= now && !session.outgoing_started.contains(id)
                })
                .map(|(id, _)| *id)
                .collect();
            for id in expired {
                session.retire_outgoing(id);
                changed = true;
            }
        }
        let expired: Vec<_> = self
            .retained_expiry
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(topic, _)| topic.clone())
            .collect();
        for topic in expired {
            self.retained
                .apply(topic.clone(), Vec::new(), 0)
                .map_err(EngineError::Retained)?;
            self.retained_expiry.remove(&topic);
            self.retained_properties.remove(&topic);
            changed = true;
        }
        if changed {
            self.ensure_bounds()?;
        }
        Ok(changed)
    }
    pub fn expire_sessions(&mut self) -> Result<bool, EngineError> {
        let now = self.effective_now();
        let before = self.sessions.len();
        self.sessions.retain(|_, session| {
            session.online || !super::temporal::is_expired(session.expires_at, now)
        });
        let mut changed = before != self.sessions.len();
        changed |= self.expire_messages()?;
        if changed {
            self.ensure_bounds()?;
        }
        Ok(changed)
    }
}
