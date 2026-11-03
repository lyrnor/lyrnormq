//! Broker-wide bounded authentication admission; never queues waiters.
use crate::BrokerConfig;
use std::{
    io,
    sync::{
        Arc, Mutex, OnceLock, PoisonError,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
const TOKEN: u128 = 1_000_000_000;
static PROFILE_ENABLED: OnceLock<bool> = OnceLock::new();

fn profile_enabled() -> bool {
    *PROFILE_ENABLED
        .get_or_init(|| std::env::var_os("RMQTT_RAFT_PROFILE").is_some_and(|value| value == "1"))
}

#[derive(Default)]
struct HandshakeProfile {
    acquired: AtomicU64,
    rejected: AtomicU64,
    observed_peak: AtomicUsize,
}

impl HandshakeProfile {
    fn record(&self, acquired: bool, active: usize) {
        if acquired {
            self.acquired.fetch_add(1, Ordering::Relaxed);
        } else {
            self.rejected.fetch_add(1, Ordering::Relaxed);
        }
        self.observed_peak.fetch_max(active, Ordering::Relaxed);
    }
}

pub(crate) struct ProfileExit<'a>(&'a Admission);

impl Drop for ProfileExit<'_> {
    fn drop(&mut self) {
        self.0.emit_profile(true);
    }
}

struct Bucket {
    credit: u128,
    last: Instant,
    rate: u32,
    burst: u32,
}
impl Bucket {
    fn new(rate: u32, burst: u32, now: Instant) -> Self {
        Self {
            credit: u128::from(burst) * TOKEN,
            last: now,
            rate,
            burst,
        }
    }
    fn charge(&mut self, now: Instant) -> bool {
        if self.rate == 0 {
            return true;
        }
        let refill = now
            .saturating_duration_since(self.last)
            .as_nanos()
            .saturating_mul(u128::from(self.rate));
        self.last = self.last.max(now);
        self.credit = self
            .credit
            .saturating_add(refill)
            .min(u128::from(self.burst) * TOKEN);
        if self.credit < TOKEN {
            return false;
        }
        self.credit -= TOKEN;
        true
    }
}
pub(crate) struct Admission {
    handshakes: Arc<Semaphore>,
    handshake_limit: usize,
    profile: Option<HandshakeProfile>,
    auth: Arc<Semaphore>,
    bucket: Mutex<Bucket>,
}
impl Admission {
    pub(crate) fn validate(config: &BrokerConfig) -> io::Result<()> {
        if config.max_handshakes == 0
            || config.max_handshakes > Semaphore::MAX_PERMITS
            || config.max_auth_tasks == 0
            || config.max_auth_tasks > Semaphore::MAX_PERMITS
            || config.auth_burst == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid authentication admission limits",
            ));
        }
        Ok(())
    }
    pub(crate) fn new(config: &BrokerConfig) -> io::Result<Self> {
        Self::validate(config)?;
        Ok(Self {
            handshakes: Arc::new(Semaphore::new(config.max_handshakes)),
            handshake_limit: config.max_handshakes,
            profile: profile_enabled().then(HandshakeProfile::default),
            auth: Arc::new(Semaphore::new(config.max_auth_tasks)),
            bucket: Mutex::new(Bucket::new(
                config.auth_rate_per_second,
                config.auth_burst,
                Instant::now(),
            )),
        })
    }
    pub(crate) fn handshake(&self) -> Option<OwnedSemaphorePermit> {
        let permit = self.handshakes.clone().try_acquire_owned().ok();
        if let Some(profile) = &self.profile {
            profile.record(permit.is_some(), self.active_handshakes());
        }
        permit
    }
    fn active_handshakes(&self) -> usize {
        self.handshake_limit
            .saturating_sub(self.handshakes.available_permits())
    }
    pub(crate) fn profile_on_exit(&self) -> ProfileExit<'_> {
        ProfileExit(self)
    }
    pub(crate) fn emit_profile(&self, final_summary: bool) {
        if let Some(profile) = &self.profile {
            let active = self.active_handshakes();
            profile.observed_peak.fetch_max(active, Ordering::Relaxed);
            // Counts are cumulative acquisition outcomes, not CONNECT successes.
            // Concurrent permit drops can make the observed peak underestimate
            // the instantaneous peak; the semaphore still owns admission state.
            tracing::warn!(
                event = "handshake_profile",
                acquired = profile.acquired.load(Ordering::Relaxed),
                rejected = profile.rejected.load(Ordering::Relaxed),
                active,
                observed_peak = profile.observed_peak.load(Ordering::Relaxed),
                limit = self.handshake_limit,
                final_summary,
                "Handshake admission cumulative counters"
            );
        }
    }
    pub(crate) fn authentication(&self) -> Option<OwnedSemaphorePermit> {
        if !self.charge_authentication() {
            return None;
        }
        self.authentication_task()
    }
    pub(crate) fn charge_authentication(&self) -> bool {
        self.bucket
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .charge(Instant::now())
    }
    pub(crate) fn authentication_task(&self) -> Option<OwnedSemaphorePermit> {
        self.auth.clone().try_acquire_owned().ok()
    }
}
#[cfg(test)]
mod tests;

#[cfg(test)]
mod profile_tests {
    use super::*;

    #[test]
    fn admission_profile_counts_acquisition_and_rejection_without_changing_release() {
        let mut admission = Admission::new(&BrokerConfig {
            max_handshakes: 2,
            ..BrokerConfig::default()
        })
        .unwrap();
        admission.profile = Some(HandshakeProfile::default());
        let first = admission.handshake().unwrap();
        let second = admission.handshake().unwrap();
        assert!(admission.handshake().is_none());
        assert_eq!(admission.active_handshakes(), 2);
        drop(first);
        assert_eq!(admission.active_handshakes(), 1);
        let third = admission.handshake().unwrap();
        drop(second);
        drop(third);
        assert_eq!(admission.active_handshakes(), 0);
        let profile = admission.profile.as_ref().unwrap();
        assert_eq!(profile.acquired.load(Ordering::Relaxed), 3);
        assert_eq!(profile.rejected.load(Ordering::Relaxed), 1);
        assert_eq!(profile.observed_peak.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn disabled_profile_preserves_the_same_permit_boundary() {
        let mut admission = Admission::new(&BrokerConfig {
            max_handshakes: 1,
            ..BrokerConfig::default()
        })
        .unwrap();
        admission.profile = None;
        let permit = admission.handshake().unwrap();
        assert!(admission.handshake().is_none());
        admission.emit_profile(false);
        drop(admission.profile_on_exit());
        assert_eq!(admission.active_handshakes(), 1);
        drop(permit);
        assert!(admission.handshake().is_some());
        assert!(admission.profile.is_none());
    }
}
