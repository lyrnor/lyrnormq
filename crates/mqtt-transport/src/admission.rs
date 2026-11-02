//! Broker-wide bounded authentication admission; never queues waiters.
use crate::BrokerConfig;
use std::{
    io,
    sync::{Arc, Mutex, PoisonError},
    time::Instant,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
const TOKEN: u128 = 1_000_000_000;
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
            auth: Arc::new(Semaphore::new(config.max_auth_tasks)),
            bucket: Mutex::new(Bucket::new(
                config.auth_rate_per_second,
                config.auth_burst,
                Instant::now(),
            )),
        })
    }
    pub(crate) fn handshake(&self) -> Option<OwnedSemaphorePermit> {
        self.handshakes.clone().try_acquire_owned().ok()
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
