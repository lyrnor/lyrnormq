use crate::LoadError;
use std::time::{Duration, Instant};

pub(super) fn nanos(epoch: Instant) -> Result<u64, LoadError> {
    u64::try_from(epoch.elapsed().as_nanos())
        .map_err(|_| LoadError::Capacity("monotonic clock offset"))
}
pub(super) fn deadline(epoch: Instant, ns: u64) -> Result<Instant, LoadError> {
    epoch
        .checked_add(Duration::from_nanos(ns))
        .ok_or(LoadError::Capacity("monotonic deadline"))
}
pub(super) fn rate_deadline(
    start: u64,
    local: u64,
    pair: u32,
    pairs: usize,
    rate: u64,
) -> Result<u64, LoadError> {
    let index = u128::from(local)
        .checked_mul(pairs as u128)
        .and_then(|x| x.checked_add(u128::from(pair)))
        .ok_or(LoadError::Capacity("rate index"))?;
    let offset = index
        .checked_mul(1_000_000_000)
        .ok_or(LoadError::Capacity("rate offset"))?
        / u128::from(rate);
    u64::try_from(u128::from(start) + offset).map_err(|_| LoadError::Capacity("rate deadline"))
}
pub(super) fn rate_count(duration_ms: u64, pair: u32, pairs: usize, rate: u64) -> u64 {
    let total = (u128::from(duration_ms) * u128::from(rate)).div_ceil(1000);
    let p = u128::from(pair);
    if total <= p {
        0
    } else {
        (((total - 1 - p) / (pairs as u128) + 1).min(u128::from(u64::MAX))) as u64
    }
}
