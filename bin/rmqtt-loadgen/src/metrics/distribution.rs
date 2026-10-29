use crate::LoadError;
use serde::Serialize;

/// Exact nearest-rank order statistics. Empty input has null values, never zero latency.
#[derive(Debug, Clone, Serialize)]
pub struct Distribution {
    pub count: u64,
    pub min_ns: Option<u64>,
    pub p50_ns: Option<u64>,
    pub p95_ns: Option<u64>,
    pub p99_ns: Option<u64>,
    pub max_ns: Option<u64>,
    pub mean_ns: Option<f64>,
}

/// Sort integer observations after the load; no bin interpolation or sample dropping.
pub fn distribution(mut observations: Vec<u64>) -> Result<Distribution, LoadError> {
    let count = u64::try_from(observations.len())
        .map_err(|_| LoadError::Capacity("latency count exceeds u64"))?;
    if observations.is_empty() {
        return Ok(Distribution {
            count,
            min_ns: None,
            p50_ns: None,
            p95_ns: None,
            p99_ns: None,
            max_ns: None,
            mean_ns: None,
        });
    }
    let sum = observations.iter().try_fold(0_u128, |sum, &value| {
        sum.checked_add(u128::from(value))
            .ok_or(LoadError::Capacity("latency sum overflow"))
    })?;
    observations.sort_unstable();
    let at = |percentage: u128| -> Result<u64, LoadError> {
        let rank = (u128::from(count) * percentage).div_ceil(100);
        let index = usize::try_from(rank - 1)
            .map_err(|_| LoadError::Capacity("quantile rank exceeds usize"))?;
        observations
            .get(index)
            .copied()
            .ok_or_else(|| LoadError::InvalidData("quantile rank outside samples".into()))
    };
    Ok(Distribution {
        count,
        min_ns: observations.first().copied(),
        p50_ns: Some(at(50)?),
        p95_ns: Some(at(95)?),
        p99_ns: Some(at(99)?),
        max_ns: observations.last().copied(),
        mean_ns: Some(sum as f64 / count as f64),
    })
}
