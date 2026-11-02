use serde_json::{Value, json};
use std::time::{Duration, Instant};

pub struct Samples {
    pub name: &'static str,
    pub units: &'static str,
    pub values: Vec<u64>,
    pub items: u64,
    pub bytes: u64,
    pub started: Instant,
    pub wall: Option<Duration>,
}
impl Samples {
    pub fn new(name: &'static str, units: &'static str) -> Self {
        Self {
            name,
            units,
            values: Vec::new(),
            items: 0,
            bytes: 0,
            started: Instant::now(),
            wall: None,
        }
    }
    pub fn add(&mut self, elapsed: Duration, items: usize, bytes: usize) {
        self.values
            .push(elapsed.as_nanos().min(u128::from(u64::MAX)) as u64);
        self.items += items as u64;
        self.bytes += bytes as u64;
    }
    pub fn stop(&mut self) {
        self.wall = Some(self.started.elapsed());
    }
    pub fn json(mut self) -> Value {
        let wall = self
            .wall
            .unwrap_or_else(|| self.started.elapsed())
            .as_secs_f64();
        let engine_ns = self.values.iter().map(|v| u128::from(*v)).sum::<u128>();
        self.values.sort_unstable();
        let percentile = |p| nearest_rank(&self.values, p);
        json!({"stage":self.name,"item_unit":self.units,"calls":self.values.len(),"items":self.items,"payload_bytes":self.bytes,"wall_seconds":wall,"engine_seconds":engine_ns as f64/1e9,"wall_items_per_second":self.items as f64/wall,"engine_items_per_second":if engine_ns>0{self.items as f64/(engine_ns as f64/1e9)}else{0.0},"wall_payload_mib_per_second":self.bytes as f64/(1024.0*1024.0)/wall,"mean_engine_ns_per_item":if self.items>0{engine_ns as f64/self.items as f64}else{0.0},"api_call_latency_ns":{"p50":percentile(50),"p95":percentile(95),"p99":percentile(99),"max":self.values.last().copied().unwrap_or(0)},"percentile_method":"nearest-rank, ceil(p*N/100), measured per API call; not per-record percentiles"})
    }
}
fn nearest_rank(values: &[u64], p: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let index = values
        .len()
        .saturating_mul(p)
        .div_ceil(100)
        .saturating_sub(1);
    values[index.min(values.len() - 1)]
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rank_is_not_batch_amortized() {
        let values: Vec<_> = (1..=100).collect();
        assert_eq!(nearest_rank(&values, 50), 50);
        assert_eq!(nearest_rank(&values, 95), 95);
        assert_eq!(nearest_rank(&values, 99), 99);
        assert_eq!(nearest_rank(&[7], 99), 7);
    }
}
