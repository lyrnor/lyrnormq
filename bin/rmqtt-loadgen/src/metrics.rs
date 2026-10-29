//! Exact sample accounting. All durations are integer nanoseconds from one epoch.
mod distribution;
mod report;
mod summarize;

pub use distribution::{Distribution, distribution};
pub use report::{Accounting, EventCounts, Latencies, Report, Throughput, Timing};
pub use summarize::summarize;

#[cfg(test)]
mod tests;
