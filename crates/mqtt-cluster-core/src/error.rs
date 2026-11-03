//! Cross-layer error markers, separate from durable and wire schemas.

/// A session Acquire was rejected because its inspected epoch is stale.
///
/// This is a confirmed state-machine rejection without an ownership change.
/// Callers may inspect the latest state and retry within their existing deadline.
/// Admission failures and ambiguous RPC outcomes must never use this marker.
#[derive(Debug)]
pub struct AcquisitionConflict;

impl std::fmt::Display for AcquisitionConflict {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("session acquisition epoch changed")
    }
}

impl std::error::Error for AcquisitionConflict {}
