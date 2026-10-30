use std::{future::Future, pin::Pin};
#[derive(Clone, Copy, Debug)]
/// Adapter encoded batch bounds: fixed overhead and per-record separator bytes.
pub struct BatchLimits {
    pub max_bytes: usize,
    pub max_events: usize,
    pub overhead: usize,
    pub separator_bytes: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    Retryable,
    Permanent,
}
#[derive(Debug)]
pub struct SendError {
    pub kind: FailureKind,
    pub code: String,
}
pub trait Sink: Send + Sync {
    fn limits(&self) -> BatchLimits;
    fn send<'a>(
        &'a self,
        events: &'a [&'a [u8]],
    ) -> Pin<Box<dyn Future<Output = Result<(), SendError>> + Send + 'a>>;
}
