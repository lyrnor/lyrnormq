use std::{error::Error, fmt};

/// Stable classification carried inside management `io::ErrorKind::WouldBlock`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManagementError {
    /// The finite management query capacity is currently occupied.
    Busy,
    /// The associated broker has not become ready or has stopped.
    Unavailable,
}
impl fmt::Display for ManagementError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Busy => "management query capacity exhausted",
            Self::Unavailable => "management unavailable",
        })
    }
}
impl Error for ManagementError {}
