use std::{error::Error, fmt};

/// Redacted journal errors never include paths, configuration or message content.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueueError {
    Io,
    Corrupt,
    Locked,
    InvalidOptions,
    Full,
    BindingConflict,
    StaleBinding,
}
impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Io => "outbound journal storage failed",
            Self::Corrupt => "outbound journal is corrupt or unsupported",
            Self::Locked => "outbound journal already has an owner",
            Self::InvalidOptions => "invalid outbound journal options",
            Self::Full => "outbound journal capacity exhausted",
            Self::BindingConflict => "pending outbound destination cannot be changed or removed",
            Self::StaleBinding => "stale outbound destination revision",
        })
    }
}
impl Error for QueueError {}
impl From<std::io::Error> for QueueError {
    fn from(_: std::io::Error) -> Self {
        Self::Io
    }
}
