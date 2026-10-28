//! Errors shared by synchronous storage implementations.

use std::{error::Error, fmt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageError {
    InvalidLimits,
    InvalidKey,
    KeyTooLarge,
    ValueTooLarge,
    CapacityExceeded,
}
impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidLimits => "storage limits must be positive",
            Self::InvalidKey => "empty key or zero packet identifier",
            Self::KeyTooLarge => "storage key exceeds limit",
            Self::ValueTooLarge => "storage value exceeds limit",
            Self::CapacityExceeded => "storage capacity exceeded",
        })
    }
}
impl Error for StorageError {}
