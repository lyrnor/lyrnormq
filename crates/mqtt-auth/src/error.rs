//! Configuration failures omit policy values, hashes and credential bytes.
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyError {
    TooLarge,
    InvalidJson,
    TooManyUsers,
    InvalidUser,
    DuplicateUser,
    InvalidRules,
    InvalidHash,
    HashParameters,
}
impl fmt::Display for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::TooLarge => "authentication policy exceeds byte limit",
            Self::InvalidJson => "invalid authentication policy JSON schema",
            Self::TooManyUsers => "authentication policy exceeds user limit",
            Self::InvalidUser => "invalid authentication policy user",
            Self::DuplicateUser => "duplicate authentication policy user",
            Self::InvalidRules => "invalid authentication policy rules",
            Self::InvalidHash => "invalid Argon2id PHC password hash",
            Self::HashParameters => "unsupported or excessive Argon2id parameters",
        })
    }
}
impl std::error::Error for PolicyError {}
