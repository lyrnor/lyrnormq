use std::{error::Error, fmt};
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuleError {
    InvalidSql,
    UnsupportedSql,
    LimitExceeded,
    InvalidTopicFilter,
    InvalidConfiguration,
    Arithmetic,
    OutputTooLarge,
}
impl fmt::Display for RuleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidSql => "invalid rule SQL",
            Self::UnsupportedSql => "unsupported rule SQL",
            Self::LimitExceeded => "rule complexity limit exceeded",
            Self::InvalidTopicFilter => "invalid rule topic filter",
            Self::InvalidConfiguration => "invalid rule configuration",
            Self::Arithmetic => "rule arithmetic error",
            Self::OutputTooLarge => "rule output exceeds limit",
        })
    }
}
impl Error for RuleError {}
