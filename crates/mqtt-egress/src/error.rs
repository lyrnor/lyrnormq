use std::{error::Error, fmt};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeError {
    InvalidConfig,
    InvalidSql,
    Busy,
    Storage,
    Stopped,
}
impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidConfig => "invalid rule or outbound configuration",
            Self::InvalidSql => "invalid or unsupported rule SQL",
            Self::Busy => "rule configuration update is busy",
            Self::Storage => "rule configuration storage failed",
            Self::Stopped => "rule runtime has stopped",
        })
    }
}
impl Error for RuntimeError {}
