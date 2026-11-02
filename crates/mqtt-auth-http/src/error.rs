use std::fmt;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidConfig,
    Busy,
    Storage,
    Stopped,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidConfig => "invalid access configuration",
            Self::Busy => "access service capacity exhausted",
            Self::Storage => "access configuration storage failed",
            Self::Stopped => "access service stopped",
        })
    }
}
impl std::error::Error for Error {}
