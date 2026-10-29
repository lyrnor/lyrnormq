use std::{error::Error, fmt};
#[derive(Debug)]
pub enum LoadError {
    Config(String),
    Io(std::io::Error),
    Protocol(mqtt_protocol::ProtocolError),
    Timeout(&'static str),
    InvalidData(String),
    Capacity(&'static str),
    Join(String),
}
impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(s) => write!(f, "configuration: {s}"),
            Self::Io(e) => write!(f, "I/O: {e}"),
            Self::Protocol(e) => write!(f, "MQTT: {e}"),
            Self::Timeout(s) => write!(f, "timeout: {s}"),
            Self::InvalidData(s) => write!(f, "invalid data: {s}"),
            Self::Capacity(s) => write!(f, "capacity: {s}"),
            Self::Join(s) => write!(f, "task: {s}"),
        }
    }
}
impl Error for LoadError {}
impl From<std::io::Error> for LoadError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<mqtt_protocol::ProtocolError> for LoadError {
    fn from(e: mqtt_protocol::ProtocolError) -> Self {
        Self::Protocol(e)
    }
}
