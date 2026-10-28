//! Incremental MQTT 3.1.1 codec for QoS 0/1/2 messaging and session flags.
//! MQTT 5, retained publications, Will and credentials remain unsupported.
//! Session persistence policy belongs to the runtime, not the packet codec.

mod codec;
mod error;
mod packet;

pub use codec::{decode, encode};
pub use error::ProtocolError;
pub use packet::Packet;

#[cfg(test)]
mod tests;
