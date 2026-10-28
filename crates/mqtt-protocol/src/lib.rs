//! Incremental MQTT 3.1.1 codec for QoS 0/1/2 messaging and session flags.
//! Includes retained PUBLISH flags, binary CONNECT Wills and credentials; MQTT 5 remains unsupported.
//! Session persistence policy belongs to the runtime, not the packet codec.

mod codec;
mod error;
mod packet;
mod password;

pub use codec::{decode, encode};
pub use error::ProtocolError;
pub use packet::{Packet, Will};
pub use password::Password;

#[cfg(test)]
mod tests;
