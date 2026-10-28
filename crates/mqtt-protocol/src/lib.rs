//! Incremental MQTT 3.1.1 codec for QoS 0/1/2 messaging and session flags.
//! Includes versioned MQTT5 frames/properties and legacy MQTT3.1.1 packets.
//! Session persistence policy belongs to the runtime, not the packet codec.

mod codec;
mod error;
mod frame;
mod packet;
mod password;
mod properties;
mod reason;
mod v5;
mod wire;

pub use codec::{decode, encode};
pub use error::ProtocolError;
pub use frame::{Frame, ProtocolVersion};
pub use packet::{Packet, Will};
pub use password::Password;
pub use properties::{Properties, Property};
pub use reason::ReasonCode;
pub use v5::{decode_frame, encode_frame, peek_connect_version};

#[cfg(test)]
mod tests;
