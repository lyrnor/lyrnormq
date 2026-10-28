//! Bounded MQTT 3.1.1 QoS primitives and the Phase 2b durable session model.
//! Incoming QoS2 delivers on PUBREL; outgoing QoS1/QoS2 releases identifiers only
//! after PUBACK/PUBCOMP. The transport drives packet IO and acknowledgement order.
//! The durable module wraps these primitives with versioned session records and
//! reconnect replay. Its proposed transitions require a successful external
//! transaction commit before network acknowledgements. The SQLite-enabled transport
//! worker supplies that commit and recovery integration; this crate performs no IO.
//! MQTT5 expiry, Will, application-side exactly-once processing and physical
//! power-loss validation are outside these guarantees.

pub mod durable;
mod error;
mod identifiers;
mod incoming;
mod outgoing;

pub use error::SessionError;
pub use identifiers::PacketIdentifiers;
pub use incoming::{IncomingQos2, PublishResult, ReleaseResult};
pub use outgoing::{AckAction, AckKind, OutgoingQos};
