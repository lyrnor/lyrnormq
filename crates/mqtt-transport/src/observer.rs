//! Optional bounded side-channel observation of accepted publications.
use std::fmt::Debug;
/// Implementations must perform only bounded nonblocking admission. No IO,
/// await, SQL, JSON decoding or blocking locks belong in this MQTT routing hook.
/// QoS1 input retries can generate duplicate observations. Wills use their
/// existing publication publisher identity (empty when none is available).
pub trait PublicationObserver: Send + Sync + Debug {
    fn observe(&self, topic: &str, payload: &[u8], client_id: &str, qos: u8, retain: bool);
}
