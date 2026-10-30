//! Bounded WebSocket upgrade and byte-stream adaptation for the MQTT network loop.
mod adapter;
mod options;
mod policy;
mod upgrade;
pub(crate) use adapter::is_framing_error;
pub use options::WebSocketOptions;
pub(crate) use upgrade::accept;
#[cfg(test)]
mod tests;
