//! Opt-in loopback management HTTP service; independent from MQTT authentication.
mod assets;
mod http;
mod settings;
#[cfg(test)]
mod tests;

pub use http::serve;
pub use settings::prepare;
