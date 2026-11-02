//! Opt-in loopback management HTTP service; independent from MQTT authentication.
mod access_control;
mod asset_rules;
mod assets;
mod http;
mod rules;
mod settings;
#[cfg(test)]
mod tests;

pub use http::serve;
pub use settings::prepare;
