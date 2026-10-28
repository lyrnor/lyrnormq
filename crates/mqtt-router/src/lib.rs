//! MQTT 3.1.1 subscription routing with an iterative topic-level trie.
//!
//! Callers enforce connection and subscription quotas. Operations are synchronous;
//! an external lock must not be held across asynchronous work. Shared subscriptions
//! remain unsupported. Retained messages use the bounded store in `retained`.

mod filter;
pub mod retained;
mod trie;

pub use filter::{matches, validate_filter};
pub use trie::Router;
