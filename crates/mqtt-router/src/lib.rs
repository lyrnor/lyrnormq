//! MQTT 3.1.1 subscription routing with an iterative topic-level trie.
//!
//! Callers enforce connection and subscription quotas. Operations are synchronous;
//! an external lock must not be held across asynchronous work. Shared group ownership and selection
//! belong to the session model. Retained messages use the bounded store in `retained`.

mod filter;
pub mod retained;
mod trie;

pub use filter::{
    matches, matches_topic_filter, split_shared_filter, validate_filter,
    validate_subscription_filter, validate_topic_filter,
};
pub use trie::Router;
