//! MQTT 3.1.1 subscription routing with an iterative topic-level trie.
//!
//! Callers enforce connection and subscription quotas. Operations are synchronous;
//! an external lock must not be held across asynchronous work. Shared group
//! membership and round-robin cursors live in [`SubscriptionIndex`]; per-session
//! eligibility belongs to the session model. Retained messages use the bounded
//! store in `retained`, or [`ConcurrentRetained`] when shared across tasks.

mod concurrent;
mod filter;
mod index;
pub mod retained;
mod trie;

pub use concurrent::ConcurrentRetained;
pub use filter::{
    matches, matches_topic_filter, split_shared_filter, validate_filter,
    validate_subscription_filter, validate_topic_filter,
};
pub use index::{IndexMatches, SharedMatch, SubscriptionIndex};
pub use trie::Router;
