//! Transport-independent bounded cluster integration contracts.
mod api;
mod model;
pub use api::{ClusterBridge, ClusterFuture, LocalBroker};
pub use model::{ClientSubscriptions, ClusterMessage, ForwardScope};
