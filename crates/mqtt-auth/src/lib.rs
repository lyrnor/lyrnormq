//! Bounded local authentication and topic access-control extension interfaces.
//!
//! Policies run synchronously on the broker's blocking worker. Static policy is
//! immutable for one run; remote authentication and policy hot reload are absent.

mod api;
mod containment;
mod error;
mod policy;

pub use api::{AccessControl, Action, AllowAnonymous, AuthError, ConnectInfo, Principal};
pub use containment::covers_filter;
pub use error::PolicyError;
pub use policy::{MAX_POLICY_BYTES, StaticPolicy};
