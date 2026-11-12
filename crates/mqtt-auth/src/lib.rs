//! Bounded local authentication and topic access-control extension interfaces.
//!
//! Policies run synchronously on the broker's blocking worker. Static policy is
//! immutable for one run; remote authentication and policy hot reload are absent.

mod api;
mod containment;
mod enhanced;
mod error;
mod hash_limit;
mod policy;
mod remote;

pub use api::{AccessControl, Action, AllowAnonymous, AuthError, ConnectInfo, Principal};
pub use containment::covers_filter;
pub use error::PolicyError;
pub use hash_limit::{PasswordHashPermit, try_acquire_password_hash_permit};
pub use policy::{MAX_POLICY_BYTES, StaticPolicy};
pub use remote::{AccessAttempt, AccessFuture, RemoteAccess, RemoteAccessError};

pub use enhanced::{
    EnhancedAuthError, EnhancedAuthStep, EnhancedAuthenticator, EnhancedConnectInfo,
    EnhancedExchange, PlainAuthenticator,
};
