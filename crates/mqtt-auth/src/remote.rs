//! Async provider admission and synchronous speculative authorization probes.
use crate::{AccessControl, ConnectInfo, Principal};
use std::{fmt, future::Future, pin::Pin, sync::Arc};

pub type AccessFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Non-secret failures, kept separate from an explicit authorization denial.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteAccessError {
    BadCredentials,
    NotAuthorized,
    Unavailable,
    Busy,
}
impl fmt::Display for RemoteAccessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::BadCredentials => "bad credentials",
            Self::NotAuthorized => "not authorized",
            Self::Unavailable => "access service unavailable",
            Self::Busy => "access service capacity exhausted",
        })
    }
}
impl std::error::Error for RemoteAccessError {}

pub trait RemoteAccess: fmt::Debug + Send + Sync {
    /// Pin one immutable configuration generation and a bounded receipt set.
    fn begin(&self) -> Arc<dyn AccessAttempt>;
}

pub trait AccessAttempt: fmt::Debug + Send + Sync {
    /// Cache-only probe; unknown decisions are speculative until resolved/sealed.
    fn policy(&self) -> Arc<dyn AccessControl>;
    /// Non-probing policy for accepted state; must not retain HTTP attempt budgets.
    fn admitted_policy(&self) -> Arc<dyn AccessControl> {
        self.policy()
    }
    fn remote_authentication(&self) -> bool;
    fn authenticate<'a>(
        &'a self,
        info: ConnectInfo<'a>,
    ) -> AccessFuture<'a, Result<Principal, RemoteAccessError>>;
    /// Also true for overflow/unavailable. A pending proposal must never commit.
    fn pending(&self) -> bool;
    fn resolve(&self) -> AccessFuture<'_, Result<(), RemoteAccessError>>;
    /// Authorization admission before the first externally visible side effect.
    fn seal(&self) -> bool;
}
