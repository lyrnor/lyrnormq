//! Public identities and bounded synchronous policy hooks.
use std::fmt;

/// Stable, non-secret identity bound to stored sessions and Will intents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    /// Static users use `user:<username>`; anonymous uses `anonymous`.
    pub name: String,
}
impl Principal {
    /// Shared identity for unauthenticated clients; it does not distinguish people.
    pub fn anonymous() -> Self {
        Self {
            name: "anonymous".into(),
        }
    }
}

/// Borrowed handshake input; implementations must never log the password.
pub struct ConnectInfo<'a> {
    pub client_id: &'a str,
    pub username: Option<&'a str>,
    pub password: Option<&'a [u8]>,
}
impl fmt::Debug for ConnectInfo<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectInfo")
            .field("client_id", &self.client_id)
            .field("username", &self.username)
            .field("password", &self.password.map(|_| "[REDACTED]"))
            .finish()
    }
}

/// Connect uses a ClientId, Subscribe a filter, Publish and Receive exact topics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Connect,
    Publish,
    Subscribe,
    Receive,
}

/// Deliberately contains no credential or account-specific error details.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthError {
    BadCredentials,
    NotAuthorized,
}
impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::BadCredentials => "bad credentials",
            Self::NotAuthorized => "not authorized",
        })
    }
}
impl std::error::Error for AuthError {}

/// Hooks must have bounded CPU/memory and avoid unbounded IO. The broker executes
/// these synchronous calls on its blocking worker, outside asynchronous IO tasks.
pub trait AccessControl: fmt::Debug + Send + Sync {
    fn authenticate(&self, info: ConnectInfo<'_>) -> Result<Principal, AuthError>;
    fn authorize(&self, principal: &Principal, action: Action, resource: &str) -> bool;
}

/// Backward-compatible open anonymous policy. Supplied credentials are refused.
#[derive(Debug, Default)]
pub struct AllowAnonymous;
impl AccessControl for AllowAnonymous {
    fn authenticate(&self, info: ConnectInfo<'_>) -> Result<Principal, AuthError> {
        if info.username.is_none() && info.password.is_none() {
            Ok(Principal::anonymous())
        } else {
            Err(AuthError::BadCredentials)
        }
    }
    fn authorize(&self, principal: &Principal, _action: Action, _resource: &str) -> bool {
        principal.name == "anonymous"
    }
}
