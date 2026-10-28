//! Bounded enhanced-authentication providers independent of MQTT wire framing.
use crate::{AccessControl, Action, AuthError, ConnectInfo, Principal};
use std::{fmt, sync::Arc};

const MAX_DATA_BYTES: usize = u16::MAX as usize;
const MAX_USERNAME_BYTES: usize = 256;

/// Borrowed, connection-resolved input. Authentication data is never Debug output.
pub struct EnhancedConnectInfo<'a> {
    pub client_id: &'a str,
    pub username: Option<&'a str>,
    pub method: &'a str,
    pub data: Option<&'a [u8]>,
}
impl fmt::Debug for EnhancedConnectInfo<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnhancedConnectInfo")
            .field("client_id", &self.client_id)
            .field("username", &self.username.map(|_| "[PRESENT]"))
            .field("method", &self.method)
            .field("data", &self.data.map(|_| "[REDACTED]"))
            .finish()
    }
}

/// Mechanism-independent failures with no credential or account-specific details.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnhancedAuthError {
    UnsupportedMethod,
    BadCredentials,
    NotAuthorized,
    Protocol,
}
impl fmt::Display for EnhancedAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnsupportedMethod => "unsupported authentication method",
            Self::BadCredentials => "bad credentials",
            Self::NotAuthorized => "not authorized",
            Self::Protocol => "invalid authentication exchange",
        })
    }
}
impl std::error::Error for EnhancedAuthError {}
impl From<AuthError> for EnhancedAuthError {
    fn from(value: AuthError) -> Self {
        match value {
            AuthError::BadCredentials => Self::BadCredentials,
            AuthError::NotAuthorized => Self::NotAuthorized,
        }
    }
}

/// A provider response. The transport frames it and applies packet/deadline limits.
/// Response data may be sensitive; callers must handle and dispose of it as a secret.
pub enum EnhancedAuthStep {
    Continue {
        data: Option<Vec<u8>>,
    },
    Complete {
        principal: Principal,
        data: Option<Vec<u8>>,
    },
}
impl fmt::Debug for EnhancedAuthStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Continue { data } => f
                .debug_struct("Continue")
                .field("data", &data.as_ref().map(|_| "[REDACTED]"))
                .finish(),
            Self::Complete { principal, data } => f
                .debug_struct("Complete")
                .field("principal", principal)
                .field("data", &data.as_ref().map(|_| "[REDACTED]"))
                .finish(),
        }
    }
}

/// One connection's bounded exchange, driven only on the blocking state worker.
/// Implementations must bound CPU/memory and avoid unbounded IO or secret logging.
pub trait EnhancedExchange: fmt::Debug + Send {
    fn step(&mut self, data: Option<&[u8]>) -> Result<EnhancedAuthStep, EnhancedAuthError>;
}

/// Factory for an initial exchange or fresh reauthentication exchange. MQTT method,
/// packet sequencing, deadline, round limits and stable-owner checks belong to IO.
pub trait EnhancedAuthenticator: fmt::Debug + Send + Sync {
    fn start(
        &self,
        info: EnhancedConnectInfo<'_>,
    ) -> Result<(Box<dyn EnhancedExchange>, EnhancedAuthStep), EnhancedAuthError>;
}

/// SASL PLAIN using the existing AccessControl credential verifier and ACLs.
///
/// Supports direct initial data or one server credential-request step. PLAIN adds
/// no encryption and requires a protected transport. It does not implement SCRAM.
/// Identity/password preparation is byte-preserving UTF-8, matching the existing
/// credential database; this provider does not apply SASLprep normalization.
pub struct PlainAuthenticator {
    access: Arc<dyn AccessControl>,
}
impl PlainAuthenticator {
    pub fn new(access: Arc<dyn AccessControl>) -> Self {
        Self { access }
    }
}
impl fmt::Debug for PlainAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlainAuthenticator").finish_non_exhaustive()
    }
}
impl EnhancedAuthenticator for PlainAuthenticator {
    fn start(
        &self,
        info: EnhancedConnectInfo<'_>,
    ) -> Result<(Box<dyn EnhancedExchange>, EnhancedAuthStep), EnhancedAuthError> {
        if info.method != "PLAIN" {
            return Err(EnhancedAuthError::UnsupportedMethod);
        }
        if info.client_id.is_empty()
            || info.client_id.len() > MAX_DATA_BYTES
            || info.client_id.contains('\0')
            || info.username.is_some_and(|name| {
                name.is_empty() || name.len() > MAX_USERNAME_BYTES || name.contains('\0')
            })
        {
            return Err(EnhancedAuthError::BadCredentials);
        }
        let mut exchange = PlainExchange {
            access: Arc::clone(&self.access),
            client_id: info.client_id.to_owned(),
            username: info.username.map(str::to_owned),
            finished: false,
        };
        let step = match info.data {
            Some(data) => exchange.step(Some(data))?,
            None => EnhancedAuthStep::Continue { data: None },
        };
        Ok((Box::new(exchange), step))
    }
}

// Neither the connection state nor temporary parsing owns a password copy. The
// caller owns the borrowed input and must dispose of its wire credential buffer.
struct PlainExchange {
    access: Arc<dyn AccessControl>,
    client_id: String,
    username: Option<String>,
    finished: bool,
}
impl fmt::Debug for PlainExchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlainExchange")
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}
impl EnhancedExchange for PlainExchange {
    fn step(&mut self, data: Option<&[u8]>) -> Result<EnhancedAuthStep, EnhancedAuthError> {
        if self.finished {
            return Err(EnhancedAuthError::Protocol);
        }
        // A failed attempt is terminal too: the exchange cannot become an oracle
        // accepting repeated guesses without a new bounded transport exchange.
        self.finished = true;
        let data = data.ok_or(EnhancedAuthError::BadCredentials)?;
        if data.len() > MAX_DATA_BYTES {
            return Err(EnhancedAuthError::BadCredentials);
        }
        let text = std::str::from_utf8(data).map_err(|_| EnhancedAuthError::BadCredentials)?;
        let mut parts = text.split('\0');
        let authorization = parts.next().ok_or(EnhancedAuthError::BadCredentials)?;
        let username = parts.next().ok_or(EnhancedAuthError::BadCredentials)?;
        let password = parts.next().ok_or(EnhancedAuthError::BadCredentials)?;
        if parts.next().is_some()
            || username.is_empty()
            || username.len() > MAX_USERNAME_BYTES
            || password.is_empty()
            || (!authorization.is_empty() && authorization != username)
            || self
                .username
                .as_deref()
                .is_some_and(|expected| expected != username)
        {
            return Err(EnhancedAuthError::BadCredentials);
        }
        let principal = self.access.authenticate(ConnectInfo {
            client_id: &self.client_id,
            username: Some(username),
            password: Some(password.as_bytes()),
        })?;
        if !self
            .access
            .authorize(&principal, Action::Connect, &self.client_id)
        {
            return Err(EnhancedAuthError::NotAuthorized);
        }
        Ok(EnhancedAuthStep::Complete {
            principal,
            data: None,
        })
    }
}

#[cfg(test)]
mod tests;
