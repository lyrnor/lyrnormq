//! Compact policy for accepted state, independent of HTTP runtime lifetimes.
use crate::runtime::{Generation, Shared};
use mqtt_auth::{AccessControl, Action, AuthError, ConnectInfo, Principal};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct AdmittedPolicy {
    authentication: bool,
    authorization: bool,
    local_accounts: bool,
    fallback: Arc<dyn AccessControl>,
}
impl AdmittedPolicy {
    pub(crate) fn snapshot(shared: &Shared, generation: &Generation) -> Arc<dyn AccessControl> {
        if !generation.config.authentication.enabled
            && !generation.config.authorization.backend.enabled
        {
            return shared.fallback.clone();
        }
        Arc::new(Self {
            authentication: generation.config.authentication.enabled,
            authorization: generation.config.authorization.backend.enabled,
            local_accounts: shared.local_accounts,
            fallback: shared.fallback.clone(),
        })
    }
}
impl AccessControl for AdmittedPolicy {
    fn authenticate(&self, info: ConnectInfo<'_>) -> Result<Principal, AuthError> {
        if self.authentication {
            Err(AuthError::BadCredentials)
        } else {
            self.fallback.authenticate(info)
        }
    }
    fn authorize(&self, principal: &Principal, action: Action, resource: &str) -> bool {
        self.authorize_for_client(
            principal,
            if matches!(action, Action::Connect) {
                resource
            } else {
                ""
            },
            action,
            resource,
        )
    }
    fn authorize_for_client(
        &self,
        principal: &Principal,
        client: &str,
        action: Action,
        resource: &str,
    ) -> bool {
        // This policy is used only for trusted accepted state. New CONNECT
        // admission uses the strict attempt probe before installing this policy.
        // In particular, old anonymous Will/session identities survive enabling
        // HTTP authentication without being treated as new credentials.
        if self.authorization || (self.authentication && !self.local_accounts) {
            return true;
        }
        self.fallback
            .authorize_for_client(principal, client, action, resource)
    }
    fn reconciles_stored_authorization(&self) -> bool {
        false
    }
}
