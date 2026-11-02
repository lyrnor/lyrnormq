//! Compact policy for accepted state, independent of HTTP runtime lifetimes.
use crate::{
    client::username,
    runtime::{Generation, Shared},
};
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
        if matches!(action, Action::Connect) {
            if self.authentication && (self.authorization || !self.local_accounts) {
                return username(principal).is_some();
            }
            return self
                .fallback
                .authorize_for_client(principal, client, action, resource);
        }
        // Point grants were checked before admission. Recovery and accepted flows
        // must never turn provider unavailability/configuration into revocation.
        if self.authorization {
            return true;
        }
        if self.authentication && !self.local_accounts {
            return username(principal).is_some();
        }
        self.fallback
            .authorize_for_client(principal, client, action, resource)
    }
    fn reconciles_stored_authorization(&self) -> bool {
        !self.authorization && self.fallback.reconciles_stored_authorization()
    }
}
