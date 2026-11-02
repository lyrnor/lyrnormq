//! Scoped business-frame probes; installed policies never retain a live attempt.
use super::*;
use mqtt_auth::{AccessAttempt, AccessControl, Action, Principal};

#[derive(Debug)]
struct Scoped {
    probe: Arc<dyn AccessControl>,
    admitted: Arc<dyn AccessControl>,
    client: String,
    action: Action,
    resources: Vec<String>,
}
impl AccessControl for Scoped {
    fn authenticate(&self, info: ConnectInfo<'_>) -> Result<Principal, AuthError> {
        self.admitted.authenticate(info)
    }
    fn authorize(&self, principal: &Principal, action: Action, resource: &str) -> bool {
        self.admitted.authorize(principal, action, resource)
    }
    fn authorize_for_client(
        &self,
        principal: &Principal,
        client: &str,
        action: Action,
        resource: &str,
    ) -> bool {
        if client == self.client
            && action == self.action
            && self.resources.iter().any(|entry| entry == resource)
        {
            self.probe
                .authorize_for_client(principal, client, action, resource)
        } else {
            self.admitted
                .authorize_for_client(principal, client, action, resource)
        }
    }
    fn reconciles_stored_authorization(&self) -> bool {
        self.admitted.reconciles_stored_authorization()
    }
}

pub(super) fn attempt(
    request: &Request,
    engine: &SessionEngine,
    config: &BrokerConfig,
) -> Option<Arc<dyn AccessAttempt>> {
    let Request::Handle { client, frame, .. } = request else {
        return None;
    };
    match &frame.packet {
        Packet::Publish {
            qos: 2,
            packet_id: Some(id),
            ..
        } if engine.has_incoming_exchange(client, *id) => None,
        Packet::Publish { .. } | Packet::Subscribe { .. } => {
            config.remote_access.as_ref().map(|remote| remote.begin())
        }
        _ => None,
    }
}
pub(super) fn policy(request: &Request, attempt: &dyn AccessAttempt) -> Arc<dyn AccessControl> {
    let Request::Handle { client, frame, .. } = request else {
        return attempt.admitted_policy();
    };
    let (action, resources) = match &frame.packet {
        Packet::Publish { topic, .. } => (Action::Publish, vec![topic.clone()]),
        Packet::Subscribe { filters, .. } => (
            Action::Subscribe,
            filters
                .iter()
                .map(|(filter, _)| {
                    filter
                        .strip_prefix("$share/")
                        .and_then(|suffix| suffix.split_once('/').map(|(_, filter)| filter))
                        .unwrap_or(filter)
                        .to_owned()
                })
                .collect(),
        ),
        _ => return attempt.admitted_policy(),
    };
    Arc::new(Scoped {
        probe: attempt.policy(),
        admitted: attempt.admitted_policy(),
        client: client.clone(),
        action,
        resources,
    })
}
