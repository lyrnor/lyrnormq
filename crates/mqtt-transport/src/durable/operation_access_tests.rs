//! Operation admission regressions using a remote provider with explicit probes.
use super::*;
use mqtt_auth::{
    AccessAttempt, AccessControl, AccessFuture, Action, Principal, RemoteAccess, RemoteAccessError,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug)]
struct Granted;
impl AccessControl for Granted {
    fn authenticate(&self, _: ConnectInfo<'_>) -> Result<Principal, AuthError> {
        Err(AuthError::BadCredentials)
    }
    fn authorize(&self, _: &Principal, _: Action, _: &str) -> bool {
        true
    }
    fn reconciles_stored_authorization(&self) -> bool {
        false
    }
}
#[derive(Debug, Default)]
struct Remote {
    calls: AtomicUsize,
    deny: AtomicBool,
    unavailable: AtomicBool,
    stale: AtomicBool,
    resources: Mutex<Vec<(Action, String)>>,
}
#[derive(Debug)]
struct Attempt {
    remote: Arc<Remote>,
    pending: AtomicBool,
    resolved: AtomicBool,
}
#[derive(Debug)]
struct Probe(Arc<Attempt>);
impl AccessControl for Probe {
    fn authenticate(&self, _: ConnectInfo<'_>) -> Result<Principal, AuthError> {
        Err(AuthError::BadCredentials)
    }
    fn authorize(&self, _: &Principal, action: Action, resource: &str) -> bool {
        if matches!(action, Action::Publish | Action::Subscribe) {
            if !self.0.resolved.load(Ordering::Relaxed) {
                self.0.pending.store(true, Ordering::Relaxed);
                self.0
                    .remote
                    .resources
                    .lock()
                    .unwrap()
                    .push((action, resource.into()));
                return true;
            }
            return !self.0.remote.deny.load(Ordering::Relaxed) && resource != "private/#";
        }
        true
    }
    fn reconciles_stored_authorization(&self) -> bool {
        false
    }
}
// Shared state makes the policy clone reflect resolve() without owning the attempt.
#[derive(Debug)]
struct Handle(Arc<Attempt>);
impl AccessAttempt for Handle {
    fn policy(&self) -> Arc<dyn AccessControl> {
        Arc::new(Probe(self.0.clone()))
    }
    fn admitted_policy(&self) -> Arc<dyn AccessControl> {
        Arc::new(Granted)
    }
    fn remote_authentication(&self) -> bool {
        true
    }
    fn authenticate<'a>(
        &'a self,
        _: ConnectInfo<'a>,
    ) -> AccessFuture<'a, Result<Principal, RemoteAccessError>> {
        Box::pin(async {
            Ok(Principal {
                name: "user:device".into(),
            })
        })
    }
    fn pending(&self) -> bool {
        self.0.pending.load(Ordering::Relaxed)
    }
    fn resolve(&self) -> AccessFuture<'_, Result<(), RemoteAccessError>> {
        Box::pin(async {
            self.0.remote.calls.fetch_add(1, Ordering::Relaxed);
            if self.0.remote.unavailable.load(Ordering::Relaxed) {
                return Err(RemoteAccessError::Unavailable);
            }
            self.0.pending.store(false, Ordering::Relaxed);
            self.0.resolved.store(true, Ordering::Relaxed);
            Ok(())
        })
    }
    fn seal(&self) -> bool {
        !self.pending() && !self.0.remote.stale.load(Ordering::Relaxed)
    }
}
#[derive(Debug)]
struct Provider(Arc<Remote>);
impl RemoteAccess for Provider {
    fn begin(&self) -> Arc<dyn AccessAttempt> {
        Arc::new(Handle(Arc::new(Attempt {
            remote: self.0.clone(),
            pending: AtomicBool::new(false),
            resolved: AtomicBool::new(false),
        })))
    }
}
async fn worker() -> (Worker, Arc<Remote>) {
    worker_with_policy(Arc::new(Granted)).await
}
async fn worker_with_policy(access_control: Arc<dyn AccessControl>) -> (Worker, Arc<Remote>) {
    let remote = Arc::new(Remote::default());
    let broker = Broker::start(
        BrokerConfig {
            access_control,
            remote_access: Some(Arc::new(Provider(remote.clone()))),
            ..BrokerConfig::default()
        },
        Arc::new(SystemClock),
        None,
    )
    .await
    .unwrap();
    let worker = Worker::new(broker);
    let (wake, _) = watch::channel(0);
    let (cancel, _) = watch::channel(false);
    let reply = worker
        .request(Operation::Connect {
            client: "source".into(),
            clean: true,
            will: None,
            username: Some("device".into()),
            password: None,
            generation: 1,
            wake,
            cancel,
        })
        .await
        .unwrap();
    assert!(matches!(reply, Reply::Connected(_)));
    (worker, remote)
}
async fn send(worker: &Worker, packet: Packet) -> io::Result<Reply> {
    worker
        .request(Operation::Handle {
            client: "source".into(),
            generation: 1,
            packet,
        })
        .await
}
fn publication(qos: u8, dup: bool) -> Packet {
    Packet::Publish {
        topic: "sensor/value".into(),
        payload: vec![1],
        qos,
        packet_id: (qos > 0).then_some(7),
        dup,
        retain: true,
    }
}
#[tokio::test]
async fn operation_denial_unavailability_and_stale_results_have_no_retained_effect() {
    let (worker, remote) = worker().await;
    remote.deny.store(true, Ordering::Relaxed);
    assert!(send(&worker, publication(1, false)).await.is_err());
    assert!(worker.broker.retained_for(&[("#".into(), 1)]).is_empty());
    remote.deny.store(false, Ordering::Relaxed);
    remote.unavailable.store(true, Ordering::Relaxed);
    let error = send(&worker, publication(1, false)).await.err().unwrap();
    assert_eq!(reason(&error), Some(ReasonCode::SERVER_UNAVAILABLE));
    assert!(worker.broker.retained_for(&[("#".into(), 1)]).is_empty());
    remote.unavailable.store(false, Ordering::Relaxed);
    remote.stale.store(true, Ordering::Relaxed);
    assert!(send(&worker, publication(1, false)).await.is_err());
    assert!(worker.broker.retained_for(&[("#".into(), 1)]).is_empty());
    worker.broker.shutdown().await.unwrap();
}
#[tokio::test]
async fn accepted_qos2_duplicate_and_pubrel_never_recheck_http() {
    let (worker, remote) = worker().await;
    send(&worker, publication(2, false)).await.unwrap();
    assert_eq!(remote.calls.load(Ordering::Relaxed), 1);
    remote.unavailable.store(true, Ordering::Relaxed);
    send(&worker, publication(2, true)).await.unwrap();
    send(&worker, Packet::PubRel { packet_id: 7 })
        .await
        .unwrap();
    assert_eq!(remote.calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        *remote.resources.lock().unwrap(),
        vec![(Action::Publish, "sensor/value".into())]
    );
    worker.broker.shutdown().await.unwrap();
}

#[tokio::test]
async fn mixed_subscribe_filters_use_per_filter_denials_and_invalid_packets_never_call_remote() {
    let (worker, remote) = worker().await;
    let response = send(
        &worker,
        Packet::Subscribe {
            packet_id: 2,
            filters: vec![("sensor/#".into(), 1), ("private/#".into(), 1)],
        },
    )
    .await
    .unwrap();
    assert!(
        matches!(response, Reply::Packets(packets) if matches!(packets.as_slice(), [Packet::SubAck { codes, .. }] if codes == &[1, 128]))
    );
    assert_eq!(remote.calls.load(Ordering::Relaxed), 1);
    assert!(
        send(
            &worker,
            Packet::Publish {
                topic: "bad/#".into(),
                payload: vec![],
                qos: 0,
                packet_id: None,
                dup: false,
                retain: false
            }
        )
        .await
        .is_err()
    );
    assert_eq!(remote.calls.load(Ordering::Relaxed), 1);
    worker.broker.shutdown().await.unwrap();
}

#[derive(Debug)]
struct LocalReject;
impl AccessControl for LocalReject {
    fn authenticate(&self, _: ConnectInfo<'_>) -> Result<Principal, AuthError> {
        Err(AuthError::BadCredentials)
    }
    fn authorize(&self, _: &Principal, _: Action, _: &str) -> bool {
        false
    }
}
#[tokio::test]
async fn authenticated_connect_uses_the_attempt_admission_policy_instead_of_local_credentials() {
    let (worker, remote) = worker_with_policy(Arc::new(LocalReject)).await;
    assert_eq!(
        remote.calls.load(Ordering::Relaxed),
        0,
        "CONNECT must not probe remote ACLs"
    );
    send(&worker, publication(1, false)).await.unwrap();
    assert_eq!(remote.calls.load(Ordering::Relaxed), 1);
    worker.broker.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_denial_from_a_retired_attempt_is_unavailable_instead_of_a_business_verdict() {
    let (worker, remote) = worker().await;
    remote.deny.store(true, Ordering::Relaxed);
    remote.stale.store(true, Ordering::Relaxed);
    let error = send(&worker, publication(1, false)).await.err().unwrap();
    assert_eq!(reason(&error), Some(ReasonCode::SERVER_UNAVAILABLE));
    assert!(worker.broker.retained_for(&[("#".into(), 1)]).is_empty());
    worker.broker.shutdown().await.unwrap();
}
