//! One tokio task per client identifier owning a one-client session partition.
//!
//! Requests are proposed on a clone, committed (row write when persistent) and
//! installed before replies or wake-ups. Client QoS>0 publications wait for
//! recipient commits before their own commit and acknowledgement.
use super::broker::{Broker, Fanout, subscribe_filters};
use super::*;
use mqtt_auth::Principal;
use mqtt_cluster_core::{DeliveryId, SessionLease, SessionMutation};
use mqtt_session::durable::{DeliveryScope, Publication};
use mqtt_storage::durable::RowChange;
use std::collections::{BTreeSet, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) enum ActorCommand {
    Activate {
        lease: SessionLease,
        reply: oneshot::Sender<io::Result<()>>,
    },
    DurableDeliver {
        publication: Arc<Publication>,
        receipt: DeliveryId,
        scope: DeliveryScope,
        reply: oneshot::Sender<io::Result<()>>,
    },
    Request {
        request: Box<Request>,
        principal: Option<Principal>,
        reply: oneshot::Sender<ResultReply>,
    },
    Deliver {
        publication: Arc<Publication>,
        scope: DeliveryScope,
        ack: Option<oneshot::Sender<bool>>,
    },
    Finish {
        token: u64,
        result: io::Result<()>,
    },
    #[cfg(test)]
    Tick {
        done: oneshot::Sender<()>,
    },
    #[cfg(test)]
    Attach {
        client: String,
        online: Online,
        done: oneshot::Sender<()>,
    },
    /// Hold the actor until `release` resolves (test barrier).
    #[cfg(test)]
    Stall {
        release: oneshot::Receiver<()>,
    },
}

#[derive(Clone)]
pub(super) struct ActorHandle {
    pub(super) id: u64,
    pub(super) sender: mpsc::Sender<ActorCommand>,
    /// Generation of the installed online connection, or 0.
    pub(super) online: Arc<AtomicU64>,
}

struct Effects {
    commit: bool,
    /// Install the proposal in memory, but skip the replicated session write.
    volatile: bool,
    notify: bool,
    housekeeping: bool,
    /// When false, install skips rebuilding the subscription index.
    filters_changed: bool,
    registration: Option<(String, Online)>,
    removal: Option<String>,
    publications: Vec<Publication>,
    receipt: Option<DeliveryId>,
    reset_session: bool,
}
impl Default for Effects {
    fn default() -> Self {
        Self {
            commit: true,
            volatile: false,
            notify: false,
            housekeeping: true,
            filters_changed: false,
            registration: None,
            removal: None,
            publications: Vec::new(),
            receipt: None,
            reset_session: false,
        }
    }
}

struct Rerun {
    client: String,
    generation: u64,
    frame: Frame,
    legacy: bool,
    received_at_millis: Option<u64>,
}
impl Rerun {
    fn request(&self) -> Request {
        Request::Handle {
            client: self.client.clone(),
            generation: self.generation,
            frame: self.frame.clone(),
            legacy: self.legacy,
            received_at_millis: self.received_at_millis,
        }
    }
}

struct Pending {
    proposal: SessionEngine,
    version: u64,
    rerun: Rerun,
    response: Reply,
    admitted: Option<Arc<dyn mqtt_auth::AccessControl>>,
    reply: oneshot::Sender<ResultReply>,
}

struct Actor {
    broker: Arc<Broker>,
    client: String,
    id: u64,
    engine: SessionEngine,
    observation: Option<Arc<Mutex<crate::management::Observation>>>,
    online: HashMap<String, Online>,
    online_generation: Arc<AtomicU64>,
    receiver: mpsc::Receiver<ActorCommand>,
    weak: mpsc::WeakSender<ActorCommand>,
    row: Option<Vec<u8>>,
    /// An activated authoritative row has not necessarily reached this node's mirror.
    mirror_dirty: bool,
    cluster_lease: Option<SessionLease>,
    cluster_invalid: bool,
    accounted: usize,
    filters: BTreeSet<String>,
    version: u64,
    next_token: u64,
    pending: HashMap<u64, Pending>,
    fresh_until: Option<tokio::time::Instant>,
    retry_at: Option<tokio::time::Instant>,
    /// Receipts accepted into a non-persistent session. They disappear with the
    /// session, so a replica write would not make a retry idempotent.
    volatile_receipts: VecDeque<String>,
    volatile_receipt_index: HashSet<String>,
}

#[cfg(test)]
mod ping_tests;

const RETRY_MILLIS: u64 = 100;
const MAX_TIMER_MILLIS: u64 = 3_600_000;

pub(super) fn spawn(
    broker: Arc<Broker>,
    client: String,
    id: u64,
    engine: SessionEngine,
    row: Option<Vec<u8>>,
    alive: mpsc::Sender<()>,
) -> ActorHandle {
    let capacity = broker
        .config
        .outbound_capacity
        .saturating_mul(4)
        .clamp(128, 65_536);
    let (sender, receiver) = mpsc::channel(capacity);
    let online = Arc::new(AtomicU64::new(0));
    let filters = engine
        .subscription_filters()
        .remove(&client)
        .unwrap_or_default();
    if !filters.is_empty() {
        broker.index.replace(&client, &filters);
    }
    let accounted = engine.live_state_bytes();
    broker.add_state(accounted);
    let fresh_until = engine
        .is_vacant()
        .then(|| tokio::time::Instant::now() + broker.config.handshake_timeout);
    let observation = broker
        .config
        .management
        .as_ref()
        .map(|management| management.register(&client, id, &engine));
    let actor = Actor {
        observation,
        broker,
        client,
        id,
        engine,
        online: HashMap::new(),
        online_generation: Arc::clone(&online),
        receiver,
        weak: sender.downgrade(),
        row,
        mirror_dirty: false,
        cluster_lease: None,
        cluster_invalid: false,
        accounted,
        filters,
        version: 0,
        next_token: 0,
        pending: HashMap::new(),
        fresh_until,
        retry_at: None,
        volatile_receipts: VecDeque::new(),
        volatile_receipt_index: HashSet::new(),
    };
    tokio::spawn(actor.run(alive));
    ActorHandle { id, sender, online }
}

/// Port of the former worker's per-request state transition.
fn apply(
    proposal: &mut SessionEngine,
    request: Request,
    principal: Option<Principal>,
    online: &HashMap<String, Online>,
    broker: &Broker,
    effects: &mut Effects,
) -> io::Result<Reply> {
    let config = &broker.config;
    match request {
        Request::Connect(request) => {
            let principal =
                principal.ok_or_else(|| io::Error::other("missing authenticated identity"))?;
            if request
                .deadline
                .is_some_and(|at| std::time::Instant::now() >= at)
            {
                return Err(failure(
                    ReasonCode::NOT_AUTHORIZED,
                    "authentication deadline exceeded",
                ));
            }
            let result = match proposal.connect_with_options(
                &request.client,
                request.will.clone(),
                principal,
                request.options.clone(),
            ) {
                Ok(result) => result,
                Err(EngineError::NotAuthorized) => {
                    effects.commit = false;
                    effects.housekeeping = false;
                    return Ok(refusal(&request, ReasonCode::NOT_AUTHORIZED));
                }
                Err(EngineError::Mqtt5(reason)) => {
                    effects.commit = false;
                    effects.housekeeping = false;
                    return Ok(refusal(&request, reason));
                }
                Err(error) => return Err(model_error(error)),
            };
            if request.options.version == ProtocolVersion::V311
                && !request.clean
                && config.storage_path.is_none()
                && !broker.durable_cluster()
            {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "persistent sessions require --storage-path",
                ));
            }
            let enhanced_frame = if request.authentication.is_some() {
                Some(enhanced::success_frame(
                    &request,
                    result.session_present,
                    config,
                )?)
            } else {
                None
            };
            let assigned = request.assigned.then(|| request.client.clone());
            let resolved = request.client.clone();
            effects.registration = Some((
                request.client,
                Online {
                    generation: request.generation,
                    wake: request.wake,
                    cancel: request.cancel,
                    close_reason: request.close_reason,
                },
            ));
            effects.filters_changed = true;
            effects.notify = true;
            effects.reset_session = !result.session_present;
            Ok(if let Some(frame) = enhanced_frame {
                Reply::Authenticated {
                    client: resolved,
                    frame,
                }
            } else {
                match assigned {
                    Some(client) => Reply::Assigned {
                        present: result.session_present,
                        client,
                    },
                    None => Reply::Connected(result.session_present),
                }
            })
        }
        Request::Handle {
            client,
            generation,
            mut frame,
            legacy,
            received_at_millis,
        } => {
            valid_generation(online, &client, generation)?;
            let mut shared_overlay = None;
            if config.cluster.is_some() && !broker.durable_cluster() {
                if matches!(&frame.packet, Packet::Publish { retain: true, .. }) {
                    return Err(failure(
                        ReasonCode::RETAIN_NOT_SUPPORTED,
                        "retained publishing is not supported in initial cluster gate",
                    ));
                }
                if let Packet::Subscribe { packet_id, filters } = &frame.packet {
                    let overlay: Vec<Option<u8>> = filters
                        .iter()
                        .map(|(filter, _)| {
                            mqtt_router::split_shared_filter(filter)
                                .is_some()
                                .then_some(if frame.version == ProtocolVersion::V5 {
                                    0x9e
                                } else {
                                    0x80
                                })
                        })
                        .collect();
                    if overlay.iter().any(Option::is_some) {
                        if overlay.iter().all(Option::is_some) {
                            effects.commit = false;
                            effects.housekeeping = false;
                            return Ok(frames_reply(
                                vec![Frame {
                                    packet: Packet::SubAck {
                                        packet_id: *packet_id,
                                        codes: overlay.into_iter().flatten().collect(),
                                    },
                                    version: frame.version,
                                    ..Frame::v5(Packet::PingReq)
                                }],
                                legacy,
                            ));
                        }
                        shared_overlay = Some((*packet_id, overlay));
                    }
                }
                if shared_overlay.is_some() {
                    let options = std::mem::take(&mut frame.subscription_options);
                    let mut kept_options = Vec::new();
                    if let Packet::Subscribe { filters, .. } = &mut frame.packet {
                        let mut index = 0;
                        filters.retain(|(filter, _)| {
                            let keep = mqtt_router::split_shared_filter(filter).is_none();
                            if !options.is_empty() {
                                if keep {
                                    kept_options.extend(options.get(index).copied());
                                }
                                index += 1;
                            }
                            keep
                        });
                    }
                    frame.subscription_options = kept_options;
                }
            }
            effects.filters_changed = matches!(
                frame.packet,
                Packet::Subscribe { .. } | Packet::Unsubscribe { .. }
            );
            // PING changes no protocol state. The model still validates it and
            // expires deadlines internally, before housekeeping can report them.
            // Remember the old deadline and conservatively commit if it could
            // have fired, including a clock advance during frame handling.
            let quiet_ping = broker.durable_cluster() && matches!(frame.packet, Packet::PingReq);
            let expiry_before = quiet_ping
                .then(|| proposal.next_expiry_deadline())
                .flatten();
            let mut replies = match received_at_millis {
                Some(received_at) => proposal.handle_frame_at(&client, frame, received_at),
                None => proposal.handle_frame(&client, frame),
            }
            .map_err(model_error)?;
            if let Some((packet_id, overlay)) = shared_overlay {
                expand_shared_suback(&mut replies, packet_id, &overlay);
            }
            effects.commit = !quiet_ping
                || expiry_before.is_some_and(|deadline| deadline <= proposal.effective_now());
            effects.notify = effects.commit;
            Ok(frames_reply(replies, legacy))
        }
        #[cfg(test)]
        Request::Poll {
            client,
            generation,
            legacy,
        } => {
            valid_generation(online, &client, generation)?;
            let outcome = proposal
                .poll_frame_with_changes(&client)
                .map_err(model_error)?;
            let qos0 = outcome
                .frame
                .as_ref()
                .is_some_and(|frame| matches!(frame.packet, Packet::Publish { qos: 0, .. }));
            effects.commit = outcome.changed || outcome.frame.is_some();
            effects.volatile =
                broker.durable_cluster() && (qos0 || !proposal.has_persistent_state());
            effects.notify = effects.commit;
            Ok(poll_reply(outcome.frame, legacy))
        }
        Request::PollDelivery { client, generation } => {
            valid_generation(online, &client, generation)?;
            let outcome = proposal
                .poll_delivery_with_changes(&client)
                .map_err(model_error)?;
            let qos0 = outcome.delivery.as_ref().is_some_and(|delivery| {
                matches!(delivery.frame.packet, Packet::Publish { qos: 0, .. })
            });
            // The popped message still has to leave the in-memory queue. QoS 0
            // and sessions without a snapshot do not need a quorum write to do it.
            effects.commit = outcome.changed || outcome.delivery.is_some();
            effects.volatile =
                broker.durable_cluster() && (qos0 || !proposal.has_persistent_state());
            effects.notify = effects.commit;
            Ok(Reply::DeliveryPolled(outcome.delivery.map(Box::new)))
        }
        Request::BeginDelivery {
            client,
            generation,
            delivery,
        } => {
            valid_generation(online, &client, generation)?;
            let qos0 = matches!(delivery.frame.packet, Packet::Publish { qos: 0, .. });
            let outcome = proposal
                .begin_delivery(&client, delivery)
                .map_err(model_error)?;
            effects.commit = outcome.changed;
            effects.volatile =
                broker.durable_cluster() && (qos0 || !proposal.has_persistent_state());
            effects.notify = outcome.changed;
            Ok(Reply::FramePolled(outcome.frame.map(Box::new)))
        }
        Request::Disconnect {
            client,
            generation,
            graceful,
            expiry,
        } => {
            if valid_generation(online, &client, generation).is_err() {
                effects.commit = false;
                effects.housekeeping = false;
                return Ok(Reply::Done);
            }
            proposal
                .disconnect_with_options(&client, graceful, expiry)
                .map_err(model_error)?;
            effects.notify = true;
            effects.filters_changed = true;
            effects.removal = Some(client);
            Ok(Reply::Done)
        }
        Request::Authenticate { .. } | Request::CancelAuthentication { .. } => {
            Err(io::Error::other("unprocessed authentication command"))
        }
    }
}

fn housekeeping(proposal: &mut SessionEngine, effects: &mut Effects) -> io::Result<()> {
    if effects.housekeeping {
        let expired = proposal.expire_sessions().map_err(model_error)?;
        let pruned = proposal.reconcile_authorization().map_err(model_error)?;
        let retried = proposal.retry_wills().map_err(model_error)?;
        if expired || pruned || retried {
            effects.commit = true;
            effects.notify = true;
            effects.filters_changed = true;
        }
    }
    Ok(())
}

impl Actor {
    async fn run(mut self, _alive: mpsc::Sender<()>) {
        let mut due = true;
        loop {
            if due {
                due = false;
                self.tick().await;
            }
            if self.retirable() {
                let receiver = &self.receiver;
                if self
                    .broker
                    .retire(&self.client, self.id, || receiver.is_empty())
                {
                    break;
                }
            }
            let deadline = self.deadline();
            tokio::select! {
                command = self.receiver.recv() => match command {
                    Some(command) => {
                        self.fresh_until = None;
                        self.command(command).await;
                    }
                    None => break,
                },
                _ = crate::packet_io::deadline(deadline) => { due = true; }
            }
        }
        for (_, pending) in self.pending.drain() {
            let _ = pending.reply.send(Err(failure(
                ReasonCode::SERVER_UNAVAILABLE,
                "session actor stopped",
            )));
        }
        if let Some(management) = &self.broker.config.management {
            management.remove(&self.client, self.id);
        }
        self.broker.actor_exited(&self.client, self.accounted);
    }

    fn retirable(&self) -> bool {
        self.engine.is_vacant()
            && self.online.is_empty()
            && self.pending.is_empty()
            && self.fresh_until.is_none()
            && self.retry_at.is_none()
    }

    fn deadline(&self) -> Option<tokio::time::Instant> {
        let expiry = (!self.broker.durable_cluster()
            || (!self.cluster_invalid && self.cluster_lease.is_some()))
        .then(|| self.engine.next_expiry_deadline())
        .flatten()
        .map(|at| {
            let delay = at
                .saturating_sub(self.engine.effective_now())
                .min(MAX_TIMER_MILLIS);
            tokio::time::Instant::now() + Duration::from_millis(delay)
        });
        [self.fresh_until, self.retry_at, expiry]
            .into_iter()
            .flatten()
            .min()
    }

    fn close_online(&mut self, reason: ReasonCode) {
        for connection in self.online.values() {
            if let Some(close) = &connection.close_reason {
                let _ = close.send(Some(reason));
            }
            let _ = connection.cancel.send(true);
        }
    }

    async fn command(&mut self, command: ActorCommand) {
        match command {
            ActorCommand::Activate { lease, reply } => {
                let _ = reply.send(self.activate_recovered(lease).await);
            }
            ActorCommand::DurableDeliver {
                publication,
                receipt,
                scope,
                reply,
            } => {
                let result = self.deliver_durable(&publication, receipt, scope).await;
                let _ = reply.send(result);
            }
            ActorCommand::Request {
                request,
                principal,
                reply,
            } => self.request(*request, principal, reply).await,
            ActorCommand::Deliver {
                publication,
                scope,
                ack,
            } => {
                let accepted = self.deliver(&publication, &scope).await;
                if let Some(ack) = ack {
                    let _ = ack.send(accepted);
                }
            }
            ActorCommand::Finish { token, result } => {
                if let Err(error) = result {
                    if let Some(pending) = self.pending.remove(&token) {
                        let _ = pending.reply.send(Err(error));
                    }
                } else {
                    self.finish(token).await;
                }
            }
            #[cfg(test)]
            ActorCommand::Tick { done } => {
                self.tick().await;
                let _ = done.send(());
            }
            #[cfg(test)]
            ActorCommand::Attach {
                client,
                online,
                done,
            } => {
                let effects = Effects {
                    commit: false,
                    registration: Some((client, online)),
                    ..Effects::default()
                };
                let _ = self.install(self.engine.clone(), effects).await;
                let _ = done.send(());
            }
            #[cfg(test)]
            ActorCommand::Stall { release } => {
                let _ = release.await;
            }
        }
    }

    async fn tick(&mut self) {
        let now = tokio::time::Instant::now();
        if self.fresh_until.is_some_and(|at| now >= at) {
            self.fresh_until = None;
        }
        if self.retry_at.is_some_and(|at| now >= at) {
            self.retry_at = None;
        }
        if self.broker.durable_cluster() && (self.cluster_invalid || self.cluster_lease.is_none()) {
            return;
        }
        let mut proposal = self.engine.clone();
        let mut effects = Effects {
            commit: false,
            ..Effects::default()
        };
        if housekeeping(&mut proposal, &mut effects).is_err() {
            return;
        }
        let publications = proposal.take_publications();
        if !effects.commit && publications.is_empty() {
            return;
        }
        if self.broker.durable_cluster() {
            effects.commit = true;
            effects.publications = publications;
            let _ = self.install(proposal, effects).await;
            return;
        }
        // A Will blocked by retained capacity stays pending and is retried.
        let mut durability = Vec::new();
        for publication in &publications {
            match self.broker.apply_retained(publication).await {
                Ok(outcome) => durability.push(outcome),
                Err(_) => {
                    self.retry_at =
                        Some(tokio::time::Instant::now() + Duration::from_millis(RETRY_MILLIS));
                    return;
                }
            }
        }
        effects.commit = true;
        if self.install(proposal, effects).await.is_ok() {
            for publication in publications {
                if let Some(fanout) = self.broker.dispatch(Arc::new(publication), false) {
                    let broker = Arc::clone(&self.broker);
                    tokio::spawn(async move {
                        let _ = broker.complete(fanout).await;
                    });
                }
            }
        }
    }

    async fn deliver(&mut self, publication: &Publication, scope: &DeliveryScope) -> bool {
        let mut proposal = self.engine.clone();
        match proposal.deliver(publication, scope) {
            Ok(true) => {}
            // No Local, expiry and stale route matches are successful no-ops,
            // unlike capacity/model failures. Preserve shared eligibility's bool.
            Ok(false) => {
                return self.broker.config.cluster.is_some();
            }
            Err(_) => return false,
        }
        let publications = proposal.take_publications();
        let effects = Effects {
            notify: true,
            housekeeping: false,
            volatile: publication.qos == 0 && self.broker.durable_cluster(),
            ..Effects::default()
        };
        if self.install(proposal, effects).await.is_err() {
            return false;
        }
        self.publish(publications).await;
        true
    }

    /// Fire-and-forget side effects of already-committed publications.
    async fn publish(&mut self, publications: Vec<Publication>) {
        for publication in publications {
            let _ = self.broker.apply_retained(&publication).await;
            if let Some(fanout) = self.broker.dispatch(Arc::new(publication), false) {
                let broker = Arc::clone(&self.broker);
                tokio::spawn(async move {
                    let _ = broker.complete(fanout).await;
                });
            }
        }
    }

    async fn request(
        &mut self,
        request: Request,
        principal: Option<Principal>,
        reply: oneshot::Sender<ResultReply>,
    ) {
        if reply.is_closed() {
            return;
        }
        if let Request::Connect(connect) = &request
            && let Some(lease) = &connect.cluster_lease
            && let Err(error) = self.activate(lease.clone())
        {
            let _ = reply.send(Err(error));
            return;
        }
        if self.broker.durable_cluster() && (self.cluster_invalid || self.cluster_lease.is_none()) {
            let _ = reply.send(Err(failure(
                ReasonCode::SERVER_UNAVAILABLE,
                "session authority unavailable",
            )));
            return;
        }
        if !self.broker.cluster_ready() && !matches!(&request, Request::Disconnect { .. }) {
            let _ = reply.send(Err(failure(
                ReasonCode::SERVER_UNAVAILABLE,
                "cluster metadata lease unavailable",
            )));
            return;
        }
        // A failed business admission does not mean that the broker is shutting
        // down. CONNECT uses the CONNACK-specific unavailable reason instead.
        let admission_failure_reason = if matches!(&request, Request::Connect(_)) {
            ReasonCode::SERVER_UNAVAILABLE
        } else {
            ReasonCode::SERVER_BUSY
        };
        let connect_attempt = match &request {
            Request::Connect(connect) => connect.access_attempt.clone(),
            _ => None,
        };
        let attempt = connect_attempt
            .clone()
            .or_else(|| operation_access::attempt(&request, &self.engine, &self.broker.config));
        let admitted = attempt.as_ref().map(|attempt| attempt.admitted_policy());
        let connect_lifecycle = match &request {
            Request::Connect(connect) => Some((connect.cancel.clone(), connect.deadline)),
            _ => None,
        };
        if let (Some(attempt), Request::Connect(connect), Some(principal)) =
            (&connect_attempt, &request, &principal)
            && !attempt.policy().authorize_for_client(
                principal,
                &connect.client,
                mqtt_auth::Action::Connect,
                &connect.client,
            )
        {
            let _ = reply.send(Ok(refusal(connect, ReasonCode::NOT_AUTHORIZED)));
            return;
        }
        let mut proposal;
        let rerun = match &request {
            Request::Handle {
                client,
                generation,
                frame,
                legacy,
                received_at_millis,
            } => Some(Rerun {
                client: client.clone(),
                generation: *generation,
                frame: frame.clone(),
                legacy: *legacy,
                received_at_millis: *received_at_millis,
            }),
            _ => None,
        };
        let staged = match &request {
            Request::Handle { frame, .. } => match subscribe_filters(frame) {
                Some(filters) => match self.broker.retained_matching(filters).await {
                    Ok(entries) => Some(entries),
                    Err(error) => {
                        let _ = reply.send(Err(error));
                        return;
                    }
                },
                None => None,
            },
            _ => None,
        };
        let max_preparations = match &request {
            Request::Handle {
                frame:
                    Frame {
                        packet: Packet::Subscribe { filters, .. },
                        ..
                    },
                ..
            } => filters.len().saturating_add(2),
            _ => 2,
        };
        let mut preparations = 0usize;
        let mut current_request = request;
        let mut current_principal = principal;
        let (response, mut effects) = loop {
            proposal = self.engine.clone();
            if let Some(attempt) = &attempt {
                proposal.set_access_control(operation_access::policy(
                    &current_request,
                    attempt.as_ref(),
                ));
            }
            let outcome = (|| {
                if let Some(entries) = staged.clone() {
                    proposal.stage_retained(entries).map_err(model_error)?;
                }
                let mut effects = Effects::default();
                let response = apply(
                    &mut proposal,
                    current_request,
                    current_principal.take(),
                    &self.online,
                    &self.broker,
                    &mut effects,
                );
                if let Some(admitted) = &admitted {
                    proposal.set_access_control(Arc::clone(admitted));
                }
                let response = response?;
                housekeeping(&mut proposal, &mut effects)?;
                if staged.is_some() {
                    proposal.clear_staged_retained().map_err(model_error)?;
                }
                Ok::<_, io::Error>((response, effects))
            })();
            if let Some(attempt) = &attempt
                && attempt.pending()
            {
                preparations = preparations.saturating_add(1);
                if preparations >= max_preparations {
                    let _ = reply.send(Err(failure(
                        admission_failure_reason,
                        "operation access preparation exhausted",
                    )));
                    return;
                }
                if attempt.resolve().await.is_err() {
                    let _ = reply.send(Err(failure(
                        admission_failure_reason,
                        "operation access service unavailable",
                    )));
                    return;
                }
                if reply.is_closed() {
                    return;
                }
                let Some(rerun) = &rerun else {
                    let _ = reply.send(Err(failure(
                        admission_failure_reason,
                        "unexpected pending CONNECT authorization",
                    )));
                    return;
                };
                if let Err(error) = valid_generation(&self.online, &rerun.client, rerun.generation)
                {
                    let _ = reply.send(Err(error));
                    return;
                }
                current_request = rerun.request();
                continue;
            }
            match outcome {
                Ok(value) => break value,
                Err(error) => {
                    // Even an error can have been produced by a speculative
                    // policy. Only report a verdict from a current attempt.
                    let error = if attempt.as_ref().is_some_and(|attempt| !attempt.seal()) {
                        failure(
                            admission_failure_reason,
                            "access policy changed before verdict",
                        )
                    } else {
                        error
                    };
                    let _ = reply.send(Err(error));
                    return;
                }
            }
        };
        if reply.is_closed()
            || connect_lifecycle
                .as_ref()
                .is_some_and(|(cancel, deadline)| {
                    *cancel.borrow() || deadline.is_some_and(|at| std::time::Instant::now() >= at)
                })
        {
            let _ = reply.send(Err(failure(
                admission_failure_reason,
                "operation cancelled",
            )));
            return;
        }
        if attempt.as_ref().is_some_and(|attempt| !attempt.seal()) {
            let _ = reply.send(Err(failure(
                admission_failure_reason,
                "access policy changed before admission",
            )));
            return;
        }
        let publications = proposal.take_publications();
        if self.broker.durable_cluster() {
            let mut ephemeral = Vec::new();
            let mut durable_pubs = Vec::new();
            for publication in publications {
                if publication.qos == 0 && !publication.retain {
                    ephemeral.push(publication);
                } else {
                    durable_pubs.push(publication);
                }
            }
            let structural = effects.filters_changed
                || effects.reset_session
                || effects.removal.is_some()
                || effects.registration.is_some()
                || effects.receipt.is_some();
            let persistent_changed = if durable_pubs.is_empty() || !proposal.has_persistent_state()
            {
                false
            } else {
                match proposal.snapshot() {
                    Ok(row) => self.row.as_ref() != Some(&row),
                    Err(error) => {
                        let _ = reply.send(Err(model_error(error)));
                        return;
                    }
                }
            };
            // Unchanged publisher state shares the node outbox, so concurrent
            // clients fill one HOST_BATCH quorum write instead of one record each.
            if !durable_pubs.is_empty() && !structural && !persistent_changed {
                if let Err(error) = self.broker.enqueue_host(&durable_pubs).await {
                    let _ = reply.send(Err(error));
                    return;
                }
                effects.publications.clear();
                effects.volatile = true;
                effects.commit = true;
                effects.notify = true;
            } else {
                if !durable_pubs.is_empty() {
                    effects.commit = true;
                    effects.notify = true;
                } else if effects.commit
                    && !structural
                    && (!ephemeral.is_empty() || !proposal.has_persistent_state())
                {
                    // A clean session has no snapshot. QoS 2 packet-id state and
                    // QoS 0 fanout stay in this process; a quorum write would not
                    // retain them and only stalls PUBREC.
                    effects.volatile = true;
                }
                effects.publications = durable_pubs;
            }
            let result = self.install(proposal, effects).await;
            if result.is_ok() {
                for publication in ephemeral {
                    self.broker.fanout_ephemeral(Arc::new(publication));
                }
            }
            let _ = reply.send(result.map(|()| response));
            return;
        }
        let wait = rerun.is_some()
            && publications
                .iter()
                .any(|p| p.qos > 0 || self.broker.config.cluster.is_some());
        let mut durability = Vec::new();
        for publication in &publications {
            match self.broker.apply_retained(publication).await {
                Ok(outcome) => durability.push(outcome),
                Err(error) => {
                    let _ = reply.send(Err(error));
                    return;
                }
            }
        }
        if let (true, Some(rerun)) = (wait, rerun) {
            for outcome in durability {
                if let Err(error) = self.broker.retained_durable(outcome).await {
                    let _ = reply.send(Err(error));
                    return;
                }
            }
            let fanouts: Vec<Fanout> = publications
                .into_iter()
                .filter_map(|publication| self.broker.dispatch(Arc::new(publication), true))
                .collect();
            let token = self.next_token;
            self.next_token = self.next_token.wrapping_add(1);
            self.pending.insert(
                token,
                Pending {
                    proposal,
                    version: self.version,
                    rerun,
                    response,
                    admitted,
                    reply,
                },
            );
            let broker = Arc::clone(&self.broker);
            let weak = self.weak.clone();
            tokio::spawn(async move {
                let mut result = Ok(());
                for fanout in fanouts {
                    if let Err(error) = broker.complete(fanout).await {
                        result = Err(error);
                        break;
                    }
                }
                if let Some(sender) = weak.upgrade() {
                    let _ = sender.send(ActorCommand::Finish { token, result }).await;
                }
            });
            return;
        }
        match self.install(proposal, effects).await {
            Ok(()) => {
                for publication in publications {
                    if let Some(fanout) = self.broker.dispatch(Arc::new(publication), false) {
                        let broker = Arc::clone(&self.broker);
                        tokio::spawn(async move {
                            let _ = broker.complete(fanout).await;
                        });
                    }
                }
                let _ = reply.send(Ok(response));
            }
            Err(error) => {
                let _ = reply.send(Err(error));
            }
        }
    }

    /// Recipients have committed; commit the publisher's own exchange. If the
    /// engine changed meanwhile, the request is re-run on current state.
    async fn finish(&mut self, token: u64) {
        let Some(pending) = self.pending.remove(&token) else {
            return;
        };
        let (proposal, response, effects) = if pending.version == self.version {
            (
                pending.proposal,
                pending.response,
                Effects {
                    notify: true,
                    ..Effects::default()
                },
            )
        } else {
            let mut proposal = self.engine.clone();
            if let Some(admitted) = &pending.admitted {
                proposal.set_access_control(Arc::clone(admitted));
            }
            let mut effects = Effects::default();
            let outcome = apply(
                &mut proposal,
                pending.rerun.request(),
                None,
                &self.online,
                &self.broker,
                &mut effects,
            )
            .and_then(|response| {
                housekeeping(&mut proposal, &mut effects)?;
                Ok(response)
            });
            match outcome {
                Ok(response) => {
                    proposal.discard_publications();
                    (proposal, response, effects)
                }
                Err(error) => {
                    let _ = pending.reply.send(Err(error));
                    return;
                }
            }
        };
        let result = self.install(proposal, effects).await.map(|()| response);
        let _ = pending.reply.send(result);
    }

    fn activate(&mut self, mut lease: SessionLease) -> io::Result<()> {
        if lease.client_id != self.client
            || lease.epoch == 0
            || lease
                .snapshot
                .as_ref()
                .is_some_and(|row| row.len() > 2 * 1024 * 1024)
        {
            return Err(failure(
                ReasonCode::SERVER_UNAVAILABLE,
                "invalid session authority",
            ));
        }
        if let Some(current) = &self.cluster_lease {
            if current.epoch > lease.epoch {
                return Err(failure(
                    ReasonCode::SESSION_TAKEN_OVER,
                    "stale session authority",
                ));
            }
            if !self.cluster_invalid
                && current.epoch == lease.epoch
                && current.revision >= lease.revision
            {
                return Ok(());
            }
        }
        let engine = match lease.snapshot.as_ref() {
            Some(row) => SessionEngine::restore_partition(
                self.broker.session_config,
                row,
                Arc::clone(&self.broker.config.access_control),
                Arc::clone(&self.broker.clock),
            ),
            None => SessionEngine::new_partition(
                self.broker.session_config,
                Arc::clone(&self.broker.config.access_control),
                Arc::clone(&self.broker.clock),
            ),
        }
        .map_err(model_error)?;
        if engine
            .subscription_filters()
            .keys()
            .any(|client| client != &self.client)
        {
            return Err(failure(
                ReasonCode::SERVER_UNAVAILABLE,
                "session identity mismatch",
            ));
        }
        let accounted = engine.live_state_bytes();
        self.broker.account(self.accounted, accounted)?;
        self.close_online(ReasonCode::SESSION_TAKEN_OVER);
        self.online.clear();
        self.online_generation.store(0, Ordering::Release);
        for (_, pending) in self.pending.drain() {
            let _ = pending.reply.send(Err(failure(
                ReasonCode::SESSION_TAKEN_OVER,
                "session authority changed",
            )));
        }
        self.filters = engine
            .subscription_filters()
            .remove(&self.client)
            .unwrap_or_default();
        self.broker.index.replace(&self.client, &self.filters);
        self.broker.cluster_state.filters(
            &self.client,
            &self.filters.iter().cloned().collect::<Vec<_>>(),
        );
        self.row = lease.snapshot.take();
        self.mirror_dirty = self.broker.persistent();
        self.cluster_lease = Some(lease);
        self.cluster_invalid = false;
        self.accounted = accounted;
        self.engine = engine;
        self.version = self.version.wrapping_add(1);
        self.fresh_until = None;
        Ok(())
    }

    /// Replica ownership without CONNECT: anchor crash Wills once and commit due
    /// publications. CONNECT activation must not take this path, or a resume would
    /// publish a delay the session engine is required to cancel.
    async fn activate_recovered(&mut self, lease: SessionLease) -> io::Result<()> {
        self.activate(lease)?;
        if !self.broker.durable_cluster() {
            return Ok(());
        }
        let mut proposal = self.engine.clone();
        proposal.recover_wills().map_err(model_error)?;
        if proposal.is_online(&self.client) && self.online.is_empty() {
            // Snapshot still marks the crashed connection online. A graceful
            // close drops that ghost without cancelling an already-pending Will.
            proposal
                .disconnect_with_reason(&self.client, true)
                .map_err(model_error)?;
        }
        let publications = proposal.take_publications();
        let snapshot = if proposal.has_persistent_state() {
            Some(proposal.snapshot().map_err(model_error)?)
        } else {
            None
        };
        if publications.is_empty() && snapshot == self.row {
            self.engine = proposal;
            return Ok(());
        }
        self.install(
            proposal,
            Effects {
                notify: true,
                housekeeping: false,
                publications,
                ..Effects::default()
            },
        )
        .await
    }

    async fn deliver_durable(
        &mut self,
        publication: &Publication,
        receipt: DeliveryId,
        scope: DeliveryScope,
    ) -> io::Result<()> {
        if !self.broker.cluster_ready() || self.cluster_invalid || self.cluster_lease.is_none() {
            return Err(failure(
                ReasonCode::SERVER_UNAVAILABLE,
                "session authority unavailable",
            ));
        }
        if self.volatile_receipt_seen(&receipt) {
            return Ok(());
        }
        let mut proposal = self.engine.clone();
        let _ = proposal.deliver(publication, &scope).map_err(model_error)?;
        let publications = proposal.take_publications();
        // A clean session has no snapshot. The receipt and the queued copy live
        // in this process; committing either would not survive a restart and
        // only adds a quorum write to every delivery.
        let volatile = self.broker.durable_cluster()
            && !proposal.has_persistent_state()
            && publications.is_empty();
        self.install(
            proposal,
            Effects {
                notify: true,
                housekeeping: false,
                volatile,
                receipt: if volatile {
                    None
                } else {
                    Some(receipt.clone())
                },
                publications,
                ..Effects::default()
            },
        )
        .await?;
        if volatile {
            self.remember_volatile_receipt(&receipt);
        }
        Ok(())
    }

    fn receipt_token(id: &DeliveryId) -> String {
        format!(
            "{}:{}:{}:{}:{}",
            id.source, id.epoch, id.revision, id.index, id.target_generation
        )
    }

    fn volatile_receipt_seen(&self, id: &DeliveryId) -> bool {
        self.volatile_receipt_index
            .contains(&Self::receipt_token(id))
    }

    fn remember_volatile_receipt(&mut self, id: &DeliveryId) {
        const LIMIT: usize = 8192;
        let token = Self::receipt_token(id);
        if !self.volatile_receipt_index.insert(token.clone()) {
            return;
        }
        self.volatile_receipts.push_back(token);
        while self.volatile_receipts.len() > LIMIT {
            if let Some(old) = self.volatile_receipts.pop_front() {
                self.volatile_receipt_index.remove(&old);
            }
        }
    }

    async fn commit_cluster(
        &mut self,
        proposal: &SessionEngine,
        effects: &mut Effects,
    ) -> io::Result<bool> {
        if self.cluster_invalid {
            return Err(failure(
                ReasonCode::SERVER_UNAVAILABLE,
                "uncertain session authority",
            ));
        }
        let lease = self.cluster_lease.as_ref().ok_or_else(|| {
            failure(
                ReasonCode::SERVER_UNAVAILABLE,
                "session authority unavailable",
            )
        })?;
        let snapshot = if proposal.has_persistent_state() {
            Some(proposal.snapshot().map_err(model_error)?)
        } else {
            None
        };
        let publications = effects
            .publications
            .iter()
            .map(|publication| super::cluster::message(publication, None))
            .collect();
        let filters = proposal
            .subscription_filters()
            .remove(&self.client)
            .unwrap_or_default()
            .into_iter()
            .collect();
        let bridge = self
            .broker
            .config
            .cluster
            .as_ref()
            .ok_or_else(|| failure(ReasonCode::SERVER_UNAVAILABLE, "cluster unavailable"))?;
        let mutation = SessionMutation {
            client_id: self.client.clone(),
            epoch: lease.epoch,
            expected_revision: lease.revision,
            snapshot,
            reset_session: effects.reset_session,
            filters,
            publications,
            receipt: effects.receipt.take(),
        };
        let committed =
            tokio::time::timeout(Duration::from_secs(5), bridge.commit_session(mutation))
                .await
                .map_err(|_| {
                    failure(ReasonCode::SERVER_UNAVAILABLE, "cluster commit deadline")
                })??;
        if let Some(lease) = &mut self.cluster_lease {
            lease.revision = committed.revision;
        }
        if !committed.duplicate {
            for publication in &effects.publications {
                self.broker.observe_cluster(publication, None);
            }
        }
        Ok(committed.duplicate)
    }

    async fn install(&mut self, proposal: SessionEngine, mut effects: Effects) -> io::Result<()> {
        if effects.commit {
            let bytes = proposal.live_state_bytes();
            self.broker.account(self.accounted, bytes)?;
            if self.broker.durable_cluster() && !effects.volatile {
                if let Err(error) = self
                    .broker
                    .commit_cluster_retained(&effects.publications)
                    .await
                {
                    let _ = self.broker.account(bytes, self.accounted);
                    return Err(error);
                }
                let result = self.commit_cluster(&proposal, &mut effects).await;
                match result {
                    Ok(true) => {
                        let _ = self.broker.account(bytes, self.accounted);
                        return Ok(());
                    }
                    Ok(false) => {}
                    Err(error) => {
                        tracing::error!(
                            event = "cluster_commit_failed",
                            client = %self.client,
                            error = %error,
                            "cluster session commit failed"
                        );
                        let _ = self.broker.account(bytes, self.accounted);
                        self.cluster_invalid = true;
                        self.close_online(ReasonCode::SERVER_UNAVAILABLE);
                        return Err(error);
                    }
                }
            }
            if self.broker.persistent() {
                let row = if proposal.has_persistent_state() {
                    match proposal.snapshot() {
                        Ok(row) => Some(row),
                        Err(error) => {
                            let _ = self.broker.account(bytes, self.accounted);
                            return Err(model_error(error));
                        }
                    }
                } else {
                    None
                };
                if self.mirror_dirty || row != self.row {
                    let change = match &row {
                        Some(bytes) => RowChange::PutSession(self.client.clone(), bytes.clone()),
                        None => RowChange::DeleteSession(self.client.clone()),
                    };
                    if let Err(error) = self.broker.commit(vec![change]).await {
                        tracing::error!(
                            event = "session_commit_failed",
                            client = %self.client,
                            error = %error,
                            "session commit failed"
                        );
                        let _ = self.broker.account(bytes, self.accounted);
                        self.cluster_invalid |= self.broker.durable_cluster();
                        self.close_online(ReasonCode::SERVER_UNAVAILABLE);
                        return Err(error);
                    }
                    self.row = row;
                    self.mirror_dirty = false;
                }
            }
            self.accounted = bytes;
            self.engine = proposal;
            self.version = self.version.wrapping_add(1);
            if let Some(observation) = &self.observation {
                observation
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .update(&self.engine, &self.client, effects.filters_changed);
            }
            if effects.filters_changed {
                let filters = self
                    .engine
                    .subscription_filters()
                    .remove(&self.client)
                    .unwrap_or_default();
                if filters != self.filters {
                    self.broker.index.replace(&self.client, &filters);
                    self.filters = filters;
                    let filters = self.filters.iter().cloned().collect::<Vec<_>>();
                    if self.broker.config.cluster.is_some() {
                        self.broker.cluster_state.filters(&self.client, &filters);
                    }
                    if let Some(cluster) = &self.broker.config.cluster
                        && !cluster.durable()
                        && !matches!(
                            tokio::time::timeout(
                                Duration::from_secs(5),
                                cluster.subscriptions(
                                    self.client.clone(),
                                    effects.registration.as_ref().map_or_else(
                                        || self.online_generation.load(Ordering::Acquire),
                                        |(_, connection)| connection.generation,
                                    ),
                                    filters
                                )
                            )
                            .await,
                            Ok(Ok(()))
                        )
                    {
                        self.close_online(ReasonCode::SERVER_UNAVAILABLE);
                        return Err(failure(
                            ReasonCode::SERVER_UNAVAILABLE,
                            "cluster subscription visibility failed",
                        ));
                    }
                }
            }
        }
        if let Some((client, connection)) = effects.registration {
            if self.broker.config.cluster.is_some() {
                self.broker.cluster_state.register(&client, &connection);
            }
            self.online_generation
                .store(connection.generation, Ordering::Release);
            let client_key = client.clone();
            if let Some(previous) = self.online.insert(client, connection) {
                self.broker
                    .cluster_state
                    .remove(&client_key, previous.generation);
                if let Some(reason) = &previous.close_reason {
                    let _ = reason.send(Some(ReasonCode::SESSION_TAKEN_OVER));
                }
                let _ = previous.cancel.send(true);
            }
        }
        if let Some(client) = effects.removal {
            if let Some(connection) = self.online.remove(&client) {
                self.broker
                    .cluster_state
                    .remove(&client, connection.generation);
                if let Some(cluster) = &self.broker.config.cluster {
                    let _ = tokio::time::timeout(
                        Duration::from_secs(5),
                        cluster.release(client.clone(), connection.generation),
                    )
                    .await;
                }
            }
            self.online_generation.store(0, Ordering::Release);
        }
        if effects.notify {
            notify(&self.engine, &self.online);
        }
        Ok(())
    }
}

fn expand_shared_suback(replies: &mut [Frame], packet_id: u16, overlay: &[Option<u8>]) {
    for frame in replies {
        let Packet::SubAck {
            packet_id: acknowledged,
            codes,
        } = &mut frame.packet
        else {
            continue;
        };
        if *acknowledged != packet_id {
            continue;
        }
        let mut granted = codes.iter().copied();
        *codes = overlay
            .iter()
            .map(|slot| slot.or_else(|| granted.next()).unwrap_or(0x80))
            .collect();
    }
}
