//! One tokio task per client identifier owning a one-client session partition.
//!
//! Requests are proposed on a clone, committed (row write when persistent) and
//! installed before replies or wake-ups. Client QoS>0 publications wait for
//! recipient commits before their own commit and acknowledgement.
use super::broker::{Broker, Fanout, subscribe_filters};
use super::*;
use mqtt_auth::Principal;
use mqtt_session::durable::{DeliveryScope, Publication};
use mqtt_storage::durable::RowChange;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) enum ActorCommand {
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
    notify: bool,
    housekeeping: bool,
    registration: Option<(String, Online)>,
    removal: Option<String>,
}
impl Default for Effects {
    fn default() -> Self {
        Self {
            commit: true,
            notify: false,
            housekeeping: true,
            registration: None,
            removal: None,
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
    reply: oneshot::Sender<ResultReply>,
}

struct Actor {
    broker: Arc<Broker>,
    client: String,
    id: u64,
    engine: SessionEngine,
    online: HashMap<String, Online>,
    online_generation: Arc<AtomicU64>,
    receiver: mpsc::Receiver<ActorCommand>,
    weak: mpsc::WeakSender<ActorCommand>,
    row: Option<Vec<u8>>,
    accounted: usize,
    filters: BTreeSet<String>,
    version: u64,
    next_token: u64,
    pending: HashMap<u64, Pending>,
    fresh_until: Option<tokio::time::Instant>,
    retry_at: Option<tokio::time::Instant>,
}

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
    let actor = Actor {
        broker,
        client,
        id,
        engine,
        online: HashMap::new(),
        online_generation: Arc::clone(&online),
        receiver,
        weak: sender.downgrade(),
        row,
        accounted,
        filters,
        version: 0,
        next_token: 0,
        pending: HashMap::new(),
        fresh_until,
        retry_at: None,
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
            effects.notify = true;
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
            frame,
            legacy,
            received_at_millis,
        } => {
            valid_generation(online, &client, generation)?;
            let replies = match received_at_millis {
                Some(received_at) => proposal.handle_frame_at(&client, frame, received_at),
                None => proposal.handle_frame(&client, frame),
            }
            .map_err(model_error)?;
            effects.notify = true;
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
            effects.commit = outcome.changed;
            effects.notify = outcome.changed;
            Ok(poll_reply(outcome.frame, legacy))
        }
        Request::PollDelivery { client, generation } => {
            valid_generation(online, &client, generation)?;
            let outcome = proposal
                .poll_delivery_with_changes(&client)
                .map_err(model_error)?;
            effects.commit = outcome.changed;
            effects.notify = outcome.changed;
            Ok(Reply::DeliveryPolled(outcome.delivery.map(Box::new)))
        }
        Request::BeginDelivery {
            client,
            generation,
            delivery,
        } => {
            valid_generation(online, &client, generation)?;
            let outcome = proposal
                .begin_delivery(&client, delivery)
                .map_err(model_error)?;
            effects.commit = outcome.changed;
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
        }
    }
    Ok(())
}

impl Actor {
    async fn run(mut self, _alive: mpsc::Sender<()>) {
        let mut fatal = self.broker.fatal.subscribe();
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
                changed = fatal.changed() => {
                    if changed.is_ok() && fatal.borrow().is_some() {
                        self.close_online(ReasonCode::SERVER_UNAVAILABLE);
                    }
                }
            }
        }
        for (_, pending) in self.pending.drain() {
            let _ = pending.reply.send(Err(failure(
                ReasonCode::SERVER_UNAVAILABLE,
                "session actor stopped",
            )));
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
        let expiry = self.engine.next_expiry_deadline().map(|at| {
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
            ActorCommand::Finish { token } => self.finish(token).await,
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
                    tokio::spawn(async move { broker.complete(fanout).await });
                }
            }
        }
    }

    async fn deliver(&mut self, publication: &Publication, scope: &DeliveryScope) -> bool {
        let mut proposal = self.engine.clone();
        if !matches!(proposal.deliver(publication, scope), Ok(true)) {
            return false;
        }
        let publications = proposal.take_publications();
        let effects = Effects {
            notify: true,
            housekeeping: false,
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
                tokio::spawn(async move { broker.complete(fanout).await });
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
        let mut proposal = self.engine.clone();
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
            Request::Handle { frame, .. } => {
                subscribe_filters(frame).map(|filters| self.broker.retained_for(filters))
            }
            _ => None,
        };
        let outcome = (|| {
            if let Some(entries) = staged.clone() {
                proposal.stage_retained(entries).map_err(model_error)?;
            }
            let mut effects = Effects::default();
            let response = apply(
                &mut proposal,
                request,
                principal,
                &self.online,
                &self.broker,
                &mut effects,
            )?;
            housekeeping(&mut proposal, &mut effects)?;
            if staged.is_some() {
                proposal.clear_staged_retained().map_err(model_error)?;
            }
            Ok::<_, io::Error>((response, effects))
        })();
        let (response, effects) = match outcome {
            Ok(value) => value,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        let publications = proposal.take_publications();
        let wait = rerun.is_some() && publications.iter().any(|p| p.qos > 0);
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
                    reply,
                },
            );
            let broker = Arc::clone(&self.broker);
            let weak = self.weak.clone();
            tokio::spawn(async move {
                for fanout in fanouts {
                    broker.complete(fanout).await;
                }
                if let Some(sender) = weak.upgrade() {
                    let _ = sender.send(ActorCommand::Finish { token }).await;
                }
            });
            return;
        }
        match self.install(proposal, effects).await {
            Ok(()) => {
                for publication in publications {
                    if let Some(fanout) = self.broker.dispatch(Arc::new(publication), false) {
                        let broker = Arc::clone(&self.broker);
                        tokio::spawn(async move { broker.complete(fanout).await });
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

    async fn install(&mut self, proposal: SessionEngine, effects: Effects) -> io::Result<()> {
        if effects.commit {
            let bytes = proposal.live_state_bytes();
            self.broker.account(self.accounted, bytes)?;
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
                if row != self.row {
                    let change = match &row {
                        Some(bytes) => RowChange::PutSession(self.client.clone(), bytes.clone()),
                        None => RowChange::DeleteSession(self.client.clone()),
                    };
                    if let Err(error) = self.broker.commit(vec![change]).await {
                        let _ = self.broker.account(bytes, self.accounted);
                        self.close_online(ReasonCode::SERVER_UNAVAILABLE);
                        return Err(error);
                    }
                    self.row = row;
                }
            }
            self.accounted = bytes;
            self.engine = proposal;
            self.version = self.version.wrapping_add(1);
            let filters = self
                .engine
                .subscription_filters()
                .remove(&self.client)
                .unwrap_or_default();
            if filters != self.filters {
                self.broker.index.replace(&self.client, &filters);
                self.filters = filters;
            }
        }
        if let Some((client, connection)) = effects.registration {
            self.online_generation
                .store(connection.generation, Ordering::Release);
            if let Some(previous) = self.online.insert(client, connection) {
                if let Some(reason) = &previous.close_reason {
                    let _ = reason.send(Some(ReasonCode::SESSION_TAKEN_OVER));
                }
                let _ = previous.cancel.send(true);
            }
        }
        if let Some(client) = effects.removal {
            self.online.remove(&client);
            self.online_generation.store(0, Ordering::Release);
        }
        if effects.notify {
            notify(&self.engine, &self.online);
        }
        Ok(())
    }
}
