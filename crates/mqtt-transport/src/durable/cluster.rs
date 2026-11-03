//! Bounded cluster hooks and actor-independent connection fencing.
use super::*;
use mqtt_cluster_core::{
    ClientSubscriptions, ClusterFuture, ClusterMessage, DeliveryId, ForwardScope, LocalBroker,
    SessionLease,
};
use mqtt_session::durable::Publication;
use std::sync::{PoisonError, Weak};

const FORWARDS: usize = 32;
const BYTES: usize = 16 * 1024 * 1024;
const DEADLINE: Duration = Duration::from_secs(5);

pub(super) struct State {
    connections: Mutex<HashMap<(String, u64), Online>>,
    filters: Mutex<HashMap<String, Vec<String>>>,
    slots: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
}
impl Default for State {
    fn default() -> Self {
        Self {
            connections: Mutex::new(HashMap::new()),
            filters: Mutex::new(HashMap::new()),
            slots: Arc::new(Semaphore::new(FORWARDS)),
            bytes: Arc::new(Semaphore::new(BYTES)),
        }
    }
}
impl State {
    pub(super) fn register(&self, client: &str, online: &Online) {
        self.connections
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((client.to_owned(), online.generation), online.clone());
    }
    pub(super) fn remove(&self, client: &str, generation: u64) {
        self.connections
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(client.to_owned(), generation));
    }
    pub(super) fn filters(&self, client: &str, filters: &[String]) {
        let mut current = self.filters.lock().unwrap_or_else(PoisonError::into_inner);
        if filters.is_empty() {
            current.remove(client);
        } else {
            current.insert(client.to_owned(), filters.to_vec());
        }
    }
    fn fence(&self, client: Option<(&str, u64)>) {
        let connections: Vec<_> = self
            .connections
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|((id, generation), _)| {
                client.is_none_or(|(target, expected)| id == target && *generation == expected)
            })
            .map(|(_, value)| value.clone())
            .collect();
        for connection in connections {
            if let Some(close) = connection.close_reason {
                let _ = close.send(Some(if client.is_some() {
                    ReasonCode::SESSION_TAKEN_OVER
                } else {
                    ReasonCode::SERVER_UNAVAILABLE
                }));
            }
            let _ = connection.cancel.send(true);
        }
    }
}
struct Local {
    broker: Weak<Broker>,
}
impl std::fmt::Debug for Local {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterLocalBroker").finish_non_exhaustive()
    }
}
fn unavailable() -> io::Error {
    failure(
        ReasonCode::SERVER_UNAVAILABLE,
        "cluster unavailable or capacity exhausted",
    )
}
fn validate_message(message: &ClusterMessage, max_size: usize) -> io::Result<()> {
    let validation = Frame {
        version: ProtocolVersion::V5,
        packet: Packet::Publish {
            topic: message.topic.clone(),
            payload: message.payload.clone(),
            qos: message.qos,
            retain: false,
            dup: false,
            packet_id: (message.qos > 0).then_some(1),
        },
        properties: message.properties.clone(),
        ..Frame::v5(Packet::PingReq)
    };
    if mqtt_protocol::encode_frame(&validation)
        .map_err(io::Error::other)?
        .len()
        > max_size
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cluster message too large",
        ));
    }
    Ok(())
}
impl LocalBroker for Local {
    fn activate_session(&self, lease: SessionLease) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            let broker = self.broker.upgrade().ok_or_else(unavailable)?;
            if !broker.durable_cluster() {
                return Err(unavailable());
            }
            let handle = broker.get_or_spawn(&lease.client_id)?;
            let (reply, response) = oneshot::channel();
            tokio::time::timeout(DEADLINE, async {
                handle
                    .sender
                    .send(super::actor::ActorCommand::Activate { lease, reply })
                    .await
                    .map_err(|_| unavailable())?;
                response.await.map_err(|_| unavailable())?
            })
            .await
            .map_err(|_| unavailable())?
        })
    }
    fn deliver_to(
        &self,
        client_id: String,
        receipt: DeliveryId,
        message: ClusterMessage,
    ) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            let broker = self.broker.upgrade().ok_or_else(unavailable)?;
            if !broker.durable_cluster()
                || !broker.cluster_ready()
                || message.retain
                || message.scope != ForwardScope::Ordinary
            {
                return Err(unavailable());
            }
            validate_message(&message, broker.config.max_packet_size)?;
            let handle = broker.handle(&client_id).ok_or_else(unavailable)?;
            let publication = Arc::new(Publication {
                topic: message.topic,
                payload: message.payload,
                qos: message.qos,
                retain: false,
                properties: message.properties,
                expires_at: message.expires_at,
                publisher: message.publisher,
                expired: false,
            });
            let (reply, response) = oneshot::channel();
            tokio::time::timeout(DEADLINE, async {
                handle
                    .sender
                    .send(super::actor::ActorCommand::DurableDeliver {
                        publication,
                        receipt,
                        reply,
                    })
                    .await
                    .map_err(|_| unavailable())?;
                response.await.map_err(|_| unavailable())?
            })
            .await
            .map_err(|_| unavailable())?
        })
    }

    fn deliver(&self, message: ClusterMessage) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            let broker = self.broker.upgrade().ok_or_else(unavailable)?;
            if !broker.cluster_ready() {
                return Err(unavailable());
            }
            if message.retain || message.scope != ForwardScope::Ordinary {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "cluster delivery feature unsupported",
                ));
            }
            validate_message(&message, broker.config.max_packet_size)?;
            let publication = Arc::new(Publication {
                topic: message.topic,
                payload: message.payload,
                qos: message.qos,
                retain: false,
                properties: message.properties,
                expires_at: message.expires_at,
                publisher: message.publisher,
                expired: false,
            });
            tokio::time::timeout(DEADLINE, async {
                if let Some(fanout) = broker.dispatch_received(publication, true) {
                    broker.complete(fanout).await?;
                }
                Ok(())
            })
            .await
            .map_err(|_| unavailable())?
        })
    }
    fn fence(&self, client_id: String, generation: u64) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            if let Some(broker) = self.broker.upgrade() {
                broker.cluster_state.fence(Some((&client_id, generation)));
            }
            Ok(())
        })
    }
    fn fence_all(&self) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            if let Some(broker) = self.broker.upgrade() {
                broker.cluster_state.fence(None);
            }
            Ok(())
        })
    }
    fn subscriptions(&self) -> ClusterFuture<'_, Vec<ClientSubscriptions>> {
        Box::pin(async move {
            let broker = self.broker.upgrade().ok_or_else(unavailable)?;
            let filters = broker
                .cluster_state
                .filters
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            Ok(filters
                .iter()
                .map(|(client_id, filters)| ClientSubscriptions {
                    client_id: client_id.clone(),
                    filters: filters.clone(),
                })
                .collect())
        })
    }
}
pub(super) fn message(publication: &Publication, observer_client: Option<&str>) -> ClusterMessage {
    ClusterMessage {
        topic: publication.topic.clone(),
        payload: publication.payload.clone(),
        qos: publication.qos,
        retain: publication.retain,
        properties: publication.properties.clone(),
        expires_at: publication.expires_at,
        publisher: publication.publisher.clone(),
        observer_client: observer_client
            .or(publication.publisher.as_deref())
            .unwrap_or("")
            .to_owned(),
        scope: ForwardScope::Ordinary,
    }
}
impl Broker {
    pub(super) fn durable_cluster(&self) -> bool {
        self.config
            .cluster
            .as_ref()
            .is_some_and(|bridge| bridge.durable())
    }
    pub(super) fn observe_cluster(&self, publication: &Publication, observer_client: Option<&str>) {
        if let Some(observer) = &self.config.publication_observer
            && !publication
                .expires_at
                .is_some_and(|at| at <= self.clock.now_millis())
        {
            observer.observe(
                &publication.topic,
                &publication.payload,
                observer_client
                    .or(publication.publisher.as_deref())
                    .unwrap_or(""),
                publication.qos,
                publication.retain,
            );
        }
    }

    pub(super) async fn release_cluster(&self, client: &str, generation: u64) {
        if let Some(cluster) = &self.config.cluster {
            self.cluster_state.remove(client, generation);
            let _ = tokio::time::timeout(DEADLINE, cluster.release(client.to_owned(), generation))
                .await;
        }
    }
    pub(super) fn cluster_ready(&self) -> bool {
        self.config
            .cluster
            .as_ref()
            .is_none_or(|cluster| cluster.ready())
            && self.fatal.borrow().is_none()
    }
    pub(super) async fn open_cluster(self: &Arc<Self>) -> io::Result<()> {
        if let Some(cluster) = &self.config.cluster {
            tokio::time::timeout(
                DEADLINE,
                cluster.open(Arc::new(Local {
                    broker: Arc::downgrade(self),
                })),
            )
            .await
            .map_err(|_| unavailable())??;
        }
        Ok(())
    }
    /// Admission is synchronous and bounded. The spawned task owns both permits.
    pub(super) fn forward_cluster(
        &self,
        publication: Arc<Publication>,
        observer_client: Option<&str>,
    ) -> Option<oneshot::Receiver<bool>> {
        let bridge = self.config.cluster.as_ref()?.clone();
        let (tx, rx) = oneshot::channel();
        let properties_bytes = match serde_json::to_vec(&publication.properties) {
            Ok(value) => value.len(),
            Err(_) => {
                let _ = tx.send(false);
                return Some(rx);
            }
        };
        let bytes = publication
            .payload
            .len()
            .saturating_add(publication.topic.len())
            .saturating_add(properties_bytes)
            .saturating_add(1024);
        let permits = if !bridge.ready() || bytes > BYTES {
            None
        } else {
            self.cluster_state
                .slots
                .clone()
                .try_acquire_owned()
                .ok()
                .and_then(|slot| {
                    self.cluster_state
                        .bytes
                        .clone()
                        .try_acquire_many_owned(bytes as u32)
                        .ok()
                        .map(|bytes| (slot, bytes))
                })
        };
        let Some(permits) = permits else {
            let _ = tx.send(false);
            return Some(rx);
        };
        let message = message(&publication, observer_client);
        let observer = self.config.publication_observer.clone();
        let clock = self.clock.clone();
        let durable = bridge.durable();
        tokio::spawn(async move {
            let _permits = permits;
            let outcome = tokio::time::timeout(DEADLINE, bridge.forward(message.clone())).await;
            let success = matches!(outcome, Ok(Ok(())));
            if success
                && durable
                && !message
                    .expires_at
                    .is_some_and(|at| at <= clock.now_millis())
                && let Some(observer) = observer
            {
                observer.observe(
                    &message.topic,
                    &message.payload,
                    &message.observer_client,
                    message.qos,
                    message.retain,
                );
            }
            let _ = tx.send(success);
        });
        Some(rx)
    }
}
