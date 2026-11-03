//! Bounded broker-origin client lifecycle publications.
use super::*;
use mqtt_session::durable::Publication;
use serde::Serialize;
use std::sync::{
    Weak,
    atomic::{AtomicU64, Ordering},
};

const CAPACITY: usize = 128;
// Two MQTT UTF-8 identity fields, each at most 65535 bytes; all other metadata
// has fixed size. Thus queued owned identity bytes are bounded below 17 MiB.
const IDENTITY_LIMIT: usize = 65535;

#[derive(Clone)]
pub(super) struct Connection {
    client: String,
    username: Option<String>,
    generation: u64,
    protocol: &'static str,
    connected_at_ms: u64,
}
struct Pending {
    connection: Connection,
    timestamp_ms: u64,
    reason: Option<&'static str>,
}
pub(super) struct Events {
    boot_id: String,
    sender: Mutex<Option<mpsc::Sender<Pending>>>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    dropped: AtomicU64,
}
#[derive(Serialize)]
struct Payload<'a> {
    event_id: String,
    event: &'static str,
    timestamp_ms: u64,
    boot_id: &'a str,
    sequence: u64,
    username: &'a Option<String>,
    client_id: &'a str,
    connection_id: String,
    protocol: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    connected_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_ms: Option<u64>,
}
impl Events {
    pub(super) fn new() -> io::Result<Self> {
        let mut random = [0u8; 16];
        getrandom::getrandom(&mut random).map_err(io::Error::other)?;
        let boot_id = random.iter().map(|byte| format!("{byte:02x}")).collect();
        Ok(Self {
            boot_id,
            sender: Mutex::new(None),
            task: Mutex::new(None),
            dropped: AtomicU64::new(0),
        })
    }
    pub(super) fn start(&self, broker: Weak<Broker>) {
        let (sender, mut receiver) = mpsc::channel::<Pending>(CAPACITY);
        *self.sender.lock().unwrap_or_else(PoisonError::into_inner) = Some(sender);
        let task = tokio::spawn(async move {
            let mut sequence = 0u64;
            while let Some(pending) = receiver.recv().await {
                let Some(broker) = broker.upgrade() else {
                    break;
                };
                let Some(next) = sequence.checked_add(1) else {
                    break;
                };
                sequence = next;
                let connection = pending.connection;
                let disconnected = pending.reason.is_some();
                let event = if disconnected {
                    "client.disconnected"
                } else {
                    "client.connected"
                };
                let payload = Payload {
                    event_id: format!("{}:{sequence}", broker.events.boot_id),
                    event,
                    timestamp_ms: pending.timestamp_ms,
                    boot_id: &broker.events.boot_id,
                    sequence,
                    username: &connection.username,
                    client_id: &connection.client,
                    connection_id: format!("{}:{}", broker.events.boot_id, connection.generation),
                    protocol: connection.protocol,
                    reason: pending.reason,
                    connected_at_ms: disconnected.then_some(connection.connected_at_ms),
                    duration_ms: disconnected.then_some(
                        pending
                            .timestamp_ms
                            .saturating_sub(connection.connected_at_ms),
                    ),
                };
                let Ok(payload) = serde_json::to_vec(&payload) else {
                    continue;
                };
                let publication = Arc::new(Publication {
                    topic: format!(
                        "$SYS/events/client/{}",
                        if disconnected {
                            "disconnected"
                        } else {
                            "connected"
                        }
                    ),
                    payload,
                    qos: 1,
                    retain: false,
                    properties: Vec::new(),
                    expires_at: None,
                    // System publications never originate from the affected client.
                    publisher: None,
                    expired: false,
                });
                if let Some(fanout) =
                    broker.dispatch_observed(publication, false, Some(&connection.client))
                {
                    broker.complete(fanout).await;
                }
            }
        });
        *self.task.lock().unwrap_or_else(PoisonError::into_inner) = Some(task);
    }
    pub(super) fn connected(
        &self,
        client: String,
        username: Option<String>,
        generation: u64,
        version: ProtocolVersion,
        now: u64,
    ) -> Option<Connection> {
        if client.len() > IDENTITY_LIMIT
            || username
                .as_ref()
                .is_some_and(|value| value.len() > IDENTITY_LIMIT)
        {
            return None;
        }
        let connection = Connection {
            client,
            username,
            generation,
            protocol: match version {
                ProtocolVersion::V311 => "3.1.1",
                ProtocolVersion::V5 => "5.0",
            },
            connected_at_ms: now,
        };
        self.submit(Pending {
            connection: connection.clone(),
            timestamp_ms: now,
            reason: None,
        });
        Some(connection)
    }
    pub(super) fn disconnected(&self, connection: Connection, now: u64, reason: &'static str) {
        self.submit(Pending {
            connection,
            timestamp_ms: now,
            reason: Some(reason),
        });
    }
    fn submit(&self, pending: Pending) {
        let sender = self.sender.lock().unwrap_or_else(PoisonError::into_inner);
        if sender
            .as_ref()
            .is_none_or(|sender| sender.try_send(pending).is_err())
        {
            let count = self
                .dropped
                .fetch_add(1, Ordering::Relaxed)
                .saturating_add(1);
            if count.is_power_of_two() {
                tracing::warn!(
                    dropped = count,
                    "client lifecycle event queue unavailable or full"
                );
            }
        }
    }
    pub(super) async fn shutdown(&self) {
        self.sender
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let task = self
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(mut task) = task
            && tokio::time::timeout(Duration::from_secs(2), &mut task)
                .await
                .is_err()
        {
            task.abort();
            let _ = task.await;
            tracing::warn!("client lifecycle event drain timed out");
        }
    }
}
