//! Bounded broker-origin publication ingress, separate from read-only management.
use super::{Broker, BrokerConfig};
use mqtt_protocol::Packet;
use mqtt_session::durable::Publication;
use std::{
    fmt, io,
    sync::{
        Arc, Mutex, PoisonError, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

const REQUESTS: u32 = 32;
const RESIDENT_BYTES: u32 = 16 * 1024 * 1024;
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);

/// A broker-origin message. `source` is observer metadata, never a MQTT Client ID.
/// No hidden session, CONNECT or Will is created by this ingress.
pub struct PublishMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
    pub source: String,
}
impl fmt::Debug for PublishMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishMessage")
            .field("topic_bytes", &self.topic.len())
            .field("payload_bytes", &self.payload.len())
            .field("qos", &self.qos)
            .field("retain", &self.retain)
            .finish_non_exhaustive()
    }
}
pub(super) struct Inner {
    broker: Mutex<Weak<Broker>>,
    ready: AtomicBool,
    claimed: AtomicBool,
    pub(super) slots: Arc<Semaphore>,
    pub(super) bytes: Arc<Semaphore>,
}
/// Host-authorized ingress installed on exactly one broker runtime.
/// Requests fail fast under pressure. Cancellation or timeout after acceptance may
/// leave retained state or partial fanout; callers must not infer retry deduplication.
#[derive(Clone)]
pub struct PublishHandle {
    pub(super) inner: Arc<Inner>,
}
impl fmt::Debug for PublishHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishHandle")
            .field("ready", &self.inner.ready.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}
impl Default for PublishHandle {
    fn default() -> Self {
        Self::new()
    }
}
impl PublishHandle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                broker: Mutex::new(Weak::new()),
                ready: AtomicBool::new(false),
                claimed: AtomicBool::new(false),
                slots: Arc::new(Semaphore::new(REQUESTS as usize)),
                bytes: Arc::new(Semaphore::new(RESIDENT_BYTES as usize)),
            }),
        }
    }
    pub(super) fn claim(&self) -> io::Result<()> {
        self.inner
            .claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| {
                problem(
                    io::ErrorKind::InvalidInput,
                    "publisher handle already claimed",
                )
            })
    }
    pub(super) fn attach(&self, broker: &Arc<Broker>) {
        *self
            .inner
            .broker
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Arc::downgrade(broker);
        self.inner.ready.store(true, Ordering::Release);
    }
    pub(super) fn stop(&self) {
        self.inner.ready.store(false, Ordering::Release);
        *self
            .inner
            .broker
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Weak::new();
    }
    pub(super) async fn drain(&self) {
        self.stop();
        // Operations have a five-second deadline. Keep actor/storage ownership
        // until active ingress futures finish, with a separate finite drain cap.
        let _ = tokio::time::timeout(
            Duration::from_secs(6),
            self.inner.slots.clone().acquire_many_owned(REQUESTS),
        )
        .await;
    }
    fn available(&self, broker: &Broker) -> bool {
        self.inner.ready.load(Ordering::Acquire) && broker.fatal.borrow().is_none()
    }
    /// Validate a privileged host publication, then reuse retained and
    /// ordinary/shared delivery paths. Success acknowledges broker acceptance only.
    pub async fn publish(&self, message: PublishMessage) -> io::Result<()> {
        validate(&message)?;
        if !self.inner.ready.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        let broker = self
            .inner
            .broker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .upgrade()
            .ok_or_else(unavailable)?;
        validate_packet_size(&message, &broker.config)?;
        if !broker.cluster_ready() {
            return Err(unavailable());
        }
        if broker.config.cluster.is_some() && message.retain {
            return Err(problem(
                io::ErrorKind::InvalidInput,
                "retained publishing is not supported in initial cluster gate",
            ));
        }
        let _slot = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable())?;
        let bytes = message
            .topic
            .len()
            .checked_add(message.payload.len())
            .and_then(|n| n.checked_add(message.source.len()))
            .and_then(|n| u32::try_from(n).ok())
            .filter(|n| *n <= RESIDENT_BYTES)
            .ok_or_else(unavailable)?;
        let _bytes = self
            .inner
            .bytes
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| unavailable())?;
        if !self.available(&broker) {
            return Err(unavailable());
        }
        let source = message.source;
        let publication = Arc::new(Publication {
            topic: message.topic,
            payload: message.payload,
            qos: message.qos,
            retain: message.retain,
            properties: Vec::new(),
            expires_at: None,
            publisher: None,
            expired: false,
        });
        tokio::time::timeout(PUBLISH_TIMEOUT, async {
            let retained = broker.apply_retained(&publication).await?;
            broker.retained_durable(retained).await?;
            if let Some(fanout) = broker.dispatch_observed(publication, true, Some(&source)) {
                broker.complete(fanout).await?;
            }
            Ok(())
        })
        .await
        .map_err(|_| problem(io::ErrorKind::WouldBlock, "publish deadline exceeded"))?
    }
}
fn problem(kind: io::ErrorKind, message: &'static str) -> io::Error {
    io::Error::new(kind, message)
}
fn unavailable() -> io::Error {
    problem(
        io::ErrorKind::WouldBlock,
        "publisher unavailable or capacity exhausted",
    )
}
fn validate(message: &PublishMessage) -> io::Result<()> {
    if message.topic == "$SYS" || message.topic.starts_with("$SYS/") {
        return Err(problem(
            io::ErrorKind::PermissionDenied,
            "reserved system topic",
        ));
    }
    if message.topic.len() > u16::MAX as usize {
        return Err(problem(io::ErrorKind::InvalidInput, "invalid topic or QoS"));
    }
    if message.source.is_empty() || message.source.len() > 1024 || message.source.contains('\0') {
        return Err(problem(
            io::ErrorKind::InvalidInput,
            "invalid publication source",
        ));
    }
    // Reuse protocol validation, including forbidden UTF-8 code points; no payload
    // clone is needed merely to validate a topic and QoS.
    mqtt_protocol::encode(&Packet::Publish {
        topic: message.topic.clone(),
        payload: Vec::new(),
        qos: message.qos,
        retain: message.retain,
        dup: false,
        packet_id: (message.qos > 0).then_some(1),
    })
    .map(|_| ())
    .map_err(|_| problem(io::ErrorKind::InvalidInput, "invalid topic or QoS"))
}
fn validate_packet_size(message: &PublishMessage, config: &BrokerConfig) -> io::Result<()> {
    // MQTT5 adds one empty property-length byte. With a dual-version listener use
    // that upper bound so every currently supported recipient format fits.
    let properties = usize::from(config.protocol_mode != crate::ProtocolMode::V311);
    let remaining = 2usize
        .checked_add(message.topic.len())
        .and_then(|n| n.checked_add(message.payload.len()))
        .and_then(|n| n.checked_add(if message.qos > 0 { 2 } else { 0 }))
        .and_then(|n| n.checked_add(properties))
        .filter(|n| *n <= 268_435_455)
        .ok_or_else(|| {
            problem(
                io::ErrorKind::InvalidData,
                "publication exceeds packet size limit",
            )
        })?;
    let mut bytes = 1;
    let mut value = remaining;
    while value >= 128 {
        bytes += 1;
        value /= 128;
    }
    if remaining.saturating_add(bytes + 1) > config.max_packet_size {
        return Err(problem(
            io::ErrorKind::InvalidData,
            "publication exceeds packet size limit",
        ));
    }
    Ok(())
}
