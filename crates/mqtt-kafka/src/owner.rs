//! An isolated runtime fences every SDK task before uncertain replies or shutdown.
use crate::KafkaConfig;
use krafka::Record;
use mqtt_egress_core::{FailureKind, SendError};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
#[path = "owner_runtime.rs"]
mod runtime;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildError {
    Invalid,
    Busy,
}
impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Invalid => "invalid Kafka configuration",
            Self::Busy => "Kafka producer capacity exhausted",
        })
    }
}
impl std::error::Error for BuildError {}
thread_local! { static QUIET: std::cell::RefCell<Option<tracing::subscriber::DefaultGuard>> = const {std::cell::RefCell::new(None)}; }
static OWNERS: AtomicUsize = AtomicUsize::new(0);
struct Slot;
impl Slot {
    fn reserve() -> Result<Self, BuildError> {
        OWNERS
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < 16).then_some(n + 1)
            })
            .map(|_| Self)
            .map_err(|_| BuildError::Busy)
    }
}
impl Drop for Slot {
    fn drop(&mut self) {
        OWNERS.fetch_sub(1, Ordering::AcqRel);
    }
}
struct Reply {
    error: Option<SendError>,
    sender: oneshot::Sender<Result<(), SendError>>,
    _permit: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}
struct Command {
    record: Record,
    deadline: Instant,
    reply: Reply,
}
#[cfg(test)]
type FenceGate = Arc<std::sync::Mutex<Option<Arc<(std::sync::Barrier, std::sync::Barrier)>>>>;
pub(crate) struct Owner {
    sender: mpsc::Sender<Command>,
    close: watch::Sender<bool>,
    done: watch::Receiver<bool>,
    stopped: Arc<AtomicBool>,
    timeout: Duration,
    permits: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    #[cfg(test)]
    fence_gate: FenceGate,
}
pub(crate) fn retry(code: &str) -> SendError {
    SendError {
        kind: FailureKind::Retryable,
        code: code.into(),
    }
}
impl Owner {
    pub fn new(id: &str, c: &KafkaConfig, timeout: u64) -> Result<Self, BuildError> {
        c.validate()?;
        crate::tls_validation::validate(&c.security)?;
        if !(1000..=30000).contains(&timeout) {
            return Err(BuildError::Invalid);
        }
        let slot = Slot::reserve()?;
        let config = c.clone();
        let id = id.to_owned();
        let (sender, mut receiver) = mpsc::channel(512);
        let (close, mut closing) = watch::channel(false);
        let (finished, done) = watch::channel(false);
        let stopped = Arc::new(AtomicBool::new(false));
        let flag = stopped.clone();
        #[cfg(test)]
        let fence_gate = Arc::new(std::sync::Mutex::new(
            None::<Arc<(std::sync::Barrier, std::sync::Barrier)>>,
        ));
        #[cfg(test)]
        let worker_gate = fence_gate.clone();
        std::thread::Builder::new()
            .name("lyrnormq-kafka-owner".into())
            .spawn(move || {
                let _slot = slot;
                let _quiet =
                    tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
                while !flag.load(Ordering::Acquire) {
                    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .on_thread_start(|| {
                            QUIET.with(|slot| {
                                *slot.borrow_mut() = Some(tracing::subscriber::set_default(
                                    tracing::subscriber::NoSubscriber::default(),
                                ))
                            })
                        })
                        .on_thread_stop(|| {
                            QUIET.with(|slot| {
                                slot.borrow_mut().take();
                            })
                        })
                        .build()
                    else {
                        break;
                    };
                    let pending = rt.block_on(runtime::run(
                        &mut receiver,
                        &mut closing,
                        &config,
                        &id,
                        Duration::from_millis(timeout),
                    ));
                    // Drop joins async tasks and blocking compression work off the broker runtime.
                    #[cfg(test)]
                    if let Some(gate) = worker_gate.lock().unwrap().take() {
                        gate.0.wait();
                        gate.1.wait();
                    }
                    drop(rt);
                    for reply in pending {
                        let _ = reply.sender.send(Err(match reply.error {
                            Some(e) => e,
                            None => retry("kafka_delivery"),
                        }));
                    }
                    if *closing.borrow() {
                        break;
                    }
                }
                flag.store(true, Ordering::Release);
                drop(receiver);
                drop(_slot);
                finished.send_replace(true);
            })
            .map_err(|_| BuildError::Busy)?;
        Ok(Self {
            sender,
            close,
            done,
            stopped,
            timeout: Duration::from_millis(timeout),
            permits: Arc::new(Semaphore::new(512)),
            bytes: Arc::new(Semaphore::new(8 * 1024 * 1024)),
            #[cfg(test)]
            fence_gate,
        })
    }
    pub async fn send(&self, record: Record) -> Result<(), SendError> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(retry("kafka_closed"));
        }
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| retry("kafka_queue_full"))?;
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(
                u32::try_from(record.estimated_size().saturating_add(1024))
                    .map_err(|_| retry("kafka_queue_full"))?,
            )
            .map_err(|_| retry("kafka_queue_full"))?;
        let (reply, receive) = oneshot::channel();
        let reply = Reply {
            error: None,
            sender: reply,
            _permit: permit,
            _bytes: bytes,
        };
        let deadline = Instant::now() + self.timeout;
        self.sender
            .try_send(Command {
                record,
                deadline,
                reply,
            })
            .map_err(|_| retry("kafka_queue_full"))?;
        match tokio::time::timeout_at((deadline + Duration::from_secs(5)).into(), receive).await {
            Ok(answer) => answer.map_err(|_| retry("kafka_closed"))?,
            Err(_) => {
                self.stop();
                Err(retry("kafka_shutdown"))
            }
        }
    }
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.close.send_replace(true);
    }
    pub async fn shutdown(&self) -> Result<(), SendError> {
        self.stop();
        let mut done = self.done.clone();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !*done.borrow_and_update() {
                done.changed().await.map_err(|_| retry("kafka_shutdown"))?;
            }
            Ok(())
        })
        .await
        .map_err(|_| retry("kafka_shutdown"))?
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use krafka::testing::{ApiKey, Control, FakeBroker};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn caller_abort_retains_admission_and_concurrent_close_joins_fence() {
        let broker = FakeBroker::start().await.unwrap();
        broker.create_topic("events", 1);
        let owner = Arc::new(
            Owner::new(
                "cancel",
                &KafkaConfig {
                    brokers: vec![broker.bootstrap_servers()],
                    topic: "events".into(),
                    ..KafkaConfig::default()
                },
                5000,
            )
            .unwrap(),
        );
        owner
            .send(Record::new("events", "warm").key("c"))
            .await
            .unwrap();
        broker.on(ApiKey::Produce, |_| {
            Control::ApplyThen(Box::new(Control::Delay(Duration::from_millis(1200))))
        });
        let sending = owner.clone();
        let task = tokio::spawn(async move {
            sending
                .send(Record::new("events", "pending").key("c"))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while broker.request_count(ApiKey::Produce) < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        task.abort();
        let _ = task.await;
        assert_eq!(
            owner.permits.available_permits(),
            511,
            "cancelled caller must not free owner-held admission"
        );
        let (first, second) = tokio::join!(owner.shutdown(), owner.shutdown());
        first.unwrap();
        second.unwrap();
        assert!(*owner.done.borrow());
        assert_eq!(owner.permits.available_permits(), 512);
        let count = broker.request_count(ApiKey::Produce);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(broker.request_count(ApiKey::Produce), count);
        tokio::time::timeout(Duration::from_secs(3), async {
            while broker.open_connections() != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
}

#[cfg(test)]
mod deadline_tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stuck_teardown_bounds_rpc_stops_owner_and_retains_capacity_until_fenced() {
        use krafka::testing::{ApiKey, Control, FakeBroker};
        let broker = FakeBroker::start().await.unwrap();
        broker.create_topic("events", 1);
        broker.on(ApiKey::Produce, |_| Control::Silence);
        let owner = Arc::new(
            Owner::new(
                "stuck",
                &KafkaConfig {
                    brokers: vec![broker.bootstrap_servers()],
                    topic: "events".into(),
                    ..KafkaConfig::default()
                },
                1000,
            )
            .unwrap(),
        );
        let gate = Arc::new((std::sync::Barrier::new(2), std::sync::Barrier::new(2)));
        *owner.fence_gate.lock().unwrap() = Some(gate.clone());
        let sending = owner.clone();
        let task =
            tokio::spawn(
                async move { sending.send(Record::new("events", "value").key("c")).await },
            );
        let ready = gate.clone();
        tokio::task::spawn_blocking(move || ready.0.wait())
            .await
            .unwrap();
        assert_eq!(owner.permits.available_permits(), 511);
        let err = tokio::time::timeout(Duration::from_secs(7), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(err.code, "kafka_shutdown");
        assert!(!*owner.done.borrow());
        assert_eq!(owner.permits.available_permits(), 511);
        assert_eq!(
            owner
                .send(Record::new("events", "late"))
                .await
                .unwrap_err()
                .code,
            "kafka_closed"
        );
        tokio::task::spawn_blocking(move || gate.1.wait())
            .await
            .unwrap();
        owner.shutdown().await.unwrap();
        assert_eq!(owner.permits.available_permits(), 512);
    }
}
