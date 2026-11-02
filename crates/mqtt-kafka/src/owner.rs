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
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
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
pub(crate) struct Owner {
    sender: mpsc::Sender<Command>,
    close: watch::Sender<bool>,
    done: watch::Receiver<bool>,
    stopped: Arc<AtomicBool>,
    timeout: Duration,
    permits: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
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
        std::thread::Builder::new()
            .name("rmqtt-kafka-owner".into())
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
        self.sender
            .try_send(Command {
                record,
                deadline: Instant::now() + self.timeout,
                reply,
            })
            .map_err(|_| retry("kafka_queue_full"))?;
        receive.await.map_err(|_| retry("kafka_closed"))?
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
