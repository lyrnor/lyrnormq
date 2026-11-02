//! A native owner retains the last producer reference; destruction never runs on Tokio.
use crate::KafkaConfig;
use rdkafka::{
    ClientConfig,
    client::ClientContext,
    config::RDKafkaLogLevel,
    error::KafkaError,
    producer::{FutureProducer, Producer, PurgeConfig},
};
use std::sync::{
    Arc, Mutex, PoisonError,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use tokio::sync::watch;
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
static OWNERS: AtomicUsize = AtomicUsize::new(0);
struct Slot;
impl Slot {
    fn reserve() -> Result<Self, BuildError> {
        let mut n = OWNERS.load(Ordering::Acquire);
        loop {
            if n >= 16 {
                return Err(BuildError::Busy);
            }
            match OWNERS.compare_exchange_weak(n, n + 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return Ok(Self),
                Err(actual) => n = actual,
            }
        }
    }
}
impl Drop for Slot {
    fn drop(&mut self) {
        OWNERS.fetch_sub(1, Ordering::AcqRel);
    }
}
pub(crate) struct Quiet {
    pub security_failure: Arc<AtomicUsize>,
}
impl ClientContext for Quiet {
    fn log(&self, _: RDKafkaLogLevel, _: &str, _: &str) {}
    fn error(&self, error: KafkaError, _: &str) {
        use rdkafka::error::RDKafkaErrorCode as C;
        match error.rdkafka_error_code() {
            Some(C::Authentication | C::SaslAuthenticationFailed | C::UnsupportedSASLMechanism) => {
                self.security_failure.store(1, Ordering::Relaxed);
            }
            Some(C::SSL) => {
                self.security_failure.store(2, Ordering::Relaxed);
            }
            _ => {}
        }
    }
}
pub(crate) type Native = FutureProducer<Quiet>;
pub(crate) struct Owner {
    pub producer: Mutex<Option<Arc<Native>>>,
    pub security_failure: Arc<AtomicUsize>,
    close: mpsc::SyncSender<()>,
    done: watch::Sender<bool>,
    #[cfg(test)]
    stop_gate: Mutex<Option<Arc<(std::sync::Barrier, std::sync::Barrier)>>>,
}
impl Owner {
    pub fn new(id: &str, c: &KafkaConfig, timeout: u64) -> Result<Self, BuildError> {
        c.validate()?;
        if !(1000..=30000).contains(&timeout) {
            return Err(BuildError::Invalid);
        }
        let slot = Slot::reserve()?;
        let c = c.clone();
        let id = id.to_owned();
        let security_failure = Arc::new(AtomicUsize::new(0));
        let security_state = Arc::clone(&security_failure);
        let (ready, receive) = mpsc::sync_channel(1);
        let (close, commands) = mpsc::sync_channel(1);
        let (done, _) = watch::channel(false);
        let finished = done.clone();
        std::thread::Builder::new()
            .name("rmqtt-kafka-owner".into())
            .spawn(move || {
                let _slot = slot;
                let built = build(&id, &c, timeout, security_state);
                match built {
                    Ok(p) => {
                        let p = Arc::new(p);
                        if ready.send(Ok(Arc::clone(&p))).is_ok() {
                            let _ = commands.recv();
                        }
                        p.purge(PurgeConfig::default().queue().inflight());
                        let _ = p.flush(std::time::Duration::from_millis(500));
                        drop(p);
                    }
                    Err(e) => {
                        let _ = ready.send(Err(e));
                    }
                }
                finished.send_replace(true);
            })
            .map_err(|_| BuildError::Busy)?;
        let p = receive.recv().map_err(|_| BuildError::Invalid)??;
        Ok(Self {
            producer: Mutex::new(Some(p)),
            security_failure,
            close,
            done,
            #[cfg(test)]
            stop_gate: Mutex::new(None),
        })
    }
    pub fn stop(&self) {
        let producer = self
            .producer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        #[cfg(test)]
        if producer.is_some()
            && let Some(gate) = self.stop_gate.lock().unwrap().take()
        {
            gate.0.wait();
            gate.1.wait();
        }
        // Only the caller that removed the adapter Arc may signal teardown.
        // A concurrent second shutdown must not let the owner drop its last Arc
        // while the first caller still holds the removed native reference.
        if let Some(producer) = producer {
            drop(producer);
            let _ = self.close.try_send(());
        }
    }
    pub async fn shutdown(&self) -> Result<(), mqtt_egress_core::SendError> {
        self.stop();
        let mut done = self.done.subscribe();
        if *done.borrow() {
            return Ok(());
        }
        let r = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !*done.borrow_and_update() {
                done.changed().await.map_err(|_| ())?;
            }
            Ok::<_, ()>(())
        })
        .await;
        match r {
            Ok(Ok(())) => Ok(()),
            _ => Err(mqtt_egress_core::SendError {
                kind: mqtt_egress_core::FailureKind::Retryable,
                code: "kafka_shutdown".into(),
            }),
        }
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.stop();
    }
}
fn build(
    id: &str,
    c: &KafkaConfig,
    timeout: u64,
    security_failure: Arc<AtomicUsize>,
) -> Result<Native, BuildError> {
    let mut n = ClientConfig::new();
    n.set("client.id", format!("rmqtt-{id}"))
        .set("bootstrap.servers", c.brokers.join(","))
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("max.in.flight.requests.per.connection", "5")
        .set("partitioner", "murmur2")
        .set("queue.buffering.max.messages", "512")
        .set("queue.buffering.max.kbytes", "8192")
        .set("message.max.bytes", "1048576")
        .set("message.timeout.ms", timeout.to_string())
        .set("request.timeout.ms", (timeout / 2).min(10000).to_string())
        .set("linger.ms", c.linger_ms.to_string())
        .set("batch.size", c.batch_bytes.to_string())
        .set("compression.type", c.compression.native())
        .set("security.protocol", c.security.protocol.native())
        .set("enable.ssl.certificate.verification", "true")
        .set("ssl.endpoint.identification.algorithm", "https");
    for (k, v) in [
        ("sasl.mechanism", &c.security.sasl_mechanism),
        ("sasl.username", &c.security.username),
        ("sasl.password", &c.security.password),
        ("ssl.ca.pem", &c.security.ca_pem),
        ("ssl.certificate.pem", &c.security.certificate_pem),
        ("ssl.key.pem", &c.security.private_key_pem),
        ("ssl.key.password", &c.security.private_key_password),
    ] {
        if let Some(v) = v {
            n.set(k, v);
        }
    }
    n.create_with_context(Quiet { security_failure })
        .map_err(|_| BuildError::Invalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn concurrent_shutdown_cannot_finish_while_first_caller_holds_native_reference() {
        let owner = Arc::new(
            Owner::new(
                "owner-test",
                &KafkaConfig {
                    brokers: vec!["127.0.0.1:1".into()],
                    topic: "events".into(),
                    ..KafkaConfig::default()
                },
                1000,
            )
            .unwrap(),
        );
        let gate = Arc::new((std::sync::Barrier::new(2), std::sync::Barrier::new(2)));
        *owner.stop_gate.lock().unwrap() = Some(Arc::clone(&gate));
        let first = Arc::clone(&owner);
        let thread = std::thread::spawn(move || first.stop());
        gate.0.wait();
        owner.stop();
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
        while !*owner.done.borrow() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let premature = *owner.done.borrow();
        gate.1.wait();
        thread.join().unwrap();
        assert!(
            !premature,
            "second shutdown signaled owner before first released native Arc"
        );
    }
}
#[cfg(test)]
mod callback_tests {
    use super::*;
    #[test]
    fn error_callback_keeps_only_structured_security_status() {
        let status = Arc::new(AtomicUsize::new(0));
        let ctx = Quiet {
            security_failure: Arc::clone(&status),
        };
        ctx.error(
            KafkaError::Global(rdkafka::error::RDKafkaErrorCode::Authentication),
            "secret-password",
        );
        assert_eq!(status.load(Ordering::Relaxed), 1);
        ctx.error(
            KafkaError::Global(rdkafka::error::RDKafkaErrorCode::SSL),
            "private-key-pem",
        );
        assert_eq!(status.load(Ordering::Relaxed), 2);
    }
}
