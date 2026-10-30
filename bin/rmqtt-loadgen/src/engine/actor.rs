use super::{
    connection::Connection,
    state::State,
    timing::{deadline, nanos, rate_count, rate_deadline},
};
use crate::{
    LoadError, config,
    model::{PairData, RunConfig, RunData},
    payload::{self, Header},
};
use mqtt_protocol::{Frame, Packet};
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::watch,
    task::JoinSet,
    time::{sleep_until, timeout, timeout_at},
};

struct Pair {
    pair: u32,
    subscriber: Connection,
    publisher: Connection,
    topic: String,
    setup_ns: u64,
}
struct Clock {
    epoch: Instant,
    warm_start: u64,
    measure_start: u64,
    measure_end: u64,
    drain_end: u64,
    run_id: u64,
}
impl Clock {
    fn now(&self) -> Result<u64, LoadError> {
        nanos(self.epoch)
    }
}

async fn setup(config: Arc<RunConfig>, pair: u32, run_id: u64) -> Result<Pair, LoadError> {
    let began = Instant::now();
    let topic = format!("rmqtt-loadgen/{run_id}/{pair}");
    let mut subscriber = Connection::open(&config, client_id(run_id, pair, 's')).await?;
    subscriber
        .send(Packet::Subscribe {
            packet_id: 1,
            filters: vec![(topic.clone(), config.qos)],
        })
        .await?;
    let ack = timeout(
        Duration::from_millis(config.connect_timeout_ms),
        subscriber.recv(),
    )
    .await
    .map_err(|_| LoadError::Timeout("SUBACK"))??;
    match ack.packet {
        Packet::SubAck {
            packet_id: 1,
            codes,
        } if codes == [config.qos] => {}
        _ => {
            return Err(LoadError::InvalidData(format!(
                "subscription rejected/downgraded: {:?}",
                ack.packet
            )));
        }
    }
    let publisher = Connection::open(&config, client_id(run_id, pair, 'p')).await?;
    Ok(Pair {
        pair,
        subscriber,
        publisher,
        topic,
        setup_ns: u64::try_from(began.elapsed().as_nanos())
            .map_err(|_| LoadError::Capacity("setup time"))?,
    })
}

/// Run only after every paired subscriber and publisher completed setup.
pub async fn run(config: RunConfig) -> Result<RunData, LoadError> {
    config::validate(&config)?;
    let epoch = Instant::now();
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| LoadError::Config("run identifier clock before Unix epoch".into()))?;
    let run_id = (stamp.as_nanos() as u64) ^ u64::from(std::process::id());
    let config = Arc::new(config);
    let mut setup_tasks = JoinSet::new();
    for pair in 0..config.pairs {
        let c = config.clone();
        let p = u32::try_from(pair).map_err(|_| LoadError::Capacity("pair identity"))?;
        setup_tasks.spawn(setup(c, p, run_id));
    }
    let mut ready = Vec::with_capacity(config.pairs);
    while let Some(result) = setup_tasks.join_next().await {
        ready.push(result.map_err(|e| LoadError::Join(e.to_string()))??);
    }
    let warm_start = nanos(epoch)?;
    let measure_start = warm_start
        .checked_add(config.warmup_ms * 1_000_000)
        .ok_or(LoadError::Capacity("warmup deadline"))?;
    let measure_end = measure_start
        .checked_add(config.duration_ms * 1_000_000)
        .ok_or(LoadError::Capacity("measurement deadline"))?;
    let drain_end = measure_end
        .checked_add(config.drain_timeout_ms * 1_000_000)
        .ok_or(LoadError::Capacity("drain deadline"))?;
    let clock = Arc::new(Clock {
        epoch,
        warm_start,
        measure_start,
        measure_end,
        drain_end,
        run_id,
    });
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let mut tasks = JoinSet::new();
    for pair in ready {
        tasks.spawn(pair_task(
            pair,
            config.clone(),
            clock.clone(),
            cancel_rx.clone(),
        ));
    }
    let mut pairs = Vec::with_capacity(config.pairs);
    let mut failures = Vec::new();
    let mut finished_ns = 0;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok((data, stopped_ns)) => {
                finished_ns = finished_ns.max(stopped_ns);
                if let Some(e) = &data.error {
                    failures.push(format!("pair{}: {e}", data.pair));
                    let _ = cancel_tx.send(true);
                }
                pairs.push(data);
            }
            Err(e) => {
                failures.push(format!("pair task join: {e}"));
                let _ = cancel_tx.send(true);
            }
        }
    }
    pairs.sort_by_key(|p| p.pair);
    let config = Arc::try_unwrap(config)
        .map_err(|_| LoadError::Join("configuration still shared after tasks".into()))?;
    Ok(RunData {
        config,
        run_id,
        measured_start_ns: measure_start,
        measured_end_ns: measure_end,
        finished_ns,
        pairs,
        failures,
    })
}

async fn pair_task(
    mut pair: Pair,
    config: Arc<RunConfig>,
    clock: Arc<Clock>,
    mut cancel: watch::Receiver<bool>,
) -> (PairData, u64) {
    let mut state = State::new(pair.pair, pair.setup_ns);
    let result = match deadline(clock.epoch, clock.drain_end) {
        Ok(end) => timeout_at(
            tokio::time::Instant::from_std(end),
            drive(&mut pair, &config, &clock, &mut state, &mut cancel),
        )
        .await
        .map_err(|_| LoadError::Timeout("overall drain deadline"))
        .and_then(|result| result),
        Err(error) => Err(error),
    };
    if config.rate == 0 {
        state.data.counters.scheduled = state.data.counters.attempted;
    } else {
        state.data.counters.scheduled =
            rate_count(config.duration_ms, pair.pair, config.pairs, config.rate);
    }
    if let Err(e) = result {
        state.data.error = Some(e.to_string());
    }
    // Stop the elapsed-to-drain clock before bounded best-effort socket cleanup.
    let stopped_ns = match clock.now() {
        Ok(now) => now,
        Err(e) => {
            state.data.error = Some(e.to_string());
            clock.drain_end
        }
    };
    let cleanup = async {
        tokio::join!(
            pair.publisher.send(Packet::Disconnect),
            pair.subscriber.send(Packet::Disconnect)
        )
    };
    let _ = timeout(Duration::from_millis(10), cleanup).await;
    (state.data, stopped_ns)
}

async fn drive(
    pair: &mut Pair,
    config: &RunConfig,
    clock: &Clock,
    state: &mut State,
    cancel: &mut watch::Receiver<bool>,
) -> Result<(), LoadError> {
    let quota = config.quota(pair.pair);
    let window = effective_window(config.qos, config.inflight, pair.publisher.receive_maximum);
    loop {
        if *cancel.borrow() {
            return Err(LoadError::InvalidData(
                "cancelled after another pair failed".into(),
            ));
        }
        let now = clock.now()?;
        if now >= clock.measure_end && state.pending.is_empty() {
            return Ok(());
        }
        if now >= clock.drain_end {
            return Err(LoadError::Timeout("outstanding messages at drain deadline"));
        }
        let measured = now >= clock.measure_start;
        let phase_end = if measured {
            clock.measure_end
        } else {
            clock.measure_start
        };
        let local = if measured {
            state.measured_sent
        } else {
            state.warm_sent
        };
        let planned = if config.rate == 0 {
            now
        } else {
            rate_deadline(
                if measured {
                    clock.measure_start
                } else {
                    clock.warm_start
                },
                local,
                pair.pair,
                config.pairs,
                config.rate,
            )?
        };
        let can_offer = now < phase_end && state.pending.len() < window && planned < phase_end;
        if can_offer && planned <= now {
            if local >= quota {
                return Err(LoadError::Capacity(if measured {
                    "measured sample quota reached"
                } else {
                    "warmup message quota reached"
                }));
            }
            let seq = state.next_sequence;
            let id = if config.qos == 0 {
                None
            } else {
                Some(state.allocate_id()?)
            };
            let h = Header {
                run_id: clock.run_id,
                pair: pair.pair,
                measured,
                sequence: seq,
                scheduled_ns: planned,
                send_started_ns: clock.now()?,
            };
            if h.send_started_ns >= phase_end {
                continue;
            }
            let bytes = payload::encode(config.payload_bytes, h)?;
            state.begin(h, config.qos, id)?;
            state.next_sequence = state
                .next_sequence
                .checked_add(1)
                .ok_or(LoadError::Capacity("message sequence"))?;
            if measured {
                state.measured_sent += 1;
            } else {
                state.warm_sent += 1;
            }
            pair.publisher
                .send(Packet::Publish {
                    topic: pair.topic.clone(),
                    payload: bytes,
                    qos: config.qos,
                    packet_id: id,
                    dup: false,
                    retain: false,
                })
                .await?;
            state.wrote(seq, clock.now()?)?;
            continue;
        }
        let mut wake = if now < clock.measure_start {
            clock.measure_start
        } else if now < clock.measure_end {
            clock.measure_end
        } else {
            clock.drain_end
        };
        if can_offer {
            wake = wake.min(planned);
        }
        // Per-message protocol/delivery timeout is independent of idle rate periods.
        if let Some(oldest) = state
            .pending
            .values()
            .map(|p| p.header.send_started_ns)
            .min()
        {
            let io_deadline = oldest.saturating_add(config.io_timeout_ms * 1_000_000);
            if now >= io_deadline {
                return Err(LoadError::Timeout("message acknowledgement/delivery"));
            }
            wake = wake.min(io_deadline);
        }
        let wake = tokio::time::Instant::from_std(deadline(clock.epoch, wake)?);
        tokio::select! {
            result=pair.publisher.recv()=>{let frame=result?;let observed=clock.now()?;source_frame(pair,config,state,frame,observed).await?;},
            result=pair.subscriber.recv()=>{let frame=result?;let observed=clock.now()?;subscriber_frame(pair,config,clock,state,frame,observed).await?;},
            _=sleep_until(wake)=>{},
            result=cancel.changed()=>{if result.is_err() || *cancel.borrow(){return Err(LoadError::InvalidData("cancelled after another pair failed".into()));}},
        }
    }
}
fn positive(frame: &Frame) -> Result<(), LoadError> {
    if frame.reason_code.value() != 0 {
        return Err(LoadError::InvalidData(format!(
            "MQTT refusal reason{}",
            frame.reason_code.value()
        )));
    }
    Ok(())
}
async fn source_frame(
    pair: &mut Pair,
    _config: &RunConfig,
    state: &mut State,
    frame: Frame,
    now: u64,
) -> Result<(), LoadError> {
    positive(&frame)?;
    let (id, kind) = match frame.packet {
        Packet::PubAck { packet_id } => (packet_id, 4),
        Packet::PubRec { packet_id } => (packet_id, 5),
        Packet::PubComp { packet_id } => (packet_id, 7),
        Packet::PingResp => return Ok(()),
        other => {
            state.data.counters.unexpected += 1;
            return Err(LoadError::InvalidData(format!(
                "unexpected publisher packet {other:?}"
            )));
        }
    };
    if state.source_ack(id, kind, now)? {
        pair.publisher
            .send(Packet::PubRel { packet_id: id })
            .await?;
    }
    Ok(())
}
async fn subscriber_frame(
    pair: &mut Pair,
    config: &RunConfig,
    clock: &Clock,
    state: &mut State,
    frame: Frame,
    now: u64,
) -> Result<(), LoadError> {
    positive(&frame)?;
    match frame.packet {
        Packet::Publish {
            topic,
            payload,
            qos,
            packet_id,
            retain,
            ..
        } => {
            if topic != pair.topic
                || qos != config.qos
                || retain
                || payload.len() != config.payload_bytes
            {
                state.data.counters.invalid_payload += 1;
                return Err(LoadError::InvalidData(
                    "delivery topic/QoS/retain/length".into(),
                ));
            }
            let h = match payload::decode(&payload) {
                Ok(h) => h,
                Err(e) => {
                    state.data.counters.invalid_payload += 1;
                    return Err(e);
                }
            };
            if h.run_id != clock.run_id {
                state.data.counters.invalid_payload += 1;
                return Err(LoadError::InvalidData("delivery run identifier".into()));
            }
            state
                .validate_header(h, now)
                .inspect_err(|_| state.data.counters.invalid_payload += 1)?;
            match qos {
                0 => state.deliver(h, now)?,
                1 => {
                    state.deliver(h, now)?;
                    pair.subscriber
                        .send(Packet::PubAck {
                            packet_id: packet_id.ok_or_else(|| {
                                LoadError::InvalidData("QoS1 delivery missing ID".into())
                            })?,
                        })
                        .await?;
                }
                2 => {
                    let id = packet_id
                        .ok_or_else(|| LoadError::InvalidData("QoS2 delivery missing ID".into()))?;
                    if let Some(old) = state.incoming.get(&id) {
                        if *old != h {
                            return Err(LoadError::InvalidData(
                                "QoS2 ID reused before PUBREL".into(),
                            ));
                        }
                    } else {
                        if state.incoming.len() >= usize::from(config.inflight) {
                            return Err(LoadError::Capacity("incoming QoS2 window"));
                        }
                        state.incoming_done.remove(&id);
                        state.incoming.insert(id, h);
                    }
                    pair.subscriber
                        .send(Packet::PubRec { packet_id: id })
                        .await?;
                }
                _ => return Err(LoadError::InvalidData("invalid delivery QoS".into())),
            }
        }
        Packet::PubRel { packet_id } => {
            if let Some(h) = state.incoming.remove(&packet_id) {
                state.deliver(h, now)?;
                state.incoming_done.insert(packet_id);
            } else if !state.incoming_done.contains(&packet_id) {
                state.data.counters.unexpected += 1;
                return Err(LoadError::InvalidData("unknown subscriber PUBREL".into()));
            }
            pair.subscriber.send(Packet::PubComp { packet_id }).await?;
        }
        Packet::PingResp => {}
        other => {
            state.data.counters.unexpected += 1;
            return Err(LoadError::InvalidData(format!(
                "unexpected subscriber packet {other:?}"
            )));
        }
    }
    Ok(())
}

/// MQTT 5 Receive Maximum governs only QoS 1 and QoS 2 publications.
pub(super) fn effective_window(qos: u8, configured: u16, peer: u16) -> usize {
    usize::from(if qos == 0 {
        configured
    } else {
        configured.min(peer)
    })
}

/// Distinct run, pair and role identifiers within MQTT 3.1.1's required 23-byte range.
pub(super) fn client_id(run_id: u64, pair: u32, role: char) -> String {
    format!("lg{run_id:016x}{pair:04x}{role}")
}
