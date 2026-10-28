//! Live clean-session packet processing and delivery.
use crate::{
    BrokerConfig,
    packet_io::{deadline, invalid, packet_identifier, write_packet},
    state::{Client, Delivery, State, active, lock, remove, route},
};
use mqtt_protocol::{Packet, ProtocolError, decode, encode};
use mqtt_router::validate_filter;
use mqtt_session::{AckAction, AckKind, IncomingQos2, OutgoingQos};
use std::{collections::HashSet, io, sync::Mutex, time::Duration};
use tokio::{
    io::AsyncReadExt,
    net::TcpStream,
    sync::{mpsc, watch},
    time::Instant,
};

async fn write_delivery(
    stream: &mut TcpStream,
    delivery: Delivery,
    outgoing: &mut OutgoingQos,
    config: &BrokerConfig,
) -> io::Result<()> {
    let Packet::Publish { topic, payload, .. } = delivery.publication.as_ref() else {
        return Err(invalid("delivery requires a PUBLISH"));
    };
    let mut packet = Packet::Publish {
        topic: topic.clone(),
        payload: payload.clone(),
        qos: delivery.qos,
        packet_id: if delivery.qos == 0 { None } else { Some(1) },
        dup: false,
        retain: false,
    };
    if delivery.qos != 0 {
        // The session retains an opaque encoded template, never reused as a
        // reconnect packet: this phase supports clean-session exchanges only.
        let bytes = encode(&packet).map_err(io::Error::other)?;
        let id = outgoing
            .start(delivery.qos, bytes)
            .map_err(io::Error::other)?;
        if let Packet::Publish { packet_id, .. } = &mut packet {
            *packet_id = Some(id.get());
        }
    }
    write_packet(stream, &packet, config).await
}

pub(super) async fn connection(
    mut stream: TcpStream,
    generation: u64,
    state: &Mutex<State>,
    config: &BrokerConfig,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let (outgoing, mut deliveries) = mpsc::channel(config.outbound_capacity);
    let (cancel_tx, mut cancel) = watch::channel(false);
    let mut connected = false;
    let mut subscriptions = HashSet::new();
    let mut incoming_qos2 = IncomingQos2::new(config.max_inflight, config.max_packet_size, 0);
    let mut outgoing_qos = OutgoingQos::new(config.max_inflight, config.max_packet_size);
    let mut keep_alive = None;
    let mut expires = Some(Instant::now() + config.handshake_timeout);
    let mut buffer = Vec::new();
    let mut scratch = [0u8; 8192];
    loop {
        if *shutdown.borrow() || *cancel.borrow() {
            return Ok(());
        }
        if expires.is_some_and(|at| Instant::now() >= at) {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "MQTT receive deadline exceeded",
            ));
        }
        match decode(&buffer, config.max_packet_size) {
            Ok(Some((packet, consumed))) => {
                let received_at = Instant::now();
                buffer.drain(..consumed);
                if !connected {
                    let Packet::Connect {
                        client_id,
                        clean_session: true,
                        keep_alive: seconds,
                    } = packet
                    else {
                        return Err(invalid("CONNECT required first"));
                    };
                    {
                        let mut state = lock(state)?;
                        if let Some(old) = state.identities.get(&client_id).copied() {
                            if let Some(client) = state.clients.get(&old) {
                                let _ = client.cancel.send(true);
                            }
                            remove(&mut state, old);
                        }
                        state.identities.insert(client_id, generation);
                        state.clients.insert(
                            generation,
                            Client {
                                outgoing: outgoing.clone(),
                                cancel: cancel_tx.clone(),
                            },
                        );
                    }
                    write_packet(
                        &mut stream,
                        &Packet::ConnAck {
                            session_present: false,
                            code: 0,
                        },
                        config,
                    )
                    .await?;
                    connected = true;
                    keep_alive = if seconds == 0 {
                        None
                    } else {
                        Some(Duration::from_millis(u64::from(seconds) * 1500))
                    };
                } else {
                    match packet {
                        publication @ Packet::Publish { .. } => {
                            let Packet::Publish {
                                qos,
                                packet_id,
                                dup,
                                ..
                            } = &publication
                            else {
                                return Err(invalid("PUBLISH expected"));
                            };
                            let (qos, packet_id, dup) = (*qos, *packet_id, *dup);
                            if qos == 2 {
                                {
                                    let state = lock(state)?;
                                    active(&state, generation)?;
                                }
                                let bytes = encode(&publication).map_err(io::Error::other)?;
                                incoming_qos2
                                    .publish(packet_identifier(packet_id)?, &bytes, dup)
                                    .map_err(io::Error::other)?;
                                write_packet(
                                    &mut stream,
                                    &Packet::PubRec {
                                        packet_id: packet_identifier(packet_id)?.get(),
                                    },
                                    config,
                                )
                                .await?;
                            } else {
                                route(state, generation, publication)?;
                                if qos == 1 {
                                    write_packet(
                                        &mut stream,
                                        &Packet::PubAck {
                                            packet_id: packet_identifier(packet_id)?.get(),
                                        },
                                        config,
                                    )
                                    .await?;
                                }
                            }
                        }
                        Packet::PubRel { packet_id } => {
                            {
                                let state = lock(state)?;
                                active(&state, generation)?;
                            }
                            let released =
                                incoming_qos2.pubrel(packet_identifier(Some(packet_id))?);
                            if let Some(bytes) = released.delivery {
                                let Some((publication, consumed)) =
                                    decode(&bytes, config.max_packet_size)
                                        .map_err(io::Error::other)?
                                else {
                                    return Err(invalid("incomplete stored PUBLISH"));
                                };
                                if consumed != bytes.len() {
                                    return Err(invalid("stored PUBLISH has trailing bytes"));
                                }
                                route(state, generation, publication)?;
                            }
                            write_packet(&mut stream, &Packet::PubComp { packet_id }, config)
                                .await?;
                        }
                        acknowledgement @ (Packet::PubAck { .. }
                        | Packet::PubRec { .. }
                        | Packet::PubComp { .. }) => {
                            let (packet_id, kind) = match acknowledgement {
                                Packet::PubAck { packet_id } => (packet_id, AckKind::PubAck),
                                Packet::PubRec { packet_id } => (packet_id, AckKind::PubRec),
                                Packet::PubComp { packet_id } => (packet_id, AckKind::PubComp),
                                _ => return Err(invalid("invalid acknowledgement")),
                            };
                            if outgoing_qos
                                .ack(packet_identifier(Some(packet_id))?, kind)
                                .map_err(io::Error::other)?
                                == AckAction::SendPubRel
                            {
                                write_packet(&mut stream, &Packet::PubRel { packet_id }, config)
                                    .await?;
                            }
                        }
                        Packet::Subscribe { packet_id, filters } => {
                            let codes = {
                                let mut state = lock(state)?;
                                active(&state, generation)?;
                                filters
                                    .into_iter()
                                    .map(|(filter, qos)| {
                                        if (!subscriptions.contains(&filter)
                                            && subscriptions.len() >= config.max_subscriptions)
                                            || !state.router.subscribe_qos(
                                                generation,
                                                filter.clone(),
                                                qos,
                                            )
                                        {
                                            0x80
                                        } else {
                                            subscriptions.insert(filter);
                                            qos
                                        }
                                    })
                                    .collect()
                            };
                            write_packet(&mut stream, &Packet::SubAck { packet_id, codes }, config)
                                .await?;
                        }
                        Packet::Unsubscribe { packet_id, filters } => {
                            if filters.iter().any(|filter| !validate_filter(filter)) {
                                return Err(invalid("invalid unsubscribe filter"));
                            }
                            {
                                let mut state = lock(state)?;
                                active(&state, generation)?;
                                for filter in filters {
                                    state.router.unsubscribe(generation, &filter);
                                    subscriptions.remove(&filter);
                                }
                            }
                            write_packet(&mut stream, &Packet::UnsubAck { packet_id }, config)
                                .await?;
                        }
                        Packet::PingReq => {
                            write_packet(&mut stream, &Packet::PingResp, config).await?
                        }
                        Packet::Disconnect => return Ok(()),
                        _ => {
                            return Err(invalid(
                                "unexpected client packet direction or repeated CONNECT",
                            ));
                        }
                    }
                }
                expires = keep_alive.map(|duration| received_at + duration);
                // Yield also under an endless stream of already-buffered packets.
                tokio::task::yield_now().await;
                continue;
            }
            Err(error) => {
                if !connected
                    && matches!(error, ProtocolError::UnsupportedVersion)
                    && buffer.first() == Some(&0x10)
                {
                    write_packet(
                        &mut stream,
                        &Packet::ConnAck {
                            session_present: false,
                            code: 1,
                        },
                        config,
                    )
                    .await?;
                }
                return Err(io::Error::new(io::ErrorKind::InvalidData, error));
            }
            Ok(None) => {}
        }
        let remaining = config.max_packet_size.saturating_sub(buffer.len());
        if remaining == 0 {
            return Err(invalid("receive buffer limit reached"));
        }
        let read_len = scratch.len().min(remaining);
        // Fair selection prevents continuous queued traffic starving ACK/PING reads.
        tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            _ = cancel.changed() => return Ok(()),
            _ = deadline(expires) => return Err(io::Error::new(io::ErrorKind::TimedOut, "MQTT receive deadline exceeded")),
            delivery = deliveries.recv(), if connected && !outgoing_qos.is_full() => {
                if let Some(delivery) = delivery { write_delivery(&mut stream, delivery, &mut outgoing_qos, config).await?; }
            }
            read = stream.read(&mut scratch[..read_len]) => {
                let size = read?;
                if size == 0 { return Ok(()); }
                buffer.extend_from_slice(&scratch[..size]);
            }
        }
    }
}

#[cfg(test)]
#[path = "live_tests.rs"]
mod tests;
