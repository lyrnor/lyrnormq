//! Packet writes, identifiers and connection deadlines.
use crate::BrokerConfig;
use mqtt_protocol::{Packet, encode};
use std::{future::pending, io, num::NonZeroU16};
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    time::{Instant, sleep_until, timeout},
};

pub(super) fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
pub(super) async fn write_packet(
    stream: &mut TcpStream,
    packet: &Packet,
    config: &BrokerConfig,
) -> io::Result<()> {
    let bytes = encode(packet).map_err(io::Error::other)?;
    if bytes.len() > config.max_packet_size {
        return Err(invalid("outgoing packet exceeds limit"));
    }
    timeout(config.write_timeout, stream.write_all(&bytes))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "client write timed out"))?
}
pub(super) fn packet_identifier(id: Option<u16>) -> io::Result<NonZeroU16> {
    id.and_then(NonZeroU16::new)
        .ok_or_else(|| invalid("missing or zero packet identifier"))
}

pub(super) async fn deadline(at: Option<Instant>) {
    match at {
        Some(at) => sleep_until(at).await,
        None => pending::<()>().await,
    }
}
