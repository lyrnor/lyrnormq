//! Listener lifecycle and connection admission.
use crate::{BrokerConfig, durable};
use std::io;
use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
    time::Instant,
};

/// Serve until shutdown becomes true or its sender is dropped. All connection
/// tasks are joined before returning. Excess connections are closed immediately.
pub async fn serve(
    listener: TcpListener,
    config: BrokerConfig,
    shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    validate(&config)?;
    durable::serve(listener, config, shutdown).await
}

/// Serve the same broker/session machinery over server-authenticated TLS.
pub async fn serve_tls(
    listener: TcpListener,
    config: BrokerConfig,
    tls: crate::TlsConfig,
    shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    validate(&config)?;
    durable::serve_tls(listener, config, tls, shutdown).await
}

/// Serve MQTT over a bounded binary WebSocket endpoint.
pub async fn serve_ws(
    listener: TcpListener,
    config: BrokerConfig,
    options: crate::WebSocketOptions,
    shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    validate(&config)?;
    options.validate()?;
    durable::serve_websocket(listener, config, None, options, shutdown).await
}
/// Serve the WebSocket endpoint over the configured authenticated TLS identity.
pub async fn serve_wss(
    listener: TcpListener,
    config: BrokerConfig,
    tls: crate::TlsConfig,
    options: crate::WebSocketOptions,
    shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    validate(&config)?;
    options.validate()?;
    durable::serve_websocket(listener, config, Some(tls), options, shutdown).await
}

fn validate(config: &BrokerConfig) -> io::Result<()> {
    crate::admission::Admission::validate(config)?;
    if config.max_retained == 0
        || config.max_sessions == 0
        || config.max_state_bytes == 0
        || config.max_packet_size < 2
        || config.max_connections == 0
        || config.outbound_capacity == 0
        || config.max_subscriptions == 0
        || !(1..=65535).contains(&config.max_inflight)
        || config.handshake_timeout.is_zero()
        || config.write_timeout.is_zero()
        || Instant::now()
            .checked_add(config.handshake_timeout)
            .is_none()
        || Instant::now().checked_add(config.write_timeout).is_none()
        || config.outbound_capacity > Semaphore::MAX_PERMITS
        || config.max_connections > Semaphore::MAX_PERMITS
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid broker resource limits",
        ));
    }
    Ok(())
}
