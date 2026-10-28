//! Listener lifecycle and connection admission.
use crate::{
    BrokerConfig,
    connection::connection,
    durable,
    state::{State, lock, remove},
};
use std::{
    io,
    sync::{Arc, Mutex},
};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
    task::JoinSet,
    time::Instant,
};

/// Serve until shutdown becomes true or its sender is dropped. All connection
/// tasks are joined before returning. Excess connections are closed immediately.
pub async fn serve(
    listener: TcpListener,
    config: BrokerConfig,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    if config.max_sessions == 0
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
    if config.storage_path.is_some() {
        return durable::serve(listener, config, shutdown).await;
    }
    let state = Arc::new(Mutex::new(State::default()));
    let permits = Arc::new(Semaphore::new(config.max_connections));
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut tasks = JoinSet::new();
    let mut generation = 0u64;
    let result = loop {
        if *shutdown.borrow() {
            break Ok(());
        }
        tokio::select! {
            biased;
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break Ok(()); } }
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(error) = result { break Err(io::Error::other(error)); }
            }
            accepted = listener.accept() => {
                let (stream, _) = match accepted { Ok(value) => value, Err(error) => break Err(error) };
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else { drop(stream); continue; };
                generation = match generation.checked_add(1) { Some(value) => value, None => break Err(io::Error::other("connection generation exhausted")) };
                let state = Arc::clone(&state);
                let config = config.clone();
                let stop = stop_rx.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    let _ = connection(stream, generation, &state, &config, stop).await;
                    if let Ok(mut state) = lock(&state) { remove(&mut state, generation); }
                });
            }
        }
    };
    let _ = stop_tx.send(true);
    while let Some(joined) = tasks.join_next().await {
        if let Err(error) = joined {
            return Err(io::Error::other(error));
        }
    }
    result
}
