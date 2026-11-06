//! Embedded cluster role startup.
use std::{io, path::Path, sync::Arc};
use tokio::sync::watch;

/// Replicated memory sessions must not silently fall back to a SQLite mirror.
pub(crate) fn validate_data_options(
    config: Option<&mqtt_cluster::Config>,
    storage_path: Option<&Path>,
) -> io::Result<()> {
    if config.is_some_and(|config| config.durable.is_some()) && storage_path.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "replicated memory mode cannot mirror sessions in SQLite",
        ));
    }
    Ok(())
}

pub(crate) async fn controller(config: mqtt_cluster::Config) -> io::Result<()> {
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = tx.send(true);
        }
    });
    mqtt_cluster::run_controller(config, rx).await
}
pub(crate) async fn learner(config: mqtt_cluster::Config) -> io::Result<()> {
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = tx.send(true);
        }
    });
    mqtt_cluster::serve_repair_learner(config, rx).await
}
pub(crate) async fn broker(
    config: mqtt_cluster::Config,
) -> io::Result<Arc<mqtt_cluster::BrokerRuntime>> {
    tokio::task::spawn_blocking(move || mqtt_cluster::BrokerRuntime::new(config))
        .await
        .map_err(io::Error::other)?
}

/// Validate existing role configurations before either service opens its journal.
pub(crate) async fn validate_combined(
    broker: &mqtt_cluster::Config,
    controller: &mqtt_cluster::Config,
) -> io::Result<()> {
    broker.validate()?;
    controller.validate()?;
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "incompatible combined role configurations",
        )
    };
    if broker.role != mqtt_cluster::Role::Broker
        || controller.role != mqtt_cluster::Role::Controller
        || broker.cluster_id != controller.cluster_id
        || broker.node_id == controller.node_id
        || broker.listen == controller.listen
        || broker.lease_ms != controller.lease_ms
        || serde_json::to_value(&broker.controllers)?
            != serde_json::to_value(&controller.controllers)?
        || serde_json::to_value(&broker.tls)? != serde_json::to_value(&controller.tls)?
    {
        return Err(invalid());
    }
    let broker = broker.clone();
    let controller = controller.clone();
    tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&broker.data_dir)?;
        std::fs::create_dir_all(&controller.data_dir)?;
        let b = std::fs::canonicalize(&broker.data_dir)?;
        let c = std::fs::canonicalize(&controller.data_dir)?;
        if b.starts_with(&c)
            || c.starts_with(&b)
            || std::fs::canonicalize(&broker.token_file)?
                != std::fs::canonicalize(&controller.token_file)?
        {
            return Err(invalid());
        }
        Ok(())
    })
    .await
    .map_err(io::Error::other)?
}

/// Host both roles on one runtime, propagate failures and drain both on shutdown.
pub(crate) async fn combined<F>(
    config: mqtt_cluster::Config,
    broker_stop: watch::Sender<bool>,
    broker: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: std::future::Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let (control_stop, control_rx) = watch::channel(false);
    let control = mqtt_cluster::run_controller(config, control_rx);
    tokio::pin!(control, broker);
    tokio::select! {
        result = &mut broker => {
            let _ = control_stop.send(true);
            let stopped = tokio::time::timeout(std::time::Duration::from_secs(30), &mut control).await;
            result?;
            stopped.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "controller shutdown timed out"))??;
            Ok(())
        }
        result = &mut control => {
            let _ = broker_stop.send(true);
            let _ = tokio::time::timeout(std::time::Duration::from_secs(30), &mut broker).await;
            result?;
            Err(io::Error::other("combined controller stopped before broker").into())
        }
    }
}

#[cfg(test)]
#[path = "cluster_tests.rs"]
mod tests;
