//! Embedded cluster role startup. Filesystem configuration loading is blocking-pool only.
use std::{
    io::{self, Read},
    path::Path,
    sync::Arc,
};
use tokio::sync::watch;

pub(crate) async fn load(path: Option<&Path>) -> io::Result<Option<mqtt_cluster::Config>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let meta = std::fs::symlink_metadata(&path)?;
        if !meta.is_file() || meta.len() > 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cluster config must be a regular file <=64KiB",
            ));
        }
        let mut bytes = Vec::new();
        std::fs::File::open(path)?
            .take(64 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cluster config exceeds limit",
            ));
        }
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid cluster config"))
    })
    .await
    .map_err(io::Error::other)?
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
pub(crate) async fn broker(
    config: mqtt_cluster::Config,
) -> io::Result<Arc<mqtt_cluster::BrokerRuntime>> {
    tokio::task::spawn_blocking(move || mqtt_cluster::BrokerRuntime::new(config))
        .await
        .map_err(io::Error::other)?
}
