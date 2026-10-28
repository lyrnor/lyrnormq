mod auth_policy;
mod cli;

use mqtt_transport::serve;
use std::error::Error;
use tokio::{net::TcpListener, sync::watch};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let Some(mut options) = cli::parse(std::env::args().skip(1))? else {
        println!("{}", cli::USAGE);
        return Ok(());
    };
    if let Some(path) = options.auth_policy {
        let policy = tokio::task::spawn_blocking(move || auth_policy::load(&path)).await??;
        options.config.access_control = std::sync::Arc::new(policy);
    }
    if options.enhanced_auth_plain {
        options.config.enhanced_auth = Some(std::sync::Arc::new(
            mqtt_auth::PlainAuthenticator::new(options.config.access_control.clone()),
        ));
    }
    let listener = TcpListener::bind(&options.address).await?;
    eprintln!("rmqtt listening on {}", listener.local_addr()?);
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = tx.send(true);
        }
    });
    serve(listener, options.config, rx).await?;
    Ok(())
}
