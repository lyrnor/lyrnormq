mod allocator;
mod auth_policy;
mod cli;
mod tls_identity;

use mqtt_transport::{export_state, serve, serve_tls, serve_ws, serve_wss};
use std::{error::Error, io::Write};
use tokio::{net::TcpListener, sync::watch};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let Some(mut options) = cli::parse(std::env::args().skip(1))? else {
        println!("{}", cli::USAGE);
        return Ok(());
    };
    if options.export_state {
        let config = options.config;
        let state = tokio::task::spawn_blocking(move || export_state(&config)).await??;
        if let Some(bytes) = state {
            let mut stdout = std::io::stdout().lock();
            stdout.write_all(&bytes)?;
            stdout.write_all(b"\n")?;
        }
        return Ok(());
    }
    if let Some(path) = options.auth_policy {
        let policy = tokio::task::spawn_blocking(move || auth_policy::load(&path)).await??;
        options.config.access_control = std::sync::Arc::new(policy);
    }
    if options.enhanced_auth_plain {
        options.config.enhanced_auth = Some(std::sync::Arc::new(
            mqtt_auth::PlainAuthenticator::new(options.config.access_control.clone()),
        ));
    }
    let tls = match (options.tls_cert, options.tls_key) {
        (Some(cert), Some(key)) => {
            Some(tokio::task::spawn_blocking(move || tls_identity::load(&cert, &key)).await??)
        }
        (None, None) => None,
        _ => return Err("TLS requires both certificate and key".into()),
    };
    let listener = TcpListener::bind(&options.address).await?;
    let transport = match (options.websocket, tls.is_some()) {
        (false, false) => "TCP",
        (false, true) => "TLS",
        (true, false) => "WS",
        (true, true) => "WSS",
    };
    eprintln!(
        "rmqtt listening on {} ({transport})",
        listener.local_addr()?
    );
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = tx.send(true);
        }
    });
    match (options.websocket, tls) {
        (false, None) => serve(listener, options.config, rx).await?,
        (false, Some(tls)) => serve_tls(listener, options.config, tls, rx).await?,
        (true, None) => serve_ws(listener, options.config, options.websocket_options, rx).await?,
        (true, Some(tls)) => {
            serve_wss(listener, options.config, tls, options.websocket_options, rx).await?
        }
    }
    Ok(())
}
