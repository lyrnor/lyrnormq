use std::{error::Error, io};
use mqtt_transport::{BrokerConfig, serve};
use tokio::{net::TcpListener, sync::watch};
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let address = match args.next().as_deref() {
        None => "127.0.0.1:1883".to_owned(),
        Some("--listen") => args.next().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing listen address"))?,
        _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, "usage: rmqtt [--listen ADDRESS]").into()),
    };
    if args.next().is_some() { return Err(io::Error::new(io::ErrorKind::InvalidInput, "unexpected argument").into()); }
    let listener = TcpListener::bind(&address).await?;
    eprintln!("rmqtt listening on {}", listener.local_addr()?);
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move { if tokio::signal::ctrl_c().await.is_ok() { let _ = tx.send(true); } });
    serve(listener, BrokerConfig::default(), rx).await?;
    Ok(())
}
