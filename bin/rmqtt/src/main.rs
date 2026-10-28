mod cli;

use mqtt_transport::serve;
use std::error::Error;
use tokio::{net::TcpListener, sync::watch};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let Some(options) = cli::parse(std::env::args().skip(1))? else {
        println!("{}", cli::USAGE);
        return Ok(());
    };
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
