mod admin;
mod allocator;
mod auth_policy;
mod cli;
mod egress_shutdown;
mod logging;
mod tls_identity;

use mqtt_transport::{export_state, serve, serve_tls, serve_ws, serve_wss};
use std::{error::Error, io::Write};
use tokio::{net::TcpListener, sync::watch};

fn main() -> Result<(), Box<dyn Error>> {
    // The writer guard outlives the runtime; flushing never blocks a Tokio worker.
    let _logging = logging::init()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run());
    if result.is_err() {
        tracing::error!(event = "broker_failed", "broker stopped with an error");
    }
    result
}

async fn run() -> Result<(), Box<dyn Error>> {
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
    if let Some(path) = options.auth_policy.clone() {
        let policy = tokio::task::spawn_blocking(move || auth_policy::load(&path)).await??;
        options.config.access_control = std::sync::Arc::new(policy);
    }
    let access = mqtt_auth_http::Runtime::open(
        options
            .access_control_config
            .clone()
            .ok_or("access configuration path required")?,
        options.config.access_control.clone(),
        options.auth_policy.is_some(),
    )
    .await?;
    options.config.access_control = access.admitted_policy();
    options.config.remote_access = Some(access.clone());
    options.config.remote_plain = options.enhanced_auth_plain;
    if options.enhanced_auth_plain {
        options.config.enhanced_auth = Some(std::sync::Arc::new(
            mqtt_auth::PlainAuthenticator::new(options.config.access_control.clone()),
        ));
    }
    let tls = match (options.tls_cert.clone(), options.tls_key.clone()) {
        (Some(cert), Some(key)) => {
            let client_ca = options.tls_client_ca.clone();
            Some(
                tokio::task::spawn_blocking(move || {
                    tls_identity::load_with_client_ca(&cert, &key, client_ca.as_deref())
                })
                .await??,
            )
        }
        (None, None) => None,
        _ => return Err("TLS requires both certificate and key".into()),
    };
    // Load bounded token/assets before accepting connections. Binding port zero
    // first lets the dashboard report the actual MQTT endpoint.
    let listener = TcpListener::bind(&options.address).await?;
    let mut admin = if let Some(address) = options.admin_listen {
        let handle = mqtt_transport::management::ManagementHandle::new();
        options.config.management = Some(handle.clone());
        options.config.publisher = Some(mqtt_transport::PublishHandle::new());
        let listen = listener.local_addr()?.to_string();
        let (returned, state) = tokio::task::spawn_blocking(move || {
            let state = admin::prepare(&options, listen, handle, None)?;
            Ok::<_, std::io::Error>((options, state))
        })
        .await??;
        options = returned;
        let listener = TcpListener::bind(address).await?;
        tracing::info!(
            event = "management_listening",
            address = %listener.local_addr()?,
            "rmqtt management on {} (loopback, bearer authentication)",
            listener.local_addr()?
        );
        Some((listener, state))
    } else {
        None
    };
    if let Some((_, state)) = &mut admin {
        let state = std::sync::Arc::get_mut(state)
            .ok_or("management state unexpectedly shared during startup")?;
        state.access_control = Some(access.clone());
    }
    let rules = if let Some(path) = options.rules_config.clone() {
        let directory = options
            .queue_directory()
            .ok_or("outbound queue directory required")?;
        let mut queue = mqtt_egress::QueueOptions::new(directory);
        queue.segment_bytes = options.egress_queue_segment_bytes;
        queue.max_bytes = options.egress_queue_max_bytes;
        queue.max_records = options.egress_queue_max_records;
        let runtime = mqtt_egress::Runtime::open_with_queue(path, queue).await?;
        options.config.publication_observer = Some(runtime.observer());
        if let Some((_, state)) = &mut admin {
            let state = std::sync::Arc::get_mut(state)
                .ok_or("management state unexpectedly shared during startup")?;
            state.rules = Some(runtime.clone());
        }
        Some(runtime)
    } else {
        None
    };
    let transport = match (options.websocket, tls.is_some()) {
        (false, false) => "TCP",
        (false, true) => "TLS",
        (true, false) => "WS",
        (true, true) => "WSS",
    };
    tracing::info!(
        event = "mqtt_listening",
        address = %listener.local_addr()?,
        transport,
        "rmqtt listening on {} ({transport})",
        listener.local_addr()?
    );
    if rules.is_some() {
        tracing::info!(
            event = "rules_started",
            "rules and durable outbound queue ready"
        );
    }
    let (tx, rx) = watch::channel(false);
    let signal_tx = tx.clone();
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                tracing::info!(event = "shutdown_requested", "shutdown requested");
                let _ = signal_tx.send(true);
            }
            Err(error) => {
                tracing::error!(event = "signal_failed", error_kind = ?error.kind(), "shutdown signal handler failed");
            }
        }
    });
    let mqtt_rx = rx.clone();
    let broker = async move {
        match (options.websocket, tls) {
            (false, None) => serve(listener, options.config, mqtt_rx).await,
            (false, Some(tls)) => serve_tls(listener, options.config, tls, mqtt_rx).await,
            (true, None) => {
                serve_ws(listener, options.config, options.websocket_options, mqtt_rx).await
            }
            (true, Some(tls)) => {
                serve_wss(
                    listener,
                    options.config,
                    tls,
                    options.websocket_options,
                    mqtt_rx,
                )
                .await
            }
        }
    };
    let has_admin = admin.is_some();
    let management = async move {
        match admin {
            Some((listener, state)) => admin::serve(listener, state, rx).await,
            None => std::future::pending::<std::io::Result<()>>().await,
        }
    };
    tokio::pin!(broker, management);
    let (broker_finished, result) = tokio::select! {
        result=&mut broker=>(true,result),
        result=&mut management=>(false,result),
    };
    let _ = tx.send(true);
    let other = if broker_finished {
        if has_admin { management.await } else { Ok(()) }
    } else {
        broker.await
    };
    if let Some(runtime) = rules {
        egress_shutdown::drain(&runtime).await;
        runtime.shutdown().await;
    }
    access.shutdown().await;
    result?;
    other?;
    tracing::info!(event = "broker_stopped", "broker stopped");
    Ok(())
}
