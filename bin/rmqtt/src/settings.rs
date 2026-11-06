//! Operator configuration: one TOML file per process, with `RMQTT_*` overrides.
use crate::cli::{Invocation, Options};
use mqtt_cluster::{Config, Controller, DurableConfig, Role, voting_members};
use mqtt_transport::ProtocolMode;
use serde::Deserialize;
use std::{
    collections::HashMap,
    fs,
    io::{self, Read},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
};

const LIMIT: u64 = 64 * 1024;

pub fn load(invocation: &Invocation) -> io::Result<Options> {
    load_with(invocation, &capture_env())
}

pub(crate) fn load_with(
    invocation: &Invocation,
    env: &HashMap<String, String>,
) -> io::Result<Options> {
    let text = read_file(&invocation.config_path)?;
    let mut raw: Raw = toml::from_str(&text)
        .map_err(|error| invalid(format!("invalid configuration: {error}")))?;
    apply_env(&mut raw, env)?;
    let node_id = raw.node.id.ok_or_else(|| invalid("node.id is required"))?;
    if node_id == 0 {
        return Err(invalid("node.id must be positive"));
    }
    let data_dir = PathBuf::from(
        raw.node
            .data_dir
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid("node.data_dir is required"))?,
    );
    let roles = match (&raw.cluster, raw.node.role.as_deref()) {
        (None, Some(_)) => {
            return Err(invalid("node.role requires [cluster]"));
        }
        (Some(_), None) => {
            return Err(invalid(
                "node.role is required with [cluster]; use controller, broker, or controller,broker",
            ));
        }
        (Some(_), Some(value)) => Some(parse_roles(value)?),
        (None, None) => None,
    };
    let controller_only = roles
        .as_ref()
        .is_some_and(|roles| roles.controller && !roles.broker);
    if controller_only && mqtt_configured(&raw.mqtt) {
        return Err(invalid("node.role controller does not use [mqtt]"));
    }
    if controller_only && raw.dashboard.is_some() {
        return Err(invalid("node.role controller does not use [dashboard]"));
    }
    if controller_only && raw.auth.is_some() {
        return Err(invalid("node.role controller does not use [auth]"));
    }
    let client = if controller_only {
        None
    } else {
        Some(mqtt_client(&raw.mqtt)?)
    };
    let cluster = match raw.cluster {
        Some(cluster) => Some(cluster_pair(node_id, &data_dir, cluster)?),
        None => None,
    };
    if let (Some(client), Some((broker, controller))) = (&client, &cluster) {
        let cluster_ports = [broker.listen.port(), controller.listen.port()];
        if cluster_ports.contains(&client.address.port())
            && (client.address.ip().is_unspecified()
                || client.address.ip() == broker.listen.ip()
                || client.address.ip() == controller.listen.ip())
        {
            return Err(invalid(
                "mqtt.listen must not use a cluster controller or broker port",
            ));
        }
    }
    let dashboard = dashboard(raw.dashboard)?;
    let auth = auth(raw.auth)?;
    if invocation.export_state && cluster.is_some() {
        return Err(invalid(
            "export-state cannot be used with a cluster configuration",
        ));
    }
    if (invocation.cluster_status || invocation.cluster_repair.is_some()) && cluster.is_none() {
        return Err(invalid("cluster commands require [cluster]"));
    }
    fs::create_dir_all(&data_dir)?;
    let mut options = Options::blank();
    if let Some(client) = client {
        options.address = client.address.to_string();
        options.config.protocol_mode = client.protocol;
        if let Some((cert, key, client_ca)) = client.tls {
            options.tls_cert = Some(cert);
            options.tls_key = Some(key);
            options.tls_client_ca = client_ca;
        }
        if let Some(path) = client.websocket {
            options.websocket = true;
            options.websocket_options.path = path;
        }
    }
    options.cluster_config = Some(invocation.config_path.clone());
    options.cluster_status = invocation.cluster_status;
    options.cluster_repair = invocation.cluster_repair;
    options.cluster_repair_target = invocation.cluster_repair_target;
    options.export_state = invocation.export_state;
    apply_limits(&mut options, raw.limits)?;
    if cluster.is_none() {
        options.config.storage_path = Some(data_dir.join("sessions.sqlite"));
    }
    let runs_broker = roles.as_ref().is_none_or(|roles| roles.broker);
    if runs_broker {
        options.rules_config = Some(data_dir.join("rules.sqlite"));
        options.access_control_config = Some(data_dir.join("access.sqlite"));
        options.egress_queue_dir = Some(data_dir.join("egress"));
    }
    if let Some((listen, token, dir)) = dashboard {
        options.admin_listen = Some(listen);
        options.admin_token_file = Some(token);
        options.dashboard_dir = dir;
    }
    if let Some((policy, plain)) = auth {
        options.auth_policy = Some(policy);
        options.enhanced_auth_plain = plain;
    }
    if let Some((broker, controller)) = cluster {
        let roles = roles.ok_or_else(|| invalid("node.role is required with [cluster]"))?;
        if roles.broker {
            broker.validate()?;
            options.cluster_broker = Some(broker);
        }
        if roles.controller {
            controller.validate()?;
            options.cluster_controller = Some(controller);
        }
        if invocation.cluster_repair == Some(crate::cli::ClusterRepair::Learner) && !roles.broker {
            return Err(invalid(
                "cluster-repair-learner requires node.role to include broker",
            ));
        }
    }
    Ok(options)
}

struct Roles {
    controller: bool,
    broker: bool,
}

struct MqttClient {
    address: SocketAddr,
    protocol: ProtocolMode,
    tls: Option<(PathBuf, PathBuf, Option<PathBuf>)>,
    websocket: Option<String>,
}

fn parse_roles(value: &str) -> io::Result<Roles> {
    let mut controller = false;
    let mut broker = false;
    if value.trim().is_empty() {
        return Err(invalid(
            "node.role must list controller, broker, or controller,broker",
        ));
    }
    for part in value.split(',') {
        match part.trim() {
            "controller" if !controller => controller = true,
            "broker" if !broker => broker = true,
            "controller" | "broker" => {
                return Err(invalid("node.role repeats a role"));
            }
            _ => {
                return Err(invalid(
                    "node.role must list controller, broker, or controller,broker",
                ));
            }
        }
    }
    Ok(Roles { controller, broker })
}

fn mqtt_configured(mqtt: &MqttRaw) -> bool {
    mqtt.listen.is_some()
        || mqtt.protocol.is_some()
        || mqtt.tls.is_some()
        || mqtt.websocket.is_some()
}

fn mqtt_client(mqtt: &MqttRaw) -> io::Result<MqttClient> {
    let listen = mqtt
        .listen
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid("mqtt.listen is required"))?;
    let address = listen
        .parse()
        .map_err(|_| invalid("mqtt.listen must be an IP:PORT"))?;
    let tls = tls_paths(mqtt.tls.clone())?;
    if tls.is_none() && public_address(address) {
        return Err(invalid("mqtt listen on a public address requires mqtt.tls"));
    }
    Ok(MqttClient {
        address,
        protocol: protocol(mqtt.protocol.as_deref())?,
        tls,
        websocket: websocket(mqtt.websocket.clone())?,
    })
}

fn read_file(path: &Path) -> io::Result<String> {
    let meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(invalid(format!(
                "config file not found: {}",
                path.display()
            )));
        }
        Err(error) => return Err(error),
    };
    if !meta.is_file() || meta.len() > LIMIT {
        return Err(invalid(
            "config must be a regular file no larger than 64KiB",
        ));
    }
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(LIMIT + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > LIMIT {
        return Err(invalid(
            "config must be a regular file no larger than 64KiB",
        ));
    }
    String::from_utf8(bytes).map_err(|_| invalid("config must be UTF-8"))
}

fn capture_env() -> HashMap<String, String> {
    let mut env = HashMap::new();
    for name in ENV_NAMES {
        if let Ok(value) = std::env::var(name) {
            let value = value.trim().to_owned();
            if !value.is_empty() {
                env.insert((*name).to_owned(), value);
            }
        }
    }
    env
}

const ENV_NAMES: &[&str] = &[
    "RMQTT_NODE_ID",
    "RMQTT_NODE_ROLE",
    "RMQTT_NODE_DATA_DIR",
    "RMQTT_MQTT_LISTEN",
    "RMQTT_MQTT_PROTOCOL",
    "RMQTT_MQTT_TLS_CERT",
    "RMQTT_MQTT_TLS_KEY",
    "RMQTT_MQTT_TLS_CLIENT_CA",
    "RMQTT_MQTT_WEBSOCKET_PATH",
    "RMQTT_CLUSTER_ID",
    "RMQTT_CLUSTER_TOKEN_FILE",
    "RMQTT_CLUSTER_CONTROLLER_PORT",
    "RMQTT_CLUSTER_BROKER_PORT",
    "RMQTT_CLUSTER_MEMBERS",
    "RMQTT_DASHBOARD_LISTEN",
    "RMQTT_DASHBOARD_TOKEN_FILE",
    "RMQTT_DASHBOARD_DIR",
    "RMQTT_AUTH_POLICY",
    "RMQTT_AUTH_PLAIN",
    "RMQTT_LIMITS_CONNECTIONS",
    "RMQTT_LIMITS_SESSIONS",
    "RMQTT_LIMITS_INFLIGHT",
    "RMQTT_LIMITS_PACKET_BYTES",
    "RMQTT_LIMITS_SUBSCRIPTIONS",
    "RMQTT_LIMITS_RETAINED",
    "RMQTT_LIMITS_STATE_BYTES",
];

fn apply_env(raw: &mut Raw, env: &HashMap<String, String>) -> io::Result<()> {
    if let Some(value) = env.get("RMQTT_NODE_ID") {
        raw.node.id = Some(parse_u64(value, "RMQTT_NODE_ID")?);
    }
    if let Some(value) = env.get("RMQTT_NODE_ROLE") {
        raw.node.role = Some(value.clone());
    }
    if let Some(value) = env.get("RMQTT_NODE_DATA_DIR") {
        raw.node.data_dir = Some(value.clone());
    }
    if let Some(value) = env.get("RMQTT_MQTT_LISTEN") {
        raw.mqtt.listen = Some(value.clone());
    }
    if let Some(value) = env.get("RMQTT_MQTT_PROTOCOL") {
        raw.mqtt.protocol = Some(value.clone());
    }
    overlay_tls(raw, env)?;
    if let Some(value) = env.get("RMQTT_MQTT_WEBSOCKET_PATH") {
        raw.mqtt
            .websocket
            .get_or_insert_with(WebSocketRaw::default)
            .path = Some(value.clone());
    }
    overlay_cluster(raw, env)?;
    overlay_dashboard(raw, env)?;
    overlay_auth(raw, env)?;
    overlay_limits(&mut raw.limits, env)?;
    Ok(())
}

fn overlay_tls(raw: &mut Raw, env: &HashMap<String, String>) -> io::Result<()> {
    let cert = env.get("RMQTT_MQTT_TLS_CERT").cloned();
    let key = env.get("RMQTT_MQTT_TLS_KEY").cloned();
    let client_ca = env.get("RMQTT_MQTT_TLS_CLIENT_CA").cloned();
    if cert.is_some() || key.is_some() || client_ca.is_some() {
        raw.mqtt.tls.get_or_insert_with(TlsRaw::default);
    }
    if let Some(tls) = &mut raw.mqtt.tls {
        if let Some(cert) = cert {
            tls.cert = Some(cert);
        }
        if let Some(key) = key {
            tls.key = Some(key);
        }
        if let Some(client_ca) = client_ca {
            tls.client_ca = Some(client_ca);
        }
    }
    Ok(())
}

fn overlay_cluster(raw: &mut Raw, env: &HashMap<String, String>) -> io::Result<()> {
    let present = [
        "RMQTT_CLUSTER_ID",
        "RMQTT_CLUSTER_TOKEN_FILE",
        "RMQTT_CLUSTER_CONTROLLER_PORT",
        "RMQTT_CLUSTER_BROKER_PORT",
        "RMQTT_CLUSTER_MEMBERS",
    ]
    .iter()
    .any(|name| env.contains_key(*name));
    if present {
        raw.cluster.get_or_insert_with(ClusterRaw::default);
    }
    let Some(cluster) = &mut raw.cluster else {
        return Ok(());
    };
    if let Some(value) = env.get("RMQTT_CLUSTER_ID") {
        cluster.id = Some(value.clone());
    }
    if let Some(value) = env.get("RMQTT_CLUSTER_TOKEN_FILE") {
        cluster.token_file = Some(value.clone());
    }
    if let Some(value) = env.get("RMQTT_CLUSTER_CONTROLLER_PORT") {
        cluster.controller_port = Some(parse_u16(value, "RMQTT_CLUSTER_CONTROLLER_PORT")?);
    }
    if let Some(value) = env.get("RMQTT_CLUSTER_BROKER_PORT") {
        cluster.broker_port = Some(parse_u16(value, "RMQTT_CLUSTER_BROKER_PORT")?);
    }
    if let Some(value) = env.get("RMQTT_CLUSTER_MEMBERS") {
        cluster.members = Some(parse_members(value)?);
    }
    Ok(())
}

fn overlay_dashboard(raw: &mut Raw, env: &HashMap<String, String>) -> io::Result<()> {
    let present = [
        "RMQTT_DASHBOARD_LISTEN",
        "RMQTT_DASHBOARD_TOKEN_FILE",
        "RMQTT_DASHBOARD_DIR",
    ]
    .iter()
    .any(|name| env.contains_key(*name));
    if present {
        raw.dashboard.get_or_insert_with(DashboardRaw::default);
    }
    let Some(dashboard) = &mut raw.dashboard else {
        return Ok(());
    };
    if let Some(value) = env.get("RMQTT_DASHBOARD_LISTEN") {
        dashboard.listen = Some(value.clone());
    }
    if let Some(value) = env.get("RMQTT_DASHBOARD_TOKEN_FILE") {
        dashboard.token_file = Some(value.clone());
    }
    if let Some(value) = env.get("RMQTT_DASHBOARD_DIR") {
        dashboard.dir = Some(value.clone());
    }
    Ok(())
}

fn overlay_auth(raw: &mut Raw, env: &HashMap<String, String>) -> io::Result<()> {
    let present = ["RMQTT_AUTH_POLICY", "RMQTT_AUTH_PLAIN"]
        .iter()
        .any(|name| env.contains_key(*name));
    if present {
        raw.auth.get_or_insert_with(AuthRaw::default);
    }
    let Some(auth) = &mut raw.auth else {
        return Ok(());
    };
    if let Some(value) = env.get("RMQTT_AUTH_POLICY") {
        auth.policy = Some(value.clone());
    }
    if let Some(value) = env.get("RMQTT_AUTH_PLAIN") {
        auth.plain = Some(parse_bool(value, "RMQTT_AUTH_PLAIN")?);
    }
    Ok(())
}

fn overlay_limits(limits: &mut LimitsRaw, env: &HashMap<String, String>) -> io::Result<()> {
    assign_u64(&mut limits.connections, env, "RMQTT_LIMITS_CONNECTIONS")?;
    assign_u64(&mut limits.sessions, env, "RMQTT_LIMITS_SESSIONS")?;
    assign_u64(&mut limits.inflight, env, "RMQTT_LIMITS_INFLIGHT")?;
    assign_u64(&mut limits.packet_bytes, env, "RMQTT_LIMITS_PACKET_BYTES")?;
    assign_u64(&mut limits.subscriptions, env, "RMQTT_LIMITS_SUBSCRIPTIONS")?;
    assign_u64(&mut limits.retained, env, "RMQTT_LIMITS_RETAINED")?;
    assign_u64(&mut limits.state_bytes, env, "RMQTT_LIMITS_STATE_BYTES")?;
    Ok(())
}

fn assign_u64(slot: &mut Option<u64>, env: &HashMap<String, String>, name: &str) -> io::Result<()> {
    if let Some(value) = env.get(name) {
        *slot = Some(parse_u64(value, name)?);
    }
    Ok(())
}

fn protocol(value: Option<&str>) -> io::Result<ProtocolMode> {
    match value.unwrap_or("both") {
        "both" => Ok(ProtocolMode::Both),
        "3.1.1" => Ok(ProtocolMode::V311),
        "5" => Ok(ProtocolMode::V5),
        _ => Err(invalid("mqtt.protocol must be both, 3.1.1 or 5")),
    }
}

fn tls_paths(tls: Option<TlsRaw>) -> io::Result<Option<(PathBuf, PathBuf, Option<PathBuf>)>> {
    let Some(tls) = tls else {
        return Ok(None);
    };
    let cert = nonempty(tls.cert, "mqtt.tls.cert")?;
    let key = nonempty(tls.key, "mqtt.tls.key")?;
    let client_ca = match tls.client_ca {
        Some(value) if value.is_empty() => {
            return Err(invalid("mqtt.tls.client_ca must be a nonempty path"));
        }
        Some(value) => Some(PathBuf::from(value)),
        None => None,
    };
    Ok(Some((PathBuf::from(cert), PathBuf::from(key), client_ca)))
}

fn websocket(websocket: Option<WebSocketRaw>) -> io::Result<Option<String>> {
    let Some(websocket) = websocket else {
        return Ok(None);
    };
    let path = websocket.path.unwrap_or_else(|| "/mqtt".to_owned());
    if !path.starts_with('/')
        || path.len() > 1024
        || path
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || matches!(c, '?' | '#'))
    {
        return Err(invalid("mqtt.websocket.path is invalid"));
    }
    Ok(Some(path))
}

fn dashboard(
    dashboard: Option<DashboardRaw>,
) -> io::Result<Option<(SocketAddr, PathBuf, Option<PathBuf>)>> {
    let Some(dashboard) = dashboard else {
        return Ok(None);
    };
    let listen = nonempty(dashboard.listen, "dashboard.listen")?;
    let address: SocketAddr = listen
        .parse()
        .map_err(|_| invalid("dashboard.listen must be a loopback IP:PORT"))?;
    if !address.ip().is_loopback() {
        return Err(invalid("dashboard.listen must be a loopback IP:PORT"));
    }
    let token = PathBuf::from(nonempty(dashboard.token_file, "dashboard.token_file")?);
    let dir = match dashboard.dir {
        Some(value) if value.is_empty() => {
            return Err(invalid("dashboard.dir must be a nonempty path"));
        }
        Some(value) => Some(PathBuf::from(value)),
        None => None,
    };
    Ok(Some((address, token, dir)))
}

fn auth(auth: Option<AuthRaw>) -> io::Result<Option<(PathBuf, bool)>> {
    let Some(auth) = auth else {
        return Ok(None);
    };
    let policy = PathBuf::from(nonempty(auth.policy, "auth.policy")?);
    Ok(Some((policy, auth.plain.unwrap_or(false))))
}

fn cluster_pair(
    node_id: u64,
    data_dir: &Path,
    cluster: ClusterRaw,
) -> io::Result<(Config, Config)> {
    let id = nonempty(cluster.id, "cluster.id")?;
    if id.len() > 128 || !id.bytes().all(|byte| (33..=126).contains(&byte)) {
        return Err(invalid("cluster.id is invalid"));
    }
    let token_file = PathBuf::from(nonempty(cluster.token_file, "cluster.token_file")?);
    let controller_port = cluster
        .controller_port
        .filter(|port| *port > 0)
        .ok_or_else(|| invalid("cluster.controller_port must be 1..65535"))?;
    let broker_port = cluster
        .broker_port
        .filter(|port| *port > 0)
        .ok_or_else(|| invalid("cluster.broker_port must be 1..65535"))?;
    if controller_port == broker_port {
        return Err(invalid(
            "cluster.controller_port and cluster.broker_port must differ",
        ));
    }
    let members = cluster
        .members
        .ok_or_else(|| invalid("cluster.members is required"))?;
    if !voting_members(members.len()) {
        return Err(invalid("cluster.members must be 3, 5, or 7"));
    }
    let mut parsed = Vec::with_capacity(members.len());
    let mut ids = std::collections::BTreeSet::new();
    let mut hosts = std::collections::BTreeSet::new();
    for member in members {
        if !(1..=99).contains(&member.id) || !ids.insert(member.id) {
            return Err(invalid("cluster member id must be unique and in 1..99"));
        }
        let host: IpAddr = member
            .host
            .parse()
            .map_err(|_| invalid("cluster member host must be an IP address"))?;
        if !hosts.insert(host) {
            return Err(invalid("cluster member hosts must be unique"));
        }
        if !private_ip(host) {
            return Err(invalid(
                "cluster member host must be a loopback or private address",
            ));
        }
        parsed.push((member.id, host));
    }
    let me = parsed
        .iter()
        .find(|(id, _)| *id == node_id)
        .copied()
        .ok_or_else(|| invalid("node.id must be one of cluster.members"))?;
    let controller_listen = SocketAddr::new(me.1, controller_port);
    let broker_listen = SocketAddr::new(me.1, broker_port);
    let controllers: Vec<Controller> = parsed
        .iter()
        .map(|(id, host)| Controller {
            id: *id,
            address: endpoint(*host, controller_port),
        })
        .collect();
    let replicas: Vec<Controller> = parsed
        .iter()
        .map(|(id, host)| Controller {
            id: *id + 100,
            address: endpoint(*host, broker_port),
        })
        .collect();
    let bootstrap_id = parsed
        .iter()
        .map(|(id, _)| *id)
        .min()
        .ok_or_else(|| invalid("cluster.members must be 3, 5, or 7"))?;
    let broker = Config {
        role: Role::Broker,
        cluster_id: id.clone(),
        node_id: me.0 + 100,
        listen: broker_listen,
        data_dir: data_dir.join("broker"),
        token_file: token_file.clone(),
        controllers: controllers.clone(),
        bootstrap: false,
        advertise: Some(endpoint(me.1, broker_port)),
        lease_ms: 3000,
        tls: None,
        durable: Some(DurableConfig { replicas }),
    };
    let controller = Config {
        role: Role::Controller,
        cluster_id: id,
        node_id: me.0,
        listen: controller_listen,
        data_dir: data_dir.join("controller"),
        token_file,
        controllers,
        bootstrap: me.0 == bootstrap_id,
        advertise: None,
        lease_ms: 3000,
        tls: None,
        durable: None,
    };
    Ok((broker, controller))
}

fn apply_limits(options: &mut Options, limits: LimitsRaw) -> io::Result<()> {
    if let Some(value) = limits.connections {
        options.config.max_connections = positive(value, "limits.connections")?;
    }
    if let Some(value) = limits.sessions {
        options.config.max_sessions = positive(value, "limits.sessions")?;
    }
    if let Some(value) = limits.inflight {
        let number = positive(value, "limits.inflight")?;
        if number > u16::MAX as usize {
            return Err(invalid("limits.inflight must be in 1..65535"));
        }
        options.config.max_inflight = number;
    }
    if let Some(value) = limits.packet_bytes {
        let number = positive(value, "limits.packet_bytes")?;
        if number < 2 {
            return Err(invalid("limits.packet_bytes must be at least 2"));
        }
        options.config.max_packet_size = number;
    }
    if let Some(value) = limits.subscriptions {
        options.config.max_subscriptions = positive(value, "limits.subscriptions")?;
    }
    if let Some(value) = limits.retained {
        options.config.max_retained = positive(value, "limits.retained")?;
    }
    if let Some(value) = limits.state_bytes {
        options.config.max_state_bytes = positive(value, "limits.state_bytes")?;
    }
    Ok(())
}

fn positive(value: u64, name: &str) -> io::Result<usize> {
    let number = usize::try_from(value).map_err(|_| invalid(format!("{name} is too large")))?;
    if number == 0 {
        return Err(invalid(format!("{name} must be positive")));
    }
    Ok(number)
}

fn parse_members(value: &str) -> io::Result<Vec<MemberRaw>> {
    let mut members = Vec::new();
    for item in value.split(',') {
        let item = item.trim();
        let (id, host) = item
            .split_once(':')
            .ok_or_else(|| invalid("RMQTT_CLUSTER_MEMBERS must be id:host pairs"))?;
        let id = parse_u64(id.trim(), "RMQTT_CLUSTER_MEMBERS")?;
        let host = host.trim();
        if host.is_empty() {
            return Err(invalid("RMQTT_CLUSTER_MEMBERS must be id:host pairs"));
        }
        members.push(MemberRaw {
            id,
            host: host.to_owned(),
        });
    }
    Ok(members)
}

fn endpoint(ip: IpAddr, port: u16) -> String {
    format!("http://{}", SocketAddr::new(ip, port))
}

fn public_address(address: SocketAddr) -> bool {
    !private_ip(address.ip())
}

fn private_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
        IpAddr::V6(ip) => ip.is_loopback(),
    }
}

fn nonempty(value: Option<String>, name: &str) -> io::Result<String> {
    match value {
        Some(value) if !value.is_empty() => Ok(value),
        _ => Err(invalid(format!("{name} is required"))),
    }
}

fn parse_u64(value: &str, name: &str) -> io::Result<u64> {
    value
        .parse()
        .map_err(|_| invalid(format!("{name} must be an integer")))
}

fn parse_u16(value: &str, name: &str) -> io::Result<u16> {
    value
        .parse()
        .map_err(|_| invalid(format!("{name} must be an integer")))
}

fn parse_bool(value: &str, name: &str) -> io::Result<bool> {
    match value {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(invalid(format!("{name} must be true or false"))),
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Raw {
    #[serde(default)]
    node: NodeRaw,
    #[serde(default)]
    mqtt: MqttRaw,
    #[serde(default)]
    cluster: Option<ClusterRaw>,
    #[serde(default)]
    dashboard: Option<DashboardRaw>,
    #[serde(default)]
    auth: Option<AuthRaw>,
    #[serde(default)]
    limits: LimitsRaw,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct NodeRaw {
    id: Option<u64>,
    role: Option<String>,
    data_dir: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct MqttRaw {
    listen: Option<String>,
    protocol: Option<String>,
    #[serde(default)]
    tls: Option<TlsRaw>,
    #[serde(default)]
    websocket: Option<WebSocketRaw>,
}

#[derive(Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct TlsRaw {
    cert: Option<String>,
    key: Option<String>,
    client_ca: Option<String>,
}

#[derive(Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct WebSocketRaw {
    path: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ClusterRaw {
    id: Option<String>,
    token_file: Option<String>,
    controller_port: Option<u16>,
    broker_port: Option<u16>,
    members: Option<Vec<MemberRaw>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MemberRaw {
    id: u64,
    host: String,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct DashboardRaw {
    listen: Option<String>,
    token_file: Option<String>,
    dir: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct AuthRaw {
    policy: Option<String>,
    plain: Option<bool>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct LimitsRaw {
    connections: Option<u64>,
    sessions: Option<u64>,
    inflight: Option<u64>,
    packet_bytes: Option<u64>,
    subscriptions: Option<u64>,
    retained: Option<u64>,
    state_bytes: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::parse;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "rmqtt-settings-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
        fn file(&self, body: &str) -> PathBuf {
            let path = self.0.join("rmqtt.toml");
            let mut file = fs::File::create(&path).unwrap();
            file.write_all(body.as_bytes()).unwrap();
            path
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn invocation(path: &Path) -> Invocation {
        let mut invocation = parse(["--config", &path.to_string_lossy()])
            .unwrap()
            .unwrap();
        invocation.config_path = path.to_owned();
        invocation
    }

    #[test]
    fn standalone_uses_fixed_paths_and_program_limits() {
        let dir = Temp::new();
        let path = dir.file(&format!(
            "[node]\nid = 1\ndata_dir = \"{}\"\n[mqtt]\nlisten = \"127.0.0.1:18831\"\n",
            dir.0.display()
        ));
        let options = load_with(&invocation(&path), &HashMap::new()).unwrap();
        assert!(options.cluster_broker.is_none());
        assert_eq!(
            options.config.storage_path.as_deref(),
            Some(dir.0.join("sessions.sqlite").as_path())
        );
        assert_eq!(
            options.rules_config.as_deref(),
            Some(dir.0.join("rules.sqlite").as_path())
        );
        assert_eq!(
            options.access_control_config.as_deref(),
            Some(dir.0.join("access.sqlite").as_path())
        );
        assert_eq!(
            options.egress_queue_dir.as_deref(),
            Some(dir.0.join("egress").as_path())
        );
        assert_eq!(options.config.max_connections, 1024);
        assert_eq!(options.config.max_inflight, 32);
        assert!(options.admin_listen.is_none());
        assert!(!options.websocket);
    }

    #[test]
    fn public_listen_without_tls_is_rejected() {
        let dir = Temp::new();
        let path = dir.file(&format!(
            "[node]\nid = 1\ndata_dir = \"{}\"\n[mqtt]\nlisten = \"8.8.8.8:1883\"\n",
            dir.0.display()
        ));
        let error = match load_with(&invocation(&path), &HashMap::new()) {
            Err(error) => error,
            Ok(_) => panic!("public listen was accepted"),
        };
        assert!(error.to_string().contains("mqtt.tls"));
    }

    #[test]
    fn cluster_derives_roles_and_ignores_member_order() {
        let dir = Temp::new();
        let path = dir.file(&format!(
            r#"
            [node]
            id = 2
            role = "controller,broker"
            data_dir = "{dir}"
            [mqtt]
            listen = "127.0.0.1:18831"
            [cluster]
            id = "rmqtt"
            token_file = "{dir}/peer.token"
            controller_port = 17001
            broker_port = 17101
            members = [
                {{ id = 3, host = "127.0.0.3" }},
                {{ id = 1, host = "127.0.0.1" }},
                {{ id = 2, host = "127.0.0.2" }},
            ]
            "#,
            dir = dir.0.display()
        ));
        let options = load_with(&invocation(&path), &HashMap::new()).unwrap();
        let broker = options.cluster_broker.unwrap();
        let controller = options.cluster_controller.unwrap();
        assert_eq!(controller.node_id, 2);
        assert_eq!(broker.node_id, 102);
        assert!(!controller.bootstrap);
        assert_eq!(controller.listen, "127.0.0.2:17001".parse().unwrap());
        assert_eq!(broker.listen, "127.0.0.2:17101".parse().unwrap());
        assert_eq!(broker.advertise.as_deref(), Some("http://127.0.0.2:17101"));
        assert!(broker.durable.is_some());
        assert!(options.config.storage_path.is_none());
        let mut ids: Vec<_> = controller.controllers.iter().map(|peer| peer.id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn five_members_are_accepted_and_even_or_oversized_lists_are_rejected() {
        let dir = Temp::new();
        let members = (1..=5)
            .map(|id| format!("{{ id = {id}, host = \"127.0.0.{id}\" }}"))
            .collect::<Vec<_>>()
            .join(",\n");
        let path = dir.file(&format!(
            "[node]\nid = 1\nrole = \"controller\"\ndata_dir = \"{dir}\"\n[cluster]\nid = \"rmqtt\"\ntoken_file = \"peer\"\ncontroller_port = 1\nbroker_port = 2\nmembers = [\n{members}\n]\n",
            dir = dir.0.display(),
        ));
        let options = load_with(&invocation(&path), &HashMap::new()).unwrap();
        assert_eq!(options.cluster_controller.unwrap().controllers.len(), 5);
        for count in [2u64, 4, 6, 8] {
            let members = (1..=count)
                .map(|id| format!("{{ id = {id}, host = \"127.0.0.{id}\" }}"))
                .collect::<Vec<_>>()
                .join(",\n");
            let path = dir.file(&format!(
                "[node]\nid = 1\nrole = \"controller\"\ndata_dir = \"{dir}\"\n[cluster]\nid = \"rmqtt\"\ntoken_file = \"peer\"\ncontroller_port = 1\nbroker_port = 2\nmembers = [\n{members}\n]\n",
                dir = dir.0.display(),
            ));
            let error = match load_with(&invocation(&path), &HashMap::new()) {
                Err(error) => error,
                Ok(_) => panic!("{count} members were accepted"),
            };
            assert!(error.to_string().contains("3, 5, or 7"), "{count}");
        }
    }

    #[test]
    fn comma_separated_roles_start_one_process_each() {
        let dir = Temp::new();
        let controller = dir.file(&format!(
            "[node]\nid = 1\nrole = \"controller\"\ndata_dir = \"{}\"\n[cluster]\nid = \"rmqtt\"\ntoken_file = \"{0}/peer.token\"\ncontroller_port = 17001\nbroker_port = 17101\nmembers = [\n{{ id = 1, host = \"127.0.0.1\" }},\n{{ id = 2, host = \"127.0.0.2\" }},\n{{ id = 3, host = \"127.0.0.3\" }},\n]\n",
            dir.0.display()
        ));
        let options = load_with(&invocation(&controller), &HashMap::new()).unwrap();
        assert!(options.cluster_broker.is_none());
        assert_eq!(options.cluster_controller.unwrap().node_id, 1);
        assert!(options.rules_config.is_none());

        let broker = dir.0.join("broker.toml");
        fs::write(
            &broker,
            format!(
                "[node]\nid = 1\nrole = \"broker\"\ndata_dir = \"{}\"\n[mqtt]\nlisten = \"127.0.0.1:18831\"\n[cluster]\nid = \"rmqtt\"\ntoken_file = \"{0}/peer.token\"\ncontroller_port = 17001\nbroker_port = 17101\nmembers = [\n{{ id = 1, host = \"127.0.0.1\" }},\n{{ id = 2, host = \"127.0.0.2\" }},\n{{ id = 3, host = \"127.0.0.3\" }},\n]\n",
                dir.0.display()
            ),
        )
        .unwrap();
        let options = load_with(&invocation(&broker), &HashMap::new()).unwrap();
        assert!(options.cluster_controller.is_none());
        assert_eq!(options.cluster_broker.unwrap().node_id, 101);
        assert!(options.rules_config.is_some());
    }

    #[test]
    fn role_rejects_both_duplicates_and_a_missing_value() {
        let dir = Temp::new();
        for role in ["both", "controller,controller", ""] {
            let path = dir.file(&format!(
                "[node]\nid = 1\nrole = \"{role}\"\ndata_dir = \"{dir}\"\n[cluster]\nid = \"rmqtt\"\ntoken_file = \"peer\"\ncontroller_port = 1\nbroker_port = 2\nmembers = [\n{{ id = 1, host = \"127.0.0.1\" }},\n{{ id = 2, host = \"127.0.0.2\" }},\n{{ id = 3, host = \"127.0.0.3\" }},\n]\n",
                role = role,
                dir = dir.0.display(),
            ));
            assert!(
                load_with(&invocation(&path), &HashMap::new()).is_err(),
                "{role}"
            );
        }
        let path = dir.file(&format!(
            "[node]\nid = 1\ndata_dir = \"{}\"\n[mqtt]\nlisten = \"127.0.0.1:1\"\n[cluster]\nid = \"rmqtt\"\ntoken_file = \"peer\"\ncontroller_port = 1\nbroker_port = 2\nmembers = [\n{{ id = 1, host = \"127.0.0.1\" }},\n{{ id = 2, host = \"127.0.0.2\" }},\n{{ id = 3, host = \"127.0.0.3\" }},\n]\n",
            dir.0.display()
        ));
        assert!(load_with(&invocation(&path), &HashMap::new()).is_err());
    }

    #[test]
    fn environment_replaces_members_and_listen() {
        let dir = Temp::new();
        let path = dir.file(&format!(
            r#"
            [node]
            id = 1
            data_dir = "{dir}"
            [mqtt]
            listen = "127.0.0.1:1"
            [limits]
            connections = 8
            "#,
            dir = dir.0.display()
        ));
        let mut env = HashMap::new();
        env.insert("RMQTT_MQTT_LISTEN".into(), "127.0.0.1:1883".into());
        env.insert("RMQTT_LIMITS_CONNECTIONS".into(), "32".into());
        env.insert("RMQTT_NODE_ID".into(), "1".into());
        let options = load_with(&invocation(&path), &env).unwrap();
        assert_eq!(options.address, "127.0.0.1:1883");
        assert_eq!(options.config.max_connections, 32);
    }

    #[test]
    fn unknown_field_is_rejected() {
        let dir = Temp::new();
        let path = dir.file(&format!(
            "[node]\nid = 1\ndata_dir = \"{}\"\n[mqtt]\nlisten = \"127.0.0.1:1\"\nextra = 1\n",
            dir.0.display()
        ));
        assert!(load_with(&invocation(&path), &HashMap::new()).is_err());
    }
}
