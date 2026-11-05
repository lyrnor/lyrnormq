//! Private authenticated protobuf RPC transport and bounded peer connection cache.
use crate::controller::diagnostics::{Kind, Stage, measure};
use crate::{
    config::{Config, read_bounded, token},
    model::*,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::BTreeMap,
    io,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};
use tonic::{
    Request, Status,
    metadata::{Ascii, MetadataValue},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig},
};
pub(crate) mod wire {
    tonic::include_proto!("rmqtt.cluster.v1");
}
pub(crate) const MAX_WIRE: usize = 64 * 1024 * 1024;
pub(crate) fn encode<T: Serialize>(value: &T) -> io::Result<wire::Envelope> {
    let data = serde_json::to_vec(value).map_err(io::Error::other)?;
    if data.len() > MAX_WIRE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cluster message exceeds limit",
        ));
    }
    Ok(wire::Envelope { data: data.into() })
}
pub(crate) fn decode<T: DeserializeOwned>(envelope: wire::Envelope) -> Result<T, Status> {
    if envelope.data.len() > MAX_WIRE {
        return Err(Status::resource_exhausted("cluster message exceeds limit"));
    }
    serde_json::from_slice(&envelope.data)
        .map_err(|_| Status::invalid_argument("invalid cluster message"))
}
pub(crate) fn io_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, format!("cluster RPC: {error}"))
}
/// The RPC was sent: transport failure is not proof that a mutation did not commit.
#[derive(Debug)]
pub(crate) struct RpcFailure {
    pub code: tonic::Code,
    pub outcome_unknown: bool,
    message: String,
}
impl std::fmt::Display for RpcFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cluster RPC {:?} (outcome_unknown={}): {}",
            self.code, self.outcome_unknown, self.message
        )
    }
}
impl std::error::Error for RpcFailure {}
pub(crate) fn not_submitted(error: &io::Error) -> bool {
    error.get_ref().is_some_and(|inner| {
        inner.is::<mqtt_cluster_core::AcquisitionConflict>()
            || inner
                .downcast_ref::<RpcFailure>()
                .is_some_and(|failure| !failure.outcome_unknown)
    })
}
fn mutation_status(status: Status) -> io::Error {
    let safe = mutation_not_submitted(&status);
    io::Error::new(
        io::ErrorKind::WouldBlock,
        RpcFailure {
            code: status.code(),
            outcome_unknown: !safe,
            message: status.message().to_owned(),
        },
    )
}
fn mutation_not_submitted(status: &Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::ResourceExhausted
            | tonic::Code::InvalidArgument
            | tonic::Code::Unauthenticated
            | tonic::Code::PermissionDenied
    ) || (status.code() == tonic::Code::FailedPrecondition
        && status.message().starts_with("metadata not submitted:"))
}
fn retry_controller(status: &Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && status.message().starts_with("metadata not submitted:")
}
#[derive(Clone, Copy)]
enum ClientDeadline {
    Regular,
    Repair,
    Applied,
    Heartbeat,
}
fn channel_cache_key(address: &str, deadline: ClientDeadline) -> String {
    match deadline {
        ClientDeadline::Regular => address.to_owned(),
        ClientDeadline::Repair => format!("repair:{address}"),
        ClientDeadline::Applied => format!("applied:{address}"),
        ClientDeadline::Heartbeat => format!("heartbeat:{address}"),
    }
}
#[derive(Clone)]
pub(crate) struct ClientRpc {
    config: Arc<Config>,
    cluster_header: MetadataValue<Ascii>,
    authorization: MetadataValue<Ascii>,
    channels: Arc<Mutex<BTreeMap<String, Channel>>>,
    preferred: Arc<Mutex<Option<String>>>,
}
impl ClientRpc {
    pub fn config_node(&self) -> u64 {
        self.config.node_id
    }
    pub async fn new(config: Arc<Config>) -> io::Result<Self> {
        let secret = token(&config).await?;
        let cluster_header = config.cluster_id.parse().map_err(io_error)?;
        let authorization = format!("Bearer {secret}").parse().map_err(io_error)?;
        Ok(Self {
            config,
            cluster_header,
            authorization,
            channels: Arc::new(Mutex::new(BTreeMap::new())),
            preferred: Arc::new(Mutex::new(None)),
        })
    }
    pub fn authorize<T>(&self, value: T) -> io::Result<Request<T>> {
        let mut request = Request::new(value);
        request
            .metadata_mut()
            .insert("x-rmqtt-cluster", self.cluster_header.clone());
        request
            .metadata_mut()
            .insert("authorization", self.authorization.clone());
        Ok(request)
    }
    pub fn check<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let cluster = request
            .metadata()
            .get("x-rmqtt-cluster")
            .map(|v| v.as_bytes());
        let supplied = request
            .metadata()
            .get("authorization")
            .map(|v| v.as_bytes())
            .unwrap_or_default();
        let expected = self.authorization.as_bytes();
        let mut difference = supplied.len() ^ expected.len();
        for (i, b) in expected.iter().enumerate() {
            difference |= usize::from(b ^ supplied.get(i).copied().unwrap_or(0));
        }
        if difference != 0 || cluster != Some(self.cluster_header.as_bytes()) {
            Err(Status::unauthenticated("cluster authentication required"))
        } else {
            Ok(())
        }
    }
    pub async fn client(
        &self,
        address: &str,
    ) -> io::Result<wire::cluster_client::ClusterClient<Channel>> {
        self.client_timeout(address, ClientDeadline::Regular).await
    }
    pub async fn heartbeat_client(
        &self,
        address: &str,
    ) -> io::Result<wire::cluster_client::ClusterClient<Channel>> {
        self.client_timeout(address, ClientDeadline::Heartbeat)
            .await
    }
    pub async fn repair_client(
        &self,
        address: &str,
    ) -> io::Result<wire::cluster_client::ClusterClient<Channel>> {
        self.client_timeout(address, ClientDeadline::Repair).await
    }
    async fn client_timeout(
        &self,
        address: &str,
        deadline: ClientDeadline,
    ) -> io::Result<wire::cluster_client::ClusterClient<Channel>> {
        let seconds = match deadline {
            ClientDeadline::Repair => 60,
            ClientDeadline::Applied => 5,
            ClientDeadline::Regular | ClientDeadline::Heartbeat => 2,
        };
        let cache_key = channel_cache_key(address, deadline);
        let cached = self
            .channels
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&cache_key)
            .cloned();
        let channel = if let Some(channel) = cached {
            channel
        } else {
            let mut endpoint = Endpoint::from_shared(address.to_owned())
                .map_err(io_error)?
                .connect_timeout(Duration::from_secs(1))
                .timeout(Duration::from_secs(seconds));
            if let Some(tls) = &self.config.tls {
                let ca = read_bounded(&tls.ca_file, 1024 * 1024).await?;
                let cert = read_bounded(&tls.cert_file, 1024 * 1024).await?;
                let key = read_bounded(&tls.key_file, 1024 * 1024).await?;
                endpoint = endpoint
                    .tls_config(
                        ClientTlsConfig::new()
                            .ca_certificate(Certificate::from_pem(ca))
                            .identity(Identity::from_pem(cert, key))
                            .domain_name(&tls.domain_name),
                    )
                    .map_err(io_error)?;
            }
            let channel = endpoint.connect().await.map_err(io_error)?;
            let mut channels = self.channels.lock().unwrap_or_else(PoisonError::into_inner);
            if channels.len() >= 1024 {
                return Err(io_error("cluster peer capacity"));
            }
            channels.insert(cache_key, channel.clone());
            channel
        };
        Ok(wire::cluster_client::ClusterClient::new(channel)
            .max_decoding_message_size(MAX_WIRE)
            .max_encoding_message_size(MAX_WIRE))
    }
    pub fn controller_addresses(&self) -> Vec<String> {
        let preferred = self
            .preferred
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut addresses: Vec<_> = self
            .config
            .controllers
            .iter()
            .map(|c| c.address.clone())
            .collect();
        addresses.sort_by_key(|address| {
            if Some(address) == preferred.as_ref() {
                0
            } else {
                1
            }
        });
        addresses
    }
    pub(crate) fn remember_controller(&self, address: String) {
        *self
            .preferred
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(address);
    }
    pub async fn metadata(&self) -> io::Result<Metadata> {
        self.request_seeds(ReadRequest::Metadata).await
    }
    pub async fn applied(&self, revision: u64) -> io::Result<u64> {
        self.request_seeds(ReadRequest::Applied { revision }).await
    }
    pub async fn acknowledge(
        &self,
        node: u64,
        incarnation: String,
        revision: u64,
    ) -> io::Result<u64> {
        self.request_seeds(ReadRequest::AppliedAck {
            node,
            incarnation,
            revision,
        })
        .await
    }
    pub async fn owner(&self, key: String) -> io::Result<Option<DurableOwner>> {
        self.request_seeds(ReadRequest::Owner { key }).await
    }
    pub async fn node(&self, node: u64) -> io::Result<Option<Node>> {
        self.request_seeds(ReadRequest::Node { node }).await
    }
    pub async fn status(&self) -> io::Result<ClusterStatus> {
        self.request_seeds(ReadRequest::Status).await
    }
    async fn request_seeds<T: DeserializeOwned>(&self, kind: ReadRequest) -> io::Result<T> {
        measure(Stage::RpcRead, Kind::read(&kind), async {
            // One deadline covers all controller attempts, including connection establishment.
            let limit = if matches!(kind, ReadRequest::Applied { .. }) {
                5
            } else {
                2
            };
            tokio::time::timeout(Duration::from_secs(limit), async {
                let mut last = io_error("controller quorum unavailable");
                for address in self.controller_addresses() {
                    let deadline = if matches!(kind, ReadRequest::Applied { .. }) {
                        ClientDeadline::Applied
                    } else {
                        ClientDeadline::Regular
                    };
                    let mut client = match self.client_timeout(&address, deadline).await {
                        Ok(client) => client,
                        Err(error) => {
                            // No RPC was sent: it is safe to try another controller.
                            last = io::Error::new(
                                io::ErrorKind::WouldBlock,
                                RpcFailure {
                                    code: tonic::Code::Unavailable,
                                    outcome_unknown: false,
                                    message: error.to_string(),
                                },
                            );
                            continue;
                        }
                    };
                    match client.read(self.authorize(encode(&kind)?)?).await {
                        Ok(reply) => {
                            if !matches!(kind, ReadRequest::Status) {
                                self.remember_controller(address);
                            }
                            return decode(reply.into_inner()).map_err(io_error);
                        }
                        Err(status) => {
                            last = mutation_status(status);
                        }
                    }
                }
                Err(last)
            })
            .await
            .map_err(|_| io_error("controller read total deadline"))?
        })
        .await
    }
    pub async fn repair_mutate(&self, command: Command) -> io::Result<Answer> {
        self.mutate_deadline(command, ClientDeadline::Repair, 60)
            .await
    }
    pub async fn mutate(&self, command: Command) -> io::Result<Answer> {
        self.mutate_deadline(command, ClientDeadline::Regular, 2)
            .await
    }
    async fn mutate_deadline(
        &self,
        command: Command,
        deadline: ClientDeadline,
        seconds: u64,
    ) -> io::Result<Answer> {
        measure(Stage::RpcMutate, Kind::command(&command), async {
            tokio::time::timeout(Duration::from_secs(seconds), async {
                let mut last = io_error("controller quorum unavailable");
                for address in self.controller_addresses() {
                    let mut client = match self.client_timeout(&address, deadline).await {
                        Ok(client) => client,
                        Err(error) => {
                            // No RPC was sent: it is safe to try another controller.
                            last = io::Error::new(
                                io::ErrorKind::WouldBlock,
                                RpcFailure {
                                    code: tonic::Code::Unavailable,
                                    outcome_unknown: false,
                                    message: error.to_string(),
                                },
                            );
                            continue;
                        }
                    };
                    match client.mutate(self.authorize(encode(&command)?)?).await {
                        Ok(reply) => {
                            let answer: Answer = decode(reply.into_inner()).map_err(io_error)?;
                            if let Some(error) = answer.error {
                                if matches!(
                                    command,
                                    Command::Acquire { .. } | Command::AcquireOnce { .. }
                                ) && error == "stale acquisition epoch"
                                {
                                    return Err(io::Error::new(
                                        io::ErrorKind::WouldBlock,
                                        mqtt_cluster_core::AcquisitionConflict,
                                    ));
                                }
                                return Err(mutation_status(Status::invalid_argument(error)));
                            }
                            self.remember_controller(address);
                            return Ok(answer);
                        }
                        Err(status) => {
                            let retry = retry_controller(&status);
                            last = mutation_status(status);
                            if !retry || !not_submitted(&last) {
                                return Err(last);
                            }
                        }
                    }
                }
                Err(last)
            })
            .await
            .map_err(|_| {
                mutation_status(Status::deadline_exceeded(
                    "metadata total deadline; outcome unknown",
                ))
            })?
        })
        .await
    }
}
#[derive(Serialize, serde::Deserialize, Clone)]
pub(crate) enum ReadRequest {
    Metadata,
    Status,
    Node {
        node: u64,
    },
    Applied {
        revision: u64,
    },
    AppliedAck {
        node: u64,
        incarnation: String,
        revision: u64,
    },
    Owner {
        key: String,
    },
}
pub(crate) async fn server_tls(config: &Config) -> io::Result<Option<ServerTlsConfig>> {
    match &config.tls {
        None => Ok(None),
        Some(tls) => {
            let cert = read_bounded(&tls.cert_file, 1024 * 1024).await?;
            let key = read_bounded(&tls.key_file, 1024 * 1024).await?;
            let ca = read_bounded(&tls.ca_file, 1024 * 1024).await?;
            Ok(Some(
                ServerTlsConfig::new()
                    .identity(Identity::from_pem(cert, key))
                    .client_ca_root(Certificate::from_pem(ca)),
            ))
        }
    }
}

#[cfg(test)]
mod buffer_tests;
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejected_acquisition_is_safe_but_unknown_write_is_not() {
        let conflict = io::Error::new(
            io::ErrorKind::WouldBlock,
            mqtt_cluster_core::AcquisitionConflict,
        );
        assert!(not_submitted(&conflict));
        assert!(not_submitted(&mutation_status(Status::resource_exhausted(
            "metadata write capacity"
        ))));
        assert!(!not_submitted(&mutation_status(Status::unavailable(
            "metadata commit deadline"
        ))));
    }
    #[test]
    fn heartbeat_channel_does_not_share_the_replication_connection() {
        let address = "http://172.16.240.182:17001";
        assert_ne!(
            channel_cache_key(address, ClientDeadline::Heartbeat),
            channel_cache_key(address, ClientDeadline::Regular)
        );
        assert_ne!(
            channel_cache_key(address, ClientDeadline::Heartbeat),
            channel_cache_key(address, ClientDeadline::Repair)
        );
    }
    #[test]
    fn status_details_survive_io_conversion() {
        let error = io_error(Status::resource_exhausted("metadata write capacity"));
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert!(error.to_string().contains("metadata write capacity"));
    }
}
