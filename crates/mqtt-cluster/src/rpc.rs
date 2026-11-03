//! Private authenticated protobuf RPC transport and bounded peer connection cache.
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
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig},
};
pub(crate) mod wire {
    tonic::include_proto!("rmqtt.cluster.v1");
}
pub(crate) const MAX_WIRE: usize = 16 * 1024 * 1024;
pub(crate) fn encode<T: Serialize>(value: &T) -> io::Result<wire::Envelope> {
    let data = serde_json::to_vec(value).map_err(io::Error::other)?;
    if data.len() > MAX_WIRE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cluster message exceeds limit",
        ));
    }
    Ok(wire::Envelope { data })
}
pub(crate) fn decode<T: DeserializeOwned>(envelope: wire::Envelope) -> Result<T, Status> {
    if envelope.data.len() > MAX_WIRE {
        return Err(Status::resource_exhausted("cluster message exceeds limit"));
    }
    serde_json::from_slice(&envelope.data)
        .map_err(|_| Status::invalid_argument("invalid cluster message"))
}
pub(crate) fn io_error(_error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, "cluster RPC unavailable")
}
#[derive(Clone)]
pub(crate) struct ClientRpc {
    config: Arc<Config>,
    token: Arc<str>,
    channels: Arc<Mutex<BTreeMap<String, Channel>>>,
    preferred: Arc<Mutex<Option<String>>>,
}
impl ClientRpc {
    pub fn config_node(&self) -> u64 {
        self.config.node_id
    }
    pub async fn new(config: Arc<Config>) -> io::Result<Self> {
        let secret = token(&config).await?;
        Ok(Self {
            config,
            token: Arc::from(secret),
            channels: Arc::new(Mutex::new(BTreeMap::new())),
            preferred: Arc::new(Mutex::new(None)),
        })
    }
    pub fn authorize<T>(&self, value: T) -> io::Result<Request<T>> {
        let mut request = Request::new(value);
        request.metadata_mut().insert(
            "x-rmqtt-cluster",
            self.config.cluster_id.parse().map_err(io_error)?,
        );
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", self.token).parse().map_err(io_error)?,
        );
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
        let expected = format!("Bearer {}", self.token);
        let mut difference = supplied.len() ^ expected.len();
        for (i, b) in expected.bytes().enumerate() {
            difference |= usize::from(b ^ supplied.get(i).copied().unwrap_or(0));
        }
        if difference != 0 || cluster != Some(self.config.cluster_id.as_bytes()) {
            Err(Status::unauthenticated("cluster authentication required"))
        } else {
            Ok(())
        }
    }
    pub async fn client(
        &self,
        address: &str,
    ) -> io::Result<wire::cluster_client::ClusterClient<Channel>> {
        let cached = self
            .channels
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(address)
            .cloned();
        let channel = if let Some(channel) = cached {
            channel
        } else {
            let mut endpoint = Endpoint::from_shared(address.to_owned())
                .map_err(io_error)?
                .connect_timeout(Duration::from_secs(1))
                .timeout(Duration::from_secs(2));
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
            let channel = endpoint.connect_lazy();
            let mut channels = self.channels.lock().unwrap_or_else(PoisonError::into_inner);
            if channels.len() >= 1024 {
                return Err(io_error("cluster peer capacity"));
            }
            channels.insert(address.to_owned(), channel.clone());
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
    fn remember_controller(&self, address: String) {
        *self
            .preferred
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(address);
    }
    pub async fn metadata(&self) -> io::Result<Metadata> {
        self.request_seeds(ReadRequest::Metadata).await
    }
    pub async fn status(&self) -> io::Result<ClusterStatus> {
        self.request_seeds(ReadRequest::Status).await
    }
    async fn request_seeds<T: DeserializeOwned>(&self, kind: ReadRequest) -> io::Result<T> {
        for address in self.controller_addresses() {
            let mut client = self.client(&address).await?;
            if let Ok(reply) = client.read(self.authorize(encode(&kind)?)?).await {
                if matches!(kind, ReadRequest::Metadata) {
                    self.remember_controller(address);
                }
                return decode(reply.into_inner()).map_err(io_error);
            }
        }
        Err(io_error("controller quorum unavailable"))
    }
    pub async fn mutate(&self, command: Command) -> io::Result<Answer> {
        for address in self.controller_addresses() {
            let mut client = self.client(&address).await?;
            if let Ok(reply) = client.mutate(self.authorize(encode(&command)?)?).await {
                let answer: Answer = decode(reply.into_inner()).map_err(io_error)?;
                if let Some(error) = answer.error {
                    return Err(io_error(error));
                }
                self.remember_controller(address);
                return Ok(answer);
            }
        }
        Err(io_error("controller quorum unavailable"))
    }
}
#[derive(Serialize, serde::Deserialize, Clone)]
pub(crate) enum ReadRequest {
    Metadata,
    Status,
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
