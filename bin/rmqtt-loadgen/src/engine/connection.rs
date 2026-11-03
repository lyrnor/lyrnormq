use crate::{LoadError, model::RunConfig};
use mqtt_protocol::{Frame, Packet, decode_frame, encode_frame};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Duration, timeout},
};

pub(super) struct Connection {
    stream: TcpStream,
    buffer: Vec<u8>,
    config: RunConfig,
    pub receive_maximum: u16,
    peer_max_packet: usize,
}
impl Connection {
    pub async fn open(
        config: &RunConfig,
        address: std::net::SocketAddr,
        client_id: String,
    ) -> Result<Self, LoadError> {
        let stream = timeout(
            Duration::from_millis(config.connect_timeout_ms),
            TcpStream::connect(address),
        )
        .await
        .map_err(|_| LoadError::Timeout("TCP connect"))??;
        stream.set_nodelay(true)?;
        let mut conn = Self {
            stream,
            buffer: Vec::with_capacity(4096.min(config.max_packet_bytes)),
            config: config.clone(),
            receive_maximum: u16::MAX,
            peer_max_packet: config.max_packet_bytes,
        };
        conn.send(Packet::Connect {
            client_id,
            clean_session: true,
            keep_alive: 0,
            will: None,
            username: None,
            password: None,
        })
        .await?;
        let frame = timeout(
            Duration::from_millis(config.connect_timeout_ms),
            conn.recv(),
        )
        .await
        .map_err(|_| LoadError::Timeout("CONNACK"))??;
        match frame.packet {
            Packet::ConnAck {
                code: 0,
                session_present: false,
            } if frame.reason_code.value() == 0 => {
                for property in frame.properties {
                    match property {
                        mqtt_protocol::Property::ReceiveMaximum(v) => conn.receive_maximum = v,
                        mqtt_protocol::Property::MaximumPacketSize(v) => {
                            conn.peer_max_packet = usize::try_from(v)
                                .map_err(|_| LoadError::Capacity("peer packet size"))?
                                .min(config.max_packet_bytes)
                        }
                        mqtt_protocol::Property::MaximumQos(v) if v < config.qos => {
                            return Err(LoadError::InvalidData(
                                "server MaximumQoS below requested".into(),
                            ));
                        }
                        _ => {}
                    }
                }
                Ok(conn)
            }
            _ => Err(LoadError::InvalidData(format!(
                "unexpected CONNACK: {:?}",
                frame.packet
            ))),
        }
    }
    pub async fn send(&mut self, packet: Packet) -> Result<(), LoadError> {
        let frame = match self.config.version {
            crate::model::Version::V311 => Frame::v311(packet),
            crate::model::Version::V5 => Frame::v5(packet),
        };
        let bytes = encode_frame(&frame)?;
        if bytes.len() > self.peer_max_packet {
            return Err(LoadError::Capacity("peer/local Maximum Packet Size"));
        }
        timeout(
            Duration::from_millis(self.config.io_timeout_ms),
            self.stream.write_all(&bytes),
        )
        .await
        .map_err(|_| LoadError::Timeout("complete MQTT write"))??;
        Ok(())
    }
    /// Cancellation safe: only read() awaits; buffered complete frame extraction does not.
    pub async fn recv(&mut self) -> Result<Frame, LoadError> {
        loop {
            if let Some((frame, n)) = decode_frame(
                &self.buffer,
                self.config.max_packet_bytes,
                Some(self.config.version.protocol()),
            )? {
                self.buffer.drain(..n);
                return Ok(frame);
            }
            let room = self
                .config
                .max_packet_bytes
                .saturating_sub(self.buffer.len());
            if room == 0 {
                return Err(LoadError::Capacity("MQTT frame buffer"));
            }
            let mut chunk = [0u8; 4096];
            let len = room.min(chunk.len());
            let n = self.stream.read(&mut chunk[..len]).await?;
            if n == 0 {
                return Err(LoadError::InvalidData("peer closed connection".into()));
            }
            self.buffer.extend_from_slice(&chunk[..n]);
        }
    }
}
