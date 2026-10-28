# Frozen module contract v1
All crates use workspace edition/version/license. No root manifest changes by agents.
## mqtt-protocol (no dependencies)
`pub enum Packet { Connect { client_id: String, clean_session: bool, keep_alive: u16 }, ConnAck { session_present: bool, code: u8 }, Publish { topic: String, payload: Vec<u8> }, Subscribe { packet_id: u16, filters: Vec<(String,u8)> }, SubAck { packet_id: u16, codes: Vec<u8> }, Unsubscribe { packet_id: u16, filters: Vec<String> }, UnsubAck { packet_id: u16 }, PingReq, PingResp, Disconnect }` derive Debug, Clone, PartialEq, Eq.
`pub enum ProtocolError { Malformed(String), UnsupportedVersion, UnsupportedFeature(String), PacketTooLarge }` impl Error/Display.
`pub fn decode(input: &[u8], max_packet_size: usize) -> Result<Option<(Packet,usize)>,ProtocolError>` incremental no consumption on partial; size is full frame. `pub fn encode(packet: &Packet) -> Result<Vec<u8>,ProtocolError>`.
Validate fixed flags, remaining length <=4 bytes, UTF8 MQTT constraints, packet identifiers/nonempty filters, QoS0 only; unsupported QoS/retain/Will/persistent sessions/credentials fail explicitly. CONNECT version 4 only; version5 -> UnsupportedVersion. Subscribe requested QoS 0/1/2 valid (server grants 0). Invalid filters can be rejected in SUBACK by router.
## mqtt-router (no dependencies)
`pub fn validate_filter(filter:&str)->bool`, `pub fn matches(filter:&str,topic:&str)->bool` incl $ system topics.
`pub struct Router` Default. `pub fn subscribe(&mut self, client:u64, filter:String)->bool`; `pub fn unsubscribe(&mut self,client:u64,filter:&str)`; `pub fn remove_client(&mut self,client:u64)`; `pub fn matching_clients(&self,topic:&str)->Vec<u64>` dedup. `pub fn subscription_count(&self,client:u64)->usize`. Bounded by caller. Shared subscriptions unsupported: reject $share/ filters. No retained in phase1.
## mqtt-transport
Dependencies protocol, router, tokio. `pub struct BrokerConfig { pub max_packet_size:usize, pub max_connections:usize, pub outbound_capacity:usize, pub max_subscriptions:usize, pub handshake_timeout:std::time::Duration, pub write_timeout:std::time::Duration }` Default.
`pub async fn serve(listener:tokio::net::TcpListener, config:BrokerConfig, shutdown:tokio::sync::watch::Receiver<bool>)->std::io::Result<()>`. Bind is binary responsibility. Handle keepalive 1.5x, all packets through protocol API, unsubscribe, bounded writes and connections, takeover via unique numeric generation; graceful shutdown joins tasks. Reject config zero limits with InvalidInput. No independent codec.
## mqtt-session
Phase2 groundwork, no runtime integration. Own public API for packet identifier allocator and explicit QoS2 incoming state machine (receive publish -> receive pubrel -> delivery once), bounded tracking; document semantics and test. No claim persistence implemented.
## mqtt-storage
Phase2 groundwork, no runtime integration. Own public synchronous Storage trait + bounded memory implementation and technical assessment in crate README. If persistent implementation is included, IO only via spawn_blocking by future callers; do not claim runtime crash recovery. Document durability semantics and limits.
## Quality
Own root tests/** .github/workflows/** benches/** fuzz/**. Black-box Python stdlib tests can use actual paho-mqtt if installed, or mosquitto CLI; no root Cargo changes. Test executable target/debug/rmqtt with `--listen 127.0.0.1:PORT`. Binary default 127.0.0.1:1883, ctrl-c watch shutdown.
