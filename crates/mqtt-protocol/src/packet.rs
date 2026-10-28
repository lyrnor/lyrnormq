//! MQTT control packet data model.

/// Connection-associated publication sent when the connection ends abnormally.
/// Payload bytes are opaque and the MQTT CONNECT field is limited to 65,535 bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Will {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    Connect {
        client_id: String,
        clean_session: bool,
        keep_alive: u16,
        will: Option<Will>,
    },
    ConnAck {
        session_present: bool,
        code: u8,
    },
    Publish {
        topic: String,
        payload: Vec<u8>,
        qos: u8,
        packet_id: Option<u16>,
        dup: bool,
        retain: bool,
    },
    PubAck {
        packet_id: u16,
    },
    PubRec {
        packet_id: u16,
    },
    PubRel {
        packet_id: u16,
    },
    PubComp {
        packet_id: u16,
    },
    Subscribe {
        packet_id: u16,
        filters: Vec<(String, u8)>,
    },
    SubAck {
        packet_id: u16,
        codes: Vec<u8>,
    },
    Unsubscribe {
        packet_id: u16,
        filters: Vec<String>,
    },
    UnsubAck {
        packet_id: u16,
    },
    PingReq,
    PingResp,
    Disconnect,
}
