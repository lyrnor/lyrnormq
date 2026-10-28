//! Validated standard MQTT5 Reason Code values.
use crate::ProtocolError;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReasonCode(u8);
impl ReasonCode {
    pub const SUCCESS: Self = Self(0);
    pub const GRANTED_QOS_1: Self = Self(1);
    pub const GRANTED_QOS_2: Self = Self(2);
    pub const DISCONNECT_WITH_WILL_MESSAGE: Self = Self(4);
    pub const NO_MATCHING_SUBSCRIBERS: Self = Self(16);
    pub const NO_SUBSCRIPTION_EXISTED: Self = Self(17);
    pub const CONTINUE_AUTHENTICATION: Self = Self(24);
    pub const REAUTHENTICATE: Self = Self(25);
    pub const UNSPECIFIED_ERROR: Self = Self(128);
    pub const MALFORMED_PACKET: Self = Self(129);
    pub const PROTOCOL_ERROR: Self = Self(130);
    pub const IMPLEMENTATION_SPECIFIC_ERROR: Self = Self(131);
    pub const UNSUPPORTED_PROTOCOL_VERSION: Self = Self(132);
    pub const CLIENT_IDENTIFIER_NOT_VALID: Self = Self(133);
    pub const BAD_USER_NAME_OR_PASSWORD: Self = Self(134);
    pub const NOT_AUTHORIZED: Self = Self(135);
    pub const SERVER_UNAVAILABLE: Self = Self(136);
    pub const SERVER_BUSY: Self = Self(137);
    pub const BANNED: Self = Self(138);
    pub const SERVER_SHUTTING_DOWN: Self = Self(139);
    pub const BAD_AUTHENTICATION_METHOD: Self = Self(140);
    pub const KEEP_ALIVE_TIMEOUT: Self = Self(141);
    pub const SESSION_TAKEN_OVER: Self = Self(142);
    pub const TOPIC_FILTER_INVALID: Self = Self(143);
    pub const TOPIC_NAME_INVALID: Self = Self(144);
    pub const PACKET_IDENTIFIER_IN_USE: Self = Self(145);
    pub const PACKET_IDENTIFIER_NOT_FOUND: Self = Self(146);
    pub const RECEIVE_MAXIMUM_EXCEEDED: Self = Self(147);
    pub const TOPIC_ALIAS_INVALID: Self = Self(148);
    pub const PACKET_TOO_LARGE: Self = Self(149);
    pub const MESSAGE_RATE_TOO_HIGH: Self = Self(150);
    pub const QUOTA_EXCEEDED: Self = Self(151);
    pub const ADMINISTRATIVE_ACTION: Self = Self(152);
    pub const PAYLOAD_FORMAT_INVALID: Self = Self(153);
    pub const RETAIN_NOT_SUPPORTED: Self = Self(154);
    pub const QOS_NOT_SUPPORTED: Self = Self(155);
    pub const USE_ANOTHER_SERVER: Self = Self(156);
    pub const SERVER_MOVED: Self = Self(157);
    pub const SHARED_SUBSCRIPTIONS_NOT_SUPPORTED: Self = Self(158);
    pub const CONNECTION_RATE_EXCEEDED: Self = Self(159);
    pub const MAXIMUM_CONNECT_TIME: Self = Self(160);
    pub const SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED: Self = Self(161);
    pub const WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED: Self = Self(162);
    pub const fn value(self) -> u8 {
        self.0
    }
}
impl TryFrom<u8> for ReasonCode {
    type Error = ProtocolError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if matches!(value, 0..=2 | 4 | 16 | 17 | 24 | 25 | 128..=162) {
            Ok(Self(value))
        } else {
            Err(ProtocolError::ProtocolViolation(
                "unknown Reason Code".into(),
            ))
        }
    }
}
/// Packet-context reason validation; numeric codes are shared, allowed sets are not.
pub(super) fn valid_reason(kind: u8, value: u8) -> bool {
    match kind {
        2 => matches!(value,0|128..=138|140|144|149|151|153..=157|159),
        4 | 5 => matches!(value, 0 | 16 | 128 | 131 | 135 | 144 | 145 | 151 | 153),
        6 | 7 => matches!(value, 0 | 146),
        9 => matches!(
            value,
            0..=2 | 128 | 131 | 135 | 143 | 145 | 151 | 158 | 161 | 162
        ),
        11 => matches!(value, 0 | 17 | 128 | 131 | 135 | 143 | 145),
        14 => matches!(value,0|4|128..=131|135|137|139|141..=144|147..=162),
        15 => matches!(value, 0 | 24 | 25),
        _ => value == 0,
    }
}
