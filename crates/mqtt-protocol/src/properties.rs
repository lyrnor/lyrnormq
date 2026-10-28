//! Ordered typed MQTT5 properties and strict packet-context validation.
use crate::wire::{Reader, append, binary, malformed, string, varint, violation};
use crate::{ProtocolError, codec::validate_string};
use serde::{Deserialize, Serialize};

pub type Properties = Vec<Property>;
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Property {
    PayloadFormatIndicator(u8),
    MessageExpiryInterval(u32),
    ContentType(String),
    ResponseTopic(String),
    CorrelationData(Vec<u8>),
    SubscriptionIdentifier(u32),
    SessionExpiryInterval(u32),
    AssignedClientIdentifier(String),
    ServerKeepAlive(u16),
    AuthenticationMethod(String),
    AuthenticationData(Vec<u8>),
    RequestProblemInformation(u8),
    WillDelayInterval(u32),
    RequestResponseInformation(u8),
    ResponseInformation(String),
    ServerReference(String),
    ReasonString(String),
    ReceiveMaximum(u16),
    TopicAliasMaximum(u16),
    TopicAlias(u16),
    MaximumQos(u8),
    RetainAvailable(u8),
    UserProperty(String, String),
    MaximumPacketSize(u32),
    WildcardSubscriptionAvailable(u8),
    SubscriptionIdentifierAvailable(u8),
    SharedSubscriptionAvailable(u8),
}
impl std::fmt::Debug for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PayloadFormatIndicator(value) => f
                .debug_tuple("PayloadFormatIndicator")
                .field(value)
                .finish(),
            Self::MessageExpiryInterval(value) => {
                f.debug_tuple("MessageExpiryInterval").field(value).finish()
            }
            Self::ContentType(value) => f.debug_tuple("ContentType").field(value).finish(),
            Self::ResponseTopic(value) => f.debug_tuple("ResponseTopic").field(value).finish(),
            Self::CorrelationData(value) => f.debug_tuple("CorrelationData").field(value).finish(),
            Self::SubscriptionIdentifier(value) => f
                .debug_tuple("SubscriptionIdentifier")
                .field(value)
                .finish(),
            Self::SessionExpiryInterval(value) => {
                f.debug_tuple("SessionExpiryInterval").field(value).finish()
            }
            Self::AssignedClientIdentifier(value) => f
                .debug_tuple("AssignedClientIdentifier")
                .field(value)
                .finish(),
            Self::ServerKeepAlive(value) => f.debug_tuple("ServerKeepAlive").field(value).finish(),
            Self::AuthenticationMethod(value) => {
                f.debug_tuple("AuthenticationMethod").field(value).finish()
            }
            Self::AuthenticationData(_) => f.write_str("AuthenticationData([REDACTED])"),
            Self::RequestProblemInformation(value) => f
                .debug_tuple("RequestProblemInformation")
                .field(value)
                .finish(),
            Self::WillDelayInterval(value) => {
                f.debug_tuple("WillDelayInterval").field(value).finish()
            }
            Self::RequestResponseInformation(value) => f
                .debug_tuple("RequestResponseInformation")
                .field(value)
                .finish(),
            Self::ResponseInformation(value) => {
                f.debug_tuple("ResponseInformation").field(value).finish()
            }
            Self::ServerReference(value) => f.debug_tuple("ServerReference").field(value).finish(),
            Self::ReasonString(value) => f.debug_tuple("ReasonString").field(value).finish(),
            Self::ReceiveMaximum(value) => f.debug_tuple("ReceiveMaximum").field(value).finish(),
            Self::TopicAliasMaximum(value) => {
                f.debug_tuple("TopicAliasMaximum").field(value).finish()
            }
            Self::TopicAlias(value) => f.debug_tuple("TopicAlias").field(value).finish(),
            Self::MaximumQos(value) => f.debug_tuple("MaximumQos").field(value).finish(),
            Self::RetainAvailable(value) => f.debug_tuple("RetainAvailable").field(value).finish(),
            Self::UserProperty(a, b) => f.debug_tuple("UserProperty").field(a).field(b).finish(),
            Self::MaximumPacketSize(value) => {
                f.debug_tuple("MaximumPacketSize").field(value).finish()
            }
            Self::WildcardSubscriptionAvailable(value) => f
                .debug_tuple("WildcardSubscriptionAvailable")
                .field(value)
                .finish(),
            Self::SubscriptionIdentifierAvailable(value) => f
                .debug_tuple("SubscriptionIdentifierAvailable")
                .field(value)
                .finish(),
            Self::SharedSubscriptionAvailable(value) => f
                .debug_tuple("SharedSubscriptionAvailable")
                .field(value)
                .finish(),
        }
    }
}

impl Property {
    /// Standard property identifier; values remain typed rather than untrusted bytes.
    pub const fn id(&self) -> u8 {
        match self {
            Self::PayloadFormatIndicator(_) => 1,
            Self::MessageExpiryInterval(_) => 2,
            Self::ContentType(_) => 3,
            Self::ResponseTopic(_) => 8,
            Self::CorrelationData(_) => 9,
            Self::SubscriptionIdentifier(_) => 11,
            Self::SessionExpiryInterval(_) => 17,
            Self::AssignedClientIdentifier(_) => 18,
            Self::ServerKeepAlive(_) => 19,
            Self::AuthenticationMethod(_) => 21,
            Self::AuthenticationData(_) => 22,
            Self::RequestProblemInformation(_) => 23,
            Self::WillDelayInterval(_) => 24,
            Self::RequestResponseInformation(_) => 25,
            Self::ResponseInformation(_) => 26,
            Self::ServerReference(_) => 28,
            Self::ReasonString(_) => 31,
            Self::ReceiveMaximum(_) => 33,
            Self::TopicAliasMaximum(_) => 34,
            Self::TopicAlias(_) => 35,
            Self::MaximumQos(_) => 36,
            Self::RetainAvailable(_) => 37,
            Self::UserProperty(_, _) => 38,
            Self::MaximumPacketSize(_) => 39,
            Self::WildcardSubscriptionAvailable(_) => 40,
            Self::SubscriptionIdentifierAvailable(_) => 41,
            Self::SharedSubscriptionAvailable(_) => 42,
        }
    }
}
/// Context 0 is Will Properties; otherwise the MQTT control packet type.
fn permitted(context: u8, id: u8) -> bool {
    match context {
        0 => matches!(id, 1 | 2 | 3 | 8 | 9 | 24 | 38),
        1 => matches!(id, 17 | 21 | 22 | 23 | 25 | 33 | 34 | 38 | 39),
        2 => matches!(
            id,
            17 | 18 | 19 | 21 | 22 | 26 | 28 | 31 | 33 | 34 | 36 | 37 | 38 | 39 | 40 | 41 | 42
        ),
        3 => matches!(id, 1 | 2 | 3 | 8 | 9 | 11 | 35 | 38),
        4..=7 | 9 | 11 => matches!(id, 31 | 38),
        8 => matches!(id, 11 | 38),
        10 => id == 38,
        14 => matches!(id, 17 | 28 | 31 | 38),
        15 => matches!(id, 21 | 22 | 31 | 38),
        _ => false,
    }
}
pub(super) fn validate(properties: &[Property], context: u8) -> Result<(), ProtocolError> {
    let mut seen = 0u64;
    for property in properties {
        let id = property.id();
        if !permitted(context, id) {
            return Err(malformed("property is not allowed on this packet"));
        }
        let repeat = id == 38 || (id == 11 && context == 3);
        if !repeat && seen & (1u64 << id) != 0 {
            return Err(violation("duplicate singleton property"));
        }
        seen |= 1u64 << id;
        match property {
            Property::PayloadFormatIndicator(n)
            | Property::RequestProblemInformation(n)
            | Property::RequestResponseInformation(n)
            | Property::MaximumQos(n)
            | Property::RetainAvailable(n)
            | Property::WildcardSubscriptionAvailable(n)
            | Property::SubscriptionIdentifierAvailable(n)
            | Property::SharedSubscriptionAvailable(n)
                if *n > 1 =>
            {
                return Err(violation("invalid boolean or Maximum QoS property"));
            }
            Property::ReceiveMaximum(0) | Property::MaximumPacketSize(0) => {
                return Err(violation("zero property value"));
            }
            Property::TopicAlias(0) => {
                return Err(ProtocolError::Mqtt5(crate::ReasonCode::TOPIC_ALIAS_INVALID));
            }
            Property::SubscriptionIdentifier(n) if *n == 0 || *n > 268_435_455 => {
                return Err(violation("invalid Subscription Identifier"));
            }
            Property::ResponseTopic(topic) => {
                validate_string(topic)?;
                if topic.is_empty() || topic.contains(['+', '#']) {
                    return Err(violation("Response Topic must be a nonempty Topic Name"));
                }
            }
            Property::AssignedClientIdentifier(s) if s.is_empty() => {
                return Err(violation("empty assigned Client Identifier"));
            }
            _ => {}
        }
    }
    if context == 15 && seen & (1 << 21) == 0 {
        return Err(violation("AUTH requires Authentication Method"));
    }
    if matches!(context, 1 | 2) && seen & (1 << 22) != 0 && seen & (1 << 21) == 0 {
        return Err(violation(
            "Authentication Data requires Authentication Method",
        ));
    }
    Ok(())
}
pub(super) fn read(reader: &mut Reader<'_>, context: u8) -> Result<Properties, ProtocolError> {
    let length = reader.varint()? as usize;
    let bytes = reader.take(length)?;
    let mut r = Reader::new(bytes);
    let mut properties = Vec::new();
    let mut seen = 0u64;
    while !r.done() {
        let id = r.varint()?;
        if id > 63 || !permitted(context, id as u8) {
            return Err(malformed("unknown or misplaced property identifier"));
        }
        let repeated = id == 38 || (id == 11 && context == 3);
        if !repeated && seen & (1u64 << id) != 0 {
            return Err(violation("duplicate singleton property"));
        }
        seen |= 1u64 << id;
        let property = match id {
            1 => Property::PayloadFormatIndicator(r.byte()?),
            2 => Property::MessageExpiryInterval(r.u32()?),
            3 => Property::ContentType(r.string()?),
            8 => Property::ResponseTopic(r.string()?),
            9 => Property::CorrelationData(r.binary()?),
            11 => Property::SubscriptionIdentifier(r.varint()?),
            17 => Property::SessionExpiryInterval(r.u32()?),
            18 => Property::AssignedClientIdentifier(r.string()?),
            19 => Property::ServerKeepAlive(r.u16()?),
            21 => Property::AuthenticationMethod(r.string()?),
            22 => Property::AuthenticationData(r.binary()?),
            23 => Property::RequestProblemInformation(r.byte()?),
            24 => Property::WillDelayInterval(r.u32()?),
            25 => Property::RequestResponseInformation(r.byte()?),
            26 => Property::ResponseInformation(r.string()?),
            28 => Property::ServerReference(r.string()?),
            31 => Property::ReasonString(r.string()?),
            33 => Property::ReceiveMaximum(r.u16()?),
            34 => Property::TopicAliasMaximum(r.u16()?),
            35 => Property::TopicAlias(r.u16()?),
            36 => Property::MaximumQos(r.byte()?),
            37 => Property::RetainAvailable(r.byte()?),
            38 => Property::UserProperty(r.string()?, r.string()?),
            39 => Property::MaximumPacketSize(r.u32()?),
            40 => Property::WildcardSubscriptionAvailable(r.byte()?),
            41 => Property::SubscriptionIdentifierAvailable(r.byte()?),
            42 => Property::SharedSubscriptionAvailable(r.byte()?),
            _ => return Err(malformed("unknown property identifier")),
        };
        properties.push(property);
    }
    validate(&properties, context)?;
    Ok(properties)
}
pub(super) fn write(
    out: &mut Vec<u8>,
    properties: &[Property],
    context: u8,
) -> Result<(), ProtocolError> {
    validate(properties, context)?;
    let mut bytes = Vec::new();
    for property in properties {
        append(&mut bytes, &[property.id()])?;
        match property {
            Property::PayloadFormatIndicator(n)
            | Property::RequestProblemInformation(n)
            | Property::RequestResponseInformation(n)
            | Property::MaximumQos(n)
            | Property::RetainAvailable(n)
            | Property::WildcardSubscriptionAvailable(n)
            | Property::SubscriptionIdentifierAvailable(n)
            | Property::SharedSubscriptionAvailable(n) => append(&mut bytes, &[*n])?,
            Property::MessageExpiryInterval(n)
            | Property::SessionExpiryInterval(n)
            | Property::WillDelayInterval(n)
            | Property::MaximumPacketSize(n) => append(&mut bytes, &n.to_be_bytes())?,
            Property::ReceiveMaximum(n)
            | Property::TopicAliasMaximum(n)
            | Property::TopicAlias(n)
            | Property::ServerKeepAlive(n) => append(&mut bytes, &n.to_be_bytes())?,
            Property::ContentType(s)
            | Property::ResponseTopic(s)
            | Property::AssignedClientIdentifier(s)
            | Property::AuthenticationMethod(s)
            | Property::ResponseInformation(s)
            | Property::ServerReference(s)
            | Property::ReasonString(s) => string(&mut bytes, s)?,
            Property::CorrelationData(data) | Property::AuthenticationData(data) => {
                binary(&mut bytes, data)?
            }
            Property::SubscriptionIdentifier(n) => varint(&mut bytes, *n as usize)?,
            Property::UserProperty(k, v) => {
                string(&mut bytes, k)?;
                string(&mut bytes, v)?;
            }
        }
    }
    varint(out, bytes.len())?;
    append(out, &bytes)
}
