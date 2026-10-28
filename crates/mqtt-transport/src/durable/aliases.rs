//! Connection-local wire aliases. Canonical worker and durable state never see them.
use super::failure;
use mqtt_protocol::{Frame, Packet, Property, ProtocolVersion, ReasonCode, encode_frame};
use std::{collections::BTreeMap, io};

pub(super) struct InboundAliases {
    maximum: u16,
    topics: BTreeMap<u16, String>,
}
impl InboundAliases {
    pub(super) fn new(maximum: u16) -> Self {
        Self {
            maximum,
            topics: BTreeMap::new(),
        }
    }
    pub(super) fn normalize(
        &mut self,
        frame: &mut Frame,
        canonical_limit: usize,
    ) -> io::Result<()> {
        if frame.version != ProtocolVersion::V5 || !matches!(frame.packet, Packet::Publish { .. }) {
            return Ok(());
        }
        let mut alias = None;
        for property in &frame.properties {
            if let Property::TopicAlias(value) = property
                && alias.replace(*value).is_some()
            {
                return Err(failure(ReasonCode::PROTOCOL_ERROR, "duplicate Topic Alias"));
            }
        }
        let Some(alias) = alias else {
            return Ok(());
        };
        if alias == 0 || alias > self.maximum {
            return Err(failure(
                ReasonCode::TOPIC_ALIAS_INVALID,
                "Topic Alias exceeds advertised maximum",
            ));
        }
        let Packet::Publish { topic, .. } = &mut frame.packet else {
            return Ok(());
        };
        let binding = if topic.is_empty() {
            *topic =
                self.topics.get(&alias).cloned().ok_or_else(|| {
                    failure(ReasonCode::TOPIC_ALIAS_INVALID, "unbound Topic Alias")
                })?;
            None
        } else {
            Some(topic.clone())
        };
        frame
            .properties
            .retain(|property| !matches!(property, Property::TopicAlias(_)));
        let size = encode_frame(frame).map_err(io::Error::other)?.len();
        if size > canonical_limit {
            return Err(failure(
                ReasonCode::QUOTA_EXCEEDED,
                "expanded canonical publication exceeds resource limit",
            ));
        }
        if let Some(topic) = binding {
            self.topics.insert(alias, topic);
        }
        Ok(())
    }
}

pub(super) struct Binding {
    alias: u16,
    topic: String,
    next: u16,
}
pub(super) struct Prepared {
    pub(super) frame: Frame,
    pub(super) binding: Option<Binding>,
}
pub(super) struct OutboundAliases {
    maximum: u16,
    topics: BTreeMap<u16, String>,
    next: u16,
}
impl OutboundAliases {
    pub(super) fn new(maximum: u16) -> Self {
        Self {
            maximum,
            topics: BTreeMap::new(),
            next: 1,
        }
    }
    /// Preparation does not install a mapping. The writer commits only after a
    /// complete successful write, so dropped/stale leases cannot create aliases.
    pub(super) fn prepare(
        &self,
        mut frame: Frame,
        peer_maximum: u16,
        packet_limit: usize,
    ) -> io::Result<Prepared> {
        let maximum = self.maximum.min(peer_maximum);
        if frame.version != ProtocolVersion::V5
            || maximum == 0
            || !matches!(frame.packet, Packet::Publish { .. })
        {
            return Ok(Prepared {
                frame,
                binding: None,
            });
        }
        if frame
            .properties
            .iter()
            .any(|p| matches!(p, Property::TopicAlias(_)))
        {
            return Err(failure(
                ReasonCode::PROTOCOL_ERROR,
                "noncanonical output contains Topic Alias",
            ));
        }
        let canonical_size = encode_frame(&frame).map_err(io::Error::other)?.len();
        if canonical_size > packet_limit {
            return Err(failure(
                ReasonCode::PACKET_TOO_LARGE,
                "canonical output exceeds limit",
            ));
        }
        let Packet::Publish { topic, .. } = &mut frame.packet else {
            return Ok(Prepared {
                frame,
                binding: None,
            });
        };
        if topic.is_empty() {
            return Err(failure(
                ReasonCode::PROTOCOL_ERROR,
                "canonical output has empty topic",
            ));
        }
        if let Some((&alias, _)) = self
            .topics
            .iter()
            .find(|(id, name)| **id <= maximum && *name == topic)
        {
            let name = std::mem::take(topic);
            frame.properties.push(Property::TopicAlias(alias));
            let size = encode_frame(&frame).map_err(io::Error::other)?.len();
            if size <= packet_limit && size < canonical_size {
                return Ok(Prepared {
                    frame,
                    binding: None,
                });
            }
            if let Packet::Publish { topic, .. } = &mut frame.packet {
                *topic = name;
            }
            frame.properties.pop();
            return Ok(Prepared {
                frame,
                binding: None,
            });
        }
        let alias = self.next.min(maximum);
        let name = topic.clone();
        frame.properties.push(Property::TopicAlias(alias));
        if encode_frame(&frame).map_err(io::Error::other)?.len() > packet_limit {
            frame.properties.pop();
            return Ok(Prepared {
                frame,
                binding: None,
            });
        }
        Ok(Prepared {
            frame,
            binding: Some(Binding {
                alias,
                topic: name,
                next: if alias == maximum { 1 } else { alias + 1 },
            }),
        })
    }
    pub(super) fn commit(&mut self, binding: Option<Binding>) {
        if let Some(binding) = binding {
            self.topics.insert(binding.alias, binding.topic);
            self.next = binding.next;
        }
    }
}

#[cfg(test)]
mod tests;
