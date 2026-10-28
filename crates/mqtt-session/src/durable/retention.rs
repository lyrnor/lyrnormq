//! Retained updates share the same proposed transaction as ordinary fanout.
use super::{EngineError, Message, SessionEngine, temporal};
impl SessionEngine {
    pub(super) fn publish(&mut self, message: Message) -> Result<(), EngineError> {
        self.validate_message(&message)?;
        self.expire_messages()?;
        let expired = temporal::is_expired(message.expires_at, self.clock.now_millis());
        if message.retain {
            if message.payload.is_empty() || expired {
                self.retained_properties.remove(&message.topic);
                self.retained_expiry.remove(&message.topic);
                self.retained
                    .apply(message.topic.clone(), Vec::new(), message.qos)
                    .map_err(EngineError::Retained)?;
            } else {
                self.retained_properties
                    .insert(message.topic.clone(), message.properties.clone());
                if let Some(deadline) = message.expires_at {
                    self.retained_expiry.insert(message.topic.clone(), deadline);
                } else {
                    self.retained_expiry.remove(&message.topic);
                }
                self.retained
                    .apply(message.topic.clone(), message.payload.clone(), message.qos)
                    .map_err(EngineError::Retained)?;
            }
        }
        if expired {
            return Ok(());
        }
        self.route(Message {
            retain: false,
            ..message
        })
    }
}
