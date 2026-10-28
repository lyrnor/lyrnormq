//! Retained updates share the same proposed transaction as ordinary fanout.
use super::{EngineError, Message, SessionEngine};

impl SessionEngine {
    pub(super) fn publish(&mut self, message: Message) -> Result<(), EngineError> {
        self.validate_message(&message)?;
        if message.retain {
            if message.payload.is_empty() {
                self.retained_properties.remove(&message.topic);
            } else {
                self.retained_properties
                    .insert(message.topic.clone(), message.properties.clone());
            }
            self.retained
                .apply(message.topic.clone(), message.payload.clone(), message.qos)
                .map_err(EngineError::Retained)?;
        }
        // Established subscriptions receive a normal publication, never a replay.
        self.route(Message {
            retain: false,
            ..message
        })
    }
}
