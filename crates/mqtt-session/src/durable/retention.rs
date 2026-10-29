//! Retained updates share the same proposed transaction as ordinary fanout.
use super::{EngineError, Message, Publication, SessionEngine, temporal};
impl SessionEngine {
    pub(super) fn publish_from(
        &mut self,
        message: Message,
        publisher: Option<&str>,
    ) -> Result<(), EngineError> {
        self.validate_properties(
            &message.packet((message.qos != 0).then_some(1), false),
            &message.properties,
        )?;
        self.validate_message(&message)?;
        self.expire_messages()?;
        let expired = temporal::is_expired(message.expires_at, self.effective_now());
        if self.partition {
            self.publications
                .push(Publication::from_message(message, publisher, expired));
            return Ok(());
        }
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
        self.route(message, publisher)
    }
}
