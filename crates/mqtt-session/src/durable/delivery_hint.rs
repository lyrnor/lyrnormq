//! Conservative installed-state hint for delivery notifications.
use super::SessionEngine;

impl SessionEngine {
    /// Whether the currently installed online session has queue/replay work.
    ///
    /// This is only a conservative notification prefilter. It performs no clock
    /// observation, authorization, encoding, mutation or admission. Expired,
    /// denied, oversized or flow-limited work may return true. Callers must still
    /// execute the original poll/begin/commit path after normal maintenance.
    pub fn delivery_may_be_ready(&self, client_id: &str) -> bool {
        self.sessions.get(client_id).is_some_and(|session| {
            session.online && (!session.queued.is_empty() || !session.replay.is_empty())
        })
    }
}

#[cfg(test)]
mod tests;
