//! Ordinary live-match projection, separate from shared group selection.
use super::*;

#[derive(Default)]
pub(super) struct Matches {
    subscriptions: Vec<MatchedSubscription>,
}
struct MatchedSubscription {
    qos: u8,
    rap: bool,
    identifier: Option<u32>,
}
pub(super) struct Choice {
    pub(super) qos: u8,
    pub(super) retain: bool,
    pub(super) identifiers: Vec<u32>,
}
impl Matches {
    pub(super) fn push(&mut self, qos: u8, rap: bool, identifier: Option<u32>) {
        self.subscriptions.push(MatchedSubscription {
            qos,
            rap,
            identifier,
        });
    }
    pub(super) fn choices(&self, version: ProtocolVersion, source_retain: bool) -> Vec<Choice> {
        let preserve = self.subscriptions.iter().any(|s| s.rap);
        let clear = self.subscriptions.iter().any(|s| !s.rap);
        // A single flag cannot independently express mixed RAP decisions. Choose
        // the permitted per-subscription multiple-copy policy only in this case.
        if version == ProtocolVersion::V5 && source_retain && preserve && clear {
            return self
                .subscriptions
                .iter()
                .map(|s| Choice {
                    qos: s.qos,
                    retain: s.rap,
                    identifiers: s.identifier.into_iter().collect(),
                })
                .collect();
        }
        vec![Choice {
            qos: self.subscriptions.iter().map(|s| s.qos).max().unwrap_or(0),
            retain: source_retain && preserve,
            identifiers: self
                .subscriptions
                .iter()
                .filter_map(|s| s.identifier)
                .collect(),
        }]
    }
}
