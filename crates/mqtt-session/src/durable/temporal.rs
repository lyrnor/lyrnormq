//! Proposal-local observed time and deadline arithmetic shared by message and Will paths.
use super::{EngineError, invalid_record};
use mqtt_protocol::{Properties, Property};

use std::sync::atomic::{AtomicU64, Ordering};

/// Clones copy the observed value, never sharing proposal mutations.
#[derive(Debug, Default)]
pub(super) struct ClockFloor(AtomicU64);
impl Clone for ClockFloor {
    fn clone(&self) -> Self {
        Self::new(self.get())
    }
}
impl ClockFloor {
    pub(super) fn new(value: u64) -> Self {
        Self(AtomicU64::new(value))
    }
    pub(super) fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
    pub(super) fn observe(&self, raw: u64) -> u64 {
        self.0.fetch_max(raw, Ordering::Relaxed).max(raw)
    }
}

pub(super) fn expiry_deadline(properties: &Properties, now: u64) -> Option<u64> {
    properties.iter().find_map(|property| match property {
        Property::MessageExpiryInterval(seconds) => {
            Some(now.saturating_add(u64::from(*seconds) * 1000))
        }
        _ => None,
    })
}
pub(super) fn is_expired(deadline: Option<u64>, now: u64) -> bool {
    deadline.is_some_and(|deadline| deadline <= now)
}
pub(super) fn remaining_properties(
    properties: &Properties,
    deadline: Option<u64>,
    now: u64,
) -> Properties {
    properties
        .iter()
        .map(|property| match (property, deadline) {
            (Property::MessageExpiryInterval(original), Some(deadline)) => {
                let millis = deadline.saturating_sub(now);
                let remaining = millis / 1000 + u64::from(millis % 1000 != 0);
                Property::MessageExpiryInterval(u64::from(*original).min(remaining) as u32)
            }
            _ => property.clone(),
        })
        .collect()
}
pub(super) fn validate_deadline(
    properties: &Properties,
    deadline: Option<u64>,
) -> Result<(), EngineError> {
    let timed = expiry_deadline(properties, 0).is_some();
    if timed != deadline.is_some() {
        return Err(invalid_record(
            "message expiry property and deadline disagree",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn arithmetic_is_absolute_rounded_and_capped() {
        let p = vec![
            Property::MessageExpiryInterval(2),
            Property::UserProperty("k".into(), "v".into()),
        ];
        let d = expiry_deadline(&p, 1000);
        assert_eq!(d, Some(3000));
        assert!(!is_expired(d, 2999));
        assert!(is_expired(d, 3000));
        assert_eq!(
            remaining_properties(&p, d, 2001)[0],
            Property::MessageExpiryInterval(1)
        );
        assert_eq!(
            remaining_properties(&p, d, 3001)[0],
            Property::MessageExpiryInterval(0)
        );
        assert_eq!(
            remaining_properties(&p, d, 0)[0],
            Property::MessageExpiryInterval(2)
        );
        assert_eq!(
            expiry_deadline(&vec![Property::MessageExpiryInterval(0)], 100),
            Some(100)
        );
        assert_eq!(
            expiry_deadline(&vec![Property::MessageExpiryInterval(u32::MAX)], 0),
            Some(u64::from(u32::MAX) * 1000)
        );
        assert_eq!(expiry_deadline(&p, u64::MAX - 1), Some(u64::MAX));
        assert!(validate_deadline(&p, None).is_err());
        assert!(validate_deadline(&Vec::new(), Some(1)).is_err());
        assert_eq!(remaining_properties(&p, d, 2001)[1], p[1]);
    }
}
