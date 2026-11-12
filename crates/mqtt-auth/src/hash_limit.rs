//! Process-wide cap on concurrent Argon2 verifications.
use std::sync::{Mutex, PoisonError};

const LIMIT: u32 = 4;
static IN_USE: Mutex<u32> = Mutex::new(0);

/// One of the four Argon2 slots. Dropping it, including after cancellation, releases the slot.
pub struct PasswordHashPermit {
    _private: (),
}
pub fn try_acquire_password_hash_permit() -> Option<PasswordHashPermit> {
    let mut in_use = IN_USE.lock().unwrap_or_else(PoisonError::into_inner);
    if *in_use >= LIMIT {
        return None;
    }
    *in_use += 1;
    Some(PasswordHashPermit { _private: () })
}
impl Drop for PasswordHashPermit {
    fn drop(&mut self) {
        let mut in_use = IN_USE.lock().unwrap_or_else(PoisonError::into_inner);
        *in_use = in_use.saturating_sub(1);
    }
}

#[cfg(test)]
mod tests {
    use super::try_acquire_password_hash_permit;

    #[test]
    fn fifth_hash_permit_is_refused_until_one_is_released() {
        let held: Vec<_> = (0..4)
            .map(|_| try_acquire_password_hash_permit().expect("slot"))
            .collect();
        assert!(try_acquire_password_hash_permit().is_none());
        drop(held);
        assert!(try_acquire_password_hash_permit().is_some());
    }
}
