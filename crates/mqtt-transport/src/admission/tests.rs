use super::*;
use std::time::Duration;
#[test]
fn fractional_refill_is_exact_and_clock_cannot_rewind() {
    let now = Instant::now();
    let mut bucket = Bucket::new(2, 1, now);
    assert!(bucket.charge(now));
    assert!(!bucket.charge(now + Duration::from_millis(250)));
    assert!(!bucket.charge(now + Duration::from_millis(499)));
    assert!(bucket.charge(now + Duration::from_millis(500)));
    assert!(!bucket.charge(now));
    assert!(!bucket.charge(now + Duration::from_millis(750)));
    assert!(bucket.charge(now + Duration::from_secs(1)));
}
#[test]
fn refill_saturates_without_overflow_and_zero_rate_only_disables_bucket() {
    let now = Instant::now();
    let mut bucket = Bucket::new(u32::MAX, u32::MAX, now);
    assert!(bucket.charge(now + Duration::from_secs(10_000_000)));
    assert_eq!(bucket.credit, (u128::from(u32::MAX) - 1) * TOKEN);
    let admission = Admission::new(&BrokerConfig {
        max_auth_tasks: 1,
        max_handshakes: 1,
        auth_rate_per_second: 0,
        ..BrokerConfig::default()
    })
    .unwrap();
    let cpu = admission.authentication().unwrap();
    assert!(admission.authentication().is_none());
    drop(cpu);
    assert!(admission.authentication().is_some());
    let handshake = admission.handshake().unwrap();
    assert!(admission.handshake().is_none());
    drop(handshake);
    assert!(admission.handshake().is_some());
}
#[test]
fn invalid_admission_limits_rejected_before_semaphore_construction() {
    for c in [
        BrokerConfig {
            max_auth_tasks: 0,
            ..BrokerConfig::default()
        },
        BrokerConfig {
            max_handshakes: usize::MAX,
            ..BrokerConfig::default()
        },
        BrokerConfig {
            auth_burst: 0,
            ..BrokerConfig::default()
        },
    ] {
        assert!(Admission::new(&c).is_err());
    }
}
