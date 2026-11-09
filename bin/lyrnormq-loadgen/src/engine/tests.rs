use super::{
    state::{Source, State},
    timing::{rate_count, rate_deadline},
};
use crate::payload::Header;
fn header(sequence: u64) -> Header {
    Header {
        run_id: 9,
        pair: 0,
        measured: true,
        sequence,
        scheduled_ns: 10,
        send_started_ns: 12,
    }
}
#[test]
fn qos2_requires_pubcomp_and_delivery_for_credit() {
    let mut s = State::new(0, 0);
    s.begin(header(0), 2, Some(7)).unwrap();
    s.next_sequence = 1;
    s.wrote(0, 13).unwrap();
    assert!(s.source_ack(7, 5, 20).unwrap());
    assert_eq!(s.data.counters.pubrec_received, 1);
    assert_eq!(s.data.samples[0].ack_ns, None);
    assert_eq!(s.pending[&0].source, Source::AwaitComp);
    assert!(s.source_ack(7, 5, 22).unwrap());
    assert_eq!(s.data.counters.pubrec_received, 1);
    s.deliver(header(0), 25).unwrap();
    assert_eq!(s.data.counters.delivered_unique, 1);
    assert_eq!(s.data.counters.completed, 0);
    assert_eq!(s.pending.len(), 1);
    assert!(!s.source_ack(7, 7, 30).unwrap());
    assert!(s.pending.is_empty());
    assert_eq!(s.data.samples[0].ack_ns, Some(30));
    assert_eq!(s.data.samples[0].completed_ns, Some(30));
    assert_eq!(s.data.counters.pubcomp_received, 1);
}
#[test]
fn duplicate_delivery_does_not_inflate_unique_throughput() {
    let mut s = State::new(0, 0);
    s.begin(header(0), 1, Some(1)).unwrap();
    s.next_sequence = 1;
    s.wrote(0, 13).unwrap();
    s.deliver(header(0), 15).unwrap();
    s.deliver(header(0), 16).unwrap();
    s.source_ack(1, 4, 17).unwrap();
    s.deliver(header(0), 20).unwrap();
    assert_eq!(s.data.counters.delivered_unique, 1);
    assert_eq!(s.data.counters.duplicates, 2);
    assert_eq!(s.data.samples[0].delivery_ns, Some(15));
    assert_eq!(s.data.counters.completed, 1);
}
#[test]
fn qos0_ack_is_not_fabricated_and_write_can_fail() {
    let mut s = State::new(0, 0);
    s.begin(header(0), 0, None).unwrap();
    s.next_sequence = 1;
    assert_eq!(s.data.counters.attempted, 1);
    assert_eq!(s.data.samples[0].write_completed_ns, None);
    assert_eq!(s.data.samples[0].ack_ns, None);
    s.wrote(0, 14).unwrap();
    s.deliver(header(0), 19).unwrap();
    assert_eq!(s.data.samples[0].ack_ns, None);
    assert_eq!(s.data.samples[0].completed_ns, Some(19));
}
#[test]
fn rejects_wrong_ack_phase_identifier_and_header() {
    let mut s = State::new(0, 0);
    s.begin(header(0), 2, Some(4)).unwrap();
    s.next_sequence = 1;
    assert!(s.source_ack(3, 5, 20).is_err());
    assert!(s.source_ack(4, 7, 20).is_err());
    let mut bad = header(0);
    bad.send_started_ns = 13;
    assert!(s.deliver(bad, 20).is_err());
    assert_eq!(s.data.counters.delivered_unique, 0);
}
#[test]
fn absolute_rate_schedule_has_no_deadline_reset() {
    assert_eq!(rate_deadline(1000, 0, 0, 3, 1000).unwrap(), 1000);
    assert_eq!(rate_deadline(1000, 0, 1, 3, 1000).unwrap(), 1_001_000);
    assert_eq!(rate_deadline(1000, 1, 0, 3, 1000).unwrap(), 3_001_000);
    assert_eq!(rate_count(100, 0, 1, 1000), 100);
    assert_eq!(
        (0..3).map(|p| rate_count(100, p, 3, 1000)).sum::<u64>(),
        100
    );
    assert_eq!((0..20).map(|p| rate_count(1, p, 20, 1)).sum::<u64>(), 1);
}
#[test]
fn packet_ids_are_not_reused_while_outstanding() {
    let mut s = State::new(0, 0);
    s.next_id = u16::MAX;
    s.outgoing.insert(u16::MAX, 0);
    s.outgoing.insert(1, 1);
    assert_eq!(s.allocate_id().unwrap(), 2);
}

#[test]
fn receive_maximum_does_not_throttle_qos0() {
    assert_eq!(super::actor::effective_window(0, 64, 1), 64);
    assert_eq!(super::actor::effective_window(1, 64, 1), 1);
    assert_eq!(super::actor::effective_window(2, 64, 1), 1);
}
#[test]
fn completed_duplicate_cannot_change_measurement_cohort() {
    let mut s = State::new(0, 0);
    s.begin(header(0), 0, None).unwrap();
    s.next_sequence = 1;
    s.wrote(0, 14).unwrap();
    s.deliver(header(0), 19).unwrap();
    let mut changed = header(0);
    changed.measured = false;
    assert!(s.deliver(changed, 21).is_err());
}

#[test]
fn client_ids_fit_mqtt311_required_range_and_remain_distinct() {
    let mut ids = std::collections::BTreeSet::new();
    for run in [0, 1, u64::MAX] {
        for pair in [0, 1, 59_999] {
            for role in ['s', 'p'] {
                let id = super::actor::client_id(run, pair, role);
                assert!(
                    id.len() <= 23,
                    "client ID exceeds basic MQTT311 limit: {id}"
                );
                assert!(id.bytes().all(|b| b.is_ascii_alphanumeric()));
                assert!(ids.insert(id));
            }
        }
    }
}
