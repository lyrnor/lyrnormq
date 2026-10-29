use super::*;
use crate::model::{Counters, PairData, RunConfig, RunData, Sample};

fn sample(sequence: u64, delivery: u64) -> Sample {
    Sample {
        pair: 0,
        sequence,
        scheduled_ns: 100,
        send_started_ns: 110,
        write_completed_ns: Some(120),
        ack_ns: None,
        delivery_ns: Some(delivery),
        completed_ns: Some(delivery.max(120)),
    }
}
fn run(samples: Vec<Sample>) -> RunData {
    let count = samples.len() as u64;
    RunData {
        config: RunConfig {
            pairs: 1,
            duration_ms: 1,
            warmup_ms: 0,
            max_messages: 1000,
            ..RunConfig::default()
        },
        run_id: 17,
        measured_start_ns: 100,
        measured_end_ns: 1_000_100,
        finished_ns: 2_000_100,
        pairs: vec![PairData {
            pair: 0,
            setup_duration_ns: 3,
            counters: Counters {
                scheduled: count,
                attempted: count,
                write_completed: count,
                delivered_unique: count,
                completed: count,
                ..Counters::default()
            },
            samples,
            error: None,
        }],
        failures: vec![],
    }
}
#[test]
fn nearest_rank_tail_not_interpolation_or_average_of_pair_percentiles() {
    let mut v = vec![10; 99];
    v.push(9_000_000_001);
    let d = distribution(v).unwrap();
    assert_eq!(d.count, 100);
    assert_eq!(d.p99_ns, Some(10));
    assert_eq!(d.max_ns, Some(9_000_000_001));
    let mut uneven = vec![10; 99];
    uneven.extend([1_000, 2_000]);
    assert_eq!(distribution(uneven).unwrap().p99_ns, Some(1_000));
    let d = distribution(vec![0, u64::MAX]).unwrap();
    assert_eq!(d.p50_ns, Some(0));
    assert_eq!(d.p95_ns, Some(u64::MAX));
    assert_eq!(d.p99_ns, Some(u64::MAX));
    assert!(d.mean_ns.unwrap().is_finite());
    let d = distribution(vec![101, 1, 8]).unwrap();
    assert_eq!(d.p50_ns, Some(8));
    assert_eq!(d.p99_ns, Some(101));
}
#[test]
fn empty_distributions_and_qos0_ack_are_null() {
    let r = summarize(&run(vec![])).unwrap();
    assert!(!r.success);
    assert_eq!(r.latencies.send_to_delivery.count, 0);
    assert_eq!(r.latencies.send_to_delivery.p99_ns, None);
    assert!(r.latencies.send_to_source_completed.is_none());
    assert_eq!(r.accounting.not_source_confirmed, None);
    assert_eq!(r.offering_window_events.source_completed, None);
    let json = serde_json::to_value(r).unwrap();
    assert!(json["latencies"]["send_to_delivery"]["p99_ns"].is_null());
    assert!(json["latencies"]["send_to_source_completed"].is_null());
}
#[test]
fn one_message_has_six_explicit_duration_semantics() {
    let mut d = run(vec![sample(0, 150)]);
    d.config.qos = 1;
    d.pairs[0].samples[0].ack_ns = Some(160);
    d.pairs[0].samples[0].completed_ns = Some(170);
    d.pairs[0].counters.puback_received = 1;
    let r = summarize(&d).unwrap();
    assert!(r.success);
    assert_eq!(r.latencies.schedule_to_send.p99_ns, Some(10));
    assert_eq!(r.latencies.send_to_write_completed.p99_ns, Some(10));
    assert_eq!(r.latencies.send_to_delivery.p99_ns, Some(40));
    assert_eq!(r.latencies.schedule_to_delivery.p99_ns, Some(50));
    assert_eq!(
        r.latencies
            .send_to_source_completed
            .as_ref()
            .unwrap()
            .p99_ns,
        Some(50)
    );
    assert_eq!(r.latencies.send_to_full_completion.p99_ns, Some(60));
}
#[test]
fn late_completion_never_inflates_offering_window_tps() {
    let d = run(vec![sample(0, 1_500_100)]);
    let r = summarize(&d).unwrap();
    assert!(r.success);
    assert_eq!(r.offering_window_events.delivered_unique, 0);
    assert_eq!(r.after_offering_window_events.delivered_unique, 1);
    assert_eq!(r.offering_window_tps.delivered_unique_per_second, Some(0.0));
    assert_eq!(
        r.measured_cohort_tps_through_drain
            .delivered_unique_per_second,
        Some(500.0)
    );
    assert_eq!(
        r.measured_cohort_tps_through_drain.denominator_ns,
        Some(2_000_000)
    );
    assert_eq!(r.latencies.send_to_delivery.p99_ns, Some(1_499_990));
}
#[test]
fn event_exactly_at_window_end_is_late() {
    let d = run(vec![sample(0, 1_000_100)]);
    let r = summarize(&d).unwrap();
    assert_eq!(r.offering_window_events.delivered_unique, 0);
    assert_eq!(r.after_offering_window_events.fully_completed, 1);
}
#[test]
fn qos2_pubrec_alone_is_never_final_source_success() {
    let mut d = run(vec![sample(0, 150)]);
    d.config.qos = 2;
    d.pairs[0].counters.pubrec_received = 1;
    d.pairs[0].counters.completed = 0;
    d.pairs[0].samples[0].completed_ns = None;
    let r = summarize(&d).unwrap();
    assert!(!r.success);
    assert_eq!(r.accounting.counters.pubrec_received, 1);
    assert_eq!(r.accounting.not_source_confirmed, Some(1));
    assert_eq!(
        r.latencies.send_to_source_completed.as_ref().unwrap().count,
        0
    );
    assert_eq!(r.offering_window_events.source_completed, Some(0));
    d.pairs[0].samples[0].ack_ns = Some(180);
    d.pairs[0].samples[0].completed_ns = Some(190);
    d.pairs[0].counters.pubcomp_received = 1;
    d.pairs[0].counters.completed = 1;
    let r = summarize(&d).unwrap();
    assert!(r.success);
    assert_eq!(
        r.latencies
            .send_to_source_completed
            .as_ref()
            .unwrap()
            .p99_ns,
        Some(70)
    );
}
#[test]
fn incomplete_and_unsent_work_is_explicit_and_unsuccessful() {
    let mut d = run(vec![sample(0, 150)]);
    d.pairs[0].samples[0].delivery_ns = None;
    d.pairs[0].samples[0].completed_ns = None;
    d.pairs[0].counters.delivered_unique = 0;
    d.pairs[0].counters.completed = 0;
    d.pairs[0].counters.scheduled = 3;
    let r = summarize(&d).unwrap();
    assert!(!r.success);
    assert_eq!(r.accounting.not_sent, 2);
    assert_eq!(r.accounting.not_delivered, 1);
    assert_eq!(r.accounting.incomplete_attempts, 1);
    assert_eq!(r.latencies.send_to_delivery.count, 0);
    assert!(r.issues.len() >= 2);
}
#[test]
fn duplicates_are_visible_but_do_not_multiply_samples_or_unique_tps() {
    let mut d = run(vec![sample(0, 150)]);
    d.pairs[0].counters.duplicates = 8;
    let r = summarize(&d).unwrap();
    assert!(r.success);
    assert_eq!(r.accounting.counters.duplicates, 8);
    assert_eq!(r.latencies.send_to_delivery.count, 1);
    assert_eq!(r.offering_window_events.delivered_unique, 1);
    assert_eq!(r.warnings.len(), 1);
}
#[test]
fn record_drop_duplicate_and_counter_corruption_rejected() {
    let mut d = run(vec![sample(0, 150)]);
    d.pairs[0].counters.attempted = 2;
    assert!(summarize(&d).is_err());
    d.pairs[0].counters.attempted = 1;
    d.pairs[0].samples[0].pair = 1;
    assert!(summarize(&d).is_err());
    let d = run(vec![sample(7, 150), sample(7, 160)]);
    assert!(summarize(&d).is_err());
    let mut d = run(vec![sample(0, 150)]);
    d.pairs[0].counters.pubrec_received = 1;
    assert!(summarize(&d).is_err());
}
#[test]
fn impossible_timestamps_never_saturate_into_zero_latency() {
    let mut d = run(vec![sample(0, 150)]);
    d.pairs[0].samples[0].delivery_ns = Some(109);
    assert!(summarize(&d).is_err());
    d.pairs[0].samples[0].delivery_ns = Some(150);
    d.pairs[0].samples[0].completed_ns = Some(130);
    assert!(summarize(&d).is_err());
    d.pairs[0].samples[0].completed_ns = Some(150);
    d.pairs[0].samples[0].write_completed_ns = Some(d.finished_ns + 1);
    assert!(summarize(&d).is_err());
}
#[test]
fn rate_plan_mismatch_and_failed_pairs_not_passed_as_capacity() {
    let mut d = run(vec![sample(0, 150)]);
    d.config.rate = 2000;
    d.pairs[0].counters.scheduled = 2;
    let r = summarize(&d).unwrap();
    assert!(!r.success);
    assert_eq!(r.accounting.not_sent, 1);
    d.pairs[0].counters.scheduled = 1;
    let r = summarize(&d).unwrap();
    assert!(r.issues.iter().any(|s| s.contains("absolute-rate")));
    d.config.rate = 0;
    d.pairs[0].error = Some("peer reset".into());
    let r = summarize(&d).unwrap();
    assert!(!r.success);
    assert_eq!(r.accounting.failed_pairs, 1);
}
#[test]
fn global_and_per_pair_sample_bounds_are_enforced() {
    let mut d = run(vec![sample(0, 150), sample(1, 160)]);
    d.config.max_messages = 1;
    assert!(summarize(&d).is_err());
    let mut d = run(vec![sample(0, 150), sample(1, 160)]);
    d.config.pairs = 2;
    d.config.max_messages = 2;
    assert!(summarize(&d).is_err());
}
#[test]
fn early_failed_horizon_never_divides_by_zero() {
    let mut d = run(vec![]);
    d.finished_ns = d.measured_start_ns;
    d.failures.push("setup interrupted".into());
    let r = summarize(&d).unwrap();
    assert!(!r.success);
    assert_eq!(r.measured_cohort_tps_through_drain.denominator_ns, None);
    assert_eq!(
        r.measured_cohort_tps_through_drain
            .delivered_unique_per_second,
        None
    );
}

#[test]
fn omitted_sequence_rejected_even_if_counters_were_also_decremented() {
    let data = run(vec![sample(17, 150), sample(19, 160)]);
    assert!(summarize(&data).is_err());
}
