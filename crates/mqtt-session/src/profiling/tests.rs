use super::*;
use crate::durable::{Clock, SessionConfig, SessionEngine};
use mqtt_protocol::Packet;
use std::{process::Command, sync::Arc};

#[derive(Debug)]
struct FixedClock;
impl Clock for FixedClock {
    fn now_millis(&self) -> u64 {
        1000
    }
}
fn config() -> SessionConfig {
    SessionConfig {
        max_sessions: 4,
        max_retained: 4,
        max_subscriptions: 4,
        max_queued: 4,
        max_inflight: 4,
        max_packet_size: 4096,
        max_state_bytes: 65536,
    }
}
fn workload() {
    let mut engine = SessionEngine::new_with_clock(
        config(),
        Arc::new(mqtt_auth::AllowAnonymous),
        Arc::new(FixedClock),
    )
    .unwrap();
    engine.connect("sub", false).unwrap();
    engine.connect("pub", false).unwrap();
    engine
        .handle(
            "sub",
            Packet::Subscribe {
                packet_id: 1,
                filters: vec![("profile/value".into(), 0)],
            },
        )
        .unwrap();
    let before = engine.snapshot().unwrap();
    let mut proposal = engine.clone();
    assert_eq!(proposal.snapshot().unwrap(), before);
    proposal
        .handle(
            "pub",
            Packet::Publish {
                topic: "profile/value".into(),
                payload: b"profile".to_vec(),
                qos: 1,
                packet_id: Some(1),
                dup: false,
                retain: false,
            },
        )
        .unwrap();
    assert_ne!(proposal.snapshot().unwrap(), before);
    assert_eq!(engine.snapshot().unwrap(), before);
    proposal.reconcile_authorization().unwrap();
    assert!(matches!(
        proposal.poll("sub").unwrap(),
        Some(Packet::Publish { .. })
    ));
    // Discarding a profiled proposal must still leave accepted durable bytes intact.
    assert_eq!(engine.snapshot().unwrap(), before);
}

#[test]
fn profile_child_probe() {
    if std::env::var_os("RMQTT_PROFILE_TEST_CHILD").is_none() {
        return;
    }
    let expected = std::env::var("RMQTT_PROFILE").is_ok_and(|value| value == "1");
    let guard = span(Operation::Clone);
    assert_eq!(guard.started.is_some(), expected);
    drop(guard);
    record_bytes(Operation::Clone, 17);
    let initial: serde_json::Value = serde_json::from_str(&report_json().unwrap()).unwrap();
    let row = &initial["operations"][0];
    assert_eq!(row["name"], "clone");
    assert_eq!(row["count"], u64::from(expected));
    assert_eq!(row["bytes"], if expected { 17 } else { 0 });
    workload();
    let report: serde_json::Value = serde_json::from_str(&report_json().unwrap()).unwrap();
    assert_eq!(report["enabled"], expected);
    assert_eq!(report["inclusive"], true);
    let rows = report["operations"].as_array().unwrap();
    assert_eq!(rows.len(), OPERATIONS.len());
    for row in rows {
        if expected {
            assert!(row["count"].as_u64().unwrap() > 0, "missing {row}");
            assert!(row["total_ns"].as_u64().unwrap() > 0, "missing {row}");
        } else {
            assert_eq!(row["count"], 0);
            assert_eq!(row["total_ns"], 0);
            assert_eq!(row["bytes"], 0);
        }
        assert_eq!(row.as_object().unwrap().len(), 4);
    }
    if expected {
        for (outer, inner) in [
            (Operation::Route, Operation::RouteBuild),
            (Operation::SerializeLive, Operation::RecordsLive),
            (Operation::SerializePersistent, Operation::RecordsPersistent),
        ] {
            assert!(
                report["operations"][outer as usize]["total_ns"]
                    .as_u64()
                    .unwrap()
                    >= report["operations"][inner as usize]["total_ns"]
                        .as_u64()
                        .unwrap()
            );
        }
    }
    for operation in [Operation::SerializeLive, Operation::SerializePersistent] {
        let row = &report["operations"][operation as usize];
        assert_eq!(row["bytes"].as_u64().unwrap() > 0, expected);
    }
}

#[test]
fn opt_in_is_process_scoped_and_disabled_spans_are_noop() {
    for setting in [None, Some("0"), Some("1"), Some("true")] {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.args([
            "--exact",
            "profiling::tests::profile_child_probe",
            "--nocapture",
        ]);
        command.env("RMQTT_PROFILE_TEST_CHILD", "1");
        command.env_remove("RMQTT_PROFILE");
        if let Some(setting) = setting {
            command.env("RMQTT_PROFILE", setting);
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{setting:?}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn counters_and_elapsed_conversion_saturate_instead_of_wrapping() {
    let count = AtomicU64::new(u64::MAX - 1);
    saturating_add(&count, 9);
    saturating_add(&count, 1);
    assert_eq!(count.load(Ordering::Relaxed), u64::MAX);
    assert_eq!(as_u64(u128::MAX), u64::MAX);
}
