mod support;
use lyrnormq_loadgen::{
    LoadError, engine,
    model::{RunConfig, RunData, Version},
};
use std::{sync::atomic::Ordering, time::Duration};
use support::{Fault, Peer, Scenario};
use tokio::time::timeout;

fn config(address: std::net::SocketAddr, version: Version, qos: u8) -> RunConfig {
    RunConfig {
        address,
        addresses: Vec::new(),
        version,
        qos,
        pairs: 1,
        setup_concurrency: 32,
        payload_bytes: 128,
        rate: 0,
        duration_ms: 100,
        warmup_ms: 0,
        drain_timeout_ms: 500,
        connect_timeout_ms: 500,
        io_timeout_ms: 300,
        inflight: 1,
        max_messages: 100_000,
        max_packet_bytes: 65536,
    }
}
async fn run(
    scenario: Scenario,
    version: Version,
    qos: u8,
    change: impl FnOnce(&mut RunConfig),
) -> (Result<RunData, LoadError>, Vec<String>, usize) {
    let peer = Peer::start(scenario).await;
    let mut c = config(peer.address, version, qos);
    change(&mut c);
    let result = timeout(Duration::from_secs(5), engine::run(c))
        .await
        .expect("load generator exceeded bounded test deadline");
    let count = peer.observed.publications.load(Ordering::SeqCst);
    assert!(
        peer.observed
            .payload_lengths
            .lock()
            .unwrap()
            .iter()
            .all(|n| *n == 128)
    );
    let errors = peer.finish().await;
    (result, errors, count)
}
fn failure(result: Result<RunData, LoadError>) {
    let data = result.expect("a measured failure must preserve partial RunData");
    assert!(
        !data.failures.is_empty() || data.pairs.iter().any(|p| p.error.is_some()),
        "fault was silently reported as success"
    );
    assert!(!data.pairs.is_empty(), "failed pair state was discarded");
}

fn successful(result: Result<RunData, LoadError>, errors: Vec<String>) -> RunData {
    assert!(errors.is_empty(), "independent peer: {errors:?}");
    let data = result.expect("engine failed valid MQTT flow");
    assert!(data.failures.is_empty(), "{:?}", data.failures);
    assert!(
        data.pairs.iter().all(|p| p.error.is_none()),
        "{:?}",
        data.pairs
    );
    data
}

#[tokio::test]
async fn both_versions_all_qos_actual_delivery_and_final_confirmation() {
    for version in [Version::V311, Version::V5] {
        for qos in 0..=2 {
            let (result, errors, count) = run(Scenario::default(), version, qos, |_| {}).await;
            let data = successful(result, errors);
            let p = &data.pairs[0];
            assert!(count > 0);
            assert!(!p.samples.is_empty());
            assert_eq!(p.counters.attempted, p.counters.write_completed);
            assert_eq!(p.counters.attempted, p.counters.delivered_unique);
            assert_eq!(p.counters.attempted, p.counters.completed);
            assert_eq!(p.samples.len() as u64, p.counters.attempted);
            for s in &p.samples {
                assert!(s.send_started_ns >= s.scheduled_ns);
                assert!(s.delivery_ns.is_some());
                assert!(s.write_completed_ns.is_some());
                let expected_completion = s
                    .write_completed_ns
                    .into_iter()
                    .chain(s.delivery_ns)
                    .chain(s.ack_ns)
                    .max();
                assert_eq!(
                    s.completed_ns, expected_completion,
                    "full completion must be the final observed write/delivery/source ACK"
                );
                if qos == 0 {
                    assert_eq!(s.ack_ns, None);
                } else {
                    assert!(s.ack_ns.is_some());
                    assert!(s.completed_ns >= s.ack_ns);
                }
            }
            match qos {
                0 => {
                    assert_eq!(p.counters.puback_received, 0);
                    assert_eq!(p.counters.pubrec_received, 0);
                    assert_eq!(p.counters.pubcomp_received, 0);
                }
                1 => assert_eq!(p.counters.puback_received, p.counters.attempted),
                2 => {
                    assert_eq!(p.counters.pubrec_received, p.counters.attempted);
                    assert_eq!(p.counters.pubcomp_received, p.counters.attempted);
                }
                _ => unreachable!(),
            }
        }
    }
}

#[tokio::test]
async fn qos2_pubrec_is_not_source_completion() {
    let scenario = Scenario {
        source_comp_delay: Duration::from_millis(70),
        ..Scenario::default()
    };
    let (result, errors, _) = run(scenario, Version::V5, 2, |c| c.duration_ms = 40).await;
    let data = successful(result, errors);
    let p = &data.pairs[0];
    assert_eq!(p.counters.attempted, 1);
    let s = &p.samples[0];
    assert!(
        s.ack_ns.unwrap() - s.delivery_ns.unwrap() >= 40_000_000,
        "ack latency counted PUBREC rather than delayed PUBCOMP"
    );
    assert!(s.ack_ns.unwrap() > data.measured_end_ns);
    assert_eq!(p.counters.pubcomp_received, 1);
}

#[tokio::test]
async fn qos2_delivery_waits_for_subscriber_pubrel() {
    let scenario = Scenario {
        subscriber_rel_delay: Duration::from_millis(70),
        ..Scenario::default()
    };
    let (result, errors, _) = run(scenario, Version::V5, 2, |c| c.duration_ms = 40).await;
    let data = successful(result, errors);
    let p = &data.pairs[0];
    assert_eq!(p.counters.attempted, 1);
    let s = &p.samples[0];
    assert!(
        s.delivery_ns.unwrap() - s.send_started_ns >= 50_000_000,
        "delivery counted initial QoS2 PUBLISH rather than PUBREL"
    );
    assert!(s.delivery_ns.unwrap() > s.ack_ns.unwrap());
}

#[tokio::test]
async fn qos0_credit_waits_for_delivery_and_late_sample_survives_drain() {
    let scenario = Scenario {
        delivery_delay: Duration::from_millis(100),
        ..Scenario::default()
    };
    let (result, errors, _) = run(scenario, Version::V311, 0, |c| c.duration_ms = 40).await;
    let data = successful(result, errors);
    let p = &data.pairs[0];
    assert_eq!(p.counters.attempted, 1);
    assert_eq!(p.counters.delivered_unique, 1);
    assert!(p.samples[0].delivery_ns.unwrap() > data.measured_end_ns);
    assert_eq!(p.samples[0].ack_ns, None);
}

#[tokio::test]
async fn duplicate_qos1_publish_is_acknowledged_but_not_new_delivery() {
    let scenario = Scenario {
        duplicate_delivery: true,
        source_comp_delay: Duration::ZERO,
        ..Scenario::default()
    };
    let (result, errors, _) = run(scenario, Version::V5, 1, |_| {}).await;
    let data = successful(result, errors);
    let p = &data.pairs[0];
    assert!(p.counters.duplicates > 0);
    assert_eq!(p.counters.delivered_unique, p.counters.attempted);
    assert_eq!(p.samples.len() as u64, p.counters.delivered_unique);
}

#[tokio::test]
async fn wrong_ack_identifier_is_never_success() {
    let (result, _, _) = run(
        Scenario {
            fault: Fault::WrongAck,
            ..Scenario::default()
        },
        Version::V5,
        1,
        |_| {},
    )
    .await;
    if let Ok(data) = &result {
        assert_eq!(data.pairs[0].counters.puback_received, 0);
        assert!(data.pairs[0].samples.iter().all(|s| s.ack_ns.is_none()));
    }
    failure(result);
}
#[tokio::test]
async fn negative_mqtt5_ack_is_not_positive_confirmation() {
    let (result, _, _) = run(
        Scenario {
            fault: Fault::RejectAck,
            ..Scenario::default()
        },
        Version::V5,
        1,
        |_| {},
    )
    .await;
    if let Ok(data) = &result {
        assert_eq!(data.pairs[0].counters.puback_received, 0);
    }
    failure(result);
}
#[tokio::test]
async fn abrupt_source_disconnect_cannot_complete_attempt() {
    let (result, _, _) = run(
        Scenario {
            fault: Fault::DropSource,
            ..Scenario::default()
        },
        Version::V311,
        1,
        |_| {},
    )
    .await;
    failure(result);
}
#[tokio::test]
async fn stalled_ack_is_bounded_and_incomplete_sample_is_retained() {
    let (result, _, count) = run(
        Scenario {
            fault: Fault::StallSource,
            ..Scenario::default()
        },
        Version::V5,
        1,
        |c| {
            c.duration_ms = 40;
            c.drain_timeout_ms = 80;
            c.io_timeout_ms = 80;
        },
    )
    .await;
    assert_eq!(count, 1);
    if let Ok(data) = &result {
        assert_eq!(data.pairs[0].samples.len(), 1);
        assert!(data.pairs[0].samples[0].completed_ns.is_none());
    }
    failure(result);
}
#[tokio::test]
async fn corrupt_payload_is_not_an_application_delivery() {
    let (result, _, _) = run(
        Scenario {
            fault: Fault::CorruptPayload,
            ..Scenario::default()
        },
        Version::V5,
        0,
        |_| {},
    )
    .await;
    if let Ok(data) = &result {
        assert_eq!(data.pairs[0].counters.delivered_unique, 0);
    }
    failure(result);
}
#[tokio::test]
async fn exhausted_sample_budget_is_explicit() {
    let (result, _, count) = run(Scenario::default(), Version::V5, 0, |c| c.max_messages = 1).await;
    assert_eq!(count, 1);
    if let Ok(data) = &result {
        assert!(data.pairs[0].samples.len() <= 1);
    }
    failure(result);
}
#[tokio::test]
async fn fixed_rate_retains_lag_and_unsent_deadlines() {
    let scenario = Scenario {
        delivery_delay: Duration::from_millis(25),
        ..Scenario::default()
    };
    let (result, errors, _) = run(scenario, Version::V5, 0, |c| {
        c.duration_ms = 100;
        c.rate = 1000;
    })
    .await;
    assert!(errors.is_empty(), "{errors:?}");
    let data = result.expect("rate-pressure result should retain raw observations");
    let p = &data.pairs[0];
    assert_eq!(p.counters.scheduled, 100);
    assert!(p.counters.attempted < p.counters.scheduled);
    assert!(
        p.samples
            .iter()
            .any(|s| s.send_started_ns - s.scheduled_ns >= 10_000_000),
        "late intended deadlines reset to now"
    );
}

#[tokio::test]
async fn multiple_pairs_window_four_and_warmup_preserve_sample_boundaries() {
    let peer = Peer::start_pairs(
        Scenario {
            delivery_delay: Duration::from_millis(5),
            ..Scenario::default()
        },
        3,
    )
    .await;
    let mut c = config(peer.address, Version::V5, 2);
    c.pairs = 3;
    c.inflight = 4;
    c.warmup_ms = 35;
    c.duration_ms = 70;
    let data = timeout(Duration::from_secs(5), engine::run(c))
        .await
        .unwrap()
        .unwrap();
    assert!(peer.finish().await.is_empty());
    assert!(data.failures.is_empty(), "{:?}", data.failures);
    assert_eq!(data.pairs.len(), 3);
    for pair in data.pairs {
        assert!(pair.error.is_none(), "{:?}", pair.error);
        assert!(!pair.samples.is_empty());
        assert_eq!(pair.counters.attempted, pair.counters.completed);
        assert!(
            pair.samples[0].sequence > 0,
            "warmup traffic must precede measurement"
        );
        assert!(
            pair.samples
                .iter()
                .all(|s| s.send_started_ns >= data.measured_start_ns
                    && s.send_started_ns < data.measured_end_ns)
        );
        assert!(
            pair.samples
                .windows(2)
                .any(|w| w[1].send_started_ns < w[0].completed_ns.unwrap()),
            "window >1 must permit overlapping messages"
        );
        for sample in &pair.samples {
            let active = pair
                .samples
                .iter()
                .filter(|s| {
                    s.send_started_ns <= sample.send_started_ns
                        && s.completed_ns.unwrap() > sample.send_started_ns
                })
                .count();
            assert!(active <= 4, "outstanding work exceeded configured window");
            assert_eq!(sample.pair, pair.pair);
            assert_eq!(
                sample.completed_ns,
                sample
                    .ack_ns
                    .max(sample.delivery_ns)
                    .max(sample.write_completed_ns)
            );
        }
    }
}

#[tokio::test]
async fn distinct_endpoint_mapping_is_balanced_with_both_versions_and_all_qos() {
    for version in [Version::V311, Version::V5] {
        for qos in 0..=2 {
            let peers = Peer::start_nodes(Scenario::default(), 3, 2).await;
            let addresses: Vec<_> = peers.iter().map(|p| p.address).collect();
            let mut c = config("127.0.0.1:1".parse().unwrap(), version, qos);
            c.addresses = addresses.clone();
            c.pairs = 6;
            let data = timeout(Duration::from_secs(5), engine::run(c))
                .await
                .unwrap()
                .unwrap();
            assert!(data.failures.is_empty(), "{:?}", data.failures);
            assert_eq!(data.pairs.len(), 6);
            for pair in &data.pairs {
                assert!(pair.error.is_none(), "{:?}", pair.error);
                assert!(pair.counters.attempted > 0);
                assert_eq!(pair.counters.attempted, pair.counters.write_completed);
                assert_eq!(pair.counters.attempted, pair.counters.delivered_unique);
                assert_eq!(pair.counters.attempted, pair.counters.completed);
                assert_eq!(pair.counters.duplicates, 0);
                assert_eq!(pair.counters.invalid_payload, 0);
                assert_eq!(pair.counters.unexpected, 0);
                match qos {
                    0 => assert_eq!(
                        pair.counters.puback_received
                            + pair.counters.pubrec_received
                            + pair.counters.pubcomp_received,
                        0
                    ),
                    1 => assert_eq!(pair.counters.puback_received, pair.counters.attempted),
                    2 => {
                        assert_eq!(pair.counters.pubrec_received, pair.counters.attempted);
                        assert_eq!(pair.counters.pubcomp_received, pair.counters.attempted);
                    }
                    _ => unreachable!(),
                }
            }
            let report = lyrnormq_loadgen::metrics::summarize(&data).unwrap();
            assert!(report.success, "{:?}", report.issues);
            assert_eq!(report.config.addresses, addresses);
            assert_eq!(report.configured_endpoint_connections.len(), 3);
            for (index, peer) in peers.into_iter().enumerate() {
                let ids = peer.observed.client_ids.lock().unwrap().clone();
                assert_eq!(ids.len(), 4, "endpoint {index} actual connection count");
                let mut publishers = 0;
                let mut subscribers = 0;
                for id in ids {
                    assert_eq!(id.len(), 23);
                    let pair = usize::from_str_radix(&id[18..22], 16).unwrap();
                    match id.as_bytes()[22] {
                        b'p' => {
                            assert_eq!(pair % 3, index);
                            publishers += 1;
                        }
                        b's' => {
                            assert_eq!((pair + 1) % 3, index);
                            subscribers += 1;
                        }
                        _ => panic!("invalid client role"),
                    }
                }
                assert_eq!((publishers, subscribers), (2, 2));
                let distribution = &report.configured_endpoint_connections[index];
                assert_eq!((distribution.publishers, distribution.subscribers), (2, 2));
                assert_eq!(distribution.address, peer.address);
                assert!(peer.finish().await.is_empty());
            }
        }
    }
}

#[tokio::test]
async fn bounded_setup_concurrency_limits_outstanding_handshakes_without_retry() {
    for concurrency in [1, 2] {
        let peer = Peer::start_pairs(
            Scenario {
                connack_delay: Duration::from_millis(20),
                ..Scenario::default()
            },
            6,
        )
        .await;
        let mut c = config(peer.address, Version::V5, 2);
        c.pairs = 6;
        c.setup_concurrency = concurrency;
        let data = timeout(Duration::from_secs(5), engine::run(c))
            .await
            .unwrap()
            .unwrap();
        let ids = peer.observed.client_ids.lock().unwrap().clone();
        assert_eq!(
            ids.len(),
            12,
            "each endpoint is opened exactly once, no setup retries"
        );
        let distinct: std::collections::BTreeSet<_> = ids.into_iter().collect();
        assert_eq!(distinct.len(), 12);
        assert_eq!(
            peer.observed.peak_handshakes.load(Ordering::SeqCst),
            concurrency
        );
        assert_eq!(peer.observed.active_handshakes.load(Ordering::SeqCst), 0);
        assert!(peer.finish().await.is_empty());
        assert!(data.failures.is_empty(), "{:?}", data.failures);
        assert_eq!(data.pairs.len(), 6);
        assert!(data.pairs.iter().all(|pair| pair.counters.attempted > 0
            && pair.counters.attempted == pair.counters.completed));
        assert!(
            data.pairs
                .iter()
                .flat_map(|pair| &pair.samples)
                .all(|sample| sample.send_started_ns >= data.measured_start_ns)
        );
        assert!(lyrnormq_loadgen::metrics::summarize(&data).unwrap().success);
    }
}
