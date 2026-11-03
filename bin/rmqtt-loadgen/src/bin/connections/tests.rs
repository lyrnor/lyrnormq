use super::*;
#[test]
fn same_second_distinct_processes_have_distinct_client_namespaces() {
    assert_ne!(
        default_prefix(1793716000, 11),
        default_prefix(1793716000, 12)
    );
}
#[test]
fn mqtt5_connect_fields() {
    let p = connect_packet("abc").unwrap();
    assert_eq!(&p[..2], &[0x10, 16]);
    assert_eq!(&p[2..13], &[0, 4, b'M', b'Q', b'T', b'T', 5, 2, 0, 0, 0]);
    assert_eq!(&p[13..], &[0, 3, b'a', b'b', b'c']);
}
#[tokio::test]
async fn successful_connack_and_ping_are_wire_validated() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let (header, _) = packet(&mut socket).await.unwrap();
        assert_eq!(header, 0x10);
        socket.write_all(&[0x20, 3, 0, 0, 0]).await.unwrap();
        assert_eq!(packet(&mut socket).await.unwrap().0, 0xc0);
        socket.write_all(&[0xd0, 0]).await.unwrap();
    });
    let (stream, _) = connect(address, "wire-test".into()).await.unwrap();
    let (live, errors) = healthy(vec![stream]).await;
    assert_eq!(live.len(), 1);
    assert!(errors.is_empty());
    peer.await.unwrap();
}
#[test]
fn exact_nearest_rank() {
    let mut n = vec![2, 1, 3, 4];
    let d = distribution(&mut n);
    assert_eq!(d["p99_us"], 4);
    assert_eq!(d["mean_us"], 2.5);
}
fn options(extra: &[&str]) -> io::Result<Options> {
    let args = ["127.0.0.1:18831", "127.0.0.1:18832", "127.0.0.1:18833"].join(",");
    // The endpoint list remains one positional argument.
    let mut values = vec![args, "500".into(), "unused-report.json".into()];
    values.extend(extra.iter().map(|value| (*value).to_owned()));
    Options::parse(values, 1793716000, 11)
}
#[test]
fn legacy_defaults_and_explicit_single_plateau_are_preserved() {
    let old = options(&[]).unwrap();
    assert_eq!(old.plateaus, 10);
    assert_eq!(old.connections_per_step, 10000);
    assert_eq!(old.index_offset, 0);
    assert!(old.start_unix_ms.is_none());
    let one = options(&[
        "1",
        "--connections-per-step",
        "2500",
        "--client-prefix",
        "same-length",
        "--index-offset",
        "7500",
    ])
    .unwrap();
    assert_eq!(one.index(1, 2499).unwrap(), 9999);
    assert_eq!(one.client_id(7500), "same-length007500");
    assert_eq!(one.endpoint(7500), one.endpoints[0]);
    assert_eq!(one.settings()["requested_plateaus"], 1);
}
#[test]
fn invalid_flags_count_prefix_and_index_overflow_are_rejected() {
    for args in [
        vec!["1", "--connections-per-step", "0"],
        vec!["2", "--connections-per-step", "50001"],
        vec!["1", "--connections-per-step", "100001"],
        vec!["1", "--client-prefix", ""],
        vec!["1", "--client-prefix", "bad/prefix"],
        vec!["1", "--client-prefix", "设备"],
        vec![
            "1",
            "--client-prefix",
            "okay",
            "--client-prefix",
            "duplicate",
        ],
        vec!["1", "--raw", "unused-report.json"],
        vec!["1", "--missing-option", "1"],
        vec![
            "1",
            "--connections-per-step",
            "2",
            "--index-offset",
            "18446744073709551615",
        ],
    ] {
        assert!(options(&args).is_err(), "{args:?}");
    }
    assert!(options(&["1", "--client-prefix", &"a".repeat(49)]).is_err());
    let last = options(&[
        "1",
        "--connections-per-step",
        "1",
        "--index-offset",
        "18446744073709551615",
    ])
    .unwrap();
    assert_eq!(last.index(1, 0).unwrap(), u64::MAX);
    assert!(last.index(2, 0).is_err());
}
#[test]
fn four_explicit_namespaces_and_offsets_cover_every_id_and_balance_endpoints() {
    let mut names = std::collections::BTreeSet::new();
    let mut endpoints = BTreeMap::new();
    for shard in 0..4 {
        let prefix = format!("m11223344{shard:02}");
        let offset = (shard * 2500).to_string();
        let settings = options(&[
            "1",
            "--connections-per-step",
            "2500",
            "--client-prefix",
            &prefix,
            "--index-offset",
            &offset,
        ])
        .unwrap();
        for i in 0..2500 {
            let index = settings.index(1, i).unwrap();
            let name = settings.client_id(index);
            assert_eq!(name.len(), 17);
            assert!(names.insert(name));
            *endpoints.entry(settings.endpoint(index)).or_insert(0) += 1;
        }
    }
    assert_eq!(names.len(), 10000);
    assert_eq!(
        endpoints.into_values().collect::<Vec<_>>(),
        vec![3334, 3333, 3333]
    );
}
#[test]
fn shared_deadlines_are_fixed_and_validate_stale_incomplete_and_impossible_windows() {
    let args = [
        "1",
        "--connections-per-step",
        "2500",
        "--start-unix-ms",
        "5000",
        "--ping-unix-ms",
        "40000",
    ];
    let settings = options(&args).unwrap();
    let monotonic = Instant::now();
    let schedule = settings.schedule(1000, monotonic).unwrap().unwrap();
    assert_eq!(
        schedule.start.duration_since(monotonic),
        Duration::from_secs(4)
    );
    assert_eq!(
        schedule.ping.duration_since(monotonic),
        Duration::from_secs(39)
    );
    assert!(settings.schedule(5000, monotonic).is_err());
    assert!(settings.schedule(5001, monotonic).is_err());
    let far = options(&[
        "1",
        "--connections-per-step",
        "1",
        "--start-unix-ms",
        "61001",
        "--ping-unix-ms",
        "71001",
    ])
    .unwrap();
    assert!(far.schedule(1000, monotonic).is_err());
    for args in [
        vec!["1", "--start-unix-ms", "5000"],
        vec!["1", "--ping-unix-ms", "40000"],
        vec!["2", "--start-unix-ms", "5000", "--ping-unix-ms", "40000"],
        vec!["1", "--start-unix-ms", "5000", "--ping-unix-ms", "14999"],
        vec!["1", "--start-unix-ms", "5000", "--ping-unix-ms", "65001"],
        vec![
            "1",
            "--connections-per-step",
            "2500",
            "--start-unix-ms",
            "5000",
            "--ping-unix-ms",
            "15000",
        ],
    ] {
        assert!(options(&args).is_err(), "{args:?}");
    }
    assert!(shared_hold_ready(
        monotonic,
        monotonic + Duration::from_secs(10)
    ));
    assert!(!shared_hold_ready(
        monotonic + Duration::from_nanos(1),
        monotonic + Duration::from_secs(10)
    ));
    assert!(!shared_hold_ready(
        monotonic + Duration::from_secs(1),
        monotonic
    ));
}
#[test]
fn raw_samples_reconstruct_global_percentiles_and_include_failed_attempt_lag() {
    let first = raw_step(1, &[1; 99], &[2; 99]);
    let second = raw_step(1, &[1000], &[3]);
    let mut merged: Vec<u64> = [first, second]
        .iter()
        .flat_map(|v| {
            v["latency_us"]
                .as_array()
                .unwrap()
                .iter()
                .map(|n| n.as_u64().unwrap())
        })
        .collect();
    let all = distribution(&mut merged);
    assert_eq!(all["count"], 100);
    assert_eq!(all["p99_us"], 1);
    assert_eq!(all["mean_us"], 10.99);
    let (mut live, mut times, mut lag, mut errors) =
        (Vec::new(), Vec::new(), Vec::new(), BTreeMap::new());
    completed(
        Ok(Attempt {
            lag_us: 12345,
            outcome: Err(failure("connection refused")),
        }),
        &mut live,
        &mut times,
        &mut lag,
        &mut errors,
    );
    assert_eq!(lag, vec![12345]);
    assert!(times.is_empty());
    assert_eq!(errors.values().sum::<u64>(), 1);
    let raw = raw_step(1, &times, &lag);
    assert_eq!(raw["latency_count"], 0);
    assert_eq!(raw["schedule_lag_count"], 1);
}
#[tokio::test]
async fn schedule_lag_includes_delay_before_task_polling_even_on_connect_failure() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let begin = Instant::now();
    let work = attempt(begin, Duration::ZERO, address, "queued-client".into());
    tokio::time::sleep(Duration::from_millis(20)).await;
    let attempt = work.await;
    assert!(attempt.outcome.is_err());
    assert!(
        attempt.lag_us >= 20000,
        "lag must include runtime delay before polling"
    );
}
