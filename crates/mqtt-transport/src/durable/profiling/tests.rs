use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "rmqtt-profile-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn profile(&self) -> PathBuf {
        self.0.join("worker.tsv")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn disabled_guards_do_not_time_count_or_create_a_file() {
    let directory = Directory::new();
    let path = directory.profile();
    {
        let profile = Profile::new(false, Some(path.clone()));
        let mut request = profile.request(&Operation::Tick);
        assert!(request.started.is_none());
        request.finish(true);
        let mut span = profile.span(SpanKind::Snapshot);
        assert!(span.started.is_none());
        span.bytes(900);
        drop(span);
        drop(request);
        profile.poll();
        profile.empty_poll();
        profile.notify(20);
        profile.checkpoint();
        assert_eq!(profile.started.get(), 0);
        assert_eq!(profile.polls.get(), 0);
        assert_eq!(profile.empty_polls.get(), 0);
        assert_eq!(profile.notification_targets.get(), 0);
        assert_eq!(
            profile.requests[Category::Tick as usize].get().metric.count,
            0
        );
        assert_eq!(profile.spans[SpanKind::Snapshot as usize].get().bytes, 0);
    }
    assert!(!path.exists());
}

#[test]
fn fixed_rows_distinguish_result_errors_aborts_and_overlapping_work() {
    let profile = Profile::new(true, None);
    for outcome in [Some(true), Some(false), None] {
        let mut request = profile.request(&Operation::PollDelivery {
            client: "not-recorded".into(),
            generation: 7,
        });
        if let Some(outcome) = outcome {
            request.finish(outcome);
        }
    }
    {
        let mut request = profile.request(&Operation::Tick);
        let mut span = profile.span(SpanKind::Snapshot);
        span.bytes(17);
        std::thread::sleep(std::time::Duration::from_millis(1));
        drop(span);
        request.finish(true);
    }
    profile.poll();
    profile.poll();
    profile.empty_poll();
    profile.notify(4);
    let mut output = Vec::new();
    profile
        .write_to(&mut output, "{\"enabled\":false}", "checkpoint")
        .unwrap();
    let text = String::from_utf8(output).unwrap();
    let poll = text
        .lines()
        .find(|line| line.starts_with("REQUEST\tpoll\t"))
        .unwrap();
    let fields: Vec<_> = poll.split('\t').collect();
    assert_eq!(&fields[2..3], &["3"]);
    assert_eq!(&fields[4..], &["1", "1", "1"]);
    assert!(fields[3].parse::<u64>().unwrap() > 0);
    assert!(text.contains("COUNTER\tpoll_delivery\t2\n"));
    assert!(text.contains("COUNTER\tempty_poll_delivery\t1\n"));
    assert!(text.contains("COUNTER\tnotification_targets\t4\n"));
    assert!(text.contains("SESSION_JSON\t{\"enabled\":false}\n"));
    assert!(!text.contains("not-recorded"));
    assert_eq!(
        text.lines()
            .filter(|line| line.starts_with("REQUEST\t"))
            .count(),
        10
    );
    assert_eq!(
        text.lines()
            .filter(|line| line.starts_with("SPAN\t"))
            .count(),
        4
    );
    let snapshot = profile.spans[SpanKind::Snapshot as usize].get();
    assert_eq!((snapshot.count, snapshot.bytes), (1, 17));
    assert!(snapshot.total_ns > 0);
    assert!(
        profile.requests[Category::Tick as usize]
            .get()
            .metric
            .total_ns
            >= snapshot.total_ns
    );
    // Avoid stderr output from this explicitly enabled, pathless unit fixture.
    let mut profile = profile;
    profile.enabled = false;
}

#[test]
fn graceful_drop_writes_complete_report_and_replaces_checkpoint_atomically() {
    let directory = Directory::new();
    let path = directory.profile();
    {
        let profile = Profile::new(true, Some(path.clone()));
        for _ in 0..1024 {
            let mut request = profile.request(&Operation::Tick);
            request.finish(true);
        }
        profile.checkpoint();
        let checkpoint = fs::read_to_string(&path).unwrap();
        assert!(checkpoint.starts_with("RMQTT_PROFILE\t1\nSTATUS\tcheckpoint\n"));
        assert!(
            checkpoint
                .lines()
                .any(|line| line.starts_with("REQUEST\ttick\t1024\t"))
        );
        assert!(!path.with_extension("tsv.tmp").exists());
        let mut request = profile.request(&Operation::Tick);
        request.finish(false);
    }
    let final_report = fs::read_to_string(&path).unwrap();
    assert!(final_report.contains("STATUS\tfinal\n"));
    assert!(
        final_report
            .lines()
            .any(|line| line.starts_with("REQUEST\ttick\t1025\t"))
    );
    assert!(final_report.contains("SESSION_JSON\t{"));
    assert!(!path.with_extension("tsv.tmp").exists());
}

#[test]
fn diagnostic_file_error_does_not_reclassify_a_successful_command() {
    let directory = Directory::new();
    let profile = Profile::new(true, Some(directory.0.join("absent/worker.tsv")));
    {
        let mut request = profile.request(&Operation::Tick);
        request.finish(true);
    }
    assert!(profile.emit("checkpoint").is_err());
    let metric = profile.requests[Category::Tick as usize].get();
    assert_eq!((metric.ok, metric.error, metric.aborted), (1, 0, 0));
    assert!(!directory.0.join("absent/worker.tsv.tmp").exists());
}

#[test]
fn request_categories_cover_control_ack_and_subscription_operations() {
    let classify = |packet| {
        Category::operation(&Operation::Handle {
            client: "not-recorded".into(),
            generation: 1,
            packet,
        })
        .name()
    };
    assert_eq!(classify(Packet::PingReq), "ping");
    assert_eq!(classify(Packet::PubAck { packet_id: 9 }), "ack");
    assert_eq!(classify(Packet::PubRec { packet_id: 9 }), "ack");
    assert_eq!(classify(Packet::PubRel { packet_id: 9 }), "ack");
    assert_eq!(classify(Packet::PubComp { packet_id: 9 }), "ack");
    assert_eq!(
        classify(Packet::Subscribe {
            packet_id: 9,
            filters: Vec::new()
        }),
        "subscribe"
    );
    assert_eq!(
        classify(Packet::Unsubscribe {
            packet_id: 9,
            filters: Vec::new()
        }),
        "subscribe"
    );
}

#[test]
fn known_reason_and_unknown_io_errors_are_separate_from_ok_refusals() {
    let directory = Directory::new();
    let profile = Profile::new(true, Some(directory.profile()));
    {
        let mut request = profile.request(&Operation::Tick);
        request.error(&super::super::failure(
            mqtt_protocol::ReasonCode::QUOTA_EXCEEDED,
            "synthetic",
        ));
    }
    {
        let mut request = profile.request(&Operation::Tick);
        request.error(&io::Error::other("synthetic"));
    }
    {
        let mut request = profile.request(&Operation::Tick);
        request.finish(true);
    }
    let mut output = Vec::new();
    profile.write_to(&mut output, "{}", "checkpoint").unwrap();
    let output = String::from_utf8(output).unwrap();
    assert!(output.contains("ERROR_REASON\t97\t1\n"));
    assert!(output.contains("COUNTER\terror_unknown\t1\n"));
    let metric = profile.requests[Category::Tick as usize].get();
    assert_eq!((metric.ok, metric.error, metric.aborted), (1, 2, 0));
    assert_eq!(
        profile.errors_by_reason.iter().map(Cell::get).sum::<u64>(),
        1
    );
}

#[test]
fn fixed_counters_saturate_instead_of_wrapping() {
    let directory = Directory::new();
    let profile = Profile::new(true, Some(directory.profile()));
    profile.errors_by_reason[0x97].set(u64::MAX);
    profile.notification_targets.set(u64::MAX);
    profile.notify(1);
    {
        let mut request = profile.request(&Operation::Tick);
        request.error(&super::super::failure(
            mqtt_protocol::ReasonCode::QUOTA_EXCEEDED,
            "synthetic",
        ));
    }
    assert_eq!(profile.errors_by_reason[0x97].get(), u64::MAX);
    assert_eq!(profile.notification_targets.get(), u64::MAX);
}

#[test]
fn queue_wait_is_separate_from_command_elapsed_and_disabled_never_records() {
    let directory = Directory::new();
    let profile = Profile::new(true, Some(directory.profile()));
    let queued = Instant::now();
    std::thread::sleep(std::time::Duration::from_millis(2));
    profile.queue_wait(Some(queued));
    profile.queue_wait(None);
    let wait = profile.spans[SpanKind::QueueWait as usize].get();
    assert_eq!(wait.count, 1);
    assert!(wait.total_ns > 0);
    assert_eq!(
        profile.requests[Category::Tick as usize].get().metric.count,
        0
    );
    let disabled = Profile::new(false, None);
    disabled.queue_wait(Some(queued));
    assert_eq!(disabled.spans[SpanKind::QueueWait as usize].get().count, 0);
}
