//! Opt-in fixed-cardinality worker diagnostics, emitted only by the blocking actor.
use super::Operation;
use mqtt_protocol::Packet;
use std::{
    cell::Cell,
    ffi::OsStr,
    fs,
    io::{self, Write},
    path::PathBuf,
    sync::OnceLock,
    time::Instant,
};

static ENABLED: OnceLock<bool> = OnceLock::new();
fn enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var_os("RMQTT_PROFILE").as_deref() == Some(OsStr::new("1")))
}
pub(super) fn queue_stamp() -> Option<Instant> {
    enabled().then(Instant::now)
}

#[derive(Clone, Copy)]
pub(super) enum Category {
    Connect,
    Publish,
    Subscribe,
    Ack,
    Ping,
    Poll,
    Begin,
    Disconnect,
    Tick,
    Other,
}
const CATEGORIES: [Category; 10] = [
    Category::Connect,
    Category::Publish,
    Category::Subscribe,
    Category::Ack,
    Category::Ping,
    Category::Poll,
    Category::Begin,
    Category::Disconnect,
    Category::Tick,
    Category::Other,
];
impl Category {
    fn name(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::Publish => "publish",
            Self::Subscribe => "subscribe",
            Self::Ack => "ack",
            Self::Ping => "ping",
            Self::Poll => "poll",
            Self::Begin => "begin",
            Self::Disconnect => "disconnect",
            Self::Tick => "tick",
            Self::Other => "other",
        }
    }
    fn packet(packet: &Packet) -> Self {
        match packet {
            Packet::Publish { .. } => Self::Publish,
            Packet::Subscribe { .. } | Packet::Unsubscribe { .. } => Self::Subscribe,
            Packet::PubAck { .. }
            | Packet::PubRec { .. }
            | Packet::PubRel { .. }
            | Packet::PubComp { .. } => Self::Ack,
            Packet::PingReq => Self::Ping,
            _ => Self::Other,
        }
    }
    fn operation(operation: &Operation) -> Self {
        match operation {
            Operation::ConnectFrameAt { .. } => Self::Connect,
            #[cfg(test)]
            Operation::ConnectFrame { .. } | Operation::Connect { .. } => Self::Connect,
            Operation::HandleFrameAt { frame, .. } => Self::packet(&frame.packet),
            #[cfg(test)]
            Operation::Handle { packet, .. } => Self::packet(packet),
            Operation::PollDelivery { .. } => Self::Poll,
            #[cfg(test)]
            Operation::Poll { .. } => Self::Poll,
            Operation::BeginDelivery { .. } => Self::Begin,
            Operation::DisconnectOptions { .. } | Operation::Disconnect { .. } => Self::Disconnect,
            Operation::Tick => Self::Tick,
            _ => Self::Other,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum SpanKind {
    Clone,
    Snapshot,
    Commit,
    QueueWait,
}
const SPANS: [SpanKind; 4] = [
    SpanKind::Clone,
    SpanKind::Snapshot,
    SpanKind::Commit,
    SpanKind::QueueWait,
];
impl SpanKind {
    fn name(self) -> &'static str {
        match self {
            Self::Clone => "proposal_clone",
            Self::Snapshot => "snapshot",
            Self::Commit => "sqlite_commit",
            Self::QueueWait => "queue_wait",
        }
    }
}
#[derive(Clone, Copy, Default)]
struct Metric {
    count: u64,
    total_ns: u64,
    bytes: u64,
}
#[derive(Clone, Copy, Default)]
struct RequestMetric {
    metric: Metric,
    ok: u64,
    error: u64,
    aborted: u64,
}
#[derive(Clone, Copy)]
enum Outcome {
    Ok,
    Error,
    Aborted,
}

pub(super) struct Profile {
    enabled: bool,
    path: Option<PathBuf>,
    requests: [Cell<RequestMetric>; 10],
    spans: [Cell<Metric>; 4],
    started: Cell<u64>,
    polls: Cell<u64>,
    empty_polls: Cell<u64>,
    notifications: Cell<u64>,
    notification_targets: Cell<u64>,
    errors_by_reason: [Cell<u64>; 256],
    error_unknown: Cell<u64>,
}
impl Profile {
    pub(super) fn from_env() -> Self {
        let enabled = enabled();
        let path = enabled
            .then(|| std::env::var_os("RMQTT_PROFILE_PATH").map(PathBuf::from))
            .flatten();
        Self::new(enabled, path)
    }
    fn new(enabled: bool, path: Option<PathBuf>) -> Self {
        Self {
            enabled,
            path,
            requests: std::array::from_fn(|_| Cell::new(RequestMetric::default())),
            spans: std::array::from_fn(|_| Cell::new(Metric::default())),
            started: Cell::new(0),
            polls: Cell::new(0),
            empty_polls: Cell::new(0),
            notifications: Cell::new(0),
            notification_targets: Cell::new(0),
            errors_by_reason: std::array::from_fn(|_| Cell::new(0)),
            error_unknown: Cell::new(0),
        }
    }
    pub(super) fn request(&self, operation: &Operation) -> RequestSpan<'_> {
        let started = self.enabled.then(Instant::now);
        if self.enabled {
            self.started.set(self.started.get().saturating_add(1));
        }
        RequestSpan {
            profile: self,
            category: Category::operation(operation),
            started,
            outcome: Outcome::Aborted,
            reason: None,
        }
    }
    pub(super) fn span(&self, kind: SpanKind) -> Span<'_> {
        Span {
            profile: self,
            kind,
            started: self.enabled.then(Instant::now),
            bytes: 0,
        }
    }
    pub(super) fn poll(&self) {
        if self.enabled {
            self.polls.set(self.polls.get().saturating_add(1));
        }
    }
    pub(super) fn empty_poll(&self) {
        if self.enabled {
            self.empty_polls
                .set(self.empty_polls.get().saturating_add(1));
        }
    }
    pub(super) fn notify(&self, targets: usize) {
        if self.enabled {
            self.notifications
                .set(self.notifications.get().saturating_add(1));
            self.notification_targets.set(
                self.notification_targets
                    .get()
                    .saturating_add(to_u64(targets as u128)),
            );
        }
    }
    pub(super) fn queue_wait(&self, queued_at: Option<Instant>) {
        if self.enabled
            && let Some(queued_at) = queued_at
        {
            let cell = &self.spans[SpanKind::QueueWait as usize];
            let mut metric = cell.get();
            metric.count = metric.count.saturating_add(1);
            metric.total_ns = metric.total_ns.saturating_add(elapsed(queued_at));
            cell.set(metric);
        }
    }
    pub(super) fn checkpoint(&self) {
        // Previous request guards have dropped before the next dequeue. Counters
        // include all completed requests up to this boundary; no per-event file IO.
        if self.enabled && self.started.get() > 0 && self.started.get().is_multiple_of(1024) {
            self.emit_or_diagnose("checkpoint");
        }
    }
    fn write_to(&self, output: &mut impl Write, session: &str, status: &str) -> io::Result<()> {
        writeln!(output, "RMQTT_PROFILE\t1")?;
        writeln!(output, "STATUS\t{status}")?;
        writeln!(
            output,
            "SCOPE\tworker_commands_after_startup_including_setup_warmup_cleanup_checkpoint_io_excluded_from_request_spans"
        )?;
        writeln!(
            output,
            "TIMING\trequest_elapsed_excludes_queue_wait_other_spans_inclusive_overlap_queue_wait_includes_mailbox_capacity_and_queue"
        )?;
        writeln!(
            output,
            "OUTCOME\tio_result_not_mqtt_acceptance_subscribe_includes_unsubscribe"
        )?;
        writeln!(
            output,
            "BYTES\tsuccessful_snapshot_output_and_commit_attempt_payload_not_disk_io"
        )?;
        for category in CATEGORIES {
            let value = self.requests[category as usize].get();
            writeln!(
                output,
                "REQUEST\t{}\t{}\t{}\t{}\t{}\t{}",
                category.name(),
                value.metric.count,
                value.metric.total_ns,
                value.ok,
                value.error,
                value.aborted
            )?;
        }
        for kind in SPANS {
            let value = self.spans[kind as usize].get();
            writeln!(
                output,
                "SPAN\t{}\t{}\t{}\t{}",
                kind.name(),
                value.count,
                value.total_ns,
                value.bytes
            )?;
        }
        for (name, value) in [
            ("poll_delivery", self.polls.get()),
            ("empty_poll_delivery", self.empty_polls.get()),
            ("notifications", self.notifications.get()),
            ("notification_targets", self.notification_targets.get()),
            ("error_unknown", self.error_unknown.get()),
        ] {
            writeln!(output, "COUNTER\t{name}\t{value}")?;
        }
        for (code, count) in self.errors_by_reason.iter().enumerate() {
            if count.get() > 0 {
                writeln!(output, "ERROR_REASON\t{code:02x}\t{}", count.get())?;
            }
        }
        writeln!(output, "SESSION_JSON\t{session}")
    }
    fn emit(&self, status: &str) -> io::Result<()> {
        let session = mqtt_session::profiling::report_json().map_err(io::Error::other)?;
        match &self.path {
            Some(path) => {
                let mut temporary = path.as_os_str().to_os_string();
                temporary.push(".tmp");
                let temporary = PathBuf::from(temporary);
                let result = (|| {
                    let file = fs::File::create(&temporary)?;
                    let mut writer = io::BufWriter::new(file);
                    self.write_to(&mut writer, &session, status)?;
                    writer.flush()?;
                    drop(writer);
                    fs::rename(&temporary, path)
                })();
                if result.is_err() {
                    let _ = fs::remove_file(&temporary);
                }
                result
            }
            None => self.write_to(&mut io::stderr().lock(), &session, status),
        }
    }
    fn emit_or_diagnose(&self, status: &str) {
        if let Err(error) = self.emit(status) {
            eprintln!("rmqtt profiling output failed: {error}");
        }
    }
}
impl Drop for Profile {
    fn drop(&mut self) {
        if self.enabled {
            self.emit_or_diagnose("final");
        }
    }
}

pub(super) struct RequestSpan<'a> {
    profile: &'a Profile,
    category: Category,
    started: Option<Instant>,
    outcome: Outcome,
    reason: Option<u8>,
}
impl RequestSpan<'_> {
    pub(super) fn finish(&mut self, success: bool) {
        self.outcome = if success { Outcome::Ok } else { Outcome::Error };
    }
    pub(super) fn error(&mut self, error: &io::Error) {
        self.outcome = Outcome::Error;
        self.reason = error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<super::WorkerFailure>())
            .map(|failure| failure.reason.value());
    }
}
impl Drop for RequestSpan<'_> {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            let cell = &self.profile.requests[self.category as usize];
            let mut value = cell.get();
            value.metric.count = value.metric.count.saturating_add(1);
            value.metric.total_ns = value.metric.total_ns.saturating_add(elapsed(started));
            match self.outcome {
                Outcome::Ok => value.ok = value.ok.saturating_add(1),
                Outcome::Error => {
                    value.error = value.error.saturating_add(1);
                    let reason = match self.reason {
                        Some(reason) => &self.profile.errors_by_reason[usize::from(reason)],
                        None => &self.profile.error_unknown,
                    };
                    reason.set(reason.get().saturating_add(1));
                }
                Outcome::Aborted => value.aborted = value.aborted.saturating_add(1),
            }
            cell.set(value);
        }
    }
}
pub(super) struct Span<'a> {
    profile: &'a Profile,
    kind: SpanKind,
    started: Option<Instant>,
    bytes: u64,
}
impl Span<'_> {
    pub(super) fn bytes(&mut self, bytes: usize) {
        if self.started.is_some() {
            self.bytes = self.bytes.saturating_add(to_u64(bytes as u128));
        }
    }
}
impl Drop for Span<'_> {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            let cell = &self.profile.spans[self.kind as usize];
            let mut value = cell.get();
            value.count = value.count.saturating_add(1);
            value.total_ns = value.total_ns.saturating_add(elapsed(started));
            value.bytes = value.bytes.saturating_add(self.bytes);
            cell.set(value);
        }
    }
}
fn elapsed(started: Instant) -> u64 {
    to_u64(started.elapsed().as_nanos())
}
fn to_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
#[cfg(test)]
mod tests;
