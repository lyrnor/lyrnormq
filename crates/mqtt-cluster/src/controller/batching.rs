//! Owned FIFO control writes. Drain already-ready work; never wait to fill a batch.
use super::*;
use crate::model::batch::{EMPTY_BYTES, MAX_BYTES, MAX_COMMANDS};
use crate::profile::{self, Metric, Timer};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};
use tokio::sync::{mpsc, oneshot};
static BATCHES: AtomicU64 = AtomicU64::new(0);
const TRACE_LIMIT: u64 = 4096;
struct Completed {
    result: Result<Answer, Status>,
    sent: Option<Instant>,
}
type Reply = oneshot::Sender<Completed>;
fn reply(sender: Reply, result: Result<Answer, Status>) {
    let _ = sender.send(Completed {
        result,
        sent: profile::enabled().then(Instant::now),
    });
}
const MAILBOX: usize = 1024;
struct Queued {
    command: Command,
    reply: Reply,
    enqueued: Option<Instant>,
    queue_us: Option<u64>,
}
pub(super) struct Writer {
    sender: mpsc::Sender<Queued>,
}
struct Ready {
    commands: Vec<Command>,
    replies: Vec<Reply>,
    bytes: usize,
    queue_sum_us: u64,
    queue_max_us: u64,
    carry: Option<Queued>,
}
impl Queued {
    fn dequeued_at(&mut self, now: Instant) {
        if self.queue_us.is_none()
            && let Some(start) = self.enqueued
        {
            let us = now
                .saturating_duration_since(start)
                .as_micros()
                .min(u128::from(u64::MAX)) as u64;
            self.queue_us = Some(us);
            profile::observe(Metric::QueueResidence, us);
        }
    }
}
impl Writer {
    pub(super) async fn write(&self, command: Command) -> Result<Answer, Status> {
        let (reply, rx) = oneshot::channel();
        let mut enqueued = false;
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            let permit = {
                let _timer = Timer::start(Metric::EnqueueWait);
                self.sender.reserve().await.map_err(|_| {
                    Status::failed_precondition("metadata not submitted: controller stopped")
                })?
            };
            permit.send(Queued {
                command,
                reply,
                enqueued: profile::enabled().then(Instant::now),
                queue_us: None,
            });
            enqueued = true;
            let completed = rx
                .await
                .map_err(|_| Status::unavailable("metadata commit worker outcome unknown"))?;
            if let Some(sent) = completed.sent {
                profile::observe(
                    Metric::ReplyWake,
                    sent.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
                );
            }
            completed.result
        })
        .await;
        match result {
            Ok(result) => result,
            Err(_) if !enqueued => Err(Status::failed_precondition(
                "metadata not submitted: queue deadline",
            )),
            Err(_) => Err(Status::unavailable("metadata commit deadline")),
        }
    }
}
fn ready(
    first: Queued,
    receiver: &mut mpsc::Receiver<Queued>,
    mut prepare: impl FnMut(&mut Command),
) -> Ready {
    let _timer = Timer::start(Metric::BatchAssemble);
    let mut queue_sum_us = 0u64;
    let mut queue_max_us = 0u64;
    let mut singleton_bytes = 0;
    let (mut commands, mut replies) = (
        Vec::with_capacity(MAX_COMMANDS),
        Vec::with_capacity(MAX_COMMANDS),
    );
    let mut bytes = EMPTY_BYTES;
    let mut candidate = Some(first);
    let mut carry = None;
    let mut examined = 0;
    while let Some(mut queued) = candidate {
        if queued.enqueued.is_some() {
            queued.dequeued_at(Instant::now());
        }
        if examined == MAX_COMMANDS {
            carry = Some(queued);
            break;
        }
        examined += 1;
        prepare(&mut queued.command);
        let size = match serde_json::to_vec(&queued.command) {
            Ok(bytes) if bytes.len() <= MAX_BYTES => bytes.len(),
            _ => {
                reply(
                    queued.reply,
                    Err(Status::invalid_argument("invalid metadata command size")),
                );
                candidate = receiver.try_recv().ok();
                continue;
            }
        };
        let projected = bytes + size + usize::from(!commands.is_empty());
        if !commands.is_empty() && projected > MAX_BYTES {
            carry = Some(queued);
            break;
        }
        bytes = projected;
        singleton_bytes = size;
        if let Some(us) = queued.queue_us {
            queue_sum_us = queue_sum_us.saturating_add(us);
            queue_max_us = queue_max_us.max(us);
        }
        commands.push(queued.command);
        replies.push(queued.reply);
        // A legal singleton may need more bytes than a Batch wrapper permits.
        if commands.len() == MAX_COMMANDS || bytes > MAX_BYTES {
            break;
        }
        candidate = receiver.try_recv().ok();
    }
    Ready {
        bytes: if commands.len() == 1 {
            singleton_bytes
        } else {
            bytes
        },
        queue_sum_us,
        queue_max_us,
        commands,
        replies,
        carry,
    }
}
fn prepare(command: &mut Command, store: &Store, lease_ms: u64, grants: &mut BTreeMap<u64, u64>) {
    if let Command::Register {
        node,
        expires_at,
        lease_ms: lease,
        ..
    }
    | Command::Heartbeat {
        node,
        expires_at,
        lease_ms: lease,
        ..
    } = command
    {
        *lease = lease_ms;
        let previous = grants.get(node).copied().unwrap_or_else(|| {
            store
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .metadata
                .nodes
                .get(node)
                .map_or(0, |node| node.expires_at)
        });
        *expires_at = now()
            .saturating_add(lease_ms)
            .max(previous.saturating_add(1));
        grants.insert(*node, *expires_at);
    }
}
fn fail(replies: Vec<Reply>, status: Status) {
    for sender in replies {
        reply(sender, Err(status.clone()));
    }
}
pub(super) fn start(raft: Raft, store: Store, node: u64, lease_ms: u64) -> Writer {
    let (sender, mut receiver) = mpsc::channel(MAILBOX);
    // Exactly one owned worker, not a spawned waiter for every incoming RPC.
    drop(super::admission::submit(async move {
        let mut carry = None;
        loop {
            let first = match carry.take() {
                Some(first) => first,
                None => match receiver.recv().await {
                    Some(first) => first,
                    None => return,
                },
            };
            let mut grants = BTreeMap::new();
            let Ready {
                mut commands,
                replies,
                bytes,
                queue_sum_us,
                queue_max_us,
                carry: next,
            } = ready(first, &mut receiver, |command| {
                prepare(command, &store, lease_ms, &mut grants)
            });
            carry = next;
            if commands.is_empty() {
                continue;
            }
            if raft.metrics().borrow().current_leader != Some(node) {
                fail(
                    replies,
                    Status::failed_precondition("metadata not submitted: not current leader"),
                );
                continue;
            }
            let count = commands.len();
            profile::observe(Metric::BatchSize, count as u64);
            profile::observe(Metric::BatchBytes, bytes as u64);
            let command = if count == 1 {
                commands.remove(0)
            } else {
                Command::Batch { commands }
            };
            let kind = Kind::command(&command);
            // Do not submit the next batch until this durable quorum/apply completes.
            let batch_id = profile::enabled()
                .then(|| BATCHES.fetch_add(1, Ordering::Relaxed).saturating_add(1));
            let submitted_us = batch_id.map(|_| profile::stamp_us());
            let result = {
                let _timer = Timer::start(Metric::BatchRaft);
                measure(Stage::RaftResult, kind, raft.client_write(command)).await
            };
            if let Some(batch_id) = batch_id.filter(|id| *id <= TRACE_LIMIT) {
                let finished_us = profile::stamp_us();
                let log_index = result.as_ref().ok().map(|r| r.log_id.index);
                tracing::warn!(event="raft_batch_profile", node, batch_id, count, bytes,
                    submitted_us=?submitted_us, finished_us, log_index=?log_index,
                    queue_sum_us, queue_max_us, success=result.is_ok(), "Raft batch timing");
            }
            match result {
                Ok(response) => {
                    let mut answers = if count == 1 {
                        vec![response.data]
                    } else if response.data.error.is_none() && response.data.results.len() == count
                    {
                        response.data.results
                    } else {
                        fail(replies, Status::internal("metadata batch outcome unknown"));
                        continue;
                    };
                    for (sender, mut answer) in replies.into_iter().zip(answers.drain(..)) {
                        answer.lease_ms = lease_ms;
                        reply(sender, Ok(answer));
                    }
                }
                Err(_) => fail(
                    replies,
                    Status::unavailable("metadata commit outcome unknown"),
                ),
            }
        }
    }));
    Writer { sender }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    fn claim(i: usize) -> Command {
        Command::Claim {
            node: 1,
            incarnation: "one".into(),
            client: format!("c-{i}"),
            generation: 1,
        }
    }
    fn queued(command: Command) -> Queued {
        Queued {
            command,
            reply: oneshot::channel().0,
            enqueued: None,
            queue_us: None,
        }
    }
    fn large(size: usize) -> Command {
        let mut command = Command::Filters {
            generation: 1,
            node: 1,
            incarnation: "one".into(),
            client: "large".into(),
            filters: vec!["x".repeat(58000); 9],
        };
        let initial = serde_json::to_vec(&command).unwrap().len();
        assert!(size >= initial);
        if let Command::Filters { filters, .. } = &mut command {
            filters[8].push_str(&"x".repeat(size - initial));
        }
        assert!(crate::model::valid_command(&command));
        assert_eq!(serde_json::to_vec(&command).unwrap().len(), size);
        command
    }
    #[test]
    fn ready_drain_is_fifo_count_bounded_and_never_waits_for_fill() {
        let (tx, mut rx) = mpsc::channel(200);
        for i in 1..130 {
            tx.try_send(queued(claim(i))).unwrap();
        }
        let first = ready(queued(claim(0)), &mut rx, |_| {});
        assert_eq!(first.commands.len(), 64);
        assert!(first.carry.is_none());
        assert_eq!(
            serde_json::to_vec(&first.commands[63]).unwrap(),
            serde_json::to_vec(&claim(63)).unwrap()
        );
        let next = rx.try_recv().unwrap();
        let second = ready(next, &mut rx, |_| {});
        assert_eq!(second.commands.len(), 64);
        let next = rx.try_recv().unwrap();
        let last = ready(next, &mut rx, |_| {});
        assert_eq!(last.commands.len(), 2);
        assert_eq!(
            last.commands.last().map(|c| serde_json::to_vec(c).unwrap()),
            Some(serde_json::to_vec(&claim(129)).unwrap())
        );
        // The sender stays open. No receive/timeout is needed to return this singleton.
        let empty = ready(queued(claim(130)), &mut rx, |_| {});
        assert_eq!(empty.commands.len(), 1);
    }
    #[test]
    fn exact_wrapper_byte_limit_and_oversize_singleton_preserve_fifo() {
        let first = claim(1);
        let size = serde_json::to_vec(&first).unwrap().len();
        let second = large(MAX_BYTES - EMPTY_BYTES - size - 1);
        let (tx, mut rx) = mpsc::channel(2);
        tx.try_send(queued(second)).unwrap();
        tx.try_send(queued(claim(3))).unwrap();
        let batch = ready(queued(first), &mut rx, |_| {});
        assert_eq!(batch.commands.len(), 2);
        assert_eq!(batch.bytes, MAX_BYTES);
        assert_eq!(
            crate::model::batch::encoded_len(&batch.commands).unwrap(),
            MAX_BYTES
        );
        assert!(batch.carry.is_some());
        let carried = ready(batch.carry.unwrap(), &mut rx, |_| {});
        assert_eq!(carried.commands.len(), 1);
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(queued(claim(4))).unwrap();
        let only = ready(queued(large(MAX_BYTES)), &mut rx, |_| {});
        assert_eq!(only.commands.len(), 1);
        assert_eq!(rx.len(), 1);
        assert!(crate::model::batch::encoded_len(&only.commands).unwrap() > MAX_BYTES);
    }
    #[test]
    fn queue_clock_starts_after_admission_and_ends_once_even_for_carry() {
        let accepted = Instant::now();
        let mut item = queued(claim(1));
        item.enqueued = Some(accepted);
        item.dequeued_at(accepted + Duration::from_micros(250));
        assert_eq!(item.queue_us, Some(250));
        item.dequeued_at(accepted + Duration::from_millis(50));
        assert_eq!(
            item.queue_us,
            Some(250),
            "carry must not charge queue residence twice"
        );
        let mut disabled = queued(claim(2));
        disabled.dequeued_at(accepted);
        assert_eq!(disabled.queue_us, None);
    }

    #[tokio::test]
    async fn cancelling_before_mailbox_capacity_submits_nothing() {
        let (sender, mut receiver) = mpsc::channel(1);
        sender.try_send(queued(claim(1))).unwrap();
        let writer = Writer { sender };
        let mut pending = Box::pin(writer.write(claim(2)));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(pending);
        let first = receiver.try_recv().unwrap();
        assert_eq!(
            serde_json::to_vec(&first.command).unwrap(),
            serde_json::to_vec(&claim(1)).unwrap()
        );
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(writer.sender.capacity(), 1);
    }

    #[tokio::test]
    async fn cancelling_after_enqueue_preserves_owned_batch_work() {
        let (sender, mut receiver) = mpsc::channel(1);
        let writer = Writer { sender };
        let mut pending = Box::pin(writer.write(claim(7)));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(pending);
        let first = receiver.try_recv().unwrap();
        assert!(first.reply.is_closed());
        let work = ready(first, &mut receiver, |_| {});
        assert_eq!(
            work.commands.len(),
            1,
            "canceled caller must not remove submitted command"
        );
        assert_eq!(
            serde_json::to_vec(&work.commands[0]).unwrap(),
            serde_json::to_vec(&claim(7)).unwrap()
        );
        assert_eq!(work.bytes, serde_json::to_vec(&claim(7)).unwrap().len());
        fail(work.replies, Status::unavailable("test completion"));
    }
}
