//! Owned FIFO control writes. Drain already-ready work; never wait to fill a batch.
use super::*;
use crate::model::batch::{EMPTY_BYTES, MAX_BYTES, MAX_COMMANDS};
use tokio::sync::{mpsc, oneshot};
const MAILBOX: usize = 1024;
struct Queued {
    command: Command,
    reply: oneshot::Sender<Result<Answer, Status>>,
}
pub(super) struct Writer {
    sender: mpsc::Sender<Queued>,
}
struct Ready {
    commands: Vec<Command>,
    replies: Vec<oneshot::Sender<Result<Answer, Status>>>,
    carry: Option<Queued>,
}
impl Writer {
    pub(super) async fn write(&self, command: Command) -> Result<Answer, Status> {
        let (reply, rx) = oneshot::channel();
        let mut enqueued = false;
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            self.sender
                .send(Queued { command, reply })
                .await
                .map_err(|_| {
                    Status::failed_precondition("metadata not submitted: controller stopped")
                })?;
            enqueued = true;
            rx.await
                .map_err(|_| Status::unavailable("metadata commit worker outcome unknown"))?
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
    let (mut commands, mut replies) = (
        Vec::with_capacity(MAX_COMMANDS),
        Vec::with_capacity(MAX_COMMANDS),
    );
    let mut bytes = EMPTY_BYTES;
    let mut candidate = Some(first);
    let mut carry = None;
    let mut examined = 0;
    while let Some(mut queued) = candidate {
        if examined == MAX_COMMANDS {
            carry = Some(queued);
            break;
        }
        examined += 1;
        prepare(&mut queued.command);
        let size = match serde_json::to_vec(&queued.command) {
            Ok(bytes) if bytes.len() <= MAX_BYTES => bytes.len(),
            _ => {
                let _ = queued.reply.send(Err(Status::invalid_argument(
                    "invalid metadata command size",
                )));
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
        commands.push(queued.command);
        replies.push(queued.reply);
        // A legal singleton may need more bytes than a Batch wrapper permits.
        if commands.len() == MAX_COMMANDS || bytes > MAX_BYTES {
            break;
        }
        candidate = receiver.try_recv().ok();
    }
    Ready {
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
fn fail(replies: Vec<oneshot::Sender<Result<Answer, Status>>>, status: Status) {
    for reply in replies {
        let _ = reply.send(Err(status.clone()));
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
            let command = if count == 1 {
                commands.remove(0)
            } else {
                Command::Batch { commands }
            };
            let kind = Kind::command(&command);
            // Do not submit the next batch until this durable quorum/apply completes.
            let result = measure(Stage::RaftResult, kind, raft.client_write(command)).await;
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
                    for (reply, mut answer) in replies.into_iter().zip(answers.drain(..)) {
                        answer.lease_ms = lease_ms;
                        let _ = reply.send(Ok(answer));
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
}
