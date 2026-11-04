//! Sequential Core submission with bounded, FIFO quorum/application completion.
use super::*;
use std::{collections::VecDeque, future::Future, pin::Pin};

const WINDOW: usize = 4;
type Applied = Result<openraft::raft::ClientWriteResponse<Types>, Status>;
type Completion = Pin<Box<dyn Future<Output = Applied> + Send>>;
type Grants = BTreeMap<u64, u64>;

trait Backend: Send + Sync + 'static {
    fn leader(&self) -> bool;
    fn prepare(&self, command: &mut Command, pending: &Grants, current: &mut Grants);
    fn submit(&self, command: Command) -> impl Future<Output = Result<Completion, Status>> + Send;
}

struct RaftBackend {
    raft: Raft,
    store: Store,
    node: u64,
    lease_ms: u64,
}

fn unknown() -> Status {
    Status::unavailable("metadata commit outcome unknown")
}

impl Backend for RaftBackend {
    fn leader(&self) -> bool {
        self.raft.metrics().borrow().current_leader == Some(self.node)
    }
    fn prepare(&self, command: &mut Command, pending: &Grants, current: &mut Grants) {
        super::prepare(command, &self.store, self.lease_ms, pending, current);
    }
    async fn submit(&self, command: Command) -> Result<Completion, Status> {
        // Await enqueue itself in worker order. Do not spawn client_write futures.
        let result = self
            .raft
            .client_write_ff(command)
            .await
            .map_err(|_| unknown())?;
        Ok(
            Box::pin(async move { result.await.map_err(|_| unknown())?.map_err(|_| unknown()) })
                as Completion,
        )
    }
}

struct Pending {
    result: Completion,
    replies: Vec<Reply>,
    grants: Grants,
    count: usize,
    bytes: usize,
    queue_sum_us: u64,
    queue_max_us: u64,
    batch_id: Option<u64>,
    submitted_us: Option<u64>,
    _timer: Timer,
    diagnostic: diagnostics::Timer,
}

impl Pending {
    fn finish(mut self, result: Applied, node: u64, lease_ms: u64) {
        self.diagnostic.finish(result.is_ok());
        if let Some(batch_id) = self.batch_id.filter(|id| *id <= TRACE_LIMIT) {
            let finished_us = profile::stamp_us();
            let log_index = result.as_ref().ok().map(|r| r.log_id.index);
            let Self {
                count,
                bytes,
                queue_sum_us,
                queue_max_us,
                submitted_us,
                ..
            } = &self;
            tracing::warn!(event="raft_batch_profile", node, batch_id, count, bytes,
                submitted_us=?submitted_us, finished_us, log_index=?log_index,
                queue_sum_us, queue_max_us, success=result.is_ok(), "Raft batch timing");
        }
        match result {
            Ok(response) => {
                let answers = if self.count == 1 {
                    vec![response.data]
                } else if response.data.error.is_none() && response.data.results.len() == self.count
                {
                    response.data.results
                } else {
                    fail(
                        self.replies,
                        Status::internal("metadata batch outcome unknown"),
                    );
                    return;
                };
                for (sender, mut answer) in self.replies.into_iter().zip(answers) {
                    answer.lease_ms = lease_ms;
                    reply(sender, Ok(answer));
                }
            }
            Err(error) => fail(self.replies, error),
        }
    }
}

fn pending_grants(pending: &VecDeque<Pending>) -> Grants {
    let mut grants = Grants::new();
    for batch in pending {
        for (&node, &expires_at) in &batch.grants {
            grants
                .entry(node)
                .and_modify(|old| *old = (*old).max(expires_at))
                .or_insert(expires_at);
        }
    }
    grants
}

fn command_grants(commands: &[Command]) -> Grants {
    let mut grants = Grants::new();
    for command in commands {
        if let Command::Register {
            node, expires_at, ..
        }
        | Command::Heartbeat {
            node, expires_at, ..
        } = command
        {
            grants
                .entry(*node)
                .and_modify(|old| *old = (*old).max(*expires_at))
                .or_insert(*expires_at);
        }
    }
    grants
}

enum Event {
    Completed(Applied),
    Input(Option<Queued>),
}

async fn run<B: Backend>(
    backend: B,
    mut receiver: mpsc::Receiver<Queued>,
    node: u64,
    lease_ms: u64,
) {
    let mut pending: VecDeque<Pending> = VecDeque::with_capacity(WINDOW);
    let mut carry = None;
    let mut closed = false;
    loop {
        let can_receive = pending.len() < WINDOW && !closed;
        let event = if let Some(front) = pending.front_mut() {
            tokio::select! {
                biased;
                result = front.result.as_mut() => Event::Completed(result),
                first = async {
                    match carry.take() {
                        Some(first) => Some(first),
                        None => receiver.recv().await,
                    }
                }, if can_receive => Event::Input(first),
            }
        } else if closed {
            return;
        } else {
            Event::Input(match carry.take() {
                Some(first) => Some(first),
                None => receiver.recv().await,
            })
        };
        let first = match event {
            Event::Completed(result) => {
                if let Some(front) = pending.pop_front() {
                    front.finish(result, node, lease_ms);
                }
                continue;
            }
            Event::Input(None) => {
                closed = true;
                continue;
            }
            Event::Input(Some(first)) => first,
        };
        let earlier = pending_grants(&pending);
        let mut current = Grants::new();
        let Ready {
            mut commands,
            replies,
            bytes,
            queue_sum_us,
            queue_max_us,
            carry: next,
        } = ready(first, &mut receiver, |command| {
            backend.prepare(command, &earlier, &mut current)
        });
        carry = next;
        if commands.is_empty() {
            continue;
        }
        if !backend.leader() {
            fail(
                replies,
                Status::failed_precondition("metadata not submitted: not current leader"),
            );
            continue;
        }
        let count = commands.len();
        let grants = command_grants(&commands);
        profile::observe(Metric::BatchSize, count as u64);
        profile::observe(Metric::BatchBytes, bytes as u64);
        let command = if count == 1 {
            commands.remove(0)
        } else {
            Command::Batch { commands }
        };
        let diagnostic = diagnostics::Timer::start(Stage::RaftResult, Kind::command(&command));
        let timer = Timer::start(Metric::BatchRaft);
        let batch_id =
            profile::enabled().then(|| BATCHES.fetch_add(1, Ordering::Relaxed).saturating_add(1));
        let submitted_us = batch_id.map(|_| profile::stamp_us());
        let submitted = {
            let _timer = Timer::start(Metric::BatchSubmit);
            backend.submit(command).await
        };
        let result = match submitted {
            Ok(result) => result,
            Err(error) => Box::pin(std::future::ready(Err(error))) as Completion,
        };
        pending.push_back(Pending {
            result,
            replies,
            grants,
            count,
            bytes,
            queue_sum_us,
            queue_max_us,
            batch_id,
            submitted_us,
            _timer: timer,
            diagnostic,
        });
        profile::observe(Metric::BatchWindow, pending.len() as u64);
    }
}

pub(super) fn start(raft: Raft, store: Store, node: u64, lease_ms: u64) -> Writer {
    let (sender, receiver) = mpsc::channel(MAILBOX);
    drop(super::super::admission::submit(run(
        RaftBackend {
            raft,
            store,
            node,
            lease_ms,
        },
        receiver,
        node,
        lease_ms,
    )));
    Writer { sender }
}

#[cfg(test)]
mod tests;
