//! One owned task polls peer futures, decides W2, then finitely drains its tail.
use crate::{
    diagnosis::{self, Failure},
    profile::{self, Metric, Timer},
    replica::{Record, Reply},
};
use std::{
    collections::VecDeque,
    future::{Future, poll_fn},
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    task::Poll,
    time::Duration,
};
use tokio::{
    sync::oneshot,
    time::{Instant, timeout_at},
};

pub(super) type PeerFuture = Pin<Box<dyn Future<Output = io::Result<Reply>> + Send>>;
type ResultValue = io::Result<Option<Record>>;
type PeerResult = Result<Reply, Failure>;
const OVERALL: Duration = Duration::from_secs(8);
const TAIL: Duration = Duration::from_secs(3);

pub(super) fn start(peers: Vec<PeerFuture>, started: Instant) -> oneshot::Receiver<ResultValue> {
    start_budgets(peers, started + OVERALL, TAIL)
}

fn start_budgets(
    peers: Vec<PeerFuture>,
    deadline: Instant,
    tail: Duration,
) -> oneshot::Receiver<ResultValue> {
    let (sender, receiver) = oneshot::channel();
    profile::observe(Metric::QuorumDriver, 1);
    // The task owns all execution, even if its original response receiver disappears.
    tokio::spawn(async move {
        let mut peers = Peers::new(peers);
        let decision = timeout_at(deadline, decide(&mut peers))
            .await
            .unwrap_or_else(|_| Err(diagnosis::reject(Failure::QuorumInsufficient)));
        let _ = sender.send(decision);
        if peers.remaining() {
            let _timer = Timer::start(Metric::QuorumTail);
            let end = (Instant::now() + tail).min(deadline);
            if timeout_at(end, async {
                while let Some(result) = peers.next().await {
                    // Tail responses cannot change a result already returned to the caller.
                    if let Err(failure) = result {
                        diagnosis::observe(failure);
                    }
                }
            })
            .await
            .is_err()
            {
                profile::observe(Metric::QuorumTailTimeout, 1);
            }
        }
    });
    receiver
}

async fn decide(peers: &mut Peers) -> ResultValue {
    let mut selected = Selected::default();
    while let Some(result) = peers.next().await {
        match result {
            Ok(reply) => {
                if selected.accept(reply)? {
                    return Ok(selected.chosen);
                }
            }
            Err(failure) => diagnosis::observe(failure),
        }
    }
    Err(diagnosis::reject(Failure::QuorumInsufficient))
}

struct Peers {
    pending: Vec<Option<PeerFuture>>,
    ready: VecDeque<PeerResult>,
}
impl Peers {
    fn new(peers: Vec<PeerFuture>) -> Self {
        Self {
            ready: VecDeque::with_capacity(peers.len()),
            pending: peers.into_iter().map(Some).collect(),
        }
    }
    fn remaining(&self) -> bool {
        !self.ready.is_empty() || self.pending.iter().any(Option::is_some)
    }
    async fn next(&mut self) -> Option<PeerResult> {
        poll_fn(|cx| {
            if let Some(ready) = self.ready.pop_front() {
                return Poll::Ready(Some(ready));
            }
            // Poll every outstanding peer before consuming this round's ready results.
            // A fast local result must not prevent remote requests from being issued.
            for slot in &mut self.pending {
                let Some(peer) = slot else {
                    continue;
                };
                match catch_unwind(AssertUnwindSafe(|| peer.as_mut().poll(cx))) {
                    Ok(Poll::Pending) => {}
                    Ok(Poll::Ready(result)) => {
                        *slot = None;
                        self.ready
                            .push_back(result.map_err(|_| Failure::QuorumPeer));
                    }
                    Err(_) => {
                        *slot = None;
                        // Preserve the previous task-per-peer panic isolation on unwind builds.
                        self.ready.push_back(Err(Failure::QuorumJoin));
                    }
                }
            }
            if let Some(ready) = self.ready.pop_front() {
                Poll::Ready(Some(ready))
            } else if self.remaining() {
                Poll::Pending
            } else {
                Poll::Ready(None)
            }
        })
        .await
    }
}

#[derive(Default)]
struct Selected {
    responses: usize,
    chosen: Option<Record>,
}
impl Selected {
    fn accept(&mut self, reply: Reply) -> io::Result<bool> {
        self.responses += 1;
        if let Some(record) = reply.record {
            if let Some(old) = &self.chosen {
                if old.version() == record.version()
                    && serde_json::to_vec(old).map_err(io::Error::other)?
                        != serde_json::to_vec(&record).map_err(io::Error::other)?
                {
                    return Err(diagnosis::reject(Failure::QuorumConflict));
                }
                if old.version() >= record.version() {
                    return Ok(self.responses >= 2);
                }
            }
            self.chosen = Some(record);
        }
        Ok(self.responses >= 2)
    }
}

#[cfg(test)]
mod tests;
