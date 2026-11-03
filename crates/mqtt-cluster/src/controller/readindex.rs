//! A fresh authority round serves only calls queued before that round starts.
use super::Raft;
use crate::profile::{self, Metric, Timer};
use std::{future::Future, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tonic::Status;

const MAILBOX: usize = 1024;
const MAX_GROUP: usize = 1024;
const DEADLINE: Duration = Duration::from_secs(1);
type Reply = oneshot::Sender<Result<(), Status>>;

pub(super) struct Reader {
    sender: mpsc::Sender<Reply>,
    worker: JoinHandle<()>,
}

fn stopped() -> Status {
    Status::unavailable("not current leader")
}

fn deadline() -> Status {
    Status::unavailable("quorum deadline")
}

impl Reader {
    pub(super) async fn check(&self) -> Result<(), Status> {
        tokio::time::timeout(DEADLINE, async {
            let (reply, result) = oneshot::channel();
            self.sender.send(reply).await.map_err(|_| stopped())?;
            result.await.map_err(|_| stopped())?
        })
        .await
        .map_err(|_| deadline())?
    }

    fn spawn<F, Fut>(mut check: F) -> Self
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), Status>> + Send,
    {
        let (sender, mut receiver) = mpsc::channel::<Reply>(MAILBOX);
        let worker = tokio::spawn(async move {
            while let Some(first) = receiver.recv().await {
                let mut group = Vec::with_capacity(MAX_GROUP);
                if !first.is_closed() {
                    group.push(first);
                }
                // Freeze membership before starting the check. In particular,
                // never attach later arrivals to an already-running round.
                // Limit examined entries too, so canceled callers cannot make
                // an unbounded synchronous drain delay a fresh authority check.
                for _ in 1..MAX_GROUP {
                    match receiver.try_recv() {
                        Ok(reply) if !reply.is_closed() => group.push(reply),
                        Ok(_) => {}
                        Err(_) => break,
                    }
                }
                if group.is_empty() {
                    continue;
                }
                profile::observe(Metric::ReadIndexGroupSize, group.len() as u64);
                let result = {
                    let _timer = Timer::start(Metric::ReadIndexRound);
                    tokio::time::timeout(DEADLINE, check())
                        .await
                        .unwrap_or_else(|_| Err(deadline()))
                };
                for reply in group {
                    let _ = reply.send(result.clone());
                }
            }
        });
        Self { sender, worker }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        // The worker owns no Sender/Reader, so dropping the Controller cannot
        // leave an orphan waiting task or pending reply channel behind.
        self.worker.abort();
    }
}

pub(super) fn start(raft: Raft) -> Reader {
    Reader::spawn(move || {
        let raft = raft.clone();
        async move {
            raft.ensure_linearizable()
                .await
                .map(|_| ())
                .map_err(|_| stopped())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn fixture() -> (Reader, mpsc::Receiver<Reply>) {
        let (begun, rounds) = mpsc::channel::<Reply>(8);
        let reader = Reader::spawn(move || {
            let begun = begun.clone();
            async move {
                let (reply, result) = oneshot::channel();
                begun.send(reply).await.unwrap();
                result.await.unwrap()
            }
        });
        (reader, rounds)
    }

    fn queue(reader: &Reader) -> oneshot::Receiver<Result<(), Status>> {
        let (reply, result) = oneshot::channel();
        reader.sender.try_send(reply).unwrap();
        result
    }

    async fn next(rounds: &mut mpsc::Receiver<Reply>) -> Reply {
        tokio::time::timeout(DEADLINE, rounds.recv())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn already_queued_calls_share_one_new_round() {
        let (reader, mut rounds) = fixture();
        let mut replies: Vec<_> = (0..10).map(|_| queue(&reader)).collect();
        let round = next(&mut rounds).await;
        assert!(rounds.try_recv().is_err());
        round.send(Ok(())).unwrap();
        replies.remove(0).await.unwrap().unwrap();
        for mut reply in replies {
            assert!(
                reply.try_recv().unwrap().is_ok(),
                "queued calls share a round"
            );
        }
        assert!(rounds.try_recv().is_err());
    }

    #[tokio::test]
    async fn arrival_during_round_requires_a_later_fresh_round() {
        let (reader, mut rounds) = fixture();
        let first = queue(&reader);
        let round = next(&mut rounds).await;
        let mut late = queue(&reader);
        round.send(Ok(())).unwrap();
        first.await.unwrap().unwrap();
        let later = next(&mut rounds).await;
        assert!(matches!(
            late.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        later.send(Err(stopped())).unwrap();
        assert_eq!(
            late.await.unwrap().unwrap_err().message(),
            "not current leader"
        );
    }

    #[tokio::test]
    async fn cancellation_cannot_cancel_other_group_members() {
        let (reader, mut rounds) = fixture();
        let canceled = queue(&reader);
        let waiting = queue(&reader);
        let round = next(&mut rounds).await;
        drop(canceled);
        round.send(Ok(())).unwrap();
        waiting.await.unwrap().unwrap();
        assert!(rounds.try_recv().is_err());
    }

    #[tokio::test]
    async fn failed_round_is_shared_and_never_cached() {
        let (reader, mut rounds) = fixture();
        let first = queue(&reader);
        let second = queue(&reader);
        next(&mut rounds).await.send(Err(stopped())).unwrap();
        assert!(first.await.unwrap().is_err());
        assert!(second.await.unwrap().is_err());
        let next_call = queue(&reader);
        next(&mut rounds).await.send(Ok(())).unwrap();
        next_call.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn dropping_reader_closes_active_and_queued_waiters() {
        let (reader, mut rounds) = fixture();
        let active = queue(&reader);
        let _round = next(&mut rounds).await;
        let pending = queue(&reader);
        drop(reader);
        assert!(active.await.is_err());
        assert!(pending.await.is_err());
    }

    #[tokio::test]
    async fn timed_out_round_does_not_poison_later_fresh_round() {
        let (reader, mut rounds) = fixture();
        let first = queue(&reader);
        let _blocked_round = next(&mut rounds).await;
        assert_eq!(
            first.await.unwrap().unwrap_err().message(),
            "quorum deadline"
        );
        let later = queue(&reader);
        next(&mut rounds).await.send(Ok(())).unwrap();
        later.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn authority_deadline_includes_queue_and_round_wait() {
        let (reader, mut rounds) = fixture();
        let blocked = queue(&reader);
        let _round = next(&mut rounds).await;
        let started = tokio::time::Instant::now();
        let error = reader.check().await.unwrap_err();
        assert_eq!(error.message(), "quorum deadline");
        assert!(started.elapsed() < DEADLINE + Duration::from_millis(500));
        assert_eq!(
            blocked.await.unwrap().unwrap_err().message(),
            "quorum deadline"
        );
    }

    #[tokio::test]
    async fn full_handoff_waits_without_capacity_rejection() {
        let (reader, mut rounds) = fixture();
        let reader = Arc::new(reader);
        let first = queue(&reader);
        let initial = next(&mut rounds).await;
        let waiting: Vec<_> = (0..MAILBOX).map(|_| queue(&reader)).collect();
        let extra = tokio::spawn({
            let reader = reader.clone();
            async move { reader.check().await }
        });
        tokio::task::yield_now().await;
        assert!(!extra.is_finished());
        initial.send(Ok(())).unwrap();
        first.await.unwrap().unwrap();
        let full = next(&mut rounds).await;
        full.send(Ok(())).unwrap();
        for reply in waiting {
            reply.await.unwrap().unwrap();
        }
        next(&mut rounds).await.send(Ok(())).unwrap();
        extra.await.unwrap().unwrap();
    }
}
