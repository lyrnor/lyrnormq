use super::*;
use std::sync::{Arc, atomic::AtomicBool};

struct Submission {
    command: Command,
    completion: oneshot::Sender<Applied>,
}
struct Fake {
    submissions: mpsc::Sender<Submission>,
    leader: Arc<AtomicBool>,
    fail_first_index: Option<u64>,
}
impl Backend for Fake {
    fn leader(&self) -> bool {
        self.leader.load(Ordering::SeqCst)
    }
    fn prepare(&self, command: &mut Command, pending: &Grants, current: &mut Grants) {
        prepare_at(command, 1000, 3000, |_| 5000, pending, current);
    }
    async fn submit(&self, command: Command) -> Result<Completion, Status> {
        if self.fail_first_index == Some(first_index(&command)) {
            return Err(unknown());
        }
        let (completion, result) = oneshot::channel();
        self.submissions
            .send(Submission {
                command,
                completion,
            })
            .await
            .unwrap();
        Ok(Box::pin(async move { result.await.map_err(|_| unknown())? }) as Completion)
    }
}
fn children(command: &Command) -> &[Command] {
    match command {
        Command::Batch { commands } => commands,
        _ => std::slice::from_ref(command),
    }
}
fn first_index(command: &Command) -> u64 {
    match &children(command)[0] {
        Command::Claim { generation, .. } => *generation,
        _ => u64::MAX,
    }
}
fn response(command: &Command) -> Applied {
    let answers: Vec<_> = children(command)
        .iter()
        .map(|c| Answer {
            revision: match c {
                Command::Claim { generation, .. } => *generation,
                _ => 1,
            },
            ..Answer::default()
        })
        .collect();
    let data = if answers.len() == 1 {
        answers.into_iter().next().unwrap()
    } else {
        Answer {
            results: answers,
            ..Answer::default()
        }
    };
    Ok(openraft::raft::ClientWriteResponse {
        log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), first_index(command)),
        data,
        membership: None,
    })
}
fn fixture(
    fail_first_index: Option<u64>,
) -> (
    mpsc::Sender<Queued>,
    mpsc::Receiver<Submission>,
    tokio::task::JoinHandle<()>,
) {
    let (sender, receiver) = mpsc::channel(MAILBOX);
    let (submissions, calls) = mpsc::channel(8);
    let backend = Fake {
        submissions,
        leader: Arc::new(AtomicBool::new(true)),
        fail_first_index,
    };
    let task = tokio::spawn(run(backend, receiver, 1, 3000));
    (sender, calls, task)
}
fn enqueue(sender: &mpsc::Sender<Queued>, command: Command) -> oneshot::Receiver<Completed> {
    let (reply, result) = oneshot::channel();
    sender
        .try_send(Queued {
            command,
            reply,
            enqueued: None,
            queue_us: None,
        })
        .unwrap();
    result
}
fn claim(generation: u64) -> Command {
    Command::Claim {
        node: 1,
        incarnation: "one".into(),
        client: format!("client-{generation}"),
        generation,
    }
}
async fn next(calls: &mut mpsc::Receiver<Submission>) -> Submission {
    tokio::time::timeout(Duration::from_millis(250), calls.recv())
        .await
        .expect("next batch must submit without earlier quorum completion")
        .unwrap()
}
fn complete(submission: Submission) {
    let response = response(&submission.command);
    submission.completion.send(response).unwrap();
}

#[tokio::test]
async fn four_batches_overlap_but_fifth_waits_and_replies_remain_fifo() {
    let (sender, mut calls, task) = fixture(None);
    let mut replies: Vec<_> = (0..320).map(|n| enqueue(&sender, claim(n))).collect();
    let first = next(&mut calls).await;
    let second = next(&mut calls).await;
    let third = next(&mut calls).await;
    let fourth = next(&mut calls).await;
    assert_eq!(
        [
            first_index(&first.command),
            first_index(&second.command),
            first_index(&third.command),
            first_index(&fourth.command)
        ],
        [0, 64, 128, 192]
    );
    assert!(calls.try_recv().is_err());
    for reply in &mut replies {
        assert!(matches!(
            reply.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }
    complete(second);
    complete(third);
    complete(fourth);
    tokio::task::yield_now().await;
    assert!(
        calls.try_recv().is_err(),
        "later completion cannot bypass the oldest result"
    );
    assert!(matches!(
        replies[64].try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    complete(first);
    let fifth = next(&mut calls).await;
    assert_eq!(first_index(&fifth.command), 256);
    complete(fifth);
    drop(sender); // Closing input still drains submitted replies.
    for (n, reply) in replies.into_iter().enumerate() {
        let answer = reply.await.unwrap().result.unwrap();
        assert_eq!(answer.revision, n as u64);
        assert_eq!(answer.lease_ms, 3000);
    }
    task.await.unwrap();
}

#[tokio::test]
async fn cancellation_and_submit_failure_do_not_discard_other_batches() {
    let (sender, mut calls, task) = fixture(Some(64));
    let mut replies: Vec<_> = (0..192).map(|n| enqueue(&sender, claim(n))).collect();
    drop(replies.remove(0));
    let first = next(&mut calls).await;
    let third = next(&mut calls).await;
    assert_eq!(
        first_index(&first.command),
        0,
        "canceled accepted work is still submitted"
    );
    assert_eq!(first_index(&third.command), 128);
    assert!(
        matches!(
            replies[63].try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ),
        "failed submission result waits behind earlier owned work"
    );
    complete(first);
    complete(third);
    drop(sender);
    for (n, reply) in (1..192).zip(replies) {
        let result = reply.await.unwrap().result;
        if (64..128).contains(&n) {
            assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
        } else {
            assert_eq!(result.unwrap().revision, n);
        }
    }
    task.await.unwrap();
}

#[tokio::test]
async fn dropped_response_and_malformed_results_fail_only_their_batches() {
    let (sender, mut calls, task) = fixture(None);
    let replies: Vec<_> = (0..192).map(|n| enqueue(&sender, claim(n))).collect();
    let first = next(&mut calls).await;
    let second = next(&mut calls).await;
    let third = next(&mut calls).await;
    drop(first.completion);
    let mut bad = response(&second.command).unwrap();
    bad.data.results.push(Answer::default());
    second.completion.send(Ok(bad)).unwrap();
    complete(third);
    drop(sender);
    for (n, reply) in replies.into_iter().enumerate() {
        let result = reply.await.unwrap().result;
        if n < 64 {
            assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
        } else if n < 128 {
            assert_eq!(result.unwrap_err().code(), tonic::Code::Internal);
        } else {
            assert_eq!(result.unwrap().revision, n as u64);
        }
    }
    task.await.unwrap();
}

fn heartbeat(node: u64) -> Command {
    Command::Heartbeat {
        node,
        incarnation: "one".into(),
        applied: 0,
        expires_at: 0,
        lease_ms: 0,
    }
}
fn expiry(command: &Command) -> u64 {
    match command {
        Command::Heartbeat { expires_at, .. } => *expires_at,
        _ => panic!("heartbeat required"),
    }
}
#[tokio::test]
async fn pending_lease_grants_are_monotonic_across_batches() {
    let (sender, mut calls, task) = fixture(None);
    let replies: Vec<_> = (0..128).map(|_| enqueue(&sender, heartbeat(7))).collect();
    let first = next(&mut calls).await;
    let second = next(&mut calls).await;
    let values: Vec<_> = children(&first.command)
        .iter()
        .chain(children(&second.command))
        .map(expiry)
        .collect();
    assert_eq!(values, (5001..=5128).collect::<Vec<_>>());
    complete(first);
    complete(second);
    drop(sender);
    for reply in replies {
        reply.await.unwrap().result.unwrap();
    }
    task.await.unwrap();
}

#[tokio::test]
async fn failed_grants_retire_without_unbounded_history() {
    let (sender, mut calls, task) = fixture(None);
    let first_reply = enqueue(&sender, heartbeat(7));
    let first = next(&mut calls).await;
    let other_reply = enqueue(&sender, heartbeat(8));
    let other = next(&mut calls).await;
    first.completion.send(Err(unknown())).unwrap();
    assert!(first_reply.await.unwrap().result.is_err());
    let later_reply = enqueue(&sender, heartbeat(7));
    let later = next(&mut calls).await;
    assert_eq!(
        expiry(&later.command),
        5001,
        "failed completed grants no longer shadow committed state"
    );
    complete(other);
    complete(later);
    drop(sender);
    other_reply.await.unwrap().result.unwrap();
    later_reply.await.unwrap().result.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn mixed_child_results_keep_their_original_positions() {
    let (sender, mut calls, task) = fixture(None);
    let replies: Vec<_> = (0..3).map(|n| enqueue(&sender, claim(n))).collect();
    let submission = next(&mut calls).await;
    let mut result = response(&submission.command).unwrap();
    result.data.results[1].error = Some("conflicting client epoch".into());
    submission.completion.send(Ok(result)).unwrap();
    drop(sender);
    for (n, reply) in replies.into_iter().enumerate() {
        let result = reply.await.unwrap().result.unwrap();
        assert_eq!(result.revision, n as u64);
        assert_eq!(
            result.error.as_deref(),
            (n == 1).then_some("conflicting client epoch")
        );
    }
    task.await.unwrap();
}

#[tokio::test]
async fn byte_limited_carry_is_drained_after_input_closes() {
    let (sender, mut calls, task) = fixture(None);
    let mut replies = Vec::new();
    for generation in 1..=3 {
        let command = Command::Filters {
            generation,
            node: 1,
            incarnation: "one".into(),
            client: format!("large-{generation}"),
            filters: vec!["x".repeat(58000); 9],
        };
        assert!(crate::model::valid_command(&command));
        assert!(serde_json::to_vec(&command).unwrap().len() <= MAX_BYTES);
        replies.push(enqueue(&sender, command));
    }
    drop(sender);
    for generation in 1..=3 {
        let submission = next(&mut calls).await;
        assert!(matches!(submission.command,Command::Filters{generation:g,..} if g==generation));
        complete(submission);
    }
    for reply in replies {
        reply.await.unwrap().result.unwrap();
    }
    task.await.unwrap();
}
