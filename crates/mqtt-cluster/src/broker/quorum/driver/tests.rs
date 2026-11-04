use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Dropped(Option<oneshot::Sender<()>>);
impl Drop for Dropped {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

fn empty() -> Reply {
    Reply { record: None }
}
fn record(epoch: u64, physical: u64, body: u8) -> Reply {
    let mut record = Record::empty(epoch);
    record.physical_revision = physical;
    record.snapshot = Some(vec![body]);
    Reply {
        record: Some(record),
    }
}
fn ready(value: Reply) -> PeerFuture {
    Box::pin(async move { Ok(value) })
}

fn fixture() -> (
    Vec<PeerFuture>,
    oneshot::Sender<Reply>,
    oneshot::Receiver<()>,
    Arc<AtomicUsize>,
) {
    fixture_with(empty(), empty())
}

fn fixture_with(
    first_reply: Reply,
    second_reply: Reply,
) -> (
    Vec<PeerFuture>,
    oneshot::Sender<Reply>,
    oneshot::Receiver<()>,
    Arc<AtomicUsize>,
) {
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let completed = Arc::new(AtomicUsize::new(0));
    let observed = completed.clone();
    let tail: PeerFuture = Box::pin(async move {
        let _guard = Dropped(Some(dropped_tx));
        let _ = started_tx.send(());
        let value = release_rx.await.map_err(io::Error::other)?;
        observed.fetch_add(1, Ordering::SeqCst);
        Ok(value)
    });
    let first: PeerFuture = Box::pin(async move {
        started_rx.await.map_err(io::Error::other)?;
        Ok(first_reply)
    });
    (
        vec![first, ready(second_reply), tail],
        release_tx,
        dropped_rx,
        completed,
    )
}

#[tokio::test]
async fn pending_third_peer_survives_w2_and_completes_after_release() {
    let (peers, release, mut dropped, completed) = fixture();
    let result = start(peers, Instant::now()).await.unwrap().unwrap();
    assert!(result.is_none());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut dropped)
            .await
            .is_err(),
        "W2 success must not cancel the third peer"
    );
    release
        .send(empty())
        .unwrap_or_else(|_| panic!("third request must remain alive"));
    tokio::time::timeout(Duration::from_secs(1), dropped)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn selected_pair_checks_versions_none_and_conflicts() {
    for (a, b, expected) in [
        (empty(), empty(), None),
        (empty(), record(2, 4, 1), Some((2, 4))),
        (record(2, 4, 1), record(2, 3, 2), Some((2, 4))),
        (record(1, 9, 1), record(2, 0, 2), Some((2, 0))),
        (record(2, 4, 1), record(2, 4, 1), Some((2, 4))),
    ] {
        let mut selected = Selected::default();
        assert!(!selected.accept(a).unwrap());
        assert!(selected.accept(b).unwrap());
        assert_eq!(selected.chosen.map(|r| r.version()), expected);
    }
    let mut selected = Selected::default();
    selected.accept(record(2, 4, 1)).unwrap();
    assert!(selected.accept(record(2, 4, 2)).is_err());
}

#[tokio::test]
async fn caller_cancellation_does_not_abort_accepted_peer_execution() {
    let (peers, release, dropped, completed) = fixture();
    drop(start(peers, Instant::now()));
    tokio::time::sleep(Duration::from_millis(20)).await;
    release
        .send(record(9, 1, 3))
        .unwrap_or_else(|_| panic!("accepted work must survive its caller"));
    tokio::time::timeout(Duration::from_secs(1), dropped)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn tail_has_absolute_deadline_and_does_not_delay_w2_result() {
    let (peers, release, dropped, completed) = fixture();
    let result = start_budgets(
        peers,
        Instant::now() + Duration::from_secs(1),
        Duration::from_millis(30),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(result.is_none());
    tokio::time::timeout(Duration::from_millis(300), dropped)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.load(Ordering::SeqCst), 0);
    assert!(release.send(empty()).is_err());
}

#[tokio::test]
async fn overall_deadline_bounds_tail_and_unresponsive_quorum() {
    let (peers, release, dropped, completed) = fixture();
    let result = start_budgets(
        peers,
        Instant::now() + Duration::from_millis(30),
        Duration::from_secs(1),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(result.is_none());
    tokio::time::timeout(Duration::from_millis(300), dropped)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.load(Ordering::SeqCst), 0);
    assert!(release.send(empty()).is_err());

    let pending = || Box::pin(std::future::pending()) as PeerFuture;
    let result = start_budgets(
        vec![pending(), pending(), ready(empty())],
        Instant::now() + Duration::from_millis(20),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert!(
        result.is_err(),
        "one vote cannot satisfy W2 before deadline"
    );
}

#[tokio::test]
async fn only_successful_replies_count_and_all_terminal_failures_return_error() {
    let failed = || Box::pin(async { Err(io::Error::other("peer unavailable")) }) as PeerFuture;
    let result = start(
        vec![failed(), ready(empty()), ready(record(4, 1, 2))],
        Instant::now(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.unwrap().version(), (4, 1));
    let result = start(vec![failed(), failed(), ready(empty())], Instant::now())
        .await
        .unwrap();
    assert!(result.is_err());
}

#[tokio::test]
async fn higher_version_tail_cannot_join_an_already_returned_quorum() {
    let (peers, release, dropped, completed) = fixture();
    let result = start(peers, Instant::now()).await.unwrap().unwrap();
    assert!(
        result.is_none(),
        "the successful empty pair is the selected quorum"
    );
    release
        .send(record(100, 100, 99))
        .unwrap_or_else(|_| panic!("tail must remain owned"));
    tokio::time::timeout(Duration::from_secs(1), dropped)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn one_peer_panic_isolated_from_the_other_two_successful_votes() {
    let panics: PeerFuture = Box::pin(async { panic!("controlled peer panic") });
    let result = start(
        vec![panics, ready(empty()), ready(record(4, 5, 7))],
        Instant::now(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.unwrap().version(), (4, 5));
}

#[tokio::test]
async fn selected_pair_conflict_replies_error_but_still_drains_tail() {
    let (peers, release, mut dropped, completed) = fixture_with(record(2, 4, 1), record(2, 4, 2));
    assert!(start(peers, Instant::now()).await.unwrap().is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut dropped)
            .await
            .is_err()
    );
    release
        .send(empty())
        .unwrap_or_else(|_| panic!("error result must not abort accepted tail"));
    tokio::time::timeout(Duration::from_secs(1), dropped)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn tail_conflict_is_ignored_after_an_identical_successful_pair() {
    let (peers, release, dropped, completed) = fixture_with(record(2, 4, 1), record(2, 4, 1));
    let result = start(peers, Instant::now())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.snapshot, Some(vec![1]));
    release
        .send(record(2, 4, 2))
        .unwrap_or_else(|_| panic!("tail must remain owned"));
    tokio::time::timeout(Duration::from_secs(1), dropped)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn all_three_peers_are_polled_before_consuming_an_immediate_quorum() {
    let starts = Arc::new(AtomicUsize::new(0));
    let peers = (0..3)
        .map(|_| {
            let starts = starts.clone();
            Box::pin(async move {
                starts.fetch_add(1, Ordering::SeqCst);
                Ok(empty())
            }) as PeerFuture
        })
        .collect();
    assert!(
        start(peers, Instant::now())
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    assert_eq!(starts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn frequent_tail_wakes_cannot_refresh_its_absolute_deadline() {
    let (sender, dropped) = oneshot::channel();
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = polls.clone();
    let tail: PeerFuture = Box::pin(async move {
        let _guard = Dropped(Some(sender));
        poll_fn(move |cx| {
            observed.fetch_add(1, Ordering::SeqCst);
            cx.waker().wake_by_ref();
            Poll::Pending
        })
        .await
    });
    let result = start_budgets(
        vec![ready(empty()), ready(empty()), tail],
        Instant::now() + Duration::from_secs(1),
        Duration::from_millis(20),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(result.is_none());
    tokio::time::timeout(Duration::from_millis(500), dropped)
        .await
        .unwrap()
        .unwrap();
    assert!(
        polls.load(Ordering::SeqCst) > 2,
        "tail must have produced repeated progress wakes"
    );
}
