use super::*;
use crate::durable::tests::{FILE_TESTS, TempDb};
use crate::durable::{Contents, RowLimits, Rows, read_contents};
use std::sync::{Barrier, mpsc};

fn limits() -> RowLimits {
    RowLimits {
        max_row_bytes: 64,
        max_sessions: 1024,
        max_retained: 16,
        max_legacy_bytes: 64,
    }
}
fn completion(sender: &mpsc::Sender<Result<(), String>>) -> Completion {
    let sender = sender.clone();
    Box::new(move |outcome| {
        let _ = sender.send(outcome.map_err(|error| error.to_string()));
    })
}

#[test]
fn queued_jobs_commit_in_one_group_and_complete_in_order() {
    let _serial = FILE_TESTS.lock().unwrap();
    let path = TempDb::new();
    let store = RowStore::open(&path.0, limits()).unwrap();
    let service = StorageService::start(store, 256, 64).unwrap();
    let (sender, receiver) = mpsc::channel();
    let gate = Arc::new(Barrier::new(2));
    let blocker = gate.clone();
    service
        .try_submit(
            vec![],
            Box::new(move |_| {
                blocker.wait();
            }),
        )
        .unwrap();
    for client in 0..100 {
        service
            .try_submit(
                vec![RowChange::PutSession(format!("c{client:03}"), vec![1])],
                completion(&sender),
            )
            .unwrap();
    }
    gate.wait();
    for _ in 0..100 {
        assert_eq!(receiver.recv().unwrap(), Ok(()));
    }
    assert!(matches!(
        service.try_submit(
            vec![RowChange::PutRetained("t".into(), vec![0; 65])],
            completion(&sender)
        ),
        Err(SubmitError::TooLarge)
    ));
    service.shutdown();
    match read_contents(&path.0, limits()).unwrap() {
        Contents::Rows(Rows { sessions, .. }) => assert_eq!(sessions.len(), 100),
        Contents::Legacy(_) => panic!("unexpected legacy container"),
    }
}

#[test]
fn full_queue_is_reported_without_blocking() {
    let _serial = FILE_TESTS.lock().unwrap();
    let path = TempDb::new();
    let store = RowStore::open(&path.0, limits()).unwrap();
    let service = StorageService::start(store, 1, 1).unwrap();
    let gate = Arc::new(Barrier::new(2));
    let blocker = gate.clone();
    service
        .try_submit(
            vec![],
            Box::new(move |_| {
                blocker.wait();
            }),
        )
        .unwrap();
    let (sender, receiver) = mpsc::channel();
    let mut full = false;
    for _ in 0..4 {
        match service.try_submit(vec![], completion(&sender)) {
            Ok(()) => {}
            Err(SubmitError::Full) => full = true,
            Err(error) => panic!("{error}"),
        }
    }
    assert!(full);
    gate.wait();
    service.shutdown();
    assert!(receiver.try_iter().all(|outcome| outcome.is_ok()));
}

#[test]
fn failed_commit_fails_the_group_and_every_later_job() {
    let _serial = FILE_TESTS.lock().unwrap();
    let path = TempDb::new();
    let store = RowStore::open(&path.0, limits()).unwrap();
    let pages: i64 = store
        .connection()
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .unwrap();
    store
        .connection()
        .pragma_update(None, "max_page_count", pages)
        .unwrap();
    let service = StorageService::start(store, 2048, 2048).unwrap();
    let (sender, receiver) = mpsc::channel();
    for client in 0..1000 {
        service
            .try_submit(
                vec![RowChange::PutSession(format!("c{client:04}"), vec![7; 64])],
                completion(&sender),
            )
            .unwrap();
    }
    let outcomes: Vec<_> = (0..1000).map(|_| receiver.recv().unwrap()).collect();
    assert!(outcomes.iter().any(Result::is_err));
    service.try_submit(vec![], completion(&sender)).unwrap();
    assert!(receiver.recv().unwrap().is_err());
    service.shutdown();
    assert!(StorageService::start(RowStore::open(&path.0, limits()).unwrap(), 1, 1).is_ok());
}

#[test]
fn legacy_store_cannot_start_a_service() {
    let _serial = FILE_TESTS.lock().unwrap();
    let path = TempDb::new();
    crate::durable::SqliteStore::open(&path.0, 64)
        .unwrap()
        .commit(b"x")
        .unwrap();
    let store = RowStore::open(&path.0, limits()).unwrap();
    assert!(matches!(
        StorageService::start(store, 1, 1),
        Err(DurableError::Schema(_))
    ));
}
