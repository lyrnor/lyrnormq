use super::*;

#[tokio::test]
async fn storage_failure_waits_for_generation_cleanup_before_returning() {
    fn fail_write(_: &mut ConfigStore, _: &Config) -> Result<(), RuntimeError> {
        Err(RuntimeError::Storage)
    }
    let directory = std::env::temp_dir().join(format!(
        "lyrnormq-stop-cleanup-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    std::fs::create_dir(&directory).unwrap();
    let runtime = Runtime::open(directory.join("rules.sqlite")).await.unwrap();
    let generation = Arc::clone(&runtime.inner.snapshot.read().unwrap().generation);
    let mut stopped = generation.cancel.subscribe();
    let gate = Arc::new(tokio::sync::Notify::new());
    let cleanup_gate = Arc::clone(&gate);
    let cleanup = tokio::spawn(async move {
        stopped.changed().await.unwrap();
        cleanup_gate.notified().await;
    });
    generation.tasks.lock().unwrap().push(cleanup);
    let writer = Arc::clone(&runtime);
    let mut update = tokio::spawn(async move {
        writer
            .replace_test_write(Config::default(), fail_write)
            .await
    });
    let before_cleanup = tokio::time::timeout(Duration::from_millis(50), &mut update).await;
    // Release even on the regression path so the fixture never leaves a task held.
    gate.notify_one();
    assert!(
        before_cleanup.is_err(),
        "storage failure returned before generation cleanup"
    );
    assert!(matches!(update.await.unwrap(), Err(RuntimeError::Storage)));
    runtime.shutdown().await;
    drop(runtime);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn failed_journal_initialization_releases_configuration_ownership() {
    let directory = std::env::temp_dir().join(format!(
        "lyrnormq-init-failure-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("rules.sqlite");
    let mut options = crate::QueueOptions::new(directory.join("queue"));
    options.segment_bytes = 0;
    assert!(
        Runtime::open_with_queue(path.clone(), options.clone())
            .await
            .is_err()
    );
    let reopened = path.clone();
    assert!(
        tokio::task::spawn_blocking(move || ConfigStore::open(&reopened, MAX_CONFIG).is_ok())
            .await
            .unwrap()
    );
    options.segment_bytes = 1024 * 1024;
    let (held, _) = Queue::open(
        options.clone(),
        vec![],
        Arc::new(Counters::default()),
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .unwrap();
    assert!(
        Runtime::open_with_queue(path.clone(), options)
            .await
            .is_err()
    );
    assert!(
        tokio::task::spawn_blocking(move || ConfigStore::open(&path, MAX_CONFIG).is_ok())
            .await
            .unwrap()
    );
    held.close().await;
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn cancelled_initialization_guard_releases_configuration_on_blocking_pool() {
    let directory = std::env::temp_dir().join(format!(
        "lyrnormq-init-cancel-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("rules.sqlite");
    let opened = path.clone();
    let store = tokio::task::spawn_blocking(move || ConfigStore::open(&opened, MAX_CONFIG))
        .await
        .unwrap()
        .unwrap();
    let owner = crate::runtime_init::ConfigOwner::new(store, tokio::runtime::Handle::current());
    let (ready, started) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _owner = owner;
        let _ = ready.send(());
        std::future::pending::<()>().await;
    });
    started.await.unwrap();
    task.abort();
    let _ = task.await;
    assert!(
        tokio::task::spawn_blocking(move || {
            for _ in 0..100 {
                if ConfigStore::open(&path, MAX_CONFIG).is_ok() {
                    return true;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            false
        })
        .await
        .unwrap()
    );
    std::fs::remove_dir_all(directory).unwrap();
}
