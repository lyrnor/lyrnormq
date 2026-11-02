use super::*;

#[tokio::test]
async fn storage_failure_waits_for_generation_cleanup_before_returning() {
    fn fail_write(_: &mut ConfigStore, _: &Config) -> Result<(), RuntimeError> {
        Err(RuntimeError::Storage)
    }
    let directory = std::env::temp_dir().join(format!(
        "rmqtt-stop-cleanup-{}-{}",
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
