//! Admission remains charged until submitted work finishes, even after caller cancellation.
use std::future::Future;
use tokio::sync::OwnedSemaphorePermit;

pub(super) fn submit<F, T>(permit: OwnedSemaphorePermit, work: F) -> tokio::task::JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    tokio::spawn(async move {
        let _permit = permit;
        work.await
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::{Notify, Semaphore};
    #[tokio::test]
    async fn canceled_reply_retains_admission_until_submitted_work_completes() {
        let slots = Arc::new(Semaphore::new(1));
        let done = Arc::new(Notify::new());
        let completed = Arc::new(Notify::new());
        let task = submit(slots.clone().acquire_owned().await.unwrap(), {
            let done = done.clone();
            let completed = completed.clone();
            async move {
                done.notified().await;
                completed.notify_one();
            }
        });
        drop(task); // Dropping a response does not cancel already-submitted work.
        tokio::task::yield_now().await;
        assert_eq!(slots.available_permits(), 0);
        assert!(slots.clone().try_acquire_owned().is_err());
        done.notify_one();
        completed.notified().await;
        let _permit = tokio::time::timeout(std::time::Duration::from_secs(1), slots.acquire())
            .await
            .unwrap()
            .unwrap();
    }
}
