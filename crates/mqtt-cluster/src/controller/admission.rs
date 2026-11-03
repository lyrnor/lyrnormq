//! Submitted Raft work continues independently of the cancellable response future.
use std::future::Future;

pub(super) fn submit<F, T>(work: F) -> tokio::task::JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    tokio::spawn(work)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Notify;
    #[tokio::test]
    async fn canceled_reply_does_not_cancel_submitted_work() {
        let done = Arc::new(Notify::new());
        let completed = Arc::new(Notify::new());
        let task = submit({
            let done = done.clone();
            let completed = completed.clone();
            async move {
                done.notified().await;
                completed.notify_one();
            }
        });
        drop(task); // Dropping a response does not cancel already-submitted work.
        tokio::task::yield_now().await;
        done.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(1), completed.notified())
            .await
            .unwrap();
    }
}
