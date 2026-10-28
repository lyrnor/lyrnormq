//! Connection receive deadlines shared by the transactional runtime.
use std::future::pending;
use tokio::time::{Instant, sleep_until};

pub(super) async fn deadline(at: Option<Instant>) {
    match at {
        Some(at) => sleep_until(at).await,
        None => pending::<()>().await,
    }
}
