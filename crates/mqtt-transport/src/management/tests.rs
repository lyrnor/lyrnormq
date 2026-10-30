use super::*;
#[tokio::test]
async fn unattached_queries_fail_without_fabricated_state() {
    let handle = ManagementHandle::new();
    assert_eq!(
        handle.overview().await.unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert!(
        handle
            .sessions(Query {
                limit: 101,
                ..Query::default()
            })
            .await
            .is_err()
    );
}
