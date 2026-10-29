use super::*;
use std::sync::Arc;

#[test]
fn metadata_follows_apply_delete_and_prune() {
    let store = ConcurrentRetained::new(2, 64).unwrap();
    store.apply("a".into(), b"1".to_vec(), 1, 10u64).unwrap();
    store.apply("b".into(), b"2".to_vec(), 0, 20u64).unwrap();
    assert_eq!(
        store.apply("c".into(), b"3".to_vec(), 0, 30u64),
        Err(RetainedError::CapacityExceeded)
    );
    let matched = store.matching("#", 2);
    assert_eq!(matched.len(), 2);
    assert_eq!(matched[0].1, 10);
    store.apply("a".into(), Vec::new(), 0, 0).unwrap();
    assert_eq!(store.len(), 1);
    assert_eq!(store.prune(|expiry| *expiry > 25), ["b"]);
    assert!(store.is_empty());
    assert!(store.entries().is_empty());
}

#[test]
fn concurrent_writers_respect_capacity() {
    let store = Arc::new(ConcurrentRetained::new(50, 1 << 20).unwrap());
    let writers: Vec<_> = (0..8)
        .map(|worker| {
            let store = store.clone();
            std::thread::spawn(move || {
                let mut accepted = 0;
                for topic in 0..20 {
                    if store
                        .apply(format!("t/{worker}/{topic}"), vec![1], 0, ())
                        .is_ok()
                    {
                        accepted += 1;
                    }
                }
                accepted
            })
        })
        .collect();
    let accepted: usize = writers.into_iter().map(|w| w.join().unwrap()).sum();
    assert_eq!(accepted, 50);
    assert_eq!(store.len(), 50);
    assert_eq!(store.entries().len(), 50);
}
