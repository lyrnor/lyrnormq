use super::*;
use std::sync::Arc;

fn set(filters: &[&str]) -> BTreeSet<String> {
    filters.iter().map(|filter| (*filter).to_owned()).collect()
}

#[test]
fn replace_diffs_ordinary_and_shared_filters() {
    let index = SubscriptionIndex::new();
    index.replace("a", &set(&["t/#", "$share/g/t/+"]));
    index.replace("b", &set(&["t/x", "$share/g/t/+"]));
    let matches = index.matches("t/x");
    assert_eq!(matches.ordinary, ["a", "b"]);
    assert_eq!(
        matches.shared,
        [SharedMatch {
            group: "$share/g/t/+".into(),
            members: vec!["a".into(), "b".into()],
        }]
    );
    index.replace("a", &set(&["$share/g/t/+"]));
    assert_eq!(index.matches("t/x").ordinary, ["b"]);
    index.remove("b");
    let matches = index.matches("t/x");
    assert!(matches.ordinary.is_empty());
    assert_eq!(matches.shared[0].members, ["a"]);
    index.remove("a");
    assert_eq!(index.matches("t/x"), IndexMatches::default());
    assert_eq!(index.client_count(), 0);
}

#[test]
fn duplicate_ordinary_matches_report_one_client() {
    let index = SubscriptionIndex::new();
    index.replace("a", &set(&["t/#", "t/+", "t/x"]));
    assert_eq!(index.matches("t/x").ordinary, ["a"]);
    assert!(index.matches("u").ordinary.is_empty());
    assert!(index.matches("$SYS/x").ordinary.is_empty());
}

#[test]
fn shared_cursor_rotates_after_selected_member_and_survives_departure() {
    let index = SubscriptionIndex::new();
    for client in ["a", "b", "c"] {
        index.replace(client, &set(&["$share/g/t"]));
    }
    let members = |index: &SubscriptionIndex| index.matches("t").shared[0].members.clone();
    assert_eq!(members(&index), ["a", "b", "c"]);
    index.select("$share/g/t", "a");
    assert_eq!(members(&index), ["b", "c", "a"]);
    index.select("$share/g/t", "b");
    index.remove("b");
    assert_eq!(members(&index), ["c", "a"]);
    index.select("$share/g/t", "c");
    assert_eq!(members(&index), ["a", "c"]);
    index.remove("a");
    index.remove("c");
    index.select("$share/g/t", "c");
    index.replace("d", &set(&["$share/g/t"]));
    assert_eq!(members(&index), ["d"]);
}

#[test]
fn concurrent_replace_and_match_converge() {
    let index = Arc::new(SubscriptionIndex::new());
    let writers: Vec<_> = (0..8)
        .map(|worker| {
            let index = index.clone();
            std::thread::spawn(move || {
                for round in 0..200 {
                    let client = format!("c{worker}-{}", round % 10);
                    index.replace(&client, &set(&["load/#", "$share/g/load/+"]));
                    let _ = index.matches("load/x");
                    if round % 3 == 0 {
                        index.remove(&client);
                    }
                }
            })
        })
        .collect();
    for writer in writers {
        writer.join().unwrap();
    }
    let matches = index.matches("load/x");
    assert_eq!(matches.ordinary.len(), index.client_count());
    assert_eq!(matches.shared[0].members, matches.ordinary);
    for client in matches.ordinary {
        index.remove(&client);
    }
    assert_eq!(index.matches("load/x"), IndexMatches::default());
}

#[test]
fn unique_topics_do_not_scan_every_subscription() {
    let index = SubscriptionIndex::new();
    const CLIENTS: usize = 20_000;
    for i in 0..CLIENTS {
        index.replace(&format!("c{i}"), &set(&[&format!("device/{i}")]));
    }
    index.replace("wild", &set(&["sensor/#"]));
    index.replace("sys", &set(&["$SYS/#"]));
    let started = std::time::Instant::now();
    let exact = index.matches("device/1234");
    let wildcard = index.matches("sensor/room/temp");
    let isolated = index.matches("$SYS/broker/load");
    let miss = index.matches("other/x");
    let elapsed = started.elapsed();
    assert_eq!(exact.ordinary, ["c1234"]);
    assert_eq!(wildcard.ordinary, ["wild"]);
    assert_eq!(isolated.ordinary, ["sys"]);
    assert!(miss.ordinary.is_empty());
    assert!(
        elapsed.as_millis() < 200,
        "indexed match of 20k unique topics took {elapsed:?}"
    );
}

#[test]
fn concurrent_match_replace_and_shared_select_keep_a_consistent_snapshot() {
    let index = Arc::new(SubscriptionIndex::new());
    for client in 0..64 {
        index.replace(&format!("c{client}"), &set(&["load/#", "$share/g/load/+"]));
    }
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let readers: Vec<_> = (0..8)
        .map(|_| {
            let index = index.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut hits = 0u64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let found = index.matches("load/x");
                    assert_eq!(found.ordinary.len(), found.shared[0].members.len());
                    for client in &found.ordinary {
                        assert!(client.starts_with('c'));
                    }
                    hits += 1;
                }
                hits
            })
        })
        .collect();
    let writers: Vec<_> = (0..4)
        .map(|worker| {
            let index = index.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut round = 0u32;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let client = format!("c{}", 64 + worker * 8 + (round % 8));
                    if round.is_multiple_of(2) {
                        index.replace(&client, &set(&["load/#", "$share/g/load/+"]));
                    } else {
                        index.remove(&client);
                    }
                    if round.is_multiple_of(5) {
                        index.select("$share/g/load/+", "c0");
                    }
                    round += 1;
                }
                round
            })
        })
        .collect();
    std::thread::sleep(std::time::Duration::from_millis(400));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let reads: u64 = readers.into_iter().map(|t| t.join().unwrap()).sum();
    let writes: u32 = writers.into_iter().map(|t| t.join().unwrap()).sum();
    assert!(reads > 1_000, "match throughput too low: {reads}");
    assert!(writes > 100, "replace throughput too low: {writes}");
    let found = index.matches("load/x");
    assert_eq!(found.ordinary.len(), found.shared[0].members.len());
    assert_eq!(found.ordinary.len(), index.client_count());
}
