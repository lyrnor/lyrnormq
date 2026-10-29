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
