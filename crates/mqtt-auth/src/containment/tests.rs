use super::covers_filter;
use mqtt_router::{matches, validate_filter};

#[test]
fn requested_language_cannot_escape_grant() {
    for (grant, request) in [
        ("a/#", "a"),
        ("a/#", "a/+"),
        ("a/#", "a//b/#"),
        ("a/+", "a/"),
        ("+/b", "a/b"),
        ("$SYS/#", "$SYS/+/load"),
        ("#", "+/foo/#"),
        ("#", "#"),
        ("+/#", "a/#"),
        ("/+/+", "//"),
        ("a/#", "a/#"),
    ] {
        assert!(
            covers_filter(grant, request),
            "{grant} must cover {request}"
        );
    }
    for (grant, request) in [
        ("a/+", "a/#"),
        ("a/b", "a/+"),
        ("a/b/#", "a/#"),
        ("a/#", "#"),
        ("#", "$SYS/#"),
        ("+/#", "$SYS/foo"),
        ("+/x", "$SYS/x"),
        ("a/+", "a"),
        ("a", "a/#"),
        ("$SYS/#", "#"),
        ("a/b", "a/b/#"),
        ("+/+", "/#"),
        ("a/#", "a+"),
        ("a/#", "$share/g/a/#"),
    ] {
        assert!(
            !covers_filter(grant, request),
            "{grant} must reject {request}"
        );
    }
}

fn strings(alphabet: &[&str], max_levels: usize) -> Vec<String> {
    let mut result = Vec::new();
    let mut level = alphabet.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
    for _ in 0..max_levels {
        result.extend(level.iter().cloned());
        level = level
            .iter()
            .flat_map(|prefix| {
                alphabet
                    .iter()
                    .map(move |suffix| format!("{prefix}/{suffix}"))
            })
            .collect();
    }
    result
}
#[test]
fn bounded_exhaustive_topic_reference_has_no_false_grants() {
    let filters = strings(&["", "a", "$SYS", "+", "#"], 3)
        .into_iter()
        .filter(|f| validate_filter(f))
        .collect::<Vec<_>>();
    let topics = strings(&["", "a", "b", "$SYS"], 4);
    let mut permitted = 0;
    for grant in &filters {
        for requested in &filters {
            if covers_filter(grant, requested) {
                permitted += 1;
                for topic in &topics {
                    assert!(
                        !matches(requested, topic) || matches(grant, topic),
                        "unsafe grant={grant:?}, request={requested:?}, topic={topic:?}"
                    );
                }
            }
        }
    }
    assert!(permitted > 200);
}
#[test]
fn deep_filters_are_iterative_and_bounded() {
    let deep = vec!["a"; 32768].join("/");
    assert_eq!(deep.len(), u16::MAX as usize);
    assert!(covers_filter(&deep, &deep));
    assert!(covers_filter("#", &deep));
    assert!(!covers_filter("#", &(deep + "a")));
    assert!(!covers_filter("", "a"));
}
