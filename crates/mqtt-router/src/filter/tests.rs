use super::*;
#[test]
fn filter_validation() {
    for valid in ["#", "+", "/", "a/", "/+", "a/#", "a/+/c", "$SYS/#", "a//b"] {
        assert!(validate_filter(valid), "{valid}");
    }
    for invalid in [
        "",
        "a#",
        "a+",
        "a/#/b",
        "##",
        "+a",
        "$share/g/a",
        "a\0b",
        "\u{ffff}",
    ] {
        assert!(!validate_filter(invalid), "{invalid}");
    }
    assert!(!validate_filter(&"a".repeat(65536)));
}
#[test]
fn wildcard_edges() {
    for (filter, topic, expected) in [
        ("sport/#", "sport", true),
        ("sport/#", "sport/", true),
        ("sport/+", "sport", false),
        ("sport/+", "sport/", true),
        ("+", "/", false),
        ("+/+", "/", true),
        ("#", "$SYS/x", false),
        ("+/#", "$SYS/x", false),
        ("$SYS/#", "$SYS", true),
        ("$SYS/+", "$SYS/x", true),
        ("a/+", "a/$b", true),
        ("a", "A", false),
        ("#", "", false),
        ("#", "bad+", false),
        ("a/#", "ab", false),
    ] {
        assert_eq!(matches(filter, topic), expected, "{filter} {topic}");
    }
}

#[test]
fn shared_filter_grammar_and_literal_inner_patterns() {
    for (input, name, inner) in [
        ("$share/g/a/+", "g", "a/+"),
        ("$share/g//finance", "g", "/finance"),
        ("$share/组/$SYS/#", "组", "$SYS/#"),
        ("$share/g/$share/h/#", "g", "$share/h/#"),
    ] {
        assert_eq!(split_shared_filter(input), Some((name, inner)));
        assert!(validate_subscription_filter(input));
        assert!(!validate_filter(input));
        assert!(validate_topic_filter(inner));
    }
    for input in [
        "$share//a",
        "$share/g/",
        "$share/g",
        "$share/+/a",
        "$share/#/a",
        "$share/g/a/#/b",
        "$share/g/a\0",
    ] {
        assert_eq!(split_shared_filter(input), None, "{input}");
        assert!(!validate_subscription_filter(input));
    }
    assert_eq!(split_shared_filter("a/+"), None);
    assert!(validate_subscription_filter("a/+"));
    assert!(validate_subscription_filter("$share"));
}

#[test]
fn plain_pattern_matcher_keeps_share_names_literal_and_system_isolated() {
    assert!(matches_topic_filter("$share/g/#", "$share/g/x"));
    assert!(!matches("$share/g/#", "$share/g/x"));
    assert!(!matches_topic_filter("#", "$share/g/x"));
    assert!(!matches_topic_filter("+/#", "$SYS/x"));
    assert!(matches_topic_filter("$SYS/#", "$SYS/x"));
    assert!(!matches_topic_filter("$share/g/#", "x"));
}
