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
