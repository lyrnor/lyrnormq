use crate::{CompiledRule, Event, RuleError};
use serde_json::json;
fn event(payload: &[u8]) -> Event {
    Event {
        topic: "devices/a/telemetry".into(),
        payload: payload.to_vec(),
        client_id: "sensor-a".into(),
        qos: 1,
        retain: false,
        timestamp_ms: 123,
    }
}
fn evaluate(sql: &str, payload: &[u8]) -> Option<serde_json::Value> {
    CompiledRule::compile(sql)
        .unwrap()
        .evaluate(&event(payload))
        .unwrap()
}
#[test]
fn filters_projects_json_and_metadata() {
    let sql = r#"SELECT client_id, topic AS source, payload.device_id AS device, payload.temperature + 1 AS temperature FROM "devices/+/telemetry" WHERE payload.temperature >= 80 AND qos = 1"#;
    assert_eq!(
        evaluate(sql, br#"{"device_id":"a","temperature":81}"#),
        Some(
            json!({"client_id":"sensor-a","source":"devices/a/telemetry","device":"a","temperature":82.0})
        )
    );
    assert_eq!(evaluate(sql, br#"{"temperature":79}"#), None);
}
#[test]
fn metadata_rules_support_binary_and_null_logic() {
    assert_eq!(
        evaluate(
            r#"SELECT topic, payload.missing AS missing FROM "devices/#" WHERE NULL OR qos = 1"#,
            b"binary\xff"
        ),
        Some(json!({"topic":"devices/a/telemetry","missing":null}))
    );
    assert_eq!(
        evaluate(
            r#"SELECT topic FROM "devices/#" WHERE NOT payload.missing"#,
            b"{}"
        ),
        None
    );
    assert_eq!(
        evaluate(
            r#"SELECT topic FROM "devices/#" WHERE payload.missing IS NULL"#,
            b"{}"
        ),
        Some(json!({"topic":"devices/a/telemetry"}))
    );
    assert_eq!(
        evaluate(
            r#"SELECT topic FROM "devices/#" WHERE NULL AND true"#,
            b"{}"
        ),
        None
    );
    assert_eq!(
        evaluate(
            r#"SELECT topic FROM "devices/#" WHERE NULL AND false"#,
            b"{}"
        ),
        None
    );
    assert_eq!(
        evaluate(
            r#"SELECT topic FROM "devices/#" WHERE false AND 1/0 > 0"#,
            b"{}"
        ),
        None
    );
}
#[test]
fn functions_aliases_and_wildcard() {
    assert_eq!(
        evaluate(
            r#"SELECT LOWER(payload.name) AS lower, UPPER('ok') AS upper, COALESCE(payload.x, 3) AS n FROM "devices/#""#,
            br#"{"name":"ALARM"}"#
        ),
        Some(json!({"lower":"alarm","upper":"OK","n":3.0}))
    );
    let output = evaluate(r#"SELECT * FROM "devices/#""#, b"opaque").unwrap();
    assert_eq!(output["payload"], json!(null));
    assert_eq!(output["timestamp"], json!(123));
}
#[test]
fn topic_filters_follow_mqtt_system_topic_rules() {
    let rule = CompiledRule::compile(r##"SELECT topic FROM "#""##).unwrap();
    assert!(rule.matches_topic("a/b"));
    assert!(!rule.matches_topic("$SYS/status"));
    assert!(!rule.matches_topic("a/+"));
    assert!(CompiledRule::compile(r#"SELECT topic FROM "a/#/b""#).is_err());
    assert!(CompiledRule::compile(r#"SELECT topic FROM "$share/a/#""#).is_err());
}
#[test]
fn rejects_unsupported_statements_clauses_functions_and_duplicates() {
    for sql in [
        r#"DELETE FROM "a""#,
        r#"SELECT topic FROM "a"; SELECT topic FROM "b""#,
        r#"SELECT topic FROM "a" LIMIT 1"#,
        r#"SELECT topic FROM "a" ORDER BY topic"#,
        r#"SELECT topic FROM "a" GROUP BY topic"#,
        r#"SELECT DISTINCT topic FROM "a""#,
        r#"SELECT topic FROM "a" JOIN "b" ON true"#,
        r#"SELECT (SELECT 1) FROM "a""#,
        r#"SELECT SLEEP(1) FROM "a""#,
        r#"SELECT LOWER(topic) OVER () FROM "a""#,
        r#"SELECT secret FROM "a""#,
        r#"SELECT topic, client_id AS topic FROM "a""#,
        r#"SELECT topic FROM a"#,
        r#"SELECT t.topic FROM "a" AS t"#,
        r#"SELECT * EXCEPT (topic) FROM "a""#,
    ] {
        assert!(CompiledRule::compile(sql).is_err(), "accepted {sql}");
    }
}
#[test]
fn limits_and_arithmetic_fail_explicitly() {
    assert!(CompiledRule::compile(&"x".repeat(8193)).is_err());
    let deep = format!("SELECT {}1{} FROM \"a\"", "(".repeat(100), ")".repeat(100));
    assert!(CompiledRule::compile(&deep).is_err());
    assert!(CompiledRule::compile(r#"SELECT 9007199254740992 FROM "a""#).is_err());
    assert!(CompiledRule::compile(r#"SELECT 1e309 FROM "a""#).is_err());
    let rule = CompiledRule::compile(r#"SELECT 1 / 0 AS value FROM "devices/#""#).unwrap();
    assert_eq!(rule.evaluate(&event(b"{}")), Err(RuleError::Arithmetic));
    let rule = CompiledRule::compile(r#"SELECT payload.n + 1 FROM "devices/#""#).unwrap();
    assert_eq!(
        rule.evaluate(&event(br#"{"n":9007199254740992}"#)),
        Err(RuleError::Arithmetic)
    );
    let rule =
        CompiledRule::compile(r#"SELECT payload.large AS a, payload.large AS b FROM "devices/#""#)
            .unwrap();
    let body = serde_json::to_vec(&json!({"large":"x".repeat(600_000)})).unwrap();
    assert_eq!(rule.evaluate(&event(&body)), Err(RuleError::OutputTooLarge));
}
