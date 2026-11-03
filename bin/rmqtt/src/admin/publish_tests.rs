use super::*;

#[test]
fn body_defaults_and_namespace_validation_are_explicit() {
    let request: Publish = serde_json::from_str(r#"{"topic":"devices/a","payload":""}"#).unwrap();
    assert_eq!((request.qos, request.retain), (1, false));
    assert!(request.validate().is_ok());
    for topic in ["$SYS", "$SYS/events/client/connected"] {
        let request = Publish {
            topic: topic.into(),
            payload: String::new(),
            qos: 1,
            retain: false,
        };
        assert_eq!(request.validate(), Err(StatusCode::FORBIDDEN));
    }
    for topic in ["", "a/+", "a/#", "a\0b"] {
        let request = Publish {
            topic: topic.into(),
            payload: String::new(),
            qos: 1,
            retain: false,
        };
        assert_eq!(request.validate(), Err(StatusCode::BAD_REQUEST));
    }
    assert!(
        serde_json::from_str::<Publish>(r#"{"topic":"a","payload":"x","client_id":"device"}"#)
            .is_err()
    );
}
