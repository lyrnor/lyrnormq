use super::*;
use mqtt_auth::{EnhancedAuthenticator, PlainAuthenticator};

fn current(online: &HashMap<String, Online>) -> impl Fn(&str, u64) -> bool + '_ {
    |client, generation| {
        online
            .get(client)
            .is_some_and(|o| o.generation == generation)
    }
}

fn connect(generation: u64, data: Option<&[u8]>, maximum: u32) -> Request {
    let mut frame = Frame::v5(Packet::Connect {
        client_id: "auth-client".into(),
        clean_session: true,
        keep_alive: 0,
        will: None,
        username: None,
        password: None,
    });
    frame
        .properties
        .push(Property::AuthenticationMethod("PLAIN".into()));
    frame.properties.push(Property::MaximumPacketSize(maximum));
    if let Some(data) = data {
        frame
            .properties
            .push(Property::AuthenticationData(data.to_vec()));
    }
    normalize(Operation::ConnectFrame {
        frame,
        generation,
        wake: watch::channel(0).0,
        cancel: watch::channel(false).0,
        close_reason: watch::channel(None).0,
    })
    .unwrap()
}
fn continuation(generation: u64, code: u8, data: Option<&[u8]>) -> Request {
    Request::Authenticate {
        generation,
        frame: auth_frame("PLAIN", code, data.map(<[u8]>::to_vec)).unwrap(),
        deadline: Instant::now() + Duration::from_secs(5),
    }
}
fn config() -> BrokerConfig {
    let mut config = BrokerConfig {
        access_control: Arc::new(super::super::enhanced_tests::Policy),
        ..BrokerConfig::default()
    };
    config.enhanced_auth = Some(Arc::new(PlainAuthenticator::new(
        config.access_control.clone(),
    )));
    config
}
#[test]
fn initial_plain_direct_challenge_cancel_and_sequence() {
    let config = config();
    let mut manager = Manager::default();
    let online = HashMap::new();
    let Processed::Ready(Request::Connect(request)) = manager
        .process(
            connect(1, Some(b"\0alice\0password"), u32::MAX),
            &config,
            current(&online),
        )
        .unwrap()
    else {
        panic!("direct success");
    };
    assert!(request.authenticated.is_some());
    assert!(
        !request
            .properties
            .iter()
            .any(|p| matches!(p, Property::AuthenticationData(_)))
    );
    assert!(matches!(
        manager
            .process(connect(2, None, u32::MAX), &config, current(&online))
            .unwrap(),
        Processed::Reply(Reply::AuthContinue(_))
    ));
    assert_eq!(manager.pending.len(), 1);
    assert!(
        manager
            .process(continuation(2, 0, None), &config, current(&online))
            .is_err()
    );
    assert!(manager.pending.is_empty());
    manager
        .process(connect(3, None, u32::MAX), &config, current(&online))
        .unwrap();
    manager
        .process(
            Request::CancelAuthentication { generation: 3 },
            &config,
            current(&online),
        )
        .unwrap();
    assert!(manager.pending.is_empty());
    assert!(
        manager
            .process(
                continuation(3, 0x18, Some(b"\0alice\0password")),
                &config,
                current(&online)
            )
            .is_err()
    );
    manager
        .process(connect(4, None, u32::MAX), &config, current(&online))
        .unwrap();
    assert!(matches!(
        manager
            .process(
                continuation(4, 0x18, Some(b"\0alice\0password")),
                &config,
                current(&online)
            )
            .unwrap(),
        Processed::Ready(Request::Connect(_))
    ));
}
#[derive(Debug)]
struct Endless;
impl EnhancedExchange for Endless {
    fn step(&mut self, _: Option<&[u8]>) -> Result<EnhancedAuthStep, EnhancedAuthError> {
        Ok(EnhancedAuthStep::Continue { data: None })
    }
}
impl EnhancedAuthenticator for Endless {
    fn start(
        &self,
        _: EnhancedConnectInfo<'_>,
    ) -> Result<(Box<dyn EnhancedExchange>, EnhancedAuthStep), EnhancedAuthError> {
        Ok((Box::new(Endless), EnhancedAuthStep::Continue { data: None }))
    }
}
#[test]
fn round_and_pending_capacity_are_bounded() {
    let config = BrokerConfig {
        enhanced_auth: Some(Arc::new(Endless)),
        max_connections: 1,
        ..BrokerConfig::default()
    };
    let mut manager = Manager::default();
    let online = HashMap::new();
    manager
        .process(connect(1, None, u32::MAX), &config, current(&online))
        .unwrap();
    assert!(
        manager
            .process(connect(2, None, u32::MAX), &config, current(&online))
            .is_err()
    );
    for _ in 1..8 {
        assert!(
            manager
                .process(continuation(1, 0x18, None), &config, current(&online))
                .is_ok()
        );
    }
    assert!(
        manager
            .process(continuation(1, 0x18, None), &config, current(&online))
            .is_err()
    );
    assert!(manager.pending.is_empty());
}
#[derive(Debug)]
struct Slow;
impl EnhancedAuthenticator for Slow {
    fn start(
        &self,
        _: EnhancedConnectInfo<'_>,
    ) -> Result<(Box<dyn EnhancedExchange>, EnhancedAuthStep), EnhancedAuthError> {
        std::thread::sleep(Duration::from_millis(20));
        Ok((
            Box::new(Endless),
            EnhancedAuthStep::Complete {
                principal: Principal::anonymous(),
                data: None,
            },
        ))
    }
}
#[test]
fn deadline_is_checked_after_blocking_provider_returns() {
    let config = BrokerConfig {
        enhanced_auth: Some(Arc::new(Slow)),
        ..BrokerConfig::default()
    };
    let Request::Connect(mut request) = connect(1, None, u32::MAX) else {
        panic!()
    };
    request.deadline = Some(Instant::now() + Duration::from_millis(5));
    let mut manager = Manager::default();
    assert!(
        manager
            .process(Request::Connect(request), &config, |_: &str, _: u64| false)
            .is_err()
    );
    assert!(manager.pending.is_empty());
    assert!(manager.established.is_empty());
}
#[test]
fn mandatory_method_success_cap_is_preflighted_before_challenge() {
    let mut manager = Manager::default();
    assert!(matches!(
        manager
            .process(connect(1, None, 27), &config(), |_: &str, _: u64| false)
            .unwrap(),
        Processed::Reply(Reply::FrameRejected(ReasonCode::PACKET_TOO_LARGE))
    ));
    assert!(manager.pending.is_empty());
}

#[derive(Debug)]
struct Response {
    bytes: usize,
    identity: &'static str,
}
impl EnhancedExchange for Response {
    fn step(&mut self, _: Option<&[u8]>) -> Result<EnhancedAuthStep, EnhancedAuthError> {
        Ok(EnhancedAuthStep::Complete {
            principal: Principal {
                name: self.identity.into(),
            },
            data: Some(vec![1; self.bytes]),
        })
    }
}
impl EnhancedAuthenticator for Response {
    fn start(
        &self,
        _: EnhancedConnectInfo<'_>,
    ) -> Result<(Box<dyn EnhancedExchange>, EnhancedAuthStep), EnhancedAuthError> {
        Ok((
            Box::new(Response {
                bytes: self.bytes,
                identity: self.identity,
            }),
            EnhancedAuthStep::Continue { data: None },
        ))
    }
}
#[test]
fn exact_final_data_cap_and_identity_change_fail_closed() {
    let config = BrokerConfig {
        enhanced_auth: Some(Arc::new(Response {
            bytes: 10,
            identity: "anonymous",
        })),
        ..config()
    };
    let mut manager = Manager::default();
    let mut online = HashMap::new();
    manager
        .process(connect(1, None, 40), &config, current(&online))
        .unwrap();
    assert!(matches!(
        manager
            .process(continuation(1, 0x18, None), &config, current(&online))
            .unwrap(),
        Processed::Reply(Reply::FrameRejected(ReasonCode::PACKET_TOO_LARGE))
    ));
    assert!(manager.pending.is_empty());
    manager.established.insert(
        2,
        Established {
            generation: 2,
            client: "auth-client".into(),
            username: None,
            principal: Principal::anonymous(),
            method: "PLAIN".into(),
            peer_maximum: 15,
        },
    );
    online.insert(
        "auth-client".into(),
        Online {
            generation: 2,
            wake: watch::channel(0).0,
            cancel: watch::channel(false).0,
            close_reason: None,
        },
    );
    manager
        .process(continuation(2, 0x19, None), &config, current(&online))
        .unwrap();
    assert!(
        manager
            .process(continuation(2, 0x18, None), &config, current(&online))
            .is_err()
    );
    assert!(manager.reauth.is_empty());
    let config = BrokerConfig {
        enhanced_auth: Some(Arc::new(Response {
            bytes: 0,
            identity: "different",
        })),
        ..config
    };
    manager
        .process(continuation(2, 0x19, None), &config, current(&online))
        .unwrap();
    assert!(
        manager
            .process(continuation(2, 0x18, None), &config, current(&online))
            .is_err()
    );
    assert_eq!(
        manager.established.get(&2).unwrap().principal,
        Principal::anonymous()
    );
}
#[test]
fn stale_generation_and_missing_method_cannot_reauthenticate() {
    let mut manager = Manager::default();
    let config = config();
    let mut online = HashMap::new();
    manager.established.insert(
        1,
        Established {
            generation: 1,
            client: "auth-client".into(),
            username: None,
            principal: Principal::anonymous(),
            method: "PLAIN".into(),
            peer_maximum: usize::MAX,
        },
    );
    online.insert(
        "auth-client".into(),
        Online {
            generation: 2,
            wake: watch::channel(0).0,
            cancel: watch::channel(false).0,
            close_reason: None,
        },
    );
    assert!(
        manager
            .process(continuation(1, 0x19, None), &config, current(&online))
            .is_err()
    );
    online.get_mut("auth-client").unwrap().generation = 1;
    let Request::Authenticate {
        generation,
        mut frame,
        deadline,
    } = continuation(1, 0x19, None)
    else {
        panic!()
    };
    frame.properties.clear();
    assert!(
        manager
            .process(
                Request::Authenticate {
                    generation,
                    frame,
                    deadline
                },
                &config,
                current(&online)
            )
            .is_err()
    );
}
