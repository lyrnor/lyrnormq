//! Direction- and state-sensitive acceptance across actual sockets and worker generations.
use super::*;

fn assert_server_role(frame: &Frame) {
    assert!(matches!(
        frame.packet,
        Packet::ConnAck { .. }
            | Packet::Publish { .. }
            | Packet::PubAck { .. }
            | Packet::PubRec { .. }
            | Packet::PubRel { .. }
            | Packet::PubComp { .. }
            | Packet::SubAck { .. }
            | Packet::UnsubAck { .. }
            | Packet::PingResp
            | Packet::Disconnect
            | Packet::Auth
    ));
    if matches!(frame.packet, Packet::Auth) {
        assert!(
            matches!(frame.reason_code.value(), 0 | 0x18),
            "server AUTH cannot initiate reauthentication"
        );
    }
    if matches!(frame.packet, Packet::Disconnect) {
        assert_ne!(
            frame.reason_code.value(),
            4,
            "Will-request reason is client-only"
        );
        assert!(
            !frame
                .properties
                .iter()
                .any(|p| matches!(p, Property::SessionExpiryInterval(_))),
            "server DISCONNECT must not contain SessionExpiry"
        );
    }
}
async fn checked(stream: &mut TcpStream) -> Frame {
    let frame = read(stream).await;
    assert_server_role(&frame);
    frame
}
fn server_only() -> Vec<Frame> {
    let mut unsuback = Frame::v5(Packet::UnsubAck { packet_id: 7 });
    unsuback.reason_codes.push(ReasonCode::SUCCESS);
    vec![
        Frame::v5(Packet::ConnAck {
            session_present: false,
            code: 0,
        }),
        Frame::v5(Packet::SubAck {
            packet_id: 7,
            codes: vec![0],
        }),
        unsuback,
        Frame::v5(Packet::PingResp),
    ]
}

#[tokio::test]
async fn server_only_types_are_rejected_in_all_four_receive_states() {
    let (address, shutdown, task) = broker(configured()).await;
    for state in 0..4 {
        for (kind, wrong) in server_only().into_iter().enumerate() {
            let mut client = TcpStream::connect(address).await.unwrap();
            if state == 1 {
                write(&mut client, connect(&format!("pending-{kind}"), None)).await;
                assert_eq!(checked(&mut client).await.packet, Packet::Auth);
            } else if state >= 2 {
                write(
                    &mut client,
                    connect(
                        &format!("connected-{state}-{kind}"),
                        Some(b"\0alice\0password"),
                    ),
                )
                .await;
                assert!(matches!(
                    checked(&mut client).await.packet,
                    Packet::ConnAck { code: 0, .. }
                ));
                if state == 3 {
                    write(&mut client, auth(0x19, None)).await;
                    assert_eq!(checked(&mut client).await.reason_code.value(), 0x18);
                }
            }
            let mut wire = mqtt_protocol::encode_frame(&wrong).unwrap();
            wire.extend(mqtt_protocol::encode_frame(&Frame::v5(Packet::PingReq)).unwrap());
            client.write_all(&wire).await.unwrap();
            if state != 0 {
                let failure = checked(&mut client).await;
                if state == 1 {
                    assert!(matches!(
                        failure.packet,
                        Packet::ConnAck {
                            session_present: false,
                            code: 0x82
                        }
                    ));
                } else {
                    assert_eq!(failure.packet, Packet::Disconnect);
                    assert_eq!(failure.reason_code, ReasonCode::PROTOCOL_ERROR);
                }
            }
            closed(&mut client).await;
        }
    }
    stop(shutdown, task).await;
}

#[tokio::test]
async fn request_problem_zero_does_not_suppress_application_properties_or_echo_control_data() {
    let (address, shutdown, task) = broker(configured()).await;
    let mut publisher = TcpStream::connect(address).await.unwrap();
    let mut request = connect("direction-publisher", Some(b"\0alice\0password"));
    request.properties.extend([
        Property::RequestProblemInformation(0),
        Property::RequestResponseInformation(0),
        Property::UserProperty("connect-marker".into(), "never-echo-control".into()),
    ]);
    write(&mut publisher, request).await;
    let connack = checked(&mut publisher).await;
    assert!(matches!(connack.packet, Packet::ConnAck { code: 0, .. }));
    assert!(
        !connack
            .properties
            .iter()
            .any(|p| matches!(p, Property::ResponseInformation(_)))
    );
    let mut subscriber = TcpStream::connect(address).await.unwrap();
    write(
        &mut subscriber,
        connect("direction-subscriber", Some(b"\0alice\0password")),
    )
    .await;
    checked(&mut subscriber).await;
    write(
        &mut subscriber,
        Frame::v5(Packet::Subscribe {
            packet_id: 31,
            filters: vec![("direction/a".into(), 2)],
        }),
    )
    .await;
    assert_eq!(
        checked(&mut subscriber).await.packet,
        Packet::SubAck {
            packet_id: 31,
            codes: vec![2]
        }
    );
    let mut publication = Frame::v5(Packet::Publish {
        topic: "direction/a".into(),
        payload: b"application".to_vec(),
        qos: 2,
        packet_id: Some(41),
        retain: false,
        dup: false,
    });
    publication.properties.push(Property::UserProperty(
        "application".into(),
        "must-forward".into(),
    ));
    write(&mut publisher, publication).await;
    let pubrec = checked(&mut publisher).await;
    assert_eq!(pubrec.packet, Packet::PubRec { packet_id: 41 });
    assert!(
        !pubrec
            .properties
            .iter()
            .any(|p| matches!(p, Property::ReasonString(_) | Property::UserProperty(_, _)))
    );
    let mut release = Frame::v5(Packet::PubRel { packet_id: 41 });
    release.properties.push(Property::ReasonString(
        "diagnostic, not application data".into(),
    ));
    write(&mut publisher, release).await;
    let complete = checked(&mut publisher).await;
    assert_eq!(complete.packet, Packet::PubComp { packet_id: 41 });
    assert!(complete.properties.is_empty());
    let forwarded = checked(&mut subscriber).await;
    assert!(matches!(forwarded.packet, Packet::Publish { qos: 2, .. }));
    assert_eq!(
        forwarded.properties,
        vec![Property::UserProperty(
            "application".into(),
            "must-forward".into()
        )]
    );
    let Packet::Publish {
        packet_id: Some(id),
        ..
    } = forwarded.packet
    else {
        panic!("missing outgoing ID")
    };
    write(&mut subscriber, Frame::v5(Packet::PubRec { packet_id: id })).await;
    assert_eq!(
        checked(&mut subscriber).await.packet,
        Packet::PubRel { packet_id: id }
    );
    write(
        &mut subscriber,
        Frame::v5(Packet::PubComp { packet_id: id }),
    )
    .await;
    write(&mut subscriber, auth(0x19, None)).await;
    checked(&mut subscriber).await;
    write(&mut subscriber, auth(0, None)).await;
    let terminal = checked(&mut subscriber).await;
    assert_eq!(terminal.packet, Packet::Disconnect);
    assert_eq!(terminal.reason_code, ReasonCode::PROTOCOL_ERROR);
    closed(&mut subscriber).await;
    write(&mut publisher, Frame::v5(Packet::Disconnect)).await;
    closed(&mut publisher).await;
    stop(shutdown, task).await;
}

async fn actor_connect(
    worker: &Worker,
    client: &str,
    generation: u64,
    clean: bool,
) -> watch::Receiver<bool> {
    let mut frame = Frame::v5(Packet::Connect {
        client_id: client.into(),
        clean_session: clean,
        keep_alive: 0,
        will: None,
        username: None,
        password: None,
    });
    frame.properties.extend([
        Property::SessionExpiryInterval(60),
        Property::ReceiveMaximum(1),
    ]);
    let (wake, _) = watch::channel(0);
    let (cancel, receiver) = watch::channel(false);
    let (close_reason, _) = watch::channel(None);
    let reply = worker
        .request(Operation::ConnectFrame {
            frame,
            generation,
            wake,
            cancel,
            close_reason,
        })
        .await
        .unwrap();
    assert!(matches!(reply, Reply::Connected(_)));
    receiver
}
async fn actor_handle(
    worker: &Worker,
    client: &str,
    generation: u64,
    frame: Frame,
) -> io::Result<Vec<Frame>> {
    match worker
        .request(Operation::HandleFrameAt {
            client: client.into(),
            generation,
            frame,
            received_at_millis: SystemClock.now_millis(),
        })
        .await?
    {
        Reply::Frames(frames) => Ok(frames),
        _ => panic!("wrong actor reply"),
    }
}
async fn actor_poll(worker: &Worker, generation: u64) -> Option<Delivery> {
    match worker
        .request(Operation::PollDelivery {
            client: "receiver".into(),
            generation,
        })
        .await
        .unwrap()
    {
        Reply::DeliveryPolled(delivery) => delivery.map(|d| *d),
        _ => panic!("wrong actor poll"),
    }
}

#[tokio::test]
async fn stale_generation_ack_and_lease_cannot_release_new_owner_qos2_window() {
    let (worker, task) = super::super::tests::worker(BrokerConfig::default()).await;
    let old_cancel = actor_connect(&worker, "receiver", 1, true).await;
    actor_handle(
        &worker,
        "receiver",
        1,
        Frame::v5(Packet::Subscribe {
            packet_id: 1,
            filters: vec![("trace/a".into(), 2)],
        }),
    )
    .await
    .unwrap();
    actor_connect(&worker, "sender", 10, true).await;
    let mut source = Frame::v5(Packet::Publish {
        topic: "trace/a".into(),
        payload: b"first".to_vec(),
        qos: 2,
        packet_id: Some(50),
        retain: false,
        dup: false,
    });
    assert_eq!(
        actor_handle(&worker, "sender", 10, source.clone())
            .await
            .unwrap()[0]
            .packet,
        Packet::PubRec { packet_id: 50 }
    );
    actor_handle(
        &worker,
        "sender",
        10,
        Frame::v5(Packet::PubRel { packet_id: 50 }),
    )
    .await
    .unwrap();
    let old_delivery = actor_poll(&worker, 1).await.unwrap();
    let Packet::Publish {
        packet_id: Some(id),
        ..
    } = old_delivery.frame.packet
    else {
        panic!("missing ID")
    };
    assert!(matches!(
        worker
            .request(Operation::BeginDelivery {
                client: "receiver".into(),
                generation: 1,
                delivery: old_delivery.clone()
            })
            .await
            .unwrap(),
        Reply::FramePolled(Some(_))
    ));
    if let Packet::Publish {
        qos,
        packet_id,
        payload,
        ..
    } = &mut source.packet
    {
        *qos = 1;
        *packet_id = Some(51);
        *payload = b"second".to_vec();
    }
    actor_handle(&worker, "sender", 10, source).await.unwrap();
    actor_connect(&worker, "receiver", 2, false).await;
    assert!(*old_cancel.borrow());
    assert!(
        actor_handle(
            &worker,
            "receiver",
            1,
            Frame::v5(Packet::PubRec { packet_id: id })
        )
        .await
        .is_err()
    );
    assert!(
        worker
            .request(Operation::BeginDelivery {
                client: "receiver".into(),
                generation: 1,
                delivery: old_delivery
            })
            .await
            .is_err()
    );
    worker
        .request(Operation::Disconnect {
            client: "receiver".into(),
            generation: 1,
            graceful: true,
        })
        .await
        .unwrap();
    worker
        .request(Operation::CancelAuthentication { generation: 1 })
        .await
        .unwrap();
    let replay = actor_poll(&worker, 2).await.unwrap();
    assert!(
        matches!(replay.frame.packet, Packet::Publish { packet_id: Some(actual), dup: true, ref payload, .. } if actual == id && payload == b"first")
    );
    assert!(matches!(
        worker
            .request(Operation::BeginDelivery {
                client: "receiver".into(),
                generation: 2,
                delivery: replay
            })
            .await
            .unwrap(),
        Reply::FramePolled(Some(_))
    ));
    assert_eq!(
        actor_handle(
            &worker,
            "receiver",
            2,
            Frame::v5(Packet::PubRec { packet_id: id })
        )
        .await
        .unwrap()[0]
            .packet,
        Packet::PubRel { packet_id: id }
    );
    assert!(
        actor_handle(
            &worker,
            "receiver",
            1,
            Frame::v5(Packet::PubComp { packet_id: id })
        )
        .await
        .is_err()
    );
    assert!(
        actor_poll(&worker, 2).await.is_none(),
        "ReceiveMaximum remains held through PUBCOMP"
    );
    actor_handle(
        &worker,
        "receiver",
        2,
        Frame::v5(Packet::PubComp { packet_id: id }),
    )
    .await
    .unwrap();
    let next = actor_poll(&worker, 2).await.unwrap();
    assert!(
        matches!(next.frame.packet, Packet::Publish { qos: 1, ref payload, .. } if payload == b"second")
    );
    drop(worker);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn puback_and_pubrec_use_legal_server_success_without_claiming_client_role_validation() {
    let (address, shutdown, task) = broker(configured()).await;
    let mut publisher = TcpStream::connect(address).await.unwrap();
    write(
        &mut publisher,
        connect("ack-role", Some(b"\0alice\0password")),
    )
    .await;
    assert!(matches!(
        checked(&mut publisher).await.packet,
        Packet::ConnAck { code: 0, .. }
    ));
    for qos in [1, 2] {
        let id = 100 + u16::from(qos);
        write(
            &mut publisher,
            Frame::v5(Packet::Publish {
                topic: "no-subscribers/a".into(),
                payload: b"data".to_vec(),
                qos,
                packet_id: Some(id),
                dup: false,
                retain: false,
            }),
        )
        .await;
        let ack = checked(&mut publisher).await;
        assert_eq!(
            ack.packet,
            if qos == 1 {
                Packet::PubAck { packet_id: id }
            } else {
                Packet::PubRec { packet_id: id }
            }
        );
        // Table3-4/3-5 permit the server to report0x10 for no subscribers, but
        // success0 remains valid. The broker does not manufacture a negative ACK.
        assert_eq!(ack.reason_code, ReasonCode::SUCCESS);
        if qos == 2 {
            write(&mut publisher, Frame::v5(Packet::PubRel { packet_id: id })).await;
            let complete = checked(&mut publisher).await;
            assert_eq!(complete.packet, Packet::PubComp { packet_id: id });
            assert_eq!(complete.reason_code, ReasonCode::SUCCESS);
        }
    }
    write(&mut publisher, Frame::v5(Packet::Disconnect)).await;
    closed(&mut publisher).await;
    stop(shutdown, task).await;
}
