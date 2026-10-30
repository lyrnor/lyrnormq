use super::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

/// Models TLS accepting plaintext immediately but stalling encrypted flush.
struct FlushStall;
impl AsyncWrite for FlushStall {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn encrypted_flush_shares_the_frame_write_deadline() {
    let config = BrokerConfig {
        write_timeout: Duration::from_millis(10),
        ..BrokerConfig::default()
    };
    let result = timeout(
        Duration::from_secs(1),
        write_frame(
            &mut FlushStall,
            &Frame::v5(Packet::PingResp),
            &config,
            usize::MAX,
        ),
    )
    .await
    .unwrap();
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
}
struct ShutdownProbe {
    writes: Arc<AtomicUsize>,
    shutdowns: Arc<AtomicUsize>,
    stall: bool,
}
impl AsyncWrite for ShutdownProbe {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.shutdowns.fetch_add(1, Ordering::Relaxed);
        if self.stall {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
#[tokio::test]
async fn terminal_close_skips_queued_business_and_bounds_shutdown() {
    for stall in [false, true] {
        let writes = Arc::new(AtomicUsize::new(0));
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let config = BrokerConfig {
            write_timeout: Duration::from_millis(10),
            ..BrokerConfig::default()
        };
        let (output, receiver) = mpsc::channel(2);
        let (terminal, terminal_rx) = watch::channel(None);
        let (cancel, _) = watch::channel(false);
        let credits = Arc::new(Mutex::new(Credits::new(10)));
        output
            .send(outgoing(Frame::v5(Packet::PingResp), &credits).unwrap())
            .await
            .unwrap();
        let failed = Arc::new(AtomicBool::new(false));
        let writer = spawn_writer(
            ShutdownProbe {
                writes: writes.clone(),
                shutdowns: shutdowns.clone(),
                stall,
            },
            receiver,
            terminal_rx,
            credits,
            WriterLimits {
                config,
                close: Arc::new(AtomicBool::new(true)),
                peer_maximum: Arc::new(AtomicUsize::new(usize::MAX)),
                peer_alias_maximum: Arc::new(AtomicUsize::new(0)),
            },
            cancel,
            failed.clone(),
        );
        let result = timeout(Duration::from_secs(1), writer)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(writes.load(Ordering::Relaxed), 0);
        assert!(shutdowns.load(Ordering::Relaxed) > 0);
        assert_eq!(failed.load(Ordering::Acquire), stall);
        if stall {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
        } else {
            result.unwrap();
        }
        drop(terminal);
    }
}

#[tokio::test]
async fn management_counts_only_successfully_flushed_output_frames() {
    let management = crate::management::ManagementHandle::new();
    let config = BrokerConfig {
        management: Some(management.clone()),
        write_timeout: Duration::from_millis(10),
        ..BrokerConfig::default()
    };
    let broker = crate::durable::start(config.clone()).await.unwrap();
    let frame = Frame::v5(Packet::Publish {
        topic: "counters".into(),
        payload: b"real-frame".to_vec(),
        qos: 0,
        packet_id: None,
        dup: false,
        retain: false,
    });
    let size = encode_frame(&frame).unwrap().len() as u64;
    write_frame(&mut tokio::io::sink(), &frame, &config, usize::MAX)
        .await
        .unwrap();
    let before = management.overview().await.unwrap();
    assert_eq!(before.messages_out_total, 1);
    assert_eq!(before.bytes_out_total, size);
    assert!(
        write_frame(&mut FlushStall, &frame, &config, usize::MAX)
            .await
            .is_err()
    );
    let after = management.overview().await.unwrap();
    assert_eq!(after.messages_out_total, 1);
    assert_eq!(after.bytes_out_total, size);
    write_frame(
        &mut tokio::io::sink(),
        &Frame::v5(Packet::PingResp),
        &config,
        usize::MAX,
    )
    .await
    .unwrap();
    let after = management.overview().await.unwrap();
    assert_eq!(after.messages_out_total, 1);
    assert_eq!(after.bytes_out_total, size + 2);
    broker.shutdown().await.unwrap();
}
