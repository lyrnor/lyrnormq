//! Direct Stream/Sink adapter: MQTT flush reaches the underlying WS/TLS socket.
use crate::io_stream::IoStream;
use futures_util::{Sink, Stream};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf, ReadHalf, WriteHalf},
    time::{Sleep, sleep},
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        self, Message,
        protocol::{CloseFrame, frame::coding::CloseCode},
    },
};
#[derive(Debug)]
struct FramingFailure(&'static str);
impl std::fmt::Display for FramingFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for FramingFailure {}
fn failure(kind: io::ErrorKind, message: &'static str) -> io::Error {
    io::Error::new(kind, FramingFailure(message))
}
pub(crate) fn is_framing_error(error: &io::Error) -> bool {
    error.get_ref().is_some_and(|e| e.is::<FramingFailure>())
}
fn websocket_error(_: tungstenite::Error) -> io::Error {
    failure(io::ErrorKind::InvalidData, "WebSocket transport failure")
}
fn closed(result: Result<(), tungstenite::Error>) -> io::Result<()> {
    match result {
        Ok(()) | Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
            Ok(())
        }
        Err(error) => Err(websocket_error(error)),
    }
}
pub(crate) struct WebSocketIo<S> {
    socket: WebSocketStream<S>,
    binary: tungstenite::Bytes,
    offset: usize,
    max_message: usize,
    write_timeout: Duration,
    control_due: Option<Pin<Box<Sleep>>>,
    send_close: Option<CloseCode>,
    close_sent: bool,
    pending_error: Option<io::Error>,
    eof: bool,
    reader: Option<Waker>,
    peer_closed: bool,
    shutdown_due: Option<Pin<Box<Sleep>>>,
    shutdown_complete: bool,
    full_close: Arc<AtomicBool>,
}
impl<S: AsyncRead + AsyncWrite + Unpin> WebSocketIo<S> {
    pub(super) fn new(
        socket: WebSocketStream<S>,
        max_message: usize,
        write_timeout: Duration,
    ) -> Self {
        Self {
            socket,
            binary: tungstenite::Bytes::new(),
            offset: 0,
            max_message,
            write_timeout,
            control_due: None,
            send_close: None,
            close_sent: false,
            pending_error: None,
            eof: false,
            reader: None,
            peer_closed: false,
            shutdown_due: None,
            shutdown_complete: false,
            full_close: Arc::new(AtomicBool::new(true)),
        }
    }
    fn control(&mut self) {
        if self.control_due.is_none() {
            self.control_due = Some(Box::pin(sleep(self.write_timeout)));
        }
    }
    fn close(&mut self, code: CloseCode) {
        if !self.close_sent {
            self.send_close = Some(code);
        }
        self.control();
    }
    fn reject(&mut self, code: CloseCode, message: &'static str) {
        self.pending_error = Some(failure(io::ErrorKind::InvalidData, message));
        self.close(code);
    }
    fn poll_control(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let Some(deadline) = &mut self.control_due else {
            return Poll::Ready(Ok(()));
        };
        if deadline.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(failure(
                io::ErrorKind::TimedOut,
                "WebSocket control write timed out",
            )));
        }
        if let Some(code) = self.send_close {
            match Pin::new(&mut self.socket).poll_ready(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(closed(Err(e))),
                Poll::Ready(Ok(())) => {}
            }
            if let Err(error) =
                Pin::new(&mut self.socket).start_send(Message::Close(Some(CloseFrame {
                    code,
                    reason: "".into(),
                })))
            {
                return Poll::Ready(closed(Err(error)));
            }
            self.send_close = None;
            self.close_sent = true;
        }
        match Pin::new(&mut self.socket).poll_flush(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.control_due = None;
                if let Some(reader) = &self.reader {
                    reader.wake_by_ref();
                }
                Poll::Ready(closed(result))
            }
        }
    }
}
impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for WebSocketIo<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        self.reader = Some(cx.waker().clone());
        for _ in 0..32 {
            match self.poll_control(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {}
            }
            if let Some(error) = self.pending_error.take() {
                return Poll::Ready(Err(error));
            }
            if self.offset < self.binary.len() {
                let n = buf.remaining().min(self.binary.len() - self.offset);
                buf.put_slice(&self.binary[self.offset..self.offset + n]);
                self.offset += n;
                if self.offset == self.binary.len() {
                    self.binary = tungstenite::Bytes::new();
                    self.offset = 0;
                }
                return Poll::Ready(Ok(()));
            }
            if self.eof {
                return Poll::Ready(Ok(()));
            }
            match Pin::new(&mut self.socket).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.eof = true;
                }
                Poll::Ready(Some(Ok(Message::Binary(bytes)))) => {
                    self.binary = bytes;
                    self.offset = 0;
                }
                Poll::Ready(Some(Ok(Message::Ping(_)))) => {
                    self.control();
                }
                Poll::Ready(Some(Ok(Message::Pong(_)))) => {}
                Poll::Ready(Some(Ok(Message::Close(_)))) => {
                    self.peer_closed = true;
                    self.close_sent = true;
                    self.eof = true;
                    self.control();
                }
                Poll::Ready(Some(Ok(Message::Text(_)))) => {
                    self.reject(
                        CloseCode::Unsupported,
                        "MQTT requires binary WebSocket messages",
                    );
                }
                Poll::Ready(Some(Ok(Message::Frame(_)))) => {
                    self.reject(CloseCode::Protocol, "unexpected raw WebSocket frame");
                }
                Poll::Ready(Some(Err(error))) => {
                    let code = match error {
                        tungstenite::Error::Capacity(_) => CloseCode::Size,
                        tungstenite::Error::Io(ref error) => {
                            super::policy::close_code(error).unwrap_or(CloseCode::Protocol)
                        }
                        _ => CloseCode::Protocol,
                    };
                    self.reject(code, "invalid WebSocket framing");
                }
            }
        }
        // Control/empty floods yield to MQTT Keep Alive, shutdown and other tasks.
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for WebSocketIo<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        match self.poll_control(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }
        if self.close_sent || self.pending_error.is_some() || self.eof {
            return Poll::Ready(Err(failure(
                io::ErrorKind::BrokenPipe,
                "WebSocket is closing",
            )));
        }
        match Pin::new(&mut self.socket).poll_ready(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(websocket_error(e))),
            Poll::Ready(Ok(())) => {}
        }
        let n = buf.len().min(self.max_message);
        match Pin::new(&mut self.socket).start_send(Message::Binary(buf[..n].to_vec().into())) {
            Ok(()) => Poll::Ready(Ok(n)),
            Err(e) => Poll::Ready(Err(websocket_error(e))),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_control(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }
        match Pin::new(&mut self.socket).poll_flush(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => Poll::Ready(closed(result)),
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.shutdown_complete {
            return Poll::Ready(Ok(()));
        }
        if self.shutdown_due.is_none() {
            self.shutdown_due = Some(Box::pin(sleep(self.write_timeout)));
        }
        if self
            .shutdown_due
            .as_mut()
            .is_some_and(|due| due.as_mut().poll(cx).is_ready())
        {
            return Poll::Ready(Err(failure(
                io::ErrorKind::TimedOut,
                "WebSocket closing handshake timed out",
            )));
        }
        if !self.close_sent {
            self.close(CloseCode::Normal);
        }
        match self.poll_control(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }
        // Sending Close is only the first half of normal WS shutdown. Retain
        // ownership until its peer acknowledgement arrives; discard business
        // input while closing rather than handing it back to the MQTT loop.
        for _ in 0..32 {
            if !self.full_close.load(Ordering::Acquire) || self.peer_closed {
                break;
            }
            match Pin::new(&mut self.socket).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.peer_closed = true;
                }
                Poll::Ready(Some(Ok(Message::Close(_)))) => {
                    self.peer_closed = true;
                    self.eof = true;
                    self.control();
                }
                Poll::Ready(Some(Ok(_))) => {}
                Poll::Ready(Some(Err(
                    tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed,
                ))) => {
                    self.peer_closed = true;
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(websocket_error(error))),
            }
        }
        if self.full_close.load(Ordering::Acquire) && !self.peer_closed {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        match self.poll_control(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }
        match Pin::new(self.socket.get_mut()).poll_shutdown(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }
        if !self.full_close.load(Ordering::Acquire) {
            // Ungraceful transport termination must not postpone MQTT cleanup
            // (notably Will) waiting for an uncooperative peer's notification.
            self.shutdown_complete = true;
            return Poll::Ready(Ok(()));
        }
        // SHUT_WR/TLS close_notify still leaves the read side open. Consume
        // reciprocal TLS notification or TCP FIN before drop, avoiding unread
        // peer Close/alert bytes turning an otherwise clean close into TCP RST.
        // This drain shares the ORIGINAL shutdown deadline and has no task.
        for _ in 0..32 {
            let mut scratch = [0; 1024];
            let mut input = ReadBuf::new(&mut scratch);
            match Pin::new(self.socket.get_mut()).poll_read(cx, &mut input) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) if input.filled().is_empty() => {
                    self.shutdown_complete = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Ok(())) => {}
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> IoStream for WebSocketIo<S> {
    const GRACEFUL_CLOSE: bool = true;
    type Reader = ReadHalf<Self>;
    type Writer = WriteHalf<Self>;
    fn full_close_policy(&self) -> Option<Arc<AtomicBool>> {
        Some(self.full_close.clone())
    }
    fn into_io(self) -> io::Result<(Self::Reader, Self::Writer)> {
        Ok(tokio::io::split(self))
    }
}
