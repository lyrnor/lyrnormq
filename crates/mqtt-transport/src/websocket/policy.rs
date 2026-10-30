//! Thin pre-codec header policy, not a second WebSocket codec. It never unmasks,
//! reassembles messages or validates control/fragment sequencing: tungstenite does.
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
#[derive(Debug)]
struct PolicyError {
    code: CloseCode,
}
impl std::fmt::Display for PolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WebSocket frame header policy violation")
    }
}
impl std::error::Error for PolicyError {}
fn invalid(code: CloseCode) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, PolicyError { code })
}
pub(super) fn close_code(error: &io::Error) -> Option<CloseCode> {
    error
        .get_ref()?
        .downcast_ref::<PolicyError>()
        .map(|e| e.code)
}
pub(crate) struct FramePolicy<S> {
    pub(super) inner: S,
    pub(super) enabled: bool,
    header: [u8; 14],
    have: usize,
    needed: usize,
    out: usize,
    len: usize,
    remaining: u64,
    budget: u8,
}
impl<S> FramePolicy<S> {
    pub(super) fn new(inner: S, enabled: bool) -> Self {
        Self {
            inner,
            enabled,
            header: [0; 14],
            have: 0,
            needed: 2,
            out: 0,
            len: 0,
            remaining: 0,
            budget: 32,
        }
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for FramePolicy<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if !self.enabled {
            return Pin::new(&mut self.inner).poll_read(cx, buf);
        }
        if self.budget == 0 {
            self.budget = 32;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.budget -= 1;
        loop {
            if self.out < self.len {
                let n = buf.remaining().min(self.len - self.out);
                buf.put_slice(&self.header[self.out..self.out + n]);
                self.out += n;
                return Poll::Ready(Ok(()));
            }
            if self.remaining > 0 {
                let n = buf
                    .remaining()
                    .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
                let mut payload = ReadBuf::new(buf.initialize_unfilled_to(n));
                let result = Pin::new(&mut self.inner).poll_read(cx, &mut payload);
                let n = payload.filled().len();
                buf.advance(n);
                self.remaining -= n as u64;
                return result;
            }
            if self.len > 0 {
                self.have = 0;
                self.needed = 2;
                self.len = 0;
                self.out = 0;
            }
            while self.have < self.needed {
                let mut header = [0; 14];
                let n = self.needed - self.have;
                let mut input = ReadBuf::new(&mut header[..n]);
                match Pin::new(&mut self.inner).poll_read(cx, &mut input) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(())) => {}
                }
                let n = input.filled().len();
                if n == 0 {
                    return Poll::Ready(Ok(()));
                }
                let at = self.have;
                self.header[at..at + n].copy_from_slice(input.filled());
                self.have += n;
                if self.have >= 2 {
                    self.needed =
                        2 + match self.header[1] & 127 {
                            126 => 2,
                            127 => 8,
                            _ => 0,
                        } + if self.header[1] & 128 != 0 { 4 } else { 0 };
                }
            }
            let length = match self.header[1] & 127 {
                126 => {
                    let n = u16::from_be_bytes([self.header[2], self.header[3]]);
                    if n < 126 {
                        return Poll::Ready(Err(invalid(CloseCode::Protocol)));
                    }
                    u64::from(n)
                }
                127 => {
                    let mut bytes = [0; 8];
                    bytes.copy_from_slice(&self.header[2..10]);
                    let n = u64::from_be_bytes(bytes);
                    if n <= u64::from(u16::MAX) || n >> 63 != 0 {
                        return Poll::Ready(Err(invalid(CloseCode::Protocol)));
                    }
                    n
                }
                n => u64::from(n),
            };
            // Let tungstenite classify RSV/masking errors first. Valid text is
            // rejected at its first header, without waiting for fragmented FIN.
            if self.header[0] & 15 == 1 && self.header[0] & 0x70 == 0 && self.header[1] & 128 != 0 {
                return Poll::Ready(Err(invalid(CloseCode::Unsupported)));
            }
            self.remaining = length;
            self.len = self.needed;
            self.out = 0;
        }
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for FramePolicy<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
