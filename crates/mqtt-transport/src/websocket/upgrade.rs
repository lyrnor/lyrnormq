use super::{WebSocketOptions, adapter::WebSocketIo, policy::FramePolicy};
use base64::{Engine, engine::general_purpose::STANDARD};
use std::{
    collections::BTreeSet,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio_tungstenite::{
    accept_hdr_async_with_config,
    tungstenite::{
        handshake::server::{ErrorResponse, Request, Response},
        http::{StatusCode, Version, header},
        protocol::WebSocketConfig,
    },
};
const HEADER_LIMIT: usize = 16 * 1024;
/// Prefix bytes include any WS frames pipelined with the HTTP headers.
pub(crate) struct Prefixed<S> {
    stream: S,
    prefix: Vec<u8>,
    offset: usize,
}
impl<S: AsyncRead + Unpin> AsyncRead for Prefixed<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.offset < self.prefix.len() {
            let n = buf.remaining().min(self.prefix.len() - self.offset);
            buf.put_slice(&self.prefix[self.offset..self.offset + n]);
            self.offset += n;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for Prefixed<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
fn refuse(status: StatusCode) -> ErrorResponse {
    let mut response = ErrorResponse::new(None);
    *response.status_mut() = status;
    response
}
// tungstenite's Callback API fixes this unboxed response type.
#[allow(clippy::result_large_err)]
fn validate(
    request: &Request,
    mut response: Response,
    path: &str,
) -> Result<Response, ErrorResponse> {
    if request.uri().path() != path {
        return Err(refuse(StatusCode::NOT_FOUND));
    }
    if request.version() != Version::HTTP_11 {
        return Err(refuse(StatusCode::BAD_REQUEST));
    }
    let headers = request.headers();
    if headers.get(header::HOST).is_none_or(|v| v.is_empty()) {
        return Err(refuse(StatusCode::BAD_REQUEST));
    }
    let mut keys = headers.get_all(header::SEC_WEBSOCKET_KEY).iter();
    let Some(key) = keys.next() else {
        return Err(refuse(StatusCode::BAD_REQUEST));
    };
    if keys.next().is_some()
        || STANDARD
            .decode(key.as_bytes())
            .map_or(true, |v| v.len() != 16)
    {
        return Err(refuse(StatusCode::BAD_REQUEST));
    }
    let mut offered = BTreeSet::new();
    for value in headers.get_all(header::SEC_WEBSOCKET_PROTOCOL) {
        let Ok(value) = value.to_str() else {
            return Err(refuse(StatusCode::BAD_REQUEST));
        };
        for token in value.split(',').map(str::trim) {
            if token.is_empty()
                || !token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
                || !offered.insert(token)
            {
                return Err(refuse(StatusCode::BAD_REQUEST));
            }
        }
    }
    if !offered.contains("mqtt") {
        return Err(refuse(StatusCode::BAD_REQUEST));
    }
    response.headers_mut().insert(
        header::SEC_WEBSOCKET_PROTOCOL,
        header::HeaderValue::from_static("mqtt"),
    );
    Ok(response)
}
#[allow(clippy::result_large_err)] // Upstream handshake callback's mandatory error type.
pub(crate) async fn accept<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    options: &WebSocketOptions,
    write_timeout: Duration,
) -> io::Result<WebSocketIo<FramePolicy<Prefixed<S>>>> {
    let mut prefix = Vec::with_capacity(4096);
    let header_end = loop {
        if let Some(at) = prefix.windows(4).position(|w| w == b"\r\n\r\n") {
            break at + 4;
        }
        if prefix.len() == HEADER_LIMIT {
            let response=b"HTTP/1.1 431 Request Header Fields Too Large\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";
            let _ = tokio::time::timeout(write_timeout, async {
                stream.write_all(response).await?;
                stream.flush().await
            })
            .await;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WebSocket HTTP header exceeds16KiB",
            ));
        }
        let mut chunk = [0; 4096];
        let cap = chunk.len().min(HEADER_LIMIT - prefix.len());
        let n = stream.read(&mut chunk[..cap]).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete WebSocket upgrade",
            ));
        }
        prefix.extend_from_slice(&chunk[..n]);
    };
    // Tungstenite intentionally rejects bytes after an HTTP request. Feed it
    // only headers, then restore pipelined WS bytes to the underlying prefix.
    let pipeline = prefix.split_off(header_end);
    let config = WebSocketConfig::default()
        .read_buffer_size(4096)
        .write_buffer_size(0)
        .max_write_buffer_size(options.max_message_size + 1024)
        .max_message_size(Some(options.max_message_size))
        .max_frame_size(Some(options.max_message_size.max(125)))
        .accept_unmasked_frames(false);
    let mut socket = accept_hdr_async_with_config(
        FramePolicy::new(
            Prefixed {
                stream,
                prefix,
                offset: 0,
            },
            false,
        ),
        |request: &Request, response: Response| validate(request, response, &options.path),
        Some(config),
    )
    .await
    .map_err(io::Error::other)?;
    socket.get_mut().inner.prefix = pipeline;
    socket.get_mut().inner.offset = 0;
    socket.get_mut().enabled = true;
    Ok(WebSocketIo::new(
        socket,
        options.max_message_size,
        write_timeout,
    ))
}
