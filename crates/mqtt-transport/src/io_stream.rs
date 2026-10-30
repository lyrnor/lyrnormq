//! Static IO dispatch keeps the plaintext TcpStream owned split path.
use std::io;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};
use tokio_rustls::server::TlsStream;
pub(crate) trait IoStream: Send + 'static {
    const GRACEFUL_CLOSE: bool;
    type Reader: AsyncRead + Unpin + Send;
    type Writer: AsyncWrite + Unpin + Send + 'static;
    fn into_io(self) -> io::Result<(Self::Reader, Self::Writer)>;
}
impl IoStream for TcpStream {
    const GRACEFUL_CLOSE: bool = false;
    type Reader = OwnedReadHalf;
    type Writer = OwnedWriteHalf;
    fn into_io(self) -> io::Result<(Self::Reader, Self::Writer)> {
        self.set_nodelay(true)?;
        Ok(self.into_split())
    }
}
impl IoStream for TlsStream<TcpStream> {
    const GRACEFUL_CLOSE: bool = true;
    type Reader = ReadHalf<Self>;
    type Writer = WriteHalf<Self>;
    fn into_io(self) -> io::Result<(Self::Reader, Self::Writer)> {
        Ok(tokio::io::split(self))
    }
}
