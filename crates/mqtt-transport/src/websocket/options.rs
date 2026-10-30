use std::io;
/// WebSocket endpoint and aggregate binary-message byte bound.
#[derive(Clone, Debug)]
pub struct WebSocketOptions {
    pub path: String,
    pub max_message_size: usize,
}
impl Default for WebSocketOptions {
    fn default() -> Self {
        Self {
            path: "/mqtt".into(),
            max_message_size: 1024 * 1024,
        }
    }
}
impl WebSocketOptions {
    pub(crate) fn validate(&self) -> io::Result<()> {
        if !self.path.starts_with('/')
            || self.path.len() > 1024
            || self
                .path
                .chars()
                .any(|c| c.is_whitespace() || c.is_control() || matches!(c, '?' | '#'))
            || !(1..=16 * 1024 * 1024).contains(&self.max_message_size)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid WebSocket endpoint or message bound",
            ));
        }
        Ok(())
    }
}
