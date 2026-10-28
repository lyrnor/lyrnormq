//! Redacted, owned binary CONNECT password.

use std::fmt;
use zeroize::Zeroizing;

/// Opaque binary password whose owned bytes are cleared on drop.
/// Debug never displays its value. Encoding returns ordinary wire bytes; this
/// wrapper does not promise that all network or process buffers are wiped.
#[derive(Clone, PartialEq, Eq)]
pub struct Password {
    bytes: Zeroizing<Vec<u8>>,
}
impl Password {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes: Zeroizing::new(bytes),
        }
    }
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }
}
impl fmt::Debug for Password {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Password([REDACTED])")
    }
}
