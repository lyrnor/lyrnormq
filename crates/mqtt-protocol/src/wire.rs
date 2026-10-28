//! Bounded common MQTT5 wire primitives, without recursive parsing.
use crate::{
    ProtocolError,
    codec::{validate_id, validate_string},
};
pub(super) const MAX_REMAINING: usize = 268_435_455;
pub(super) fn malformed(s: &str) -> ProtocolError {
    ProtocolError::Malformed(s.into())
}
pub(super) fn violation(s: &str) -> ProtocolError {
    ProtocolError::ProtocolViolation(s.into())
}

pub(super) struct Reader<'a> {
    pub bytes: &'a [u8],
    pub pos: usize,
}
impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], ProtocolError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| malformed("length overflow"))?;
        let result = self
            .bytes
            .get(self.pos..end)
            .ok_or_else(|| malformed("truncated field"))?;
        self.pos = end;
        Ok(result)
    }
    pub fn byte(&mut self) -> Result<u8, ProtocolError> {
        Ok(self.take(1)?[0])
    }
    pub fn u16(&mut self) -> Result<u16, ProtocolError> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }
    pub fn u32(&mut self) -> Result<u32, ProtocolError> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    pub fn id(&mut self) -> Result<u16, ProtocolError> {
        let id = self.u16()?;
        validate_id(id)?;
        Ok(id)
    }
    pub fn string(&mut self) -> Result<String, ProtocolError> {
        let n = usize::from(self.u16()?);
        let value = std::str::from_utf8(self.take(n)?).map_err(|_| malformed("invalid UTF8"))?;
        validate_string(value)?;
        Ok(value.to_owned())
    }
    pub fn binary(&mut self) -> Result<Vec<u8>, ProtocolError> {
        let n = usize::from(self.u16()?);
        Ok(self.take(n)?.to_vec())
    }
    pub fn varint(&mut self) -> Result<u32, ProtocolError> {
        let mut result = 0u32;
        for index in 0..4 {
            let b = self.byte()?;
            result |= u32::from(b & 127) << (7 * index);
            if b & 128 == 0 {
                if index > 0 && b == 0 {
                    return Err(malformed("noncanonical variable integer"));
                }
                return Ok(result);
            }
        }
        Err(malformed("variable integer exceeds four bytes"))
    }
    pub fn done(&self) -> bool {
        self.pos == self.bytes.len()
    }
    pub fn rest(&mut self) -> Result<Vec<u8>, ProtocolError> {
        Ok(self.take(self.bytes.len() - self.pos)?.to_vec())
    }
}
pub(super) fn append(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ProtocolError> {
    if bytes.len() > MAX_REMAINING.saturating_sub(out.len()) {
        return Err(ProtocolError::PacketTooLarge);
    }
    out.extend_from_slice(bytes);
    Ok(())
}
pub(super) fn string(out: &mut Vec<u8>, s: &str) -> Result<(), ProtocolError> {
    validate_string(s)?;
    let n = u16::try_from(s.len()).map_err(|_| malformed("string too long"))?;
    append(out, &n.to_be_bytes())?;
    append(out, s.as_bytes())
}
pub(super) fn binary(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ProtocolError> {
    let n = u16::try_from(bytes.len()).map_err(|_| malformed("binary data too long"))?;
    append(out, &n.to_be_bytes())?;
    append(out, bytes)
}
pub(super) fn varint(out: &mut Vec<u8>, mut n: usize) -> Result<(), ProtocolError> {
    if n > MAX_REMAINING {
        return Err(ProtocolError::PacketTooLarge);
    }
    loop {
        let digit = (n % 128) as u8;
        n /= 128;
        out.push(digit | if n > 0 { 128 } else { 0 });
        if n == 0 {
            return Ok(());
        }
    }
}
