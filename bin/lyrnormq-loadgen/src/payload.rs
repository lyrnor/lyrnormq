use crate::LoadError;
pub const HEADER_BYTES: usize = 48;
const MAGIC: &[u8; 8] = b"RMQTB001";
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub run_id: u64,
    pub pair: u32,
    pub measured: bool,
    pub sequence: u64,
    pub scheduled_ns: u64,
    pub send_started_ns: u64,
}
pub fn encode(size: usize, h: Header) -> Result<Vec<u8>, LoadError> {
    if size < HEADER_BYTES {
        return Err(LoadError::Config("payload must be at least48 bytes".into()));
    }
    let mut b = vec![(h.sequence as u8) ^ (h.pair as u8) ^ 0xa5; size];
    b[..8].copy_from_slice(MAGIC);
    b[8..16].copy_from_slice(&h.run_id.to_be_bytes());
    b[16..20].copy_from_slice(&h.pair.to_be_bytes());
    b[20..24].copy_from_slice(&u32::from(h.measured).to_be_bytes());
    b[24..32].copy_from_slice(&h.sequence.to_be_bytes());
    b[32..40].copy_from_slice(&h.scheduled_ns.to_be_bytes());
    b[40..48].copy_from_slice(&h.send_started_ns.to_be_bytes());
    Ok(b)
}
pub fn decode(b: &[u8]) -> Result<Header, LoadError> {
    if b.len() < HEADER_BYTES || &b[..8] != MAGIC {
        return Err(LoadError::InvalidData("payload header".into()));
    }
    fn arr<const N: usize>(b: &[u8]) -> Result<[u8; N], LoadError> {
        b.try_into()
            .map_err(|_| LoadError::InvalidData("payload field".into()))
    }
    let flags = u32::from_be_bytes(arr(&b[20..24])?);
    if flags > 1 {
        return Err(LoadError::InvalidData("payload flags".into()));
    }
    let h = Header {
        run_id: u64::from_be_bytes(arr(&b[8..16])?),
        pair: u32::from_be_bytes(arr(&b[16..20])?),
        measured: flags == 1,
        sequence: u64::from_be_bytes(arr(&b[24..32])?),
        scheduled_ns: u64::from_be_bytes(arr(&b[32..40])?),
        send_started_ns: u64::from_be_bytes(arr(&b[40..48])?),
    };
    if b[48..]
        .iter()
        .any(|v| *v != ((h.sequence as u8) ^ (h.pair as u8) ^ 0xa5))
    {
        return Err(LoadError::InvalidData("payload padding".into()));
    }
    Ok(h)
}
#[cfg(test)]
mod tests;
