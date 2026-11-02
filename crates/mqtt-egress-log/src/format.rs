use crate::{
    QueueError,
    model::{MAX_GROUP_BYTES, MAX_META, Transaction},
};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

pub(crate) const HEADER: u64 = 24;
const MAGIC: &[u8; 4] = b"REQJ";
const MAX_FRAME: u64 = (MAX_META + MAX_GROUP_BYTES + 4) as u64;

const fn table() -> [u32; 256] {
    let mut values = [0; 256];
    let mut n = 0;
    while n < 256 {
        let mut c = n as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 == 1 {
                0xedb88320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        values[n] = c;
        n += 1;
    }
    values
}
const TABLE: [u32; 256] = table();
pub(crate) fn checksum(bytes: &[u8]) -> u32 {
    !update(!0, bytes)
}
fn update(mut crc: u32, bytes: &[u8]) -> u32 {
    for b in bytes {
        crc = TABLE[((crc ^ u32::from(*b)) & 255) as usize] ^ (crc >> 8);
    }
    crc
}

pub(crate) struct Frame {
    pub txn: Transaction,
    pub raw_offset: u64,
    pub raw_len: u64,
    pub end: u64,
}
pub(crate) enum Scan {
    End,
    Partial,
    Frame(Frame),
}

pub(crate) fn json(txn: &Transaction) -> Result<Vec<u8>, QueueError> {
    let bytes = serde_json::to_vec(txn).map_err(|_| QueueError::Io)?;
    if bytes.len() > MAX_META {
        return Err(QueueError::Full);
    }
    Ok(bytes)
}
pub(crate) fn encoded_len(json: &[u8], payloads: &[&[u8]]) -> Result<u64, QueueError> {
    let raw = payloads
        .iter()
        .try_fold(0usize, |n, p| n.checked_add(p.len()))
        .ok_or(QueueError::Full)?;
    if raw > MAX_GROUP_BYTES {
        return Err(QueueError::Full);
    }
    HEADER
        .checked_add(4)
        .and_then(|n| n.checked_add(json.len() as u64))
        .and_then(|n| n.checked_add(raw as u64))
        .ok_or(QueueError::Full)
}
pub(crate) fn write_frame(
    file: &mut File,
    json: &[u8],
    payloads: &[&[u8]],
) -> Result<(u64, u64), QueueError> {
    let len = encoded_len(json, payloads)?;
    let body = len - HEADER;
    let body32 = u32::try_from(body).map_err(|_| QueueError::Full)?;
    let json32 = u32::try_from(json.len())
        .map_err(|_| QueueError::Full)?
        .to_le_bytes();
    let mut crc = update(!0, &json32);
    crc = update(crc, json);
    for payload in payloads {
        crc = update(crc, payload);
    }
    let mut header = [0u8; HEADER as usize];
    header[..4].copy_from_slice(MAGIC);
    header[4] = 1;
    header[8..12].copy_from_slice(&body32.to_le_bytes());
    header[12..16].copy_from_slice(&(!crc).to_le_bytes());
    let header_crc = checksum(&header[..16]);
    header[16..20].copy_from_slice(&header_crc.to_le_bytes());
    let start = file.seek(SeekFrom::End(0))?;
    file.write_all(&header)?;
    file.write_all(&json32)?;
    file.write_all(json)?;
    for payload in payloads {
        file.write_all(payload)?;
    }
    Ok((start + HEADER + 4 + json.len() as u64, len))
}
pub(crate) fn scan(file: &mut File, offset: u64, max_bytes: u64) -> Result<Scan, QueueError> {
    let file_len = file.metadata()?.len();
    if offset == file_len {
        return Ok(Scan::End);
    }
    let remaining = file_len.checked_sub(offset).ok_or(QueueError::Corrupt)?;
    if remaining < HEADER {
        return Ok(Scan::Partial);
    }
    file.seek(SeekFrom::Start(offset))?;
    let mut header = [0u8; HEADER as usize];
    file.read_exact(&mut header)?;
    if &header[..4] != MAGIC || header[4] != 1 || header[5..8] != [0, 0, 0] {
        return Err(QueueError::Corrupt);
    }
    let expected_header =
        u32::from_le_bytes(header[16..20].try_into().map_err(|_| QueueError::Corrupt)?);
    if checksum(&header[..16]) != expected_header || header[20..24] != [0, 0, 0, 0] {
        return Err(QueueError::Corrupt);
    }
    let body = u64::from(u32::from_le_bytes(
        header[8..12].try_into().map_err(|_| QueueError::Corrupt)?,
    ));
    if !(4..=MAX_FRAME).contains(&body) || body.saturating_add(HEADER) > max_bytes {
        return Err(QueueError::Corrupt);
    }
    if remaining < HEADER + body {
        return Ok(Scan::Partial);
    }
    let mut prefix = [0u8; 4];
    file.read_exact(&mut prefix)?;
    let json_len = u32::from_le_bytes(prefix) as usize;
    if json_len > MAX_META || json_len as u64 > body - 4 {
        return Err(QueueError::Corrupt);
    }
    let mut metadata = vec![0u8; json_len];
    file.read_exact(&mut metadata)?;
    let mut crc = update(update(!0, &prefix), &metadata);
    let raw_len = body - 4 - json_len as u64;
    let mut left = raw_len;
    let mut scratch = [0u8; 65536];
    while left > 0 {
        let count = left.min(scratch.len() as u64) as usize;
        file.read_exact(&mut scratch[..count])?;
        crc = update(crc, &scratch[..count]);
        left -= count as u64;
    }
    let expected = u32::from_le_bytes(header[12..16].try_into().map_err(|_| QueueError::Corrupt)?);
    if !crc != expected {
        return Err(QueueError::Corrupt);
    }
    let txn = serde_json::from_slice(&metadata).map_err(|_| QueueError::Corrupt)?;
    Ok(Scan::Frame(Frame {
        txn,
        raw_offset: offset + HEADER + 4 + json_len as u64,
        raw_len,
        end: offset + HEADER + body,
    }))
}
