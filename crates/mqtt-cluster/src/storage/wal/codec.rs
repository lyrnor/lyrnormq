//! Length-bounded codecs: corrupt lengths never cause unbounded allocations.
use super::*;
use serde::{Serialize, de::DeserializeOwned};
use std::io::{Read, Write};
pub(super) fn write_blob(mut writer: impl Write, bytes: &[u8], max: usize) -> io::Result<usize> {
    if bytes.len() > max {
        return Err(invalid("WAL codec size limit"));
    }
    let length = u32::try_from(bytes.len()).map_err(invalid)?;
    writer.write_all(&length.to_be_bytes())?;
    writer.write_all(bytes)?;
    Ok(bytes.len() + 4)
}
pub(super) fn read_blob(mut reader: impl Read, max: usize) -> io::Result<Vec<u8>> {
    let mut size = [0; 4];
    reader.read_exact(&mut size)?;
    let size = u32::from_be_bytes(size) as usize;
    if size > max {
        return Err(invalid("WAL codec size limit"));
    }
    let mut bytes = vec![0; size];
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
}
fn write_json<T: Serialize>(writer: impl Write, value: &T, max: usize) -> io::Result<usize> {
    write_blob(writer, &serde_json::to_vec(value).map_err(invalid)?, max)
}
fn read_json<T: DeserializeOwned>(reader: impl Read, max: usize) -> io::Result<T> {
    serde_json::from_slice(&read_blob(reader, max)?).map_err(invalid)
}
macro_rules! json_codec {
    ($name:ty,$max:expr) => {
        impl raft_log::codeq::Encode for $name {
            fn encode<W: Write>(&self, writer: W) -> io::Result<usize> {
                write_json(writer, self, $max)
            }
        }
        impl raft_log::codeq::Decode for $name {
            fn decode<R: Read>(reader: R) -> io::Result<Self> {
                read_json(reader, $max)
            }
        }
    };
}
json_codec!(LogKey, 1024);
json_codec!(VoteKey, 2048);
json_codec!(Metadata, 16 * 1024);
impl raft_log::codeq::Encode for Payload {
    fn encode<W: Write>(&self, writer: W) -> io::Result<usize> {
        write_blob(writer, &self.0, MAX_ENTRY)
    }
}
impl raft_log::codeq::Decode for Payload {
    fn decode<R: Read>(reader: R) -> io::Result<Self> {
        read_blob(reader, MAX_ENTRY).map(Self)
    }
}
