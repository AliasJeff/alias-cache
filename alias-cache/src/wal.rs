use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::state_machine::{ApplyResult, Operation, StateMachine};

const MAGIC: [u8; 4] = *b"ALOG";
const VERSION: u16 = 3;
const HEADER_LEN: usize = 4 + 2 + 8 + 4;
const CHECKSUM_LEN: usize = 4;

#[derive(Debug)]
pub struct Wal {
    path: PathBuf,
    file: File,
    next_index: u64,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|source| Error::io("create WAL directory", source))?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .map_err(|source| Error::io("open WAL", source))?;
        let next_index = scan_and_recover(&mut file)?;
        Ok(Self {
            path,
            file,
            next_index,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(&mut self, operation: &Operation) -> Result<u64> {
        let payload = bincode::serialize(operation)
            .map_err(|error| Error::server(format!("serialize WAL operation: {error}")))?;
        let index = self.next_index;
        let mut record = Vec::with_capacity(HEADER_LEN + payload.len() + CHECKSUM_LEN);
        record.extend_from_slice(&MAGIC);
        record.extend_from_slice(&VERSION.to_le_bytes());
        record.extend_from_slice(&index.to_le_bytes());
        record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        record.extend_from_slice(&payload);
        record.extend_from_slice(&checksum(&record).to_le_bytes());
        self.file
            .write_all(&record)
            .map_err(|source| Error::io("append WAL record", source))?;
        self.file
            .sync_data()
            .map_err(|source| Error::io("sync WAL record", source))?;
        self.next_index = self
            .next_index
            .checked_add(1)
            .ok_or_else(|| Error::server("WAL index overflow"))?;
        Ok(index)
    }

    pub fn replay(&mut self, state: &mut StateMachine) -> Result<usize> {
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|source| Error::io("seek WAL for replay", source))?;
        let mut applied = 0;
        loop {
            match read_record(&mut self.file) {
                Ok(Some((_index, operation))) => {
                    match state.apply(operation) {
                        ApplyResult::Response(_) | ApplyResult::NoResponse => {}
                    }
                    applied += 1;
                }
                Ok(None) => break,
                Err(WalReadError::Truncated) => {
                    return Err(Error::server("WAL is truncated during replay"));
                }
                Err(WalReadError::Corrupt(message)) => return Err(Error::server(message)),
                Err(WalReadError::Io(source)) => return Err(Error::io("read WAL", source)),
            }
        }
        self.file
            .seek(SeekFrom::End(0))
            .map_err(|source| Error::io("seek WAL end after replay", source))?;
        Ok(applied)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.file
            .sync_all()
            .map_err(|source| Error::io("sync WAL", source))
    }
}

fn scan_and_recover(file: &mut File) -> Result<u64> {
    file.seek(SeekFrom::Start(0))
        .map_err(|source| Error::io("seek WAL for recovery", source))?;
    let mut valid_end = 0_u64;
    let mut last_index: Option<u64> = None;
    loop {
        let record_start = file
            .stream_position()
            .map_err(|source| Error::io("read WAL position", source))?;
        match read_record(file) {
            Ok(Some((index, _))) => {
                if let Some(previous) = last_index
                    && index != previous.saturating_add(1)
                {
                    return Err(Error::server("WAL indexes are not contiguous"));
                }
                last_index = Some(index);
                valid_end = file
                    .stream_position()
                    .map_err(|source| Error::io("read WAL end position", source))?;
            }
            Ok(None) => break,
            Err(WalReadError::Truncated) if valid_end == record_start => {
                file.set_len(valid_end)
                    .map_err(|source| Error::io("truncate partial WAL record", source))?;
                break;
            }
            Err(WalReadError::Truncated) => {
                file.set_len(valid_end)
                    .map_err(|source| Error::io("truncate partial WAL tail", source))?;
                break;
            }
            Err(WalReadError::Corrupt(message)) => return Err(Error::server(message)),
            Err(WalReadError::Io(source)) => return Err(Error::io("read WAL", source)),
        }
    }
    file.seek(SeekFrom::End(0))
        .map_err(|source| Error::io("seek WAL end", source))?;
    Ok(last_index.map_or(0, |index| index.saturating_add(1)))
}

#[derive(Debug)]
enum WalReadError {
    Truncated,
    Corrupt(String),
    Io(io::Error),
}

fn read_record(file: &mut File) -> std::result::Result<Option<(u64, Operation)>, WalReadError> {
    let mut header = [0_u8; HEADER_LEN];
    let read = read_up_to(file, &mut header).map_err(WalReadError::Io)?;
    if read == 0 {
        return Ok(None);
    }
    if read != HEADER_LEN {
        return Err(WalReadError::Truncated);
    }
    if header[..4] != MAGIC {
        return Err(WalReadError::Corrupt("WAL magic is invalid".into()));
    }
    if u16::from_le_bytes([header[4], header[5]]) != VERSION {
        return Err(WalReadError::Corrupt("WAL version is unsupported".into()));
    }
    let index = u64::from_le_bytes(header[6..14].try_into().unwrap());
    let length = u32::from_le_bytes(header[14..18].try_into().unwrap()) as usize;
    let mut payload = vec![0_u8; length];
    if read_up_to(file, &mut payload).map_err(WalReadError::Io)? != length {
        return Err(WalReadError::Truncated);
    }
    let mut checksum_bytes = [0_u8; CHECKSUM_LEN];
    if read_up_to(file, &mut checksum_bytes).map_err(WalReadError::Io)? != CHECKSUM_LEN {
        return Err(WalReadError::Truncated);
    }
    let expected = u32::from_le_bytes(checksum_bytes);
    let mut check_data = Vec::with_capacity(HEADER_LEN + payload.len());
    check_data.extend_from_slice(&header);
    check_data.extend_from_slice(&payload);
    if checksum(&check_data) != expected {
        return Err(WalReadError::Corrupt("WAL checksum mismatch".into()));
    }
    let operation = bincode::deserialize(&payload)
        .map_err(|error| WalReadError::Corrupt(format!("WAL payload is invalid: {error}")))?;
    Ok(Some((index, operation)))
}

fn read_up_to(file: &mut File, buffer: &mut [u8]) -> io::Result<usize> {
    let mut total = 0;
    while total < buffer.len() {
        match file.read(&mut buffer[total..]) {
            Ok(0) => break,
            Ok(read) => total += read,
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(total)
}

fn checksum(bytes: &[u8]) -> u32 {
    let mut hash = 2_166_136_261_u32;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(16_777_619);
    }
    hash
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use bytes::Bytes;

    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("alias-cache-{name}-{}", std::process::id()))
    }

    #[test]
    fn appends_and_replays_operations() {
        let path = temp_path("wal-replay");
        let _ = std::fs::remove_file(&path);
        let mut wal = Wal::open(&path).unwrap();
        wal.append(&Operation::Set {
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"value"),
        })
        .unwrap();
        wal.append(&Operation::Delete {
            keys: vec![Bytes::from_static(b"missing")],
        })
        .unwrap();

        let mut restored = StateMachine::default();
        assert_eq!(wal.replay(&mut restored).unwrap(), 2);
        assert_eq!(
            restored.snapshot().get(&Bytes::from_static(b"key")),
            Some(&crate::state_machine::Value::String(Bytes::from_static(
                b"value"
            )))
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn replays_ttl_metadata_and_logical_time() {
        let path = temp_path("wal-ttl-replay");
        let _ = std::fs::remove_file(&path);
        let mut wal = Wal::open(&path).unwrap();
        wal.append(&Operation::SetWithExpiration {
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"value"),
            expire_at: Some(2_000),
            write_time: 1_000,
            only_if_exists: false,
            only_if_missing: false,
            keep_ttl: false,
            return_previous: false,
        })
        .unwrap();
        wal.append(&Operation::AdvanceTime { write_time: 1_500 })
            .unwrap();

        let mut restored = StateMachine::default();
        assert_eq!(wal.replay(&mut restored).unwrap(), 2);
        assert_eq!(restored.logical_time(), 1_500);
        assert_eq!(
            restored
                .execute_request(crate::protocol::Request::Command(vec![
                    Bytes::from_static(b"PTTL"),
                    Bytes::from_static(b"key"),
                ]))
                .unwrap()
                .1,
            crate::protocol::Frame::Integer(500)
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn truncates_partial_tail_on_open() {
        let path = temp_path("wal-tail");
        let _ = std::fs::remove_file(&path);
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&Operation::Set {
                key: Bytes::from_static(b"key"),
                value: Bytes::from_static(b"value"),
            })
            .unwrap();
        }
        let valid_len = std::fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(valid_len + 3).unwrap();
        drop(file);

        let wal = Wal::open(&path).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_len);
        assert_eq!(wal.next_index, 1);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn rejects_corrupt_record() {
        let path = temp_path("wal-corrupt");
        let _ = std::fs::remove_file(&path);
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&Operation::Set {
                key: Bytes::from_static(b"key"),
                value: Bytes::from_static(b"value"),
            })
            .unwrap();
        }
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[HEADER_LEN] ^= 1;
        std::fs::write(&path, bytes).unwrap();
        assert!(Wal::open(&path).is_err());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn rejects_wal_from_older_format_version() {
        let path = temp_path("wal-version");
        let _ = std::fs::remove_file(&path);
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&Operation::Set {
                key: Bytes::from_static(b"key"),
                value: Bytes::from_static(b"value"),
            })
            .unwrap();
        }
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[4..6].copy_from_slice(&1_u16.to_le_bytes());
        std::fs::write(&path, bytes).unwrap();
        assert!(Wal::open(&path).is_err());
        let _ = std::fs::remove_file(path);
    }
}
