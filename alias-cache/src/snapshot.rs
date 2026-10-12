use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::state_machine::StateMachine;

const MAGIC: [u8; 4] = *b"ASNP";
const VERSION: u16 = 4;
const HEADER_LEN: usize = 4 + 2 + 8;
const CHECKSUM_LEN: usize = 4;

#[derive(Debug)]
pub struct SnapshotStore {
    path: PathBuf,
}

impl SnapshotStore {
    pub fn new(data_dir: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(data_dir.as_ref())
            .map_err(|source| Error::io("create snapshot directory", source))?;
        Ok(Self {
            path: data_dir.as_ref().join("snapshot.bin"),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn save(&self, state: &StateMachine) -> Result<()> {
        let payload = bincode::serialize(state)
            .map_err(|error| Error::server(format!("serialize snapshot: {error}")))?;
        let mut record = Vec::with_capacity(HEADER_LEN + payload.len() + CHECKSUM_LEN);
        record.extend_from_slice(&MAGIC);
        record.extend_from_slice(&VERSION.to_le_bytes());
        record.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        record.extend_from_slice(&payload);
        record.extend_from_slice(&checksum(&record).to_le_bytes());

        let temporary_path = self.path.with_extension("bin.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary_path)
            .map_err(|source| Error::io("open snapshot temporary file", source))?;
        file.write_all(&record)
            .map_err(|source| Error::io("write snapshot", source))?;
        file.sync_all()
            .map_err(|source| Error::io("sync snapshot", source))?;
        drop(file);
        fs::rename(&temporary_path, &self.path)
            .map_err(|source| Error::io("publish snapshot", source))?;
        sync_parent(&self.path)?;
        Ok(())
    }

    pub fn load(&self) -> Result<Option<StateMachine>> {
        let mut file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(Error::io("open snapshot", source)),
        };
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|source| Error::io("read snapshot", source))?;
        if bytes.len() < HEADER_LEN + CHECKSUM_LEN {
            return Err(Error::server("snapshot is truncated"));
        }
        if bytes[..4] != MAGIC {
            return Err(Error::server("snapshot magic is invalid"));
        }
        if u16::from_le_bytes([bytes[4], bytes[5]]) != VERSION {
            return Err(Error::server("snapshot version is unsupported"));
        }
        let length = u64::from_le_bytes(bytes[6..14].try_into().unwrap());
        let length = usize::try_from(length)
            .map_err(|_| Error::server("snapshot payload length overflows"))?;
        let expected_size = HEADER_LEN
            .checked_add(length)
            .and_then(|size| size.checked_add(CHECKSUM_LEN))
            .ok_or_else(|| Error::server("snapshot size overflows"))?;
        if bytes.len() != expected_size {
            return Err(Error::server("snapshot length does not match header"));
        }
        let expected_checksum =
            u32::from_le_bytes(bytes[HEADER_LEN + length..].try_into().unwrap());
        if checksum(&bytes[..HEADER_LEN + length]) != expected_checksum {
            return Err(Error::server("snapshot checksum mismatch"));
        }
        let state = bincode::deserialize(&bytes[HEADER_LEN..HEADER_LEN + length])
            .map_err(|error| Error::server(format!("snapshot payload is invalid: {error}")))?;
        Ok(Some(state))
    }
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::server("snapshot has no parent directory"))?;
    let directory =
        File::open(parent).map_err(|source| Error::io("open snapshot directory", source))?;
    directory
        .sync_all()
        .map_err(|source| Error::io("sync snapshot directory", source))
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
    use bytes::Bytes;

    use super::*;
    use crate::state_machine::{Operation, Value};

    fn temp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("alias-cache-{name}-{}", std::process::id()))
    }

    fn populated_state() -> StateMachine {
        let mut state = StateMachine::default();
        state.apply(Operation::Set {
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"value"),
        });
        state
    }

    #[test]
    fn saves_and_loads_state() {
        let directory = temp_dir("snapshot-roundtrip");
        let _ = fs::remove_dir_all(&directory);
        let store = SnapshotStore::new(&directory).unwrap();
        store.save(&populated_state()).unwrap();
        let restored = store.load().unwrap().unwrap();
        assert_eq!(
            restored.snapshot().get(&Bytes::from_static(b"key")),
            Some(&Value::String(Bytes::from_static(b"value")))
        );
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn preserves_ttl_metadata_and_logical_time() {
        let directory = temp_dir("snapshot-ttl");
        let _ = fs::remove_dir_all(&directory);
        let store = SnapshotStore::new(&directory).unwrap();
        let mut state = StateMachine::default();
        state.apply(Operation::SetWithExpiration {
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"value"),
            expire_at: Some(2_000),
            write_time: 1_000,
            only_if_exists: false,
            only_if_missing: false,
            keep_ttl: false,
            return_previous: false,
        });
        store.save(&state).unwrap();

        let mut restored = store.load().unwrap().unwrap();
        assert_eq!(restored.logical_time(), 1_000);
        assert_eq!(
            restored
                .execute_request(crate::protocol::Request::Command(vec![
                    Bytes::from_static(b"PTTL"),
                    Bytes::from_static(b"key"),
                ]))
                .unwrap()
                .1,
            crate::protocol::Frame::Integer(1_000)
        );
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn rejects_corrupt_snapshot() {
        let directory = temp_dir("snapshot-corrupt");
        let _ = fs::remove_dir_all(&directory);
        let store = SnapshotStore::new(&directory).unwrap();
        store.save(&populated_state()).unwrap();
        let mut bytes = fs::read(store.path()).unwrap();
        bytes[HEADER_LEN] ^= 1;
        fs::write(store.path(), bytes).unwrap();
        assert!(store.load().is_err());
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn rejects_snapshot_from_older_format_version() {
        let directory = temp_dir("snapshot-version");
        let _ = fs::remove_dir_all(&directory);
        let store = SnapshotStore::new(&directory).unwrap();
        store.save(&populated_state()).unwrap();
        let mut bytes = fs::read(store.path()).unwrap();
        bytes[4..6].copy_from_slice(&1_u16.to_le_bytes());
        fs::write(store.path(), bytes).unwrap();
        assert!(store.load().is_err());
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn failed_temporary_write_does_not_publish_partial_file() {
        let directory = temp_dir("snapshot-atomic");
        let _ = fs::remove_dir_all(&directory);
        let store = SnapshotStore::new(&directory).unwrap();
        store.save(&populated_state()).unwrap();
        let previous = fs::read(store.path()).unwrap();
        fs::write(store.path().with_extension("bin.tmp"), b"partial").unwrap();
        assert_eq!(fs::read(store.path()).unwrap(), previous);
        let _ = fs::remove_dir_all(directory);
    }
}
