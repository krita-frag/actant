use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use rkyv::Archive;
use serde::{Deserialize, Serialize};

use super::hlc::HlcTimestamp;
use crate::common::wire::constants::store_keys::CHECKPOINT;
use crate::common::{
    serialization::{deserialize_rkyv_value, serialize_rkyv},
    ActantError, ActorId, Result,
};

const ENTRY_HEADER_SIZE: usize = 20;

#[derive(Debug, Clone, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(bytecheck())]
pub(crate) struct WalEvent {
    pub(crate) actor_id: ActorId,
    pub(crate) sequence: u64,
    pub(crate) payload: Vec<u8>,
}

#[doc(hidden)]
pub struct WalWriter {
    writer: BufWriter<File>,
    offset: u64,
    path: PathBuf,
    sync_on_write: bool,
}

pub(crate) struct WalReader {
    path: PathBuf,
}

impl WalWriter {
    pub fn open(path: &PathBuf) -> Result<Self> {
        Self::open_with_sync(path, false)
    }

    pub fn open_with_sync(path: &PathBuf, sync_on_write: bool) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        let offset = file.metadata()?.len();

        Ok(Self {
            writer: BufWriter::new(file),
            offset,
            path: path.clone(),
            sync_on_write,
        })
    }

    pub fn append(&mut self, timestamp: HlcTimestamp, data: &[u8]) -> Result<u64> {
        let offset = self.offset;
        let len = data.len() as u32;
        let mut header = [0u8; ENTRY_HEADER_SIZE];
        header[0..8].copy_from_slice(&timestamp.wall_time().to_le_bytes());
        header[8..12].copy_from_slice(&timestamp.logical().to_le_bytes());
        header[12..16].copy_from_slice(&len.to_le_bytes());
        header[16..20].copy_from_slice(&calculate_checksum(data).to_le_bytes());

        self.writer.write_all(&header)?;
        self.writer.write_all(data)?;
        self.writer.flush()?;

        self.offset = offset + ENTRY_HEADER_SIZE as u64 + data.len() as u64;

        Ok(offset)
    }

    pub(crate) fn append_event(
        &mut self,
        timestamp: HlcTimestamp,
        event: &WalEvent,
    ) -> Result<u64> {
        let data = serialize_rkyv(event)?;
        let offset = self.append(timestamp, &data)?;
        if self.sync_on_write {
            self.sync()?;
        }
        Ok(offset)
    }

    pub fn current_offset(&self) -> u64 {
        self.offset
    }

    pub fn sync(&mut self) -> Result<()> {
        self.writer.get_mut().sync_all()?;
        Ok(())
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    #[cfg(test)]
    pub(crate) fn truncate_to(&mut self, offset: u64) -> Result<()> {
        if offset > self.offset {
            return Ok(());
        }
        self.writer.get_mut().seek(SeekFrom::Start(offset))?;
        self.writer.get_mut().set_len(offset)?;
        self.writer.get_mut().seek(SeekFrom::End(0))?;
        self.offset = offset;
        Ok(())
    }
}

impl WalReader {
    pub fn open(path: PathBuf) -> Result<Self> {
        if !path.exists() {
            fs::write(&path, [])?;
        }
        Ok(Self { path })
    }

    pub fn read_from(&self, offset: u64) -> Result<Vec<(HlcTimestamp, Vec<u8>)>> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(offset))?;

        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            let mut header = [0u8; ENTRY_HEADER_SIZE];
            match reader.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let wall_time = u64::from_le_bytes(
                header[0..8]
                    .try_into()
                    .map_err(|_| ActantError::Serialization("Invalid header bytes".to_string()))?,
            );
            let logical = u32::from_le_bytes(
                header[8..12]
                    .try_into()
                    .map_err(|_| ActantError::Serialization("Invalid header bytes".to_string()))?,
            );
            let len = u32::from_le_bytes(
                header[12..16]
                    .try_into()
                    .map_err(|_| ActantError::Serialization("Invalid header bytes".to_string()))?,
            ) as usize;

            let mut data = vec![0u8; len];
            reader.read_exact(&mut data)?;
            let stored_checksum = u32::from_le_bytes(
                header[16..20]
                    .try_into()
                    .map_err(|_| ActantError::Serialization("Invalid header bytes".to_string()))?,
            );
            if calculate_checksum(&data) != stored_checksum {
                tracing::warn!(
                    "WAL checksum mismatch at offset {}: expected {}, calculated {}, stopping read",
                    offset,
                    stored_checksum,
                    calculate_checksum(&data),
                );
                break;
            }

            entries.push((HlcTimestamp::from_parts(wall_time, logical), data));
        }

        Ok(entries)
    }

    pub(crate) fn recover_events(&self, offset: u64) -> Result<Vec<(HlcTimestamp, WalEvent)>> {
        let entries = self.read_from(offset)?;
        let mut events = Vec::new();
        for (ts, data) in entries {
            let event: WalEvent = deserialize_rkyv_value(&data)?;
            events.push((ts, event));
        }
        Ok(events)
    }
}

fn calculate_checksum(data: &[u8]) -> u32 {
    xxhash_rust::xxh3::xxh3_64(data) as u32
}

pub(crate) struct WalCompactor {
    wal_path: std::path::PathBuf,
    current_offset: u64,
    checkpoint_manager: super::CheckpointManager,
    /// 压缩后每个 Actor 保留的最新检查点数量。
    keep_latest: usize,
}

impl WalCompactor {
    pub fn new(
        wal_path: std::path::PathBuf,
        current_offset: u64,
        checkpoint_manager: super::CheckpointManager,
        keep_latest: usize,
    ) -> Self {
        Self {
            wal_path,
            current_offset,
            checkpoint_manager,
            keep_latest: keep_latest.max(1),
        }
    }

    pub fn compact(&self) -> Result<u64> {
        let prefix = CHECKPOINT;
        let entries = self.checkpoint_manager.store().scan_prefix(prefix)?;
        if entries.is_empty() {
            return Ok(0);
        }

        let mut min_offset = u64::MAX;
        let mut actor_ids: std::collections::HashSet<ActorId> = std::collections::HashSet::new();
        for (_key, data) in &entries {
            if let Ok(snapshot) = deserialize_rkyv_value::<super::checkpoint::ActorSnapshot>(data) {
                actor_ids.insert(snapshot.actor_id.clone());
            }
        }

        for actor_id in &actor_ids {
            if let Ok(Some(latest)) = self.checkpoint_manager.load_latest(actor_id) {
                min_offset = min_offset.min(latest.wal_offset);
            }
        }

        if min_offset == u64::MAX || min_offset == 0 {
            return Ok(0);
        }

        let old_offset = self.current_offset;
        if min_offset >= old_offset {
            return Ok(0);
        }

        let original_path = self.wal_path.clone();
        let reader = WalReader::open(original_path.clone())?;
        let remaining = reader.read_from(min_offset)?;
        let reclaimed = min_offset;

        // 原子交换：将压缩数据写入临时文件，然后重命名。
        // 确保 WAL 文件不存在缺失或空窗期。
        let temp_path = original_path.with_extension("wal.compact");

        {
            let temp_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temp_path)?;
            let mut w = BufWriter::new(temp_file);
            for (ts, data) in &remaining {
                let len = data.len() as u32;
                let mut header = [0u8; ENTRY_HEADER_SIZE];
                header[0..8].copy_from_slice(&ts.wall_time().to_le_bytes());
                header[8..12].copy_from_slice(&ts.logical().to_le_bytes());
                header[12..16].copy_from_slice(&len.to_le_bytes());
                header[16..20].copy_from_slice(&calculate_checksum(data).to_le_bytes());
                w.write_all(&header)?;
                w.write_all(data)?;
            }
            w.flush()?;
            w.get_ref().sync_all()?;
        }

        // 将临时文件重命名以替换原文件。调用方负责同步活跃 writer 并在此返回后重新打开。
        if let Err(e) = fs::rename(&temp_path, &original_path) {
            // 清理临时文件，避免遗留 .compact 文件
            if let Err(cleanup_err) = fs::remove_file(&temp_path) {
                tracing::warn!(
                    "failed to clean up temp WAL file {}: {}",
                    temp_path.display(),
                    cleanup_err
                );
            }
            return Err(ActantError::Storage(format!(
                "WAL compact rename failed: {}",
                e
            )));
        }

        for actor_id in &actor_ids {
            self.checkpoint_manager
                .delete_old(actor_id, self.keep_latest)?;
        }

        Ok(reclaimed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn append_and_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut writer = WalWriter::open(&path).unwrap();
        let ts = HlcTimestamp::from_parts(1000, 0);
        let offset = writer.append(ts, b"hello").unwrap();
        writer.sync().unwrap();
        drop(writer);

        let reader = WalReader::open(path).unwrap();
        let entries = reader.read_from(offset).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0.wall_time(), 1000);
        assert_eq!(entries[0].1, b"hello");
    }

    #[test]
    fn offset_tracks_correctly() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut writer = WalWriter::open(&path).unwrap();
        let ts = HlcTimestamp::from_parts(1000, 0);
        assert_eq!(writer.current_offset(), 0);
        writer.append(ts, b"hello").unwrap();
        assert_eq!(writer.current_offset(), ENTRY_HEADER_SIZE as u64 + 5);
        writer.append(ts, b"world").unwrap();
        assert_eq!(writer.current_offset(), 2 * (ENTRY_HEADER_SIZE as u64 + 5));
    }

    #[test]
    fn truncate_removes_old_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut writer = WalWriter::open(&path).unwrap();
        let ts = HlcTimestamp::from_parts(1000, 0);
        writer.append(ts, b"first").unwrap();
        let keep_offset = writer.current_offset();
        writer.append(ts, b"second").unwrap();
        writer.append(ts, b"third").unwrap();
        assert!(writer.current_offset() > keep_offset);

        writer.truncate_to(keep_offset).unwrap();
        assert_eq!(writer.current_offset(), keep_offset);
        drop(writer);

        let reader = WalReader::open(path).unwrap();
        let entries = reader.read_from(0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, b"first");

        let from_offset = reader.read_from(keep_offset).unwrap();
        assert_eq!(from_offset.len(), 0);
    }

    #[test]
    fn truncate_noop_if_offset_exceeds_current() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut writer = WalWriter::open(&path).unwrap();
        let ts = HlcTimestamp::from_parts(1000, 0);
        writer.append(ts, b"data").unwrap();
        let offset_before = writer.current_offset();

        writer.truncate_to(offset_before + 1000).unwrap();
        assert_eq!(writer.current_offset(), offset_before);
    }

    #[test]
    fn compactor_truncates_based_on_checkpoints() {
        use super::super::checkpoint::{ActorSnapshot, CheckpointManager};
        use super::super::engine::Store;

        let dir = tempdir().unwrap();
        let db_path = dir.path().join("store");
        let wal_path = dir.path().join("test.wal");

        let store = Store::open(&db_path).unwrap();
        let cm = CheckpointManager::new(store);
        let mut writer = WalWriter::open(&wal_path).unwrap();

        let ts = HlcTimestamp::from_parts(1000, 0);

        writer.append(ts, b"old-event-1").unwrap();
        let checkpoint_offset = writer.current_offset();
        writer.append(ts, b"old-event-2").unwrap();
        writer.append(ts, b"new-event").unwrap();
        let offset_before_compact = writer.current_offset();

        cm.save(&ActorSnapshot {
            actor_id: ActorId("actor-1".into()),
            actor_type: "test".into(),
            state: b"state".to_vec(),
            timestamp: ts,
            sequence: 1,
            wal_offset: checkpoint_offset,
        })
        .unwrap();

        let compactor = WalCompactor::new(wal_path.clone(), offset_before_compact, cm, 1);
        let reclaimed = compactor.compact().unwrap();

        assert!(reclaimed > 0, "should have reclaimed some bytes");

        // 在压缩后的文件上重新打开 writer 以验证
        let writer = WalWriter::open(&wal_path).unwrap();
        assert!(
            writer.current_offset() < offset_before_compact,
            "WAL should be shorter after compaction"
        );

        let reader = WalReader::open(wal_path).unwrap();
        let entries = reader.read_from(0).unwrap();
        assert!(
            entries.iter().all(|(_, data)| data != b"old-event-1"),
            "events before checkpoint should be removed"
        );
    }
}
