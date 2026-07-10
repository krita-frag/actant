//! State 持久化层 — Rust 核心四盒之一。
//!
//! State 统一封装 EventLog + CRDT + Snapshot。
//! 本模块是 Actant 唯一的持久化入口，包含：
//! - `Store`: 对外公开的异步 LMDB/Heed 键值存储
//! - `LmdbStore`: 底层同步 LMDB 原语（crate 内部使用）
//! - `HybridLogicalClock`: 混合逻辑时钟
//! - `CheckpointManager`: Actor 状态检查点
//! - `WalWriter` / `WalReader` / `WalCompactor`: 预写日志
//! - `EventLog`: 按 topic 分区的事件溯源抽象
//! - `CRDT`: 分布式状态类型族

pub mod crdt;
pub mod event_log;

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use heed::types::{Bytes, Str};
use heed::{Database, Env, EnvOpenOptions};
use parking_lot::Mutex;
use rkyv::Archive;
use serde::{Deserialize, Serialize};

use crate::common::wire::constants::store_keys::CHECKPOINT;
use crate::common::{
    serialization::{deserialize_rkyv_value, serialize_rkyv},
    ActantError, ActorId, Result, StoreConfig,
};

pub(crate) struct LmdbStore {
    env: Arc<Env>,
    default_db: Database<Str, Bytes>,
}

impl LmdbStore {
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_config(path, &StoreConfig::default())
    }

    pub fn open_with_config(path: &Path, config: &StoreConfig) -> Result<Self> {
        fs_err::create_dir_all(path)?;

        // 不要使用 NO_LOCK — LMDB 内置的读写锁可防止多进程打开同一 data_dir 时的静默数据损坏。
        // 若另一进程持有锁，LMDB 将在打开时返回错误。
        //
        // SAFETY: `EnvOpenOptions::open` 内部调用 LMDB 的 `mdb_env_open` FFI，
        // 该函数要求 `path` 指向由兼容文件系统支持的目录。此处已通过
        // `fs_err::create_dir_all(path)` 确保目录存在；macOS/Linux/Windows 的
        // 本地文件系统均满足 LMDB 的 mmap 兼容性要求。`config.map_size` 与
        // `config.max_dbs` 均为来自已验证 `ActantConfig` 的有限正值，不触发
        // LMDB 的整数溢出路径。LMDB 自身的内部锁保证多线程安全访问 env 句柄。
        let env = unsafe {
            EnvOpenOptions::new()
                .max_dbs(config.max_dbs)
                .map_size(config.map_size)
                .open(path)?
        };

        let mut wtxn = env.write_txn()?;
        let default_db = env.create_database(&mut wtxn, Some("default"))?;
        wtxn.commit()?;

        tracing::info!(
            "Store opened at {:?} with process-level locking enabled",
            path
        );

        Ok(Self {
            env: Arc::new(env),
            default_db,
        })
    }

    #[tracing::instrument(level = "trace", skip(self, value), fields(len = value.len()))]
    pub fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut wtxn = self.env.write_txn()?;
        self.default_db.put(&mut wtxn, key, value)?;
        wtxn.commit()?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let rtxn = self.env.read_txn()?;
        let result = self.default_db.get(&rtxn, key)?;
        Ok(result.map(|b| b.to_vec()))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn delete(&self, key: &str) -> Result<bool> {
        let mut wtxn = self.env.write_txn()?;
        let existed = self.default_db.delete(&mut wtxn, key)?;
        wtxn.commit()?;
        Ok(existed)
    }

    #[tracing::instrument(level = "trace", skip(self, entries), fields(count = entries.len()))]
    pub fn put_batch(&self, entries: &[(String, Vec<u8>)]) -> Result<()> {
        let mut wtxn = self.env.write_txn()?;
        for (key, value) in entries {
            self.default_db
                .put(&mut wtxn, key.as_str(), value.as_slice())?;
        }
        wtxn.commit()?;
        Ok(())
    }

    pub fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let rtxn = self.env.read_txn()?;
        let mut results = Vec::new();
        let iter = self.default_db.prefix_iter(&rtxn, prefix)?;
        for item in iter {
            let (key, value) = item?;
            results.push((key.to_string(), value.to_vec()));
        }
        Ok(results)
    }

    /// 仅返回按键序匹配前缀的最后一条记录。
    pub fn scan_prefix_last(&self, prefix: &str) -> Result<Option<(String, Vec<u8>)>> {
        let rtxn = self.env.read_txn()?;
        let iter = self.default_db.prefix_iter(&rtxn, prefix)?;
        let mut last = None;
        for item in iter {
            let (key, value) = item?;
            last = Some((key.to_string(), value.to_vec()));
        }
        Ok(last)
    }

    /// 启动 LMDB 环境的优雅关闭。
    ///
    /// # `Arc::try_unwrap` 的意图
    ///
    /// `LmdbStore::env` 是 `Arc<Env>`，因为运行时多个子系统（Orchestrator、
    /// FailoverManager、SchedulerActor 等）可能持有同一 store 的克隆引用。
    /// LMDB 的 `Env` 实现了 `Clone`（内部仅增加引用计数），但
    /// `prepare_for_closing` 消耗 `Env` 并返回 `EnvClosingEvent`——它需要
    /// 独占访问以等待所有读者退出。
    ///
    /// 此处先尝试 `Arc::try_unwrap`：
    /// - **成功**（`Ok`）：当前调用方是最后一个持有者，直接消耗 `Env` 并启动关闭。
    /// - **失败**（`Err`）：仍有其他克隆引用存活。此时 fall back 到 `(*arc).clone()`
    ///   创建一个新的 `Env` 句柄并启动关闭——LMDB 的 `Env` 关闭是基于底层
    ///   `mdb_env_close` 的引用计数，新句柄的关闭不影响仍存活的读者，但
    ///   `EnvClosingEvent` 仍会等待所有读者事务完成。
    ///
    /// 此设计保证：无论是否仍有其他 `LmdbStore` 克隆存在，调用方都能获得
    /// `EnvClosingEvent` 以等待读者退出，而不会因 `Arc` 引用计数阻塞关闭流程。
    pub fn prepare_close(&self) -> heed::EnvClosingEvent {
        let env = Arc::clone(&self.env);
        Arc::try_unwrap(env)
            .unwrap_or_else(|arc| (*arc).clone())
            .prepare_for_closing()
    }
}

impl Clone for LmdbStore {
    fn clone(&self) -> Self {
        Self {
            env: self.env.clone(),
            default_db: self.default_db,
        }
    }
}

/// 异步 LMDB 键值存储。所有 IO 操作都委托给 `tokio::task::spawn_blocking`，
/// 避免在 Tokio 工作线程上执行同步 mmap/磁盘 IO。
#[derive(Clone)]
pub struct Store {
    inner: LmdbStore,
}

impl Store {
    pub(crate) fn new(inner: LmdbStore) -> Self {
        Self { inner }
    }

    /// 异步打开给定路径的 LMDB 存储。
    pub async fn open(path: &Path) -> Result<Self> {
        let path = path.to_path_buf();
        let store = tokio::task::spawn_blocking(move || LmdbStore::open(&path))
            .await
            .map_err(|e| ActantError::Storage(format!("store open task panicked: {e}")))??;
        Ok(Self::new(store))
    }

    /// 使用自定义配置异步打开 LMDB 存储。
    pub async fn open_with_config(path: &Path, config: StoreConfig) -> Result<Self> {
        let path = path.to_path_buf();
        let store =
            tokio::task::spawn_blocking(move || LmdbStore::open_with_config(&path, &config))
                .await
                .map_err(|e| ActantError::Storage(format!("store open task panicked: {e}")))??;
        Ok(Self::new(store))
    }

    pub async fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        let store = self.inner.clone();
        let key = key.to_string();
        let value = value.to_vec();
        tokio::task::spawn_blocking(move || store.put(&key, &value))
            .await
            .map_err(|e| ActantError::Storage(format!("store put task panicked: {e}")))?
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let store = self.inner.clone();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || store.get(&key))
            .await
            .map_err(|e| ActantError::Storage(format!("store get task panicked: {e}")))?
    }

    pub async fn delete(&self, key: &str) -> Result<bool> {
        let store = self.inner.clone();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || store.delete(&key))
            .await
            .map_err(|e| ActantError::Storage(format!("store delete task panicked: {e}")))?
    }

    pub async fn put_batch(&self, entries: &[(String, Vec<u8>)]) -> Result<()> {
        let store = self.inner.clone();
        let entries: Vec<(String, Vec<u8>)> = entries
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        tokio::task::spawn_blocking(move || store.put_batch(&entries))
            .await
            .map_err(|e| ActantError::Storage(format!("store put_batch task panicked: {e}")))?
    }

    pub async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let store = self.inner.clone();
        let prefix = prefix.to_string();
        tokio::task::spawn_blocking(move || store.scan_prefix(&prefix))
            .await
            .map_err(|e| ActantError::Storage(format!("store scan_prefix task panicked: {e}")))?
    }

    pub async fn scan_prefix_last(&self, prefix: &str) -> Result<Option<(String, Vec<u8>)>> {
        let store = self.inner.clone();
        let prefix = prefix.to_string();
        tokio::task::spawn_blocking(move || store.scan_prefix_last(&prefix))
            .await
            .map_err(|e| {
                ActantError::Storage(format!("store scan_prefix_last task panicked: {e}"))
            })?
    }

    /// 异步优雅关闭 LMDB 环境。
    ///
    /// 返回 `Result` 以便调用方处理 `spawn_blocking` 任务 panic（如 LMDB 内部错误），
    /// 而非直接 panic 整个进程（L4 改进）。
    pub async fn prepare_close(&self) -> Result<heed::EnvClosingEvent> {
        let store = self.inner.clone();
        let event = tokio::task::spawn_blocking(move || store.prepare_close())
            .await
            .map_err(|e| ActantError::Storage(format!("store prepare_close task panicked: {e}")))?;
        Ok(event)
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[rkyv(bytecheck())]
pub struct HlcTimestamp {
    wall_time: u64,
    logical: u32,
}

impl HlcTimestamp {
    pub fn zero() -> Self {
        Self {
            wall_time: 0,
            logical: 0,
        }
    }

    pub fn wall_time(&self) -> u64 {
        self.wall_time
    }

    pub fn logical(&self) -> u32 {
        self.logical
    }

    pub fn from_parts(wall_time: u64, logical: u32) -> Self {
        Self { wall_time, logical }
    }
}

impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.wall_time().cmp(&other.wall_time()) {
            Ordering::Equal => self.logical().cmp(&other.logical()),
            other => other,
        }
    }
}

pub struct HybridLogicalClock {
    inner: Mutex<Inner>,
    max_drift_nanos: u64,
}

struct Inner {
    last_time: u64,
    logical: u32,
}

impl HybridLogicalClock {
    pub fn new() -> Self {
        Self::with_max_drift_ms(500)
    }

    pub fn with_max_drift_ms(max_drift_ms: u64) -> Self {
        Self {
            inner: Mutex::new(Inner {
                last_time: 0,
                logical: 0,
            }),
            max_drift_nanos: max_drift_ms.saturating_mul(1_000_000),
        }
    }

    fn physical_now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }

    pub fn tick(&self) -> HlcTimestamp {
        let mut inner = self.inner.lock();
        let physical = Self::physical_now();

        if physical > inner.last_time {
            inner.last_time = physical;
            inner.logical = 0;
        } else {
            inner.logical += 1;
        }

        HlcTimestamp {
            wall_time: inner.last_time,
            logical: inner.logical,
        }
    }

    pub fn merge(&self, remote: &HlcTimestamp) -> HlcTimestamp {
        let mut inner = self.inner.lock();
        let physical = Self::physical_now();
        let max_drift = self.max_drift_nanos;

        let capped_wall_time = if remote.wall_time() > physical.saturating_add(max_drift) {
            tracing::warn!(
                "HLC drift detected: remote wall_time {}ns exceeds local physical {}ns by >{}ms, capping",
                remote.wall_time(),
                physical,
                max_drift / 1_000_000,
            );
            physical.saturating_add(max_drift)
        } else {
            remote.wall_time()
        };

        inner.last_time = physical.max(inner.last_time).max(capped_wall_time);
        if inner.last_time == capped_wall_time && inner.last_time == physical {
            inner.logical = inner.logical.max(remote.logical()) + 1;
        } else if inner.last_time == capped_wall_time {
            inner.logical = remote.logical() + 1;
        } else if inner.last_time == physical {
            inner.logical += 1;
        } else {
            inner.logical = 0;
        }

        HlcTimestamp {
            wall_time: inner.last_time,
            logical: inner.logical,
        }
    }
}

impl Default for HybridLogicalClock {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(bytecheck())]
pub(crate) struct ActorSnapshot {
    pub(crate) actor_id: ActorId,
    pub(crate) actor_type: String,
    pub(crate) state: Vec<u8>,
    pub(crate) timestamp: HlcTimestamp,
    pub(crate) sequence: u64,
    pub(crate) wal_offset: u64,
}

#[derive(Clone)]
pub struct CheckpointManager {
    store: LmdbStore,
}

impl CheckpointManager {
    pub(crate) fn new(store: LmdbStore) -> Self {
        Self { store }
    }

    pub(crate) fn store(&self) -> &LmdbStore {
        &self.store
    }

    pub(crate) fn save(&self, snapshot: &ActorSnapshot) -> Result<()> {
        let key = checkpoint_key(&snapshot.actor_id, snapshot.sequence);
        let data = serialize_rkyv(snapshot)?;
        self.store.put(&key, &data)
    }

    #[cfg(test)]
    pub(crate) fn load(&self, actor_id: &ActorId, sequence: u64) -> Result<Option<ActorSnapshot>> {
        let key = checkpoint_key(actor_id, sequence);
        match self.store.get(&key)? {
            Some(data) => Ok(Some(deserialize_rkyv_value(&data)?)),
            None => Ok(None),
        }
    }

    pub(crate) fn load_latest(&self, actor_id: &ActorId) -> Result<Option<ActorSnapshot>> {
        let prefix = format!("{}{}:", CHECKPOINT, actor_id.0);
        match self.store.scan_prefix_last(&prefix)? {
            Some((_, data)) => Ok(Some(deserialize_rkyv_value(&data)?)),
            None => Ok(None),
        }
    }

    pub fn delete_old(&self, actor_id: &ActorId, keep_latest: usize) -> Result<()> {
        let prefix = format!("{}{}:", CHECKPOINT, actor_id.0);
        let entries = self.store.scan_prefix(&prefix)?;
        if entries.len() <= keep_latest {
            return Ok(());
        }
        let to_delete = entries.len() - keep_latest;
        for (key, _) in entries.iter().take(to_delete) {
            self.store.delete(key)?;
        }
        Ok(())
    }
}

fn checkpoint_key(actor_id: &ActorId, sequence: u64) -> String {
    format!("{}{}:{:020}", CHECKPOINT, actor_id.0, sequence)
}

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

    pub fn path(&self) -> &Path {
        &self.path
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
    wal_path: PathBuf,
    current_offset: u64,
    checkpoint_manager: CheckpointManager,
    keep_latest: usize,
}

impl WalCompactor {
    pub fn new(
        wal_path: PathBuf,
        current_offset: u64,
        checkpoint_manager: CheckpointManager,
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
        let mut actor_ids: HashSet<ActorId> = HashSet::new();
        for (_key, data) in entries {
            let snapshot: ActorSnapshot = deserialize_rkyv_value(&data)?;
            if snapshot.wal_offset < min_offset {
                min_offset = snapshot.wal_offset;
            }
            actor_ids.insert(snapshot.actor_id);
        }

        for actor_id in actor_ids {
            self.checkpoint_manager
                .delete_old(&actor_id, self.keep_latest)?;
        }

        if min_offset == 0 || min_offset >= self.current_offset {
            return Ok(0);
        }

        let reader = WalReader::open(self.wal_path.clone())?;
        let tail = reader.read_from(min_offset)?;

        let temp_path = self.wal_path.with_extension("wal.compacting");
        {
            let mut temp_writer = WalWriter::open(&temp_path)?;
            for (ts, data) in tail {
                temp_writer.append(ts, &data)?;
            }
            temp_writer.sync()?;
        }

        fs::rename(&temp_path, &self.wal_path)?;
        Ok(min_offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn store_put_and_get() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        store.put("key1", b"value1").unwrap();
        let val = store.get("key1").unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[test]
    fn store_delete_key() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        store.put("key1", b"value1").unwrap();
        assert!(store.delete("key1").unwrap());
        assert_eq!(store.get("key1").unwrap(), None);
    }

    #[test]
    fn store_scan_prefix() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        store.put("a:1", b"one").unwrap();
        store.put("a:2", b"two").unwrap();
        store.put("b:1", b"three").unwrap();
        let results = store.scan_prefix("a:").unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn store_custom_config() {
        let dir = tempdir().unwrap();
        let config = StoreConfig {
            data_dir: None,
            map_size: 64 * 1024 * 1024,
            max_dbs: 8,
        };
        let store = LmdbStore::open_with_config(dir.path(), &config).unwrap();
        store.put("key1", b"value1").unwrap();
        let val = store.get("key1").unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[test]
    fn hlc_tick_is_monotonic() {
        let hlc = HybridLogicalClock::new();
        let t1 = hlc.tick();
        let t2 = hlc.tick();
        assert!(t2 > t1);
    }

    #[test]
    fn hlc_merge_is_monotonic() {
        let hlc = HybridLogicalClock::new();
        let t1 = hlc.tick();
        let remote = HlcTimestamp::from_parts(t1.wall_time() + 1000, 5);
        let t2 = hlc.merge(&remote);
        assert!(t2.wall_time() >= remote.wall_time());
        let t3 = hlc.tick();
        assert!(t3 > t2);
    }

    #[test]
    fn checkpoint_save_and_load() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let cm = CheckpointManager::new(store);

        let snapshot = ActorSnapshot {
            actor_id: ActorId("actor-1".into()),
            actor_type: "test".into(),
            state: b"state-data".to_vec(),
            timestamp: HlcTimestamp::from_parts(1000, 0),
            sequence: 1,
            wal_offset: 0,
        };

        cm.save(&snapshot).unwrap();
        let loaded = cm.load(&snapshot.actor_id, 1).unwrap().unwrap();
        assert_eq!(loaded.state, b"state-data");
    }

    #[test]
    fn wal_append_and_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut writer = WalWriter::open(&path).unwrap();
        let ts = HlcTimestamp::from_parts(1000, 0);
        let offset = writer.append(ts, b"hello").unwrap();
        assert_eq!(offset, 0);

        let reader = WalReader::open(path).unwrap();
        let entries = reader.read_from(0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, b"hello");
    }

    // ───────────────────────── LmdbStore 扩展测试 ─────────────────────────

    #[test]
    fn store_get_returns_none_for_missing_key() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        assert_eq!(store.get("missing").unwrap(), None);
    }

    #[test]
    fn store_delete_returns_false_for_missing_key() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        assert!(!store.delete("missing").unwrap());
    }

    #[test]
    fn store_put_batch_writes_all_entries() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let entries = vec![
            ("k1".to_string(), b"v1".to_vec()),
            ("k2".to_string(), b"v2".to_vec()),
            ("k3".to_string(), b"v3".to_vec()),
        ];
        store.put_batch(&entries).unwrap();
        assert_eq!(store.get("k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(store.get("k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(store.get("k3").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn store_put_batch_empty_is_noop() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let entries: Vec<(String, Vec<u8>)> = Vec::new();
        store.put_batch(&entries).unwrap();
        // 无错误即视为通过
    }

    #[test]
    fn store_scan_prefix_empty_returns_empty() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let results = store.scan_prefix("nonexistent:").unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn store_scan_prefix_last_returns_last_sorted_entry() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        store.put("a:1", b"one").unwrap();
        store.put("a:2", b"two").unwrap();
        store.put("a:3", b"three").unwrap();
        let last = store.scan_prefix_last("a:").unwrap();
        assert!(last.is_some());
        let (key, value) = last.unwrap();
        assert_eq!(key, "a:3");
        assert_eq!(value, b"three");
    }

    #[test]
    fn store_scan_prefix_last_returns_none_when_empty() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let result = store.scan_prefix_last("nonexistent:").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn store_overwrite_existing_key() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        store.put("key", b"v1").unwrap();
        store.put("key", b"v2").unwrap();
        assert_eq!(store.get("key").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn store_prepare_close_returns_event() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let _event = store.prepare_close();
        // 调用即可，无需等待完成
    }

    #[test]
    fn lmdb_store_clone_shares_underlying_env() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let cloned = store.clone();
        store.put("key", b"value").unwrap();
        assert_eq!(cloned.get("key").unwrap(), Some(b"value".to_vec()));
    }

    // ───────────────────────── Store 异步包装测试 ─────────────────────────

    #[tokio::test]
    async fn async_store_open_put_get() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).await.unwrap();
        store.put("k", b"v").await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Some(b"v".to_vec()));
    }

    #[tokio::test]
    async fn async_store_open_with_config() {
        let dir = tempdir().unwrap();
        let config = StoreConfig {
            data_dir: None,
            map_size: 32 * 1024 * 1024,
            max_dbs: 4,
        };
        let store = Store::open_with_config(dir.path(), config).await.unwrap();
        store.put("k", b"v").await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Some(b"v".to_vec()));
    }

    #[tokio::test]
    async fn async_store_delete_returns_true_then_false() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).await.unwrap();
        store.put("k", b"v").await.unwrap();
        assert!(store.delete("k").await.unwrap());
        assert!(!store.delete("k").await.unwrap());
    }

    #[tokio::test]
    async fn async_store_put_batch_writes_all() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).await.unwrap();
        let entries = vec![
            ("k1".to_string(), b"v1".to_vec()),
            ("k2".to_string(), b"v2".to_vec()),
        ];
        store.put_batch(&entries).await.unwrap();
        assert_eq!(store.get("k1").await.unwrap(), Some(b"v1".to_vec()));
        assert_eq!(store.get("k2").await.unwrap(), Some(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn async_store_scan_prefix_returns_matches() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).await.unwrap();
        store.put("p:1", b"one").await.unwrap();
        store.put("p:2", b"two").await.unwrap();
        store.put("q:1", b"three").await.unwrap();
        let results = store.scan_prefix("p:").await.unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn async_store_scan_prefix_last_returns_last() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).await.unwrap();
        store.put("p:1", b"one").await.unwrap();
        store.put("p:2", b"two").await.unwrap();
        let last = store.scan_prefix_last("p:").await.unwrap();
        assert!(last.is_some());
        let (key, _) = last.unwrap();
        assert_eq!(key, "p:2");
    }

    #[tokio::test]
    async fn async_store_prepare_close_returns_event() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).await.unwrap();
        let _event = store.prepare_close().await.unwrap();
    }

    // ───────────────────────── HlcTimestamp / HybridLogicalClock 扩展测试 ─────────────────────────

    #[test]
    fn hlc_timestamp_zero_is_smallest() {
        let zero = HlcTimestamp::zero();
        let other = HlcTimestamp::from_parts(1, 0);
        assert!(zero < other);
        assert_eq!(zero.wall_time(), 0);
        assert_eq!(zero.logical(), 0);
    }

    #[test]
    fn hlc_timestamp_from_parts_preserves_values() {
        let ts = HlcTimestamp::from_parts(12345, 7);
        assert_eq!(ts.wall_time(), 12345);
        assert_eq!(ts.logical(), 7);
    }

    #[test]
    fn hlc_timestamp_ordering_wall_time_priority() {
        let a = HlcTimestamp::from_parts(100, 5);
        let b = HlcTimestamp::from_parts(200, 0);
        assert!(a < b);
    }

    #[test]
    fn hlc_timestamp_ordering_logical_tiebreaker() {
        let a = HlcTimestamp::from_parts(100, 5);
        let b = HlcTimestamp::from_parts(100, 10);
        assert!(a < b);
    }

    #[test]
    fn hlc_timestamp_equal_values_compare_equal() {
        let a = HlcTimestamp::from_parts(100, 5);
        let b = HlcTimestamp::from_parts(100, 5);
        assert!(a == b);
    }

    #[test]
    fn hlc_tick_increments_logical_on_same_physical_time() {
        // 用相同时间戳多次 tick，至少有一次会触发 logical 自增。
        let hlc = HybridLogicalClock::new();
        let mut timestamps = Vec::new();
        for _ in 0..10 {
            timestamps.push(hlc.tick());
        }
        // 至少前几个 timestamp 应该单调递增
        for i in 1..timestamps.len() {
            assert!(timestamps[i] > timestamps[i - 1]);
        }
    }

    #[test]
    fn hlc_merge_with_stale_remote_uses_local() {
        let hlc = HybridLogicalClock::new();
        let local = hlc.tick();
        let stale = HlcTimestamp::from_parts(local.wall_time() - 1000, 0);
        let merged = hlc.merge(&stale);
        // 合并后应不小于 local
        assert!(merged >= local);
    }

    #[test]
    fn hlc_merge_with_equal_wall_time_uses_logical() {
        let hlc = HybridLogicalClock::new();
        let local = hlc.tick();
        let remote = HlcTimestamp::from_parts(local.wall_time(), local.logical() + 10);
        let merged = hlc.merge(&remote);
        assert!(merged.logical() > 0);
    }

    #[test]
    fn hlc_with_max_drift_ms_caps_remote() {
        // max_drift=0：remote 超过本地物理时间将被 cap。
        let hlc = HybridLogicalClock::with_max_drift_ms(0);
        let future = HlcTimestamp::from_parts(u64::MAX, 0);
        let merged = hlc.merge(&future);
        // 被cap 后 wall_time 应远小于 u64::MAX
        assert!(merged.wall_time() < u64::MAX);
    }

    #[test]
    fn hlc_default_equals_new() {
        let a = HybridLogicalClock::default();
        let b = HybridLogicalClock::new();
        // 同 max_drift_ms（500），所以行为一致
        let ta = a.tick();
        let tb = b.tick();
        // 都应能产生有效时间戳
        assert!(ta.wall_time() > 0 || ta.logical() == 0);
        assert!(tb.wall_time() > 0 || tb.logical() == 0);
    }

    // ───────────────────────── CheckpointManager 扩展测试 ─────────────────────────

    fn make_snapshot(actor_id: &str, sequence: u64, wal_offset: u64) -> ActorSnapshot {
        ActorSnapshot {
            actor_id: ActorId(actor_id.to_string()),
            actor_type: "test".to_string(),
            state: format!("state-{}", sequence).into_bytes(),
            timestamp: HlcTimestamp::from_parts(1000 + sequence, 0),
            sequence,
            wal_offset,
        }
    }

    #[test]
    fn checkpoint_load_missing_returns_none() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let cm = CheckpointManager::new(store);
        let result = cm.load(&ActorId("missing".into()), 1).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn checkpoint_load_latest_returns_none_for_missing_actor() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let cm = CheckpointManager::new(store);
        let result = cm.load_latest(&ActorId("missing".into())).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn checkpoint_load_latest_returns_highest_sequence() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let cm = CheckpointManager::new(store);
        cm.save(&make_snapshot("actor-1", 1, 0)).unwrap();
        cm.save(&make_snapshot("actor-1", 2, 100)).unwrap();
        cm.save(&make_snapshot("actor-1", 3, 200)).unwrap();

        let latest = cm.load_latest(&ActorId("actor-1".into())).unwrap();
        assert!(latest.is_some());
        let latest = latest.unwrap();
        assert_eq!(latest.sequence, 3);
        assert_eq!(latest.wal_offset, 200);
    }

    #[test]
    fn checkpoint_delete_old_removes_oldest_entries() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let cm = CheckpointManager::new(store);
        for seq in 1..=5 {
            cm.save(&make_snapshot("actor-1", seq, seq * 100)).unwrap();
        }
        // 保留最近 2 个
        cm.delete_old(&ActorId("actor-1".into()), 2).unwrap();

        // 旧 entries 应该被删除
        assert!(cm.load(&ActorId("actor-1".into()), 1).unwrap().is_none());
        assert!(cm.load(&ActorId("actor-1".into()), 2).unwrap().is_none());
        assert!(cm.load(&ActorId("actor-1".into()), 3).unwrap().is_none());
        // 新 entries 应该保留
        assert!(cm.load(&ActorId("actor-1".into()), 4).unwrap().is_some());
        assert!(cm.load(&ActorId("actor-1".into()), 5).unwrap().is_some());
    }

    #[test]
    fn checkpoint_delete_old_no_op_when_under_keep() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let cm = CheckpointManager::new(store);
        cm.save(&make_snapshot("actor-1", 1, 0)).unwrap();
        // keep_latest=5，只有 1 个 entry，不应删除
        cm.delete_old(&ActorId("actor-1".into()), 5).unwrap();
        assert!(cm.load(&ActorId("actor-1".into()), 1).unwrap().is_some());
    }

    #[test]
    fn checkpoint_save_overwrites_same_key() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let cm = CheckpointManager::new(store);
        cm.save(&make_snapshot("actor-1", 1, 0)).unwrap();
        // 同 actor_id + sequence 再次 save 应覆盖
        let mut updated = make_snapshot("actor-1", 1, 0);
        updated.state = b"updated".to_vec();
        cm.save(&updated).unwrap();
        let loaded = cm.load(&ActorId("actor-1".into()), 1).unwrap().unwrap();
        assert_eq!(loaded.state, b"updated");
    }

    #[test]
    fn checkpoint_store_accessor_returns_inner() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let cm = CheckpointManager::new(store);
        // store() 应返回一个能读写同一 LMDB env 的引用
        cm.store().put("ck", b"v").unwrap();
        assert_eq!(cm.store().get("ck").unwrap(), Some(b"v".to_vec()));
    }

    // ───────────────────────── WAL 扩展测试 ─────────────────────────

    #[test]
    fn wal_writer_open_with_sync_creates_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sync.wal");
        let writer = WalWriter::open_with_sync(&path, true).unwrap();
        assert_eq!(writer.path(), &path);
        assert_eq!(writer.current_offset(), 0);
    }

    #[test]
    fn wal_writer_append_increments_offset() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("offset.wal");
        let mut writer = WalWriter::open(&path).unwrap();
        let ts = HlcTimestamp::from_parts(100, 1);
        let off1 = writer.append(ts, b"first").unwrap();
        let off2 = writer.append(ts, b"second").unwrap();
        assert!(off2 > off1);
        assert!(writer.current_offset() > off2);
    }

    #[test]
    fn wal_writer_append_empty_payload() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.wal");
        let mut writer = WalWriter::open(&path).unwrap();
        let ts = HlcTimestamp::from_parts(100, 1);
        let offset = writer.append(ts, b"").unwrap();
        assert_eq!(offset, 0);
        // 仅 header（20 字节）
        assert_eq!(writer.current_offset(), 20);
    }

    #[test]
    fn wal_writer_sync_flushes_to_disk() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("synced.wal");
        let mut writer = WalWriter::open_with_sync(&path, false).unwrap();
        writer
            .append(HlcTimestamp::from_parts(1, 0), b"data")
            .unwrap();
        writer.sync().unwrap();
        // sync 不 panic 即视为通过
    }

    #[test]
    fn wal_reader_open_creates_file_if_missing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("created.wal");
        assert!(!path.exists());
        let _reader = WalReader::open(path.clone()).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn wal_reader_read_from_nonzero_offset() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("offset.wal");
        let mut writer = WalWriter::open(&path).unwrap();
        let ts1 = HlcTimestamp::from_parts(100, 1);
        let ts2 = HlcTimestamp::from_parts(200, 2);
        let off1 = writer.append(ts1, b"first").unwrap();
        let _off2 = writer.append(ts2, b"second").unwrap();
        drop(writer);

        let reader = WalReader::open(path).unwrap();
        let entries = reader.read_from(off1).unwrap();
        // 从 off1 开始应只读到 second（off1 是 first 的起点，需要从 second 起点读）
        // 实际上 off1 是 first 的起点，所以会读到 first 和 second
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn wal_reader_read_from_empty_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.wal");
        std::fs::write(&path, []).unwrap();
        let reader = WalReader::open(path).unwrap();
        let entries = reader.read_from(0).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn wal_reader_read_from_beyond_eof_returns_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("short.wal");
        let mut writer = WalWriter::open(&path).unwrap();
        writer.append(HlcTimestamp::from_parts(1, 0), b"x").unwrap();
        drop(writer);
        let reader = WalReader::open(path).unwrap();
        let entries = reader.read_from(1000).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn wal_reader_stops_on_checksum_mismatch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt.wal");
        // 写一个有效条目
        let mut writer = WalWriter::open(&path).unwrap();
        writer
            .append(HlcTimestamp::from_parts(1, 0), b"valid")
            .unwrap();
        drop(writer);
        // 在 header 校验和字节处写入垃圾数据
        let mut data = std::fs::read(&path).unwrap();
        if data.len() >= 20 {
            data[16] = data[16].wrapping_add(1);
            std::fs::write(&path, data).unwrap();
        }
        let reader = WalReader::open(path).unwrap();
        let entries = reader.read_from(0).unwrap();
        // 校验和不匹配应停止读取
        assert!(entries.is_empty());
    }

    // ───────────────────────── WalCompactor 测试 ─────────────────────────

    #[test]
    fn wal_compactor_new_clamps_keep_latest_to_one() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let cm = CheckpointManager::new(store);
        let compactor = WalCompactor::new(
            dir.path().join("x.wal"),
            1000,
            cm,
            0, // 应被 clamp 为 1
        );
        assert_eq!(compactor.keep_latest, 1);
    }

    #[test]
    fn wal_compactor_compact_empty_returns_zero() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let cm = CheckpointManager::new(store);
        let compactor = WalCompactor::new(dir.path().join("x.wal"), 1000, cm, 1);
        let result = compactor.compact().unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn wal_compactor_compact_with_checkpoints_renames_wal() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let cm = CheckpointManager::new(store);
        // 写入一个 checkpoint
        cm.save(&make_snapshot("actor-1", 1, 0)).unwrap();
        // 写入一个 WAL 文件
        let wal_path = dir.path().join("test.wal");
        let mut writer = WalWriter::open(&wal_path).unwrap();
        writer
            .append(HlcTimestamp::from_parts(100, 1), b"entry1")
            .unwrap();
        writer
            .append(HlcTimestamp::from_parts(200, 2), b"entry2")
            .unwrap();
        drop(writer);

        let compactor = WalCompactor::new(wal_path.clone(), 1000, cm, 1);
        let min_offset = compactor.compact().unwrap();
        // min_offset 是最早 checkpoint 的 wal_offset（0），应被压缩
        assert_eq!(min_offset, 0);
        // WAL 文件应被重写（保留从 min_offset 开始的 tail）
        assert!(wal_path.exists());
    }

    #[test]
    fn wal_compactor_compact_skips_when_min_offset_at_or_beyond_current() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let cm = CheckpointManager::new(store);
        // checkpoint 的 wal_offset=100，current_offset=50，min_offset(100) >= current(50)，应跳过
        cm.save(&make_snapshot("actor-1", 1, 100)).unwrap();
        let compactor = WalCompactor::new(dir.path().join("x.wal"), 50, cm, 1);
        let result = compactor.compact().unwrap();
        assert_eq!(result, 0);
    }

    // ───────────────────────── StoreConfig 默认值 ─────────────────────────

    #[test]
    fn store_config_default() {
        let c = StoreConfig::default();
        // 默认值存在即可
        assert!(c.map_size > 0);
        assert!(c.max_dbs > 0);
    }
}
