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

    /// 异步写入单个 key-value。每次调用独占一个 LMDB 写事务并 commit（含 fsync）。
    ///
    /// 适用于偶发单 key 写入或必须立即持久化的关键路径。批量写入场景
    /// （如 workflow 状态快照、事件日志回放）应使用 [`Store::put_batch`]，
    /// 后者在单次事务内完成所有写入，显著降低 fsync 开销。
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
#[path = "../../tests/rust/unit/runtime/state.rs"]
mod tests;
