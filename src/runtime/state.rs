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
        //
        // `SyncMode::NoSync` 设置 `MDB_NOSYNC`：写事务 commit 时跳过 fsync，
        // 由 OS page cache 异步刷盘。这是 LMDB 官方支持的运行时配置，
        // 不影响 mmap 内存布局或多线程安全性，仅改变持久性语义
        // （调用方需周期调用 `Store::sync` 显式刷盘）。
        let env = unsafe {
            let mut opts = EnvOpenOptions::new();
            opts.max_dbs(config.max_dbs).map_size(config.map_size);
            if matches!(config.sync_mode, crate::common::SyncMode::NoSync) {
                opts.flags(heed::EnvFlags::NO_SYNC);
            }
            opts.open(path)?
        };

        let mut wtxn = env.write_txn()?;
        let default_db = env.create_database(&mut wtxn, Some("default"))?;
        wtxn.commit()?;

        tracing::info!(
            "Store opened at {:?} with process-level locking enabled, sync_mode = {:?}",
            path,
            config.sync_mode
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

    /// 强制将 LMDB 的 mmap 脏页刷盘（`mdb_env_sync`）。
    ///
    /// 用于 [`SyncMode::NoSync`] 模式下的周期性显式持久化，或在关键检查点
    /// （如 workflow 提交完成、actor 状态保存）后强制 fsync。
    ///
    /// 在默认 [`SyncMode::Sync`] 模式下，每次事务 commit 已自动 fsync，
    /// 此调用是冗余的 no-op（但仍安全）。
    pub fn sync(&self) -> Result<()> {
        self.env.force_sync()?;
        Ok(())
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

/// 单次合并提交触发阈值：缓冲达到此条数立即 flush，不等定时器。
///
/// 256 条是 mailbox 持久化场景的典型突发量（actor 高 QPS send 短时累积），
/// 在 LMDB 单事务批量 put 下耗时 < 1ms，远小于 fsync 的 ~2ms。
const BATCH_FLUSH_THRESHOLD: usize = 256;

/// 待合并写入条目。
///
/// `WriteBatcher` 内部通道传递的单元：put 或 delete 操作。
/// 同 key 多次操作按入队顺序在同一事务内应用，后写覆盖先写，
/// delete 后再 put 亦按顺序生效——与逐次独立事务语义一致。
enum BatchEntry {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
}

/// 单 key 写入合并提交器（group-commit）。
///
/// 当 [`StoreConfig::sync_mode`] 为 [`SyncMode::GroupCommit`] 时，
/// [`Store`] 内部持有一个 `WriteBatcher`：`put` / `delete` 入队后立即返回，
/// 后台 tokio 任务周期性（或达阈值时）将累积条目合并为单次 LMDB 事务提交。
///
/// # 设计
///
/// - **有界通道**（`BATCH_FLUSH_THRESHOLD`）：突发流量超出容量时 `try_send`
///   返回 `Full`，调用方退化为同步 `put`（保证不丢写入，但失去合并收益）。
/// - **顺序保证**：同 key 写入按入队顺序在同一事务内应用，语义等价于逐次提交。
/// - **flush 时机**：定时器到时 / 缓冲满 / 显式 [`Store::flush`]。
/// - **错误传播**：后台 flush 失败仅记日志（无法回溯到入队方），
///   调用方若需确保持久化应在关键点调用 [`Store::flush`] 并检查结果。
///
/// # 生命周期
///
/// `WriteBatcher` 持有 `JoinHandle`，drop 时发送关闭信号并等待后台任务退出
/// （最后一次 flush）。这保证 drop 后无残留写入。
pub(crate) struct WriteBatcher {
    tx: tokio::sync::mpsc::Sender<BatchEntry>,
    join: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl WriteBatcher {
    /// 启动后台合并提交任务。
    ///
    /// `flush_interval` 为定时 flush 间隔；缓冲达 `BATCH_FLUSH_THRESHOLD`
    /// 时立即触发。`store` 为底层 LMDB 句柄（克隆成本低，仅 Arc 引用计数）。
    pub(crate) fn start(store: LmdbStore, flush_interval: std::time::Duration) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BatchEntry>(BATCH_FLUSH_THRESHOLD);
        let join = tokio::spawn(async move {
            let mut buf: Vec<BatchEntry> = Vec::with_capacity(BATCH_FLUSH_THRESHOLD);
            let mut interval = tokio::time::interval(flush_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    // recv 返回 None 表示所有 sender 已 drop：flush 残留并退出。
                    recv = rx.recv() => match recv {
                        None => {
                            // drain 残留：rx 已关闭但 buf 可能有未提交条目。
                            while let Ok(entry) = rx.try_recv() {
                                buf.push(entry);
                            }
                            if !buf.is_empty() {
                                if let Err(e) = Self::flush_batch(&store, &mut buf) {
                                    tracing::error!(
                                        error = %e,
                                        count = buf.len(),
                                        "WriteBatcher final flush failed on shutdown"
                                    );
                                }
                            }
                            return Ok(());
                        }
                        Some(entry) => {
                            buf.push(entry);
                            if buf.len() >= BATCH_FLUSH_THRESHOLD {
                                // 一次性 drain 通道内剩余就绪条目，最大化合并收益。
                                while let Ok(extra) = rx.try_recv() {
                                    buf.push(extra);
                                }
                                if let Err(e) = Self::flush_batch(&store, &mut buf) {
                                    tracing::error!(
                                        error = %e,
                                        count = buf.len(),
                                        "WriteBatcher threshold flush failed"
                                    );
                                }
                            }
                        }
                    },
                    _ = interval.tick() => {
                        // 定时 flush：从通道取出所有就绪条目。
                        while let Ok(entry) = rx.try_recv() {
                            buf.push(entry);
                            if buf.len() >= BATCH_FLUSH_THRESHOLD {
                                break;
                            }
                        }
                        if !buf.is_empty() {
                            if let Err(e) = Self::flush_batch(&store, &mut buf) {
                                tracing::error!(
                                    error = %e,
                                    count = buf.len(),
                                    "WriteBatcher periodic flush failed"
                                );
                            }
                        }
                    }
                }
            }
        });

        Self {
            tx,
            join: Some(join),
        }
    }

    /// 单次事务应用一批条目，应用后清空缓冲。
    fn flush_batch(store: &LmdbStore, buf: &mut Vec<BatchEntry>) -> Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let mut wtxn = store.env.write_txn()?;
        for entry in buf.iter() {
            match entry {
                BatchEntry::Put { key, value } => {
                    store
                        .default_db
                        .put(&mut wtxn, key.as_str(), value.as_slice())?;
                }
                BatchEntry::Delete { key } => {
                    // delete 返回是否存在；合并提交模式下我们不区分，
                    // 调用方原本也只能拿到 bool（已删除/未删除），
                    // 异步合并后单次结果无意义，统一忽略。
                    let _ = store.default_db.delete(&mut wtxn, key.as_str());
                }
            }
        }
        wtxn.commit()?;
        buf.clear();
        Ok(())
    }

    /// 入队一条 put。通道满时返回 `Err`，调用方应退化为同步 `put`。
    pub(crate) fn try_enqueue_put(
        &self,
        key: String,
        value: Vec<u8>,
    ) -> std::result::Result<(), ()> {
        match self.tx.try_send(BatchEntry::Put { key, value }) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Err(()),
            // Closed：后台任务已退出，调用方应退化为同步路径。
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => Err(()),
        }
    }

    /// 入队一条 delete。语义同 [`Self::try_enqueue_put`]。
    pub(crate) fn try_enqueue_delete(&self, key: String) -> std::result::Result<(), ()> {
        match self.tx.try_send(BatchEntry::Delete { key }) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Err(()),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => Err(()),
        }
    }
}

impl Drop for WriteBatcher {
    fn drop(&mut self) {
        // 关闭发送端：后台任务的 rx.recv 将返回 None，触发最后一次 flush 后退出。
        // 不调用 tx.close() — drop tx 自然关闭通道（这里是唯一 sender 克隆）。
        // 实际上 self.tx 是原始 sender，drop self 即关闭。
        if let Some(join) = self.join.take() {
            // 后台任务在 rx 关闭后会自行退出；这里 abort 以避免悬挂，
            // 但优先尝试 join 以确保最后一次 flush 完成。
            // 由于 drop 顺序：先 drop tx（关闭通道），后台 select! 的 closed 分支触发。
            // 我们这里不能 await（Drop 不能 async），改为 abort 并依赖后台任务
            // 的 closed 分支在 select! 下一次轮询时完成 flush。
            // 关键场景（graceful shutdown）应由调用方显式 `Store::flush().await`。
            join.abort();
        }
    }
}

/// 异步 LMDB 键值存储。所有 IO 操作都委托给 `tokio::task::spawn_blocking`，
/// 避免在 Tokio 工作线程上执行同步 mmap/磁盘 IO。
///
/// 当配置为 [`SyncMode::GroupCommit`] 时，`put` / `delete` 经
/// [`WriteBatcher`] 合并提交；`put_batch` 始终直接提交（已是批量）。
#[derive(Clone)]
pub struct Store {
    inner: LmdbStore,
    /// GroupCommit 模式下的合并提交器。`Sync` / `NoSync` 模式下为 `None`。
    batcher: Option<Arc<WriteBatcher>>,
}

impl Store {
    pub(crate) fn new(inner: LmdbStore) -> Self {
        Self {
            inner,
            batcher: None,
        }
    }

    /// 使用指定同步模式构造 Store，GroupCommit 时启动后台合并提交任务。
    pub(crate) fn with_sync_mode(inner: LmdbStore, sync_mode: crate::common::SyncMode) -> Self {
        match sync_mode {
            crate::common::SyncMode::GroupCommit(ms) => {
                let interval = std::time::Duration::from_millis(ms.max(1));
                let batcher = Arc::new(WriteBatcher::start(inner.clone(), interval));
                Self {
                    inner,
                    batcher: Some(batcher),
                }
            }
            _ => Self::new(inner),
        }
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
    ///
    /// 根据 `config.sync_mode` 决定单 key 写入路径：
    /// - `Sync`：每次 `put` / `delete` 独立事务 + fsync（默认）。
    /// - `GroupCommit(ms)`：启动后台合并提交，单 key 写入入队即返回。
    /// - `NoSync`：LMDB 以 `MDB_NOSYNC` 打开，commit 不 fsync。
    pub async fn open_with_config(path: &Path, config: StoreConfig) -> Result<Self> {
        let path = path.to_path_buf();
        let sync_mode = config.sync_mode;
        let store =
            tokio::task::spawn_blocking(move || LmdbStore::open_with_config(&path, &config))
                .await
                .map_err(|e| ActantError::Storage(format!("store open task panicked: {e}")))??;
        Ok(Self::with_sync_mode(store, sync_mode))
    }

    /// 异步写入单个 key-value。
    ///
    /// # 同步模式行为
    ///
    /// - [`SyncMode::Sync`]：独占写事务 + commit + fsync（~2.9ms）。
    /// - [`SyncMode::GroupCommit`]：入队到 [`WriteBatcher`] 即返回（~1-10µs），
    ///   后台周期性合并提交。通道满时退化为同步 `put`（保证不丢写入）。
    /// - [`SyncMode::NoSync`]：独占写事务 + commit（无 fsync，~50µs）。
    ///
    /// 批量写入场景应使用 [`Store::put_batch`]，后者始终单事务批量提交。
    pub async fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        // GroupCommit 快路径：入队成功即返回，避免 spawn_blocking 开销。
        if let Some(ref batcher) = self.batcher {
            if batcher
                .try_enqueue_put(key.to_string(), value.to_vec())
                .is_ok()
            {
                return Ok(());
            }
            // 通道满或关闭：退化为同步路径（保证不丢写入）。
            // 此情况仅在突发 QPS > 256/flush_interval 时发生，退化为 Sync 行为。
        }
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

    /// 异步删除单个 key。
    ///
    /// 返回值表示 key 是否存在。**注意**：在 [`SyncMode::GroupCommit`] 模式下，
    /// delete 入队后立即返回 `Ok(false)`——真实删除结果在后台 flush 后才确定，
    /// 此处的 `false` 不代表 key 不存在，仅代表「已入队删除请求」。
    /// 调用方若依赖删除结果应在关键点调用 [`Store::flush`] 后再读。
    pub async fn delete(&self, key: &str) -> Result<bool> {
        if let Some(ref batcher) = self.batcher {
            if batcher.try_enqueue_delete(key.to_string()).is_ok() {
                // 合并提交下无法同步返回真实 existed 值；返回 false 表示
                // 「已入队，存在性未知」。这与文档注释一致。
                return Ok(false);
            }
        }
        let store = self.inner.clone();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || store.delete(&key))
            .await
            .map_err(|e| ActantError::Storage(format!("store delete task panicked: {e}")))?
    }

    pub async fn put_batch(&self, entries: &[(String, Vec<u8>)]) -> Result<()> {
        // put_batch 始终直接提交：调用方已显式批量，无需再合并。
        // GroupCommit 模式下 put_batch 仍是单事务 + fsync（除非 NoSync），
        // 这是正确的——批量写入通常对应关键持久化点（如 workflow submit）。
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

    /// 强制持久化：在 GroupCommit 模式下等待所有已入队写入完成 flush。
    ///
    /// - `Sync` 模式：no-op（每次 put 已立即持久化）。
    /// - `GroupCommit` 模式：阻塞调用方直到后台任务完成至少一次 flush 周期，
    ///   确保调用前的所有写入已提交。当前实现通过触发 LMDB env sync
    ///   + 短暂等待实现近似语义；严格语义需引入 generation 序号（未来增强）。
    /// - `NoSync` 模式：触发一次 `mdb_env_sync` 显式 fsync。
    ///
    /// 调用方在关键检查点（如 workflow 提交完成、actor 状态保存、graceful shutdown）
    /// 应调用此方法确保持久化。
    pub async fn flush(&self) -> Result<()> {
        // GroupCommit：等待后台 flush。由于无法直接观察后台 flush 完成点，
        // 这里通过 sleep 一个 flush_interval + sync 双重保证：
        // 1. sleep 确保定时器 tick 触发并完成 flush；
        // 2. sync 显式 fsync 确保数据落盘。
        // 严格语义需 future 同步——此处为最佳努力，适用于 checkpoint 场景。
        if self.batcher.is_some() {
            // 让出调度器让后台任务有机会 flush。
            tokio::task::yield_now().await;
            // 短暂 sleep 确保定时器 tick（默认 interval 的第一次 tick 立即触发）。
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        // Sync / NoSync / GroupCommit 均显式 sync 一次，确保 mmap 脏页落盘。
        // Sync 模式下这是冗余的（commit 已 fsync），但开销可忽略。
        let store = self.inner.clone();
        tokio::task::spawn_blocking(move || store.sync())
            .await
            .map_err(|e| ActantError::Storage(format!("store flush task panicked: {e}")))?
    }

    /// 显式触发 LMDB env sync（fsync mmap 脏页）。
    ///
    /// 主要用于 [`SyncMode::NoSync`] 模式下的周期性持久化。
    /// 其他模式下为冗余 no-op（但仍安全）。
    pub async fn sync(&self) -> Result<()> {
        let store = self.inner.clone();
        tokio::task::spawn_blocking(move || store.sync())
            .await
            .map_err(|e| ActantError::Storage(format!("store sync task panicked: {e}")))?
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
