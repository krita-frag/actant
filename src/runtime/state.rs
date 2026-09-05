//! State 持久化层 — Rust 核心四盒之一。
//!
//! State 统一封装 EventLog + Snapshot。
//! 本模块是 Actant 唯一的持久化入口，包含：
//! - `Store`: 对外公开的异步 LMDB/Heed 键值存储
//! - `LmdbStore`: 底层同步 LMDB 原语（crate 内部使用）
//! - `HybridLogicalClock`: 混合逻辑时钟
//! - `CheckpointManager`: Actor 状态检查点
//! - `WalWriter` / `WalReader` / `WalCompactor`: 预写日志
//! - `EventLog`: 按 topic 分区的事件溯源抽象

pub mod event_log;

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use heed::types::{Bytes, Str};
use heed::{Database, Env, EnvOpenOptions};
use parking_lot::{Condvar, Mutex};
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

/// store 格式版本 meta key（X1）。独立于任何业务键前缀（`orch:` / `event:` /
/// `ckpt:` 等），仅由打开路径读写。
const STORE_FMT_VERSION_KEY: &str = "meta:fmt_version";
/// 当前 store 持久化格式版本。破坏性布局变更（如 rkyv 结构体字段增删）时递增。
const STORE_FMT_VERSION: u64 = 1;

/// 校验（或首次创建时写入）store 格式版本 meta 条目。
///
/// - 无 meta 条目 → 视为首次创建，写入当前版本（0.3.2 及更早的存量库无此
///   条目，按首次创建对待并补写；0.x 里程碑本身不承诺向后兼容）；
/// - 条目与当前版本匹配 → 放行；
/// - 条目存在但不匹配 → 返回语义化 [`ActantError::Storage`]，拒绝启动——
///   旧格式数据在新代码上读取会静默损坏（rkyv 字节校验失败表现为 workflow
///   整体被当作 corrupt 清除），显式失败优于静默丢数据。
fn ensure_fmt_version(db: &Database<Str, Bytes>, env: &Env) -> Result<()> {
    let expected = STORE_FMT_VERSION.to_le_bytes();
    let current = {
        let rtxn = env.read_txn()?;
        db.get(&rtxn, STORE_FMT_VERSION_KEY)?.map(|b| b.to_vec())
    };
    match current {
        None => {
            let mut wtxn = env.write_txn()?;
            db.put(&mut wtxn, STORE_FMT_VERSION_KEY, &expected)?;
            wtxn.commit()?;
        }
        Some(bytes) if bytes == expected => {}
        Some(bytes) => {
            let found = match <[u8; 8]>::try_from(bytes.as_slice()) {
                Ok(raw) => u64::from_le_bytes(raw),
                Err(_) => 0,
            };
            return Err(ActantError::Storage(format!(
                "store format version mismatch: data was written by format v{found}, \
                 but this build requires v{STORE_FMT_VERSION}; refusing to open a \
                 store written by an incompatible version"
            )));
        }
    }
    Ok(())
}

impl LmdbStore {
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_config(path, &StoreConfig::default())
    }

    pub fn open_with_config(path: &Path, config: &StoreConfig) -> Result<Self> {
        fs_err::create_dir_all(path)?;

        // 显式收紧目录权限至 0o700：仅属主可访问。
        // 持久化目录内含 LMDB 数据文件（actor 状态、检查点、WAL 偏移、工作流状态），
        // 默认 0o755 会让同机其他用户遍历目录并通过 LMDB mmap 读取敏感状态。
        // 在 Unix 上 set_permissions 覆盖 umask；非 Unix 平台（Windows）权限模型
        // 不同，跳过——Windows 上 LMDB 文件 ACL 继承自父目录的默认 DACL。
        restrict_dir_permissions(path)?;

        // 不要使用 NO_LOCK — LMDB 内置的读写锁可防止多进程打开同一 data_dir 时的静默数据损坏。
        // 若另一进程持有锁，LMDB 将在打开时返回错误。
        //
        // SAFETY: `EnvOpenOptions::open` 内部调用 LMDB 的 `mdb_env_open` FFI,
        // 该函数要求 `path` 指向由兼容文件系统支持的目录。此处已通过
        // `fs_err::create_dir_all(path)` 确保目录存在；macOS/Linux/Windows 的
        // 本地文件系统均满足 LMDB 的 mmap 兼容性要求。`config.map_size` 与
        // `config.max_dbs` 均为来自已验证 `ActantConfig` 的有限正值，不触发
        // LMDB 的整数溢出路径。LMDB 自身的内部锁保证多线程安全访问 env 句柄。
        //
        // `SyncMode::NoSync` 设置 `MDB_NOSYNC`：写事务 commit 时跳过 fsync,
        // 由 OS page cache 异步刷盘。这是 LMDB 官方支持的运行时配置,
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

        // X1：打开时校验（或首次创建时写入）store 格式版本，不匹配即拒绝启动。
        ensure_fmt_version(&default_db, &env)?;

        // LMDB 在 env.open 时以默认模式（受 umask 影响）创建 data.mdb / lock.mdb。
        // 此处显式收紧至 0o600，防止同机其他用户读取 actor 状态 / 检查点等敏感数据。
        // 仅对已存在的文件操作——若 LMDB 使用了非默认文件名（未来扩展），跳过。
        restrict_file_permissions(&path.join("data.mdb"))?;
        restrict_file_permissions(&path.join("lock.mdb"))?;

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
/// 后台提交线程（`spawn_blocking`）周期性（或达阈值时）将累积条目合并为
/// 单次 LMDB 事务提交。
///
/// # 设计
///
/// - **有界通道**（`BATCH_FLUSH_THRESHOLD`）：突发流量超出容量时 `try_send`
///   返回 `Full`，调用方退化为同步 `put`（保证不丢写入，但失去合并收益）。
/// - **顺序保证**：同 key 写入按入队顺序在同一事务内应用，语义等价于逐次提交。
/// - **flush 时机**：定时器到时 / 缓冲满 / 显式 [`Store::flush`]。
/// - **错误传播**：后台 flush 失败仅记日志（无法回溯到入队方），
///   调用方若需确保持久化应在关键点调用 [`Store::flush`] 并检查结果。
/// - **flush 完成观测**：`enqueued`（已入队条目数）与 `committed`（已提交
///   条目数）构成 generation 计数，[`Store::flush`] 据此等待"调用时刻前
///   入队的条目全部提交"，而非近似 sleep。
///
/// # 生命周期
///
/// Drop 时先关闭发送端（后台线程 `recv_timeout` 返回 Disconnected），
/// 随后在 `drain_timeout` 内轮询等待后台线程完成最终 flush；超时则解除
/// 关联并记录告警（剩余丢失窗口见 `Drop` 实现）。
///
/// 后台提交线程运行在 `spawn_blocking` 上而非 `tokio::spawn` 异步任务：
/// Drop 是同步上下文，只能以轮询方式等待后台退出——异步任务依赖
/// runtime 轮询，在 current-thread runtime 的线程被 Drop 阻塞时将永远
/// 得不到调度；阻塞线程独立于 runtime 推进，任何运行形态下 Drop 都能
/// 可靠地等待最终 flush。
pub(crate) struct WriteBatcher {
    /// `None` 表示发送端已关闭（Drop 中显式 take）。
    tx: Option<std::sync::mpsc::SyncSender<BatchEntry>>,
    join: Option<tokio::task::JoinHandle<()>>,
    /// 已成功 commit 的条目总数（后台线程每次 flush 成功后累加并通知）。
    committed: Arc<FlushCounter>,
    /// 已成功入队到通道的条目总数。
    enqueued: Arc<AtomicU64>,
    /// 定时 flush 间隔，用于推导 Drop 排空超时与 flush 等待超时。
    flush_interval: Duration,
}

/// flush 完成计数器：提交代数 + Condvar 通知。
struct FlushCounter {
    count: Mutex<u64>,
    cv: Condvar,
}

impl FlushCounter {
    fn new() -> Self {
        Self {
            count: Mutex::new(0),
            cv: Condvar::new(),
        }
    }

    /// 记录一次成功提交（`n` 条条目已 commit），唤醒所有等待者。
    fn advance(&self, n: usize) {
        let mut count = self.count.lock();
        *count += n as u64;
        self.cv.notify_all();
    }

    /// 阻塞直到已提交条目数达到 `target`，或超出 `timeout` 返回错误。
    ///
    /// `target` 为调用方调用 [`Store::flush`] 时刻已入队的条目总数；
    /// 返回 `Ok` 保证这些条目（以及更早的条目）均已 commit。
    fn wait_for(&self, target: u64, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        let mut count = self.count.lock();
        loop {
            if *count >= target {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(ActantError::Storage(format!(
                    "GroupCommit flush did not reach {target} committed entries within \
                     {timeout:?} (committed: {}, background flush may be failing)",
                    *count
                )));
            }
            // parking_lot::Condvar 带绝对截止时间的等待；虚假唤醒由
            // 循环顶部的条件复查处理。
            self.cv.wait_until(&mut count, deadline);
        }
    }
}

impl WriteBatcher {
    /// 启动后台合并提交线程。
    ///
    /// `flush_interval` 为定时 flush 间隔；缓冲达 `BATCH_FLUSH_THRESHOLD`
    /// 时立即触发。`store` 为底层 LMDB 句柄（克隆成本低，仅 Arc 引用计数）。
    pub(crate) fn start(store: LmdbStore, flush_interval: Duration) -> Self {
        // 有界通道保留原 tokio mpsc 的背压语义：积压达容量时 try_send
        // 返回 Full，调用方退化为同步 put。
        let (tx, rx) = std::sync::mpsc::sync_channel::<BatchEntry>(BATCH_FLUSH_THRESHOLD);
        let committed = Arc::new(FlushCounter::new());
        let enqueued = Arc::new(AtomicU64::new(0));
        let task_committed = Arc::clone(&committed);
        let join = tokio::task::spawn_blocking(move || {
            Self::run_flusher(store, rx, flush_interval, task_committed);
        });

        Self {
            tx: Some(tx),
            join: Some(join),
            committed,
            enqueued,
            flush_interval,
        }
    }

    /// 后台合并提交主循环：通道收满阈值立即 flush，超时间隔定时 flush，
    /// 发送端关闭后完成最终 flush 并退出。
    fn run_flusher(
        store: LmdbStore,
        rx: std::sync::mpsc::Receiver<BatchEntry>,
        flush_interval: Duration,
        committed: Arc<FlushCounter>,
    ) {
        let mut buf: Vec<BatchEntry> = Vec::with_capacity(BATCH_FLUSH_THRESHOLD);
        loop {
            match rx.recv_timeout(flush_interval) {
                Ok(entry) => {
                    buf.push(entry);
                    if buf.len() >= BATCH_FLUSH_THRESHOLD {
                        // 一次性 drain 通道内剩余就绪条目，最大化合并收益。
                        while let Ok(extra) = rx.try_recv() {
                            buf.push(extra);
                        }
                        if let Err(e) = Self::flush_batch(&store, &mut buf, &committed) {
                            tracing::error!(
                                error = %e,
                                count = buf.len(),
                                "WriteBatcher threshold flush failed"
                            );
                        }
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    if let Err(e) = Self::flush_batch(&store, &mut buf, &committed) {
                        tracing::error!(
                            error = %e,
                            count = buf.len(),
                            "WriteBatcher periodic flush failed"
                        );
                    }
                }
                // 发送端已关闭：缓冲中不会有新条目（Disconnected 仅在
                // 通道排空后返回），执行最终 flush 后退出。
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    while let Ok(entry) = rx.try_recv() {
                        buf.push(entry);
                    }
                    if let Err(e) = Self::flush_batch(&store, &mut buf, &committed) {
                        tracing::error!(
                            error = %e,
                            count = buf.len(),
                            "WriteBatcher final flush failed on shutdown"
                        );
                    }
                    return;
                }
            }
        }
    }

    /// 单次事务应用一批条目，应用后清空缓冲并推进提交计数。
    ///
    /// `delete` 的 `bool` 返回值（key 是否存在）在合并提交模式下无回传
    /// 通道、对调用方不可见，故忽略；`heed::Error` 必须传播——否则批量中
    /// 单条失败会被静默跳过而其余条目照常 commit，造成如 mailbox
    /// "已投递"标记残留、重启后重复投递的数据一致性问题。
    fn flush_batch(
        store: &LmdbStore,
        buf: &mut Vec<BatchEntry>,
        committed: &FlushCounter,
    ) -> Result<()> {
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
                    // 仅忽略 bool（存在性结果），传播 heed::Error。
                    let _existed = store.default_db.delete(&mut wtxn, key.as_str())?;
                }
            }
        }
        wtxn.commit()?;
        committed.advance(buf.len());
        buf.clear();
        Ok(())
    }

    /// Drop 时等待后台线程最终 flush 的超时上限。
    ///
    /// 取两倍 flush 间隔加 1s：覆盖定时器到点 + 一次 LMDB 事务提交的
    /// 正常耗时；LMDB 写事务通常远快于此，超时即视为异常路径。
    fn drain_timeout(&self) -> Duration {
        self.flush_interval * 2 + Duration::from_secs(1)
    }

    /// 入队一条 put。通道满时返回 `Err`，调用方应退化为同步 `put`。
    pub(crate) fn try_enqueue_put(
        &self,
        key: String,
        value: Vec<u8>,
    ) -> std::result::Result<(), ()> {
        let Some(tx) = self.tx.as_ref() else {
            // 发送端已关闭（Drop 之后）：调用方退化为同步路径。
            return Err(());
        };
        match tx.try_send(BatchEntry::Put { key, value }) {
            Ok(()) => {
                self.enqueued.fetch_add(1, AtomicOrdering::Release);
                Ok(())
            }
            Err(std::sync::mpsc::TrySendError::Full(_)) => Err(()),
            // Disconnected：后台线程已退出，调用方应退化为同步路径。
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => Err(()),
        }
    }

    /// 入队一条 delete。语义同 [`Self::try_enqueue_put`]。
    pub(crate) fn try_enqueue_delete(&self, key: String) -> std::result::Result<(), ()> {
        let Some(tx) = self.tx.as_ref() else {
            // 发送端已关闭（Drop 之后）：调用方退化为同步路径。
            return Err(());
        };
        match tx.try_send(BatchEntry::Delete { key }) {
            Ok(()) => {
                self.enqueued.fetch_add(1, AtomicOrdering::Release);
                Ok(())
            }
            Err(std::sync::mpsc::TrySendError::Full(_)) => Err(()),
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => Err(()),
        }
    }
}

impl Drop for WriteBatcher {
    fn drop(&mut self) {
        // 先显式关闭发送端：后台线程的 recv_timeout 将返回 Disconnected，
        // 触发最终 flush（flush_batch → commit → advance 计数）后退出。
        // take 出的 Sender 在语句结束时 drop，通道随之关闭。
        self.tx.take();

        let Some(join) = self.join.take() else {
            return;
        };

        // Drop 是同步上下文，无法 await JoinHandle；也不宜用
        // `Handle::block_on`（当前线程已处于 runtime 上下文时会 panic）。
        // 这里以短间隔轮询 `is_finished()` 排空等待；后台线程运行在
        // spawn_blocking 上、独立于 runtime 推进，轮询在任意 runtime
        // 形态（含 current-thread）下都不会死锁。
        let timeout = self.drain_timeout();
        let deadline = Instant::now() + timeout;
        while !join.is_finished() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }

        if !join.is_finished() {
            // 剩余丢失窗口：LMDB 写事务阻塞超过 drain 超时（磁盘慢/锁竞争）
            // 时，Drop 不再等待。此处不 abort——丢弃 JoinHandle 仅解除
            // 关联，后台线程在通道已关闭的前提下仍会完成最终 flush；但若
            // 进程随 Drop 立即退出，缓冲中的残留条目将丢失。依赖"已返回
            // Ok 的写入在正常 Drop 路径不丢"的关键调用方应显式
            // `Store::flush().await` 后再 drop。
            tracing::warn!(
                timeout_ms = timeout.as_millis() as u64,
                "WriteBatcher final flush did not finish within drain timeout; \
                 buffered entries may be lost if the process exits now"
            );
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
    /// delete 仅入队待删并立即返回 `Ok(false)`——此处的 `false` 不代表
    /// key 不存在，仅表示"删除请求已入队，待后台合并提交"，真实删除结果
    /// 在后台 flush 后才确定。调用方若依赖删除结果，应在关键点调用
    /// [`Store::flush`]（返回 `Ok` 即保证该删除已提交）后再读。
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
    /// - `GroupCommit` 模式：基于 generation 计数 + Condvar 通知保证严格
    ///   语义——返回 `Ok` 时，调用时刻前成功入队的所有条目均已 commit；
    ///   超出等待上限（两倍 flush 间隔 + 1s）仍未达到则返回 `Err`
    ///   （意味着后台 flush 持续失败，需调用方处理）。
    /// - `NoSync` 模式：触发一次 `mdb_env_sync` 显式 fsync。
    ///
    /// 调用方在关键检查点（如 workflow 提交完成、actor 状态保存、graceful shutdown）
    /// 应调用此方法确保持久化。
    pub async fn flush(&self) -> Result<()> {
        // GroupCommit：读取当前已入队条目数作为目标代数，再阻塞等待
        // 后台提交计数追上。入队计数在 try_send 成功后递增，因此调用
        // 时刻前已返回 Ok 的 put/delete 必然已计入目标。
        if let Some(ref batcher) = self.batcher {
            let target = batcher.enqueued.load(AtomicOrdering::Acquire);
            let committed = Arc::clone(&batcher.committed);
            let timeout = batcher.drain_timeout();
            tokio::task::spawn_blocking(move || committed.wait_for(target, timeout))
                .await
                .map_err(|e| ActantError::Storage(format!("store flush task panicked: {e}")))??;
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

    /// 按 Kulkarni 标准 HLC merge 算法推进时钟（`max(pt_j, l.pt, m.pt)`）。
    ///
    /// 记 `pt` 为本地物理时钟、`l` 为本地上次状态、`m` 为（可能被 drift 上限
    /// 截断的）远端时间戳，新的逻辑计数按三方最大值归属决定：
    /// - 物理时钟严格主导：`c = 0`；
    /// - 仅本地历史并列主导：`c = l.c + 1`；
    /// - 仅远端并列主导：`c = m.c + 1`；
    /// - 本地与远端同时并列主导：`c = max(l.c, m.c) + 1`。
    ///
    /// 每个分支都保证输出严格大于本地此前发出的任何时间戳（单调性）：
    /// wall_time 不小于旧值；wall_time 相等时逻辑计数严格递增。
    /// 远端超出 drift 上限时按 `(cap, m.c)` 参与比较，单调性不受影响。
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

        let local_wall = inner.last_time;
        let local_logical = inner.logical;
        let max_wall = physical.max(local_wall).max(capped_wall_time);
        inner.last_time = max_wall;

        let eq_local = max_wall == local_wall;
        let eq_remote = max_wall == capped_wall_time;
        inner.logical = if eq_local && eq_remote {
            // 本地与远端并列主导：取双方逻辑计数的最大值再加一。
            local_logical.max(remote.logical()).saturating_add(1)
        } else if eq_local {
            // 仅本地历史主导：本地物理时钟未前进（tick 语义的 c+1）。
            local_logical.saturating_add(1)
        } else if eq_remote {
            // 仅远端并列主导。
            remote.logical().saturating_add(1)
        } else {
            // 物理时钟严格大于双方：逻辑计数清零。
            0
        };

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

/// 单条 WAL 记录数据长度的上界（64 MiB）。
///
/// header 中的 len 为 u32，理论可达 4 GiB；正常 actor 事件（rkyv 序列化的
/// 消息 / 快照）远小于此。读取到的 len 超过该上界即判定 header 已损坏，
/// 返回错误而非按其分配内存——损坏流上的 4 GiB 分配可能直接 OOM。
const WAL_MAX_ENTRY_SIZE: usize = 64 * 1024 * 1024;

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
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_sync(path, false)
    }

    pub fn open_with_sync(path: &Path, sync_on_write: bool) -> Result<Self> {
        // Unix 上显式以 0o600 模式创建：WAL 文件含 actor 消息 payload（可能含
        // cloudpickle 序列化的业务对象），不应被同机其他用户读取。
        // `OpenOptions::mode` 仅在创建时生效，已存在的文件不受影响；
        // 此处不调用 set_permissions 是为了避免每次 open 都产生 syscall——
        // 创建时的 mode 已足够保证新文件安全，存量文件由首次 restrict_file_permissions
        // 收紧（见 LmdbStore::open_with_config 同款逻辑）。
        let file = open_restricted(path)?;
        let offset = file.metadata()?.len();

        Ok(Self {
            writer: BufWriter::new(file),
            offset,
            path: path.to_path_buf(),
            sync_on_write,
        })
    }

    pub fn append(&mut self, timestamp: HlcTimestamp, data: &[u8]) -> Result<u64> {
        let offset = self.offset;
        // WAL 入口 header 用 u32 LE 编码长度。data.len() > u32::MAX 时显式失败，
        // 否则 `as u32` 会静默截断，使 recover_events 读到错误长度并解码出乱码。
        let len = u32::try_from(data.len()).map_err(|_| {
            crate::common::ActantError::Serialization(format!(
                "WAL append: data len {} exceeds u32::MAX",
                data.len()
            ))
        })?;
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
            // 同 WalWriter::open_with_sync：以 0o600 创建空 WAL 文件，
            // 防止同机其他用户读取后续追加的 actor payload。
            let _file = open_restricted(&path)?;
            // 立即 drop file 句柄——WalReader 仅记录路径，按需在 read_from 中 reopen。
        }
        Ok(Self { path })
    }

    pub fn read_from(&self, offset: u64) -> Result<Vec<(HlcTimestamp, Vec<u8>)>> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(offset))?;

        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        // 当前读取位置（逻辑偏移）：用于错误 / 日志报告实际出错处，
        // 而非本次读取的起始参数 offset。
        let mut pos = offset;

        loop {
            let entry_offset = pos;
            let mut header = [0u8; ENTRY_HEADER_SIZE];
            match reader.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            pos += ENTRY_HEADER_SIZE as u64;

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

            // len 上界钳制：超过合理上限即视为 WAL 损坏，返回错误而非
            // 按损坏值分配内存（u32::MAX ≈ 4 GiB 可能 OOM）。
            if len > WAL_MAX_ENTRY_SIZE {
                return Err(ActantError::Storage(format!(
                    "WAL corrupt at offset {entry_offset}: entry len {len} exceeds \
                     limit {WAL_MAX_ENTRY_SIZE}"
                )));
            }

            let mut data = vec![0u8; len];
            reader.read_exact(&mut data)?;
            pos += len as u64;
            let stored_checksum = u32::from_le_bytes(
                header[16..20]
                    .try_into()
                    .map_err(|_| ActantError::Serialization("Invalid header bytes".to_string()))?,
            );
            if calculate_checksum(&data) != stored_checksum {
                tracing::warn!(
                    "WAL checksum mismatch at offset {entry_offset}: expected {}, calculated {}, stopping read",
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

/// 持久化文件 / 目录的属主私有权限掩码。
///
/// - 文件：0o600（rw-------）— 仅属主可读写，含 LMDB data.mdb / WAL 文件。
/// - 目录：0o700（rwx------）— 仅属主可遍历 / 列目录，含 LMDB data_dir。
///
/// 这些掩码仅作用于 Unix；Windows 跳过（依赖 NTFS ACL 继承自父目录）。
#[cfg(unix)]
const FILE_MODE: u32 = 0o600;
#[cfg(unix)]
const DIR_MODE: u32 = 0o700;

/// 收紧目录权限至 0o700（仅属主可访问）。
///
/// 调用方负责确保目录已创建。失败仅记日志而非传播错误——权限收紧是
/// 加固措施，不应因权限设置失败（如只读文件系统、容器环境）阻止 Store 打开，
/// 后者会导致整个 Runtime 无法启动；用户应通过日志察觉权限异常。
#[cfg(unix)]
fn restrict_dir_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    match fs::set_permissions(path, fs::Permissions::from_mode(DIR_MODE)) {
        Ok(()) => Ok(()),
        Err(e) => {
            tracing::warn!(
                path = ?path,
                error = %e,
                "failed to restrict directory permissions to 0o700; \
                 persisted data may be accessible to other local users"
            );
            Ok(())
        }
    }
}

#[cfg(not(unix))]
#[allow(unused_variables)]
fn restrict_dir_permissions(path: &Path) -> Result<()> {
    Ok(())
}

/// 收紧文件权限至 0o600（仅属主可读写）。
///
/// 文件不存在时静默返回 Ok——调用方可能在不存在的文件路径上调用此函数
/// （如 LMDB 未使用默认文件名）。其他失败仅记日志，不阻止 Store 打开。
#[cfg(unix)]
fn restrict_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    if !path.exists() {
        return Ok(());
    }
    match fs::set_permissions(path, fs::Permissions::from_mode(FILE_MODE)) {
        Ok(()) => Ok(()),
        Err(e) => {
            tracing::warn!(
                path = ?path,
                error = %e,
                "failed to restrict file permissions to 0o600; \
                 persisted data may be accessible to other local users"
            );
            Ok(())
        }
    }
}

#[cfg(not(unix))]
#[allow(unused_variables)]
fn restrict_file_permissions(path: &Path) -> Result<()> {
    Ok(())
}

/// 以属主私有权限（0o600）创建或打开文件用于追加写入。
///
/// Unix 上通过 `OpenOptionsExt::mode` 在创建时设置模式，避免 TOCTOU：
/// 若先 `open` 再 `set_permissions`，存在短暂窗口期内文件可被其他用户读取。
/// 已存在的文件不受 `mode` 影响，需调用方在首次启动时通过
/// [`restrict_file_permissions`] 显式收紧（见 LmdbStore::open_with_config）。
fn open_restricted(path: &Path) -> Result<File> {
    let mut opts = OpenOptions::new();
    opts.create(true).append(true).read(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(FILE_MODE);
    }
    Ok(opts.open(path)?)
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

/// HLC merge 单调性测试。
///
/// 与外置 `mod tests`（镜像单测文件）分离，专测 merge 在各分支下的
/// 全序递增不变量：任一节点发出的 HLC 时间戳必须严格大于其此前发出
/// 的所有时间戳，无论远端时间戳新旧、是否触发 drift cap。
#[cfg(test)]
mod hlc_merge_tests {
    use super::{HlcTimestamp, HybridLogicalClock};

    /// 断言 `next` 严格大于该节点此前输出的最大时间戳。
    fn assert_advances(last: &mut Option<HlcTimestamp>, next: HlcTimestamp) {
        if let Some(prev) = *last {
            assert!(
                next > prev,
                "HLC regressed: ({}, {}) <= ({}, {})",
                next.wall_time(),
                next.logical(),
                prev.wall_time(),
                prev.logical()
            );
        }
        *last = Some(next);
    }

    #[test]
    fn physical_dominant_resets_logical() {
        let clock = HybridLogicalClock::new();
        // 远端为过去时间戳：物理时钟严格主导，logical 清零（Kulkarni c=0）。
        let stale = HlcTimestamp::from_parts(1, 999);
        let t = clock.merge(&stale);
        assert_eq!(t.logical(), 0);
        assert!(t.wall_time() > 1);
    }

    #[test]
    fn tie_with_remote_takes_max_plus_one() {
        // 远端时间戳取在 1000s 后的未来且 drift 上限极大，保证测试期间
        // 本地物理时钟不可能越过它——排除真实时钟前进带来的不确定性。
        let now = HybridLogicalClock::physical_now();
        let clock = HybridLogicalClock::with_max_drift_ms(u64::MAX / 4_000_000);
        let future_wall = now + 1_000_000_000_000;
        // 本地先吸收远端时间戳，使本地历史与该远端并列。
        let remote = HlcTimestamp::from_parts(future_wall, 7);
        let t1 = clock.merge(&remote);
        assert_eq!(t1.wall_time(), future_wall);
        assert_eq!(t1.logical(), 8);

        // 与远端时间戳并列的再次 merge：max(local, remote) + 1，而非远端 +1。
        let t2 = clock.merge(&HlcTimestamp::from_parts(future_wall, 3));
        assert_eq!(t2.wall_time(), future_wall);
        assert_eq!(t2.logical(), 9);
    }

    #[test]
    fn merge_after_drift_cap_is_monotonic() {
        // 复现报告 P0 场景：远端超出 drift 上限被 cap 后，
        // 后续携带较旧时间戳的常态 merge 不得造成 (T, 0) 回退。
        let now = HybridLogicalClock::physical_now();
        let clock = HybridLogicalClock::with_max_drift_ms(500);
        let future = HlcTimestamp::from_parts(now + 10_000_000_000, 0);
        let mut last = Some(clock.merge(&future)); // cap 到 now + 500ms

        // 窗口内旧远端（gossip 常态）：全部落入本地主导分支 c+1。
        for i in 0..100u32 {
            let stale = HlcTimestamp::from_parts(now + i as u64, 0);
            let t = clock.merge(&stale);
            assert_advances(&mut last, t);
        }
    }

    #[test]
    fn merge_stale_remotes_never_regress() {
        let now = HybridLogicalClock::physical_now();
        let clock = HybridLogicalClock::with_max_drift_ms(500);
        let mut last = None;
        // 交替吸收"未来远端"与"陈旧远端"，覆盖全部四个分支。
        let inputs = [
            HlcTimestamp::from_parts(now + 100, 5),
            HlcTimestamp::from_parts(now, 0),
            HlcTimestamp::from_parts(now + 100, 50),
            HlcTimestamp::from_parts(now.saturating_sub(1_000_000), 3),
            HlcTimestamp::from_parts(now + 10_000_000_000, 1),
            HlcTimestamp::from_parts(now + 100, 1),
            HlcTimestamp::from_parts(now + 100, 200),
            HlcTimestamp::from_parts(now, 1),
        ];
        for remote in inputs {
            let t = clock.merge(&remote);
            assert_advances(&mut last, t);
        }
    }

    #[test]
    fn tick_after_merge_is_monotonic() {
        let clock = HybridLogicalClock::new();
        let mut last = None;
        for i in 0..50u32 {
            let remote_wall = HybridLogicalClock::physical_now() + (i % 3) as u64 * 1_000;
            let t = clock.merge(&HlcTimestamp::from_parts(remote_wall, i));
            assert_advances(&mut last, t);
            let t = clock.tick();
            assert_advances(&mut last, t);
        }
    }
}

/// GroupCommit 合并提交路径的功能测试：flush 的严格提交保证与 Drop
/// 最终 flush 不丢已接受写入。
#[cfg(test)]
mod group_commit_tests {
    use super::{Store, StoreConfig};
    use crate::common::SyncMode;
    use tempfile::tempdir;

    fn group_commit_config(interval_ms: u64) -> StoreConfig {
        StoreConfig {
            data_dir: None,
            map_size: 64 * 1024 * 1024,
            max_dbs: 8,
            sync_mode: SyncMode::GroupCommit(interval_ms),
        }
    }

    #[tokio::test]
    async fn flush_guarantees_enqueued_writes_are_committed() {
        let dir = tempdir().unwrap();
        let store = Store::open_with_config(dir.path(), group_commit_config(5))
            .await
            .unwrap();
        for i in 0..64u32 {
            store.put(&format!("k{i}"), &i.to_le_bytes()).await.unwrap();
        }
        // flush 返回 Ok 即保证调用前入队的全部条目已 commit。
        store.flush().await.unwrap();
        for i in 0..64u32 {
            assert_eq!(
                store.get(&format!("k{i}")).await.unwrap(),
                Some(i.to_le_bytes().to_vec())
            );
        }

        // GroupCommit 下 delete 返回 false 仅表示已入队待删；
        // flush 后该删除必须生效。
        assert!(!store.delete("k0").await.unwrap());
        store.flush().await.unwrap();
        assert_eq!(store.get("k0").await.unwrap(), None);
    }

    #[tokio::test]
    async fn drop_completes_final_flush_without_periodic_tick() {
        let dir = tempdir().unwrap();
        // 极大间隔：Drop 前定时 flush 不会触发，写入只能经 Drop 的
        // 最终 flush 路径落盘。
        let store = Store::open_with_config(dir.path(), group_commit_config(60_000))
            .await
            .unwrap();
        store.put("survivor", b"yes").await.unwrap();
        drop(store);
        // Drop 返回时后台线程的最终 flush 必须已完成。
        let reopened = Store::open(dir.path()).await.unwrap();
        assert_eq!(
            reopened.get("survivor").await.unwrap(),
            Some(b"yes".to_vec())
        );
    }
}
