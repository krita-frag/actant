//! EventLog 抽象 — State 四盒之一的事件溯源层。
//!
//! 提供按 topic 分区的 append-only 事件流，支持：
//! - 追加事件（带 HLC 时间戳与 topic 内序列号）
//! - 按时间戳/序列号读取后续事件
//! - 读取最新事件
//!
//! 底层基于 `LmdbStore`（LMDB），事件 ID 全局唯一且可排序。

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::common::{ActantError, Result};
use crate::runtime::state::{HybridLogicalClock, LmdbStore};

/// 事件分页读取的页大小。一次读事务只覆盖一页，避免长读事务长时间
/// 阻止 COW 旧页回收；结果集仍按调用方接口整体返回。
const EVENT_PAGE_SIZE: usize = 256;

/// topic 最大序号的持久化前缀。
///
/// 与事件条目在同一个写事务中更新，重启时无需扫描全部事件即可恢复序号。
const SEQ_KEY_PREFIX: &str = "meta:event_seq:";
/// 隐藏 topic 的序号元数据前缀。
///
/// `hide_topic` 把 seq 键**移入**该前缀而非删除：恢复期装载这些键，使
/// 同 id 重用时序号继续单调、时钟下界仍覆盖隐藏 topic 的历史事件（否则
/// 回拨时钟会让新事件键排进旧代事件中间，破坏审计序与恢复期代际判定）。
/// 该前缀不参与 `list_topics` 的 topic 扫描，隐藏语义不受影响。
const HIDDEN_SEQ_KEY_PREFIX: &str = "meta:event_seq_hidden:";
/// 存量序号迁移完成标记。故意不以 `SEQ_KEY_PREFIX` 开头，避免被当作 topic 元数据解析。
const SEQ_MIGRATED_KEY: &str = "meta:event_seq_migrated";

/// 事件唯一标识。
///
/// 由 HLC 时间戳与 topic 内序列号组成，保证同一 topic 内严格有序。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EventId {
    pub timestamp: crate::runtime::state::HlcTimestamp,
    pub sequence: u64,
}

impl EventId {
    /// 返回一个永远最小的 ID，用于 `read_after(None)` 从头读取。
    pub fn zero() -> Self {
        Self {
            timestamp: crate::runtime::state::HlcTimestamp::zero(),
            sequence: 0,
        }
    }
}

/// 单条日志记录。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub id: EventId,
    pub topic: String,
    pub payload: Vec<u8>,
}

/// EventLog 抽象。
///
/// 实现必须是线程安全且支持并发的。
pub trait EventLog: Send + Sync {
    /// 追加一条事件到指定 topic，返回全局唯一事件 ID。
    ///
    /// **同步是刻意的，不要改成 `async fn`**，两条理由：
    ///
    /// 1. 返回的 [`EventId`] 会被调用方立刻记为持久化水位
    ///    （`Orchestrator::log_event` → `record_event_seq`，随后用于
    ///    `trim_absorbed` 与重放上界）。把落盘推迟到后台线程就等于让水位越过
    ///    尚未持久化的事件，崩溃恢复后重放会缺一段历史。
    /// 2. 调用方里存在**无法 await 的同步路径**：`signal_wait_point` 由绑定层
    ///    以 `#[pymethods] fn`（非 async）调用，再向下走到 `log_event`。改成
    ///    async 只能靠 `block_on` 兜底，在 tokio 上下文内调用会直接 panic。
    ///
    /// 因此阻塞成本（默认 `SyncMode::Sync` 下每次 append 一次 fsync）不走
    /// `spawn_blocking`，而是由 `StoreConfig::sync_mode` 这个既有旋钮调节。
    fn append(&self, topic: &str, payload: &[u8]) -> Result<EventId>;

    /// 读取指定 topic 在 `after` 之后的所有事件。
    ///
    /// `after = None` 表示从头读取。
    fn read_after(&self, topic: &str, after: Option<&EventId>) -> Result<Vec<LogEntry>>;

    /// 读取指定 topic 的最新一条事件。
    fn latest(&self, topic: &str) -> Result<Option<LogEntry>>;

    /// 返回指定 topic 的事件总数（仅用于测试/调试）。
    fn count(&self, topic: &str) -> Result<usize>;

    /// 原子追加一批事件到同一 topic。
    ///
    /// 用于 journal-first 提交：`Submitted` 与全部 `NodeAdded` 必须要么全部
    /// 可见、要么全部不可见，恢复期才不会重放出半个 DAG。
    ///
    /// 默认实现逐条调用 [`Self::append`]（非原子，仅为外部实现保持编译兼容）；
    /// 内置实现必须覆盖为单事务/单锁。
    fn append_batch(&self, topic: &str, payloads: &[Vec<u8>]) -> Result<Vec<EventId>> {
        let mut ids = Vec::with_capacity(payloads.len());
        for payload in payloads {
            ids.push(self.append(topic, payload)?);
        }
        Ok(ids)
    }

    /// 列出已有事件的 topic（按字典序）。
    ///
    /// 用于恢复期从 journal 发现「快照缺失但事件已落盘」的工作流：
    /// journal-first 提交在写快照前崩溃时，只有 topic 能证明工作流存在。
    fn list_topics(&self) -> Result<Vec<String>>;

    /// 裁剪**已被快照吸收**的历史：`up_to` 通常取持久化水位。
    ///
    /// 删除 `id <= up_to` 的条目中最旧的若干条，使该区间至多保留 `keep` 条；
    /// **`up_to` 之后的条目永不裁剪**——它们是重放所需的增量
    /// （`replay_events_after_watermarks` 读的就是这一段）。
    ///
    /// 返回实际删除条数。`keep == 0` 表示不裁剪（保守默认）。
    ///
    /// 这是事件历史唯一的留存机制：
    /// 只增不删会让长跑工作流的历史无限增长，但按"保留最近 N 条"裁剪会破坏
    /// 重放，故裁剪上界必须是水位。
    fn trim_absorbed(&self, topic: &str, up_to: &EventId, keep: usize) -> Result<usize>;

    /// 把 topic 从 [`EventLog::list_topics`] 的结果中隐藏，**保留其事件数据**。
    ///
    /// 已淘汰的工作流不应再被恢复期的 topic 扫描发现；但工作流 journal
    /// 是审计面，仍要能按 topic 直读。实现只需让 `list_topics` 不再返回该
    /// topic，`read_after` / `latest` 行为不变。
    fn hide_topic(&self, topic: &str) -> Result<()>;
}

/// 基于 LMDB `LmdbStore` 的 EventLog 实现。
pub struct LmdbEventLog {
    store: LmdbStore,
    clock: HybridLogicalClock,
    /// 每个 topic 的**已分配**序列号下界。值用原子计数器承载，取号只做一次
    /// 原子自增，不与 LMDB 提交共享临界区。
    sequences: DashMap<String, Arc<AtomicU64>>,
    /// 每个 topic 的提交锁：meta 写入取号后、put_batch 前重新在锁内读取
    /// 计数器，保证并发提交乱序时 meta 仍单调不减（详见 `append`）。
    /// 只串行化同 topic 的提交 IO，不同 topic 互不影响。
    commit_locks: DashMap<String, Arc<parking_lot::Mutex<()>>>,
}

impl LmdbEventLog {
    /// 打开或创建基于给定 `LmdbStore` 的 EventLog。
    ///
    /// 启动时扫描已持久化的事件，恢复每个 topic 的最大序列号，
    /// 防止跨重启追加同 topic 事件时 sequence 回绕导致 EventId 冲突。
    pub(crate) fn new(store: LmdbStore) -> Self {
        let (sequences, max_event) = Self::recover_state(&store);
        let clock = HybridLogicalClock::new();
        if let Some(id) = max_event {
            // 物理时钟可能在重启间回拨；用已持久化的最大事件 ID 建立下界，
            // 避免新事件排在重放水位之前而被 read_after 永久跳过。
            clock.restore(id.timestamp);
        }
        Self {
            store,
            clock,
            sequences,
            commit_locks: DashMap::new(),
        }
    }

    /// 恢复序号元数据与已持久化的最大事件 ID。
    ///
    /// 序号与事件条目同事务写入 `meta:event_seq:{topic}`，因此正常路径
    /// 只需 O(topic 数) 次读取。存量库或部分迁移的库缺少某些 topic 的
    /// 元数据时，退回一次全量事件扫描补齐——只补缺失 topic，不覆盖已有
    /// 序号，也不降低任何 topic 的计数。
    ///
    /// 扫描完成后写入完成标记，后续启动走快路径；标记缺失时（旧版本写入
    /// 过部分 topic 元数据）仍会扫一次，保证不会留下重复序号的 topic。
    fn recover_state(store: &LmdbStore) -> (DashMap<String, Arc<AtomicU64>>, Option<EventId>) {
        let sequences = DashMap::new();
        match store.scan_prefix(SEQ_KEY_PREFIX) {
            Ok(entries) => {
                for (key, value) in entries {
                    let Some(topic) = key.strip_prefix(SEQ_KEY_PREFIX) else {
                        continue;
                    };
                    match <[u8; 8]>::try_from(value.as_slice()) {
                        Ok(raw) => {
                            sequences.insert(
                                topic.to_string(),
                                Arc::new(AtomicU64::new(u64::from_le_bytes(raw))),
                            );
                        }
                        Err(_) => {
                            tracing::warn!(
                                key = %key,
                                len = value.len(),
                                "ignoring corrupt event sequence meta entry"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!("failed to read event sequence metadata: {e}");
            }
        }

        // 隐藏 topic 的序号元数据：hide_topic 把 seq 键移入 hidden 命名空间。
        // 装载进 sequences 使同 id 重用时序号继续单调，并让
        // `max_recovered_event` 的时钟下界覆盖其历史事件。正常键优先
        // （append 重写后两键并存时，正常键是较新者）。
        match store.scan_prefix(HIDDEN_SEQ_KEY_PREFIX) {
            Ok(entries) => {
                for (key, value) in entries {
                    let Some(topic) = key.strip_prefix(HIDDEN_SEQ_KEY_PREFIX) else {
                        continue;
                    };
                    if sequences.contains_key(topic) {
                        continue;
                    }
                    match <[u8; 8]>::try_from(value.as_slice()) {
                        Ok(raw) => {
                            sequences.insert(
                                topic.to_string(),
                                Arc::new(AtomicU64::new(u64::from_le_bytes(raw))),
                            );
                        }
                        Err(_) => {
                            tracing::warn!(
                                key = %key,
                                len = value.len(),
                                "ignoring corrupt hidden event sequence meta entry"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!("failed to read hidden event sequence metadata: {e}");
            }
        }

        if Self::migration_complete(store) {
            let max_event = Self::max_recovered_event(store, &sequences);
            return (sequences, max_event);
        }

        // 未标记完成：可能完全没有元数据，也可能只迁移了一部分 topic。
        // 全量扫描一次，补齐缺失 topic（不覆盖已有值），然后写完成标记。
        let max_event = Self::migrate_legacy_sequences(store, &sequences);
        (sequences, max_event)
    }

    /// 是否已完成存量序号迁移。
    fn migration_complete(store: &LmdbStore) -> bool {
        matches!(store.get(SEQ_MIGRATED_KEY), Ok(Some(_)))
    }

    /// 读取每个已知 topic 的最后一条事件，取全局最大 `EventId`。
    ///
    /// 事件键以 topic 开头，无法用一次全局反向扫描得到时间上的最大值，
    /// 因此按 topic 逐个 `scan_prefix_last`。topic 数远小于事件数，成本可控。
    fn max_recovered_event(
        store: &LmdbStore,
        sequences: &DashMap<String, Arc<AtomicU64>>,
    ) -> Option<EventId> {
        let mut max_event: Option<EventId> = None;
        for entry in sequences.iter() {
            let topic = entry.key();
            match store.scan_prefix_last(&Self::prefix(topic)) {
                Ok(Some((_, value))) => match postcard::from_bytes::<LogEntry>(&value) {
                    Ok(event) => {
                        if max_event.map(|current| event.id > current).unwrap_or(true) {
                            max_event = Some(event.id);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            topic = %topic,
                            error = %e,
                            "ignoring corrupt event while restoring HLC floor"
                        );
                    }
                },
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(
                        topic = %topic,
                        error = %e,
                        "failed to read last event while restoring HLC floor"
                    );
                }
            }
        }
        max_event
    }

    /// 为没有序号元数据的 topic 做一次性迁移。
    ///
    /// 全量扫描事件日志，只为**缺失**元数据的 topic 补写序号（已有元数据
    /// 不覆盖、不回退），并在同一批次写入完成标记。返回扫描过程中见到的
    /// 最大事件 ID，供恢复 HLC 下界。
    ///
    /// 扫描或写入失败时不写完成标记，下次启动会重试；已补写的 topic 不会
    /// 被重复覆盖。
    fn migrate_legacy_sequences(
        store: &LmdbStore,
        sequences: &DashMap<String, Arc<AtomicU64>>,
    ) -> Option<EventId> {
        let entries = match store.scan_prefix("event:") {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("failed to scan legacy event log: {e}, starting from 0");
                return None;
            }
        };

        let mut max_by_topic: HashMap<String, u64> = HashMap::new();
        let mut max_event: Option<EventId> = None;
        for (_, value) in entries {
            match postcard::from_bytes::<LogEntry>(&value) {
                Ok(entry) => {
                    if max_event.map(|current| entry.id > current).unwrap_or(true) {
                        max_event = Some(entry.id);
                    }
                    if sequences.contains_key(entry.topic.as_str()) {
                        continue;
                    }
                    let seq = max_by_topic.entry(entry.topic).or_insert(0);
                    if entry.id.sequence > *seq {
                        *seq = entry.id.sequence;
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "ignoring corrupt legacy event entry");
                }
            }
        }

        let mut batch: Vec<(String, Vec<u8>)> = max_by_topic
            .iter()
            .map(|(topic, seq)| (Self::seq_key(topic), seq.to_le_bytes().to_vec()))
            .collect();
        // 完成标记与缺失序号同批提交：要么都写入，要么都不写入。
        batch.push((SEQ_MIGRATED_KEY.to_string(), vec![1]));

        if let Err(e) = store.put_batch(&batch) {
            // 迁移失败不阻止启动：内存中的序号仍然正确，下次启动会重试。
            tracing::warn!(error = %e, "failed to persist migrated event sequence metadata");
            for (topic, seq) in max_by_topic {
                sequences.insert(topic, Arc::new(AtomicU64::new(seq)));
            }
            return max_event;
        }

        let migrated = max_by_topic.len();
        for (topic, seq) in max_by_topic {
            sequences.insert(topic, Arc::new(AtomicU64::new(seq)));
        }
        tracing::info!(
            topics = migrated,
            total_topics = sequences.len(),
            "migrated missing event sequence metadata from existing event log"
        );
        max_event
    }

    fn seq_key(topic: &str) -> String {
        format!("{SEQ_KEY_PREFIX}{topic}")
    }

    fn hidden_seq_key(topic: &str) -> String {
        format!("{HIDDEN_SEQ_KEY_PREFIX}{topic}")
    }

    fn key(topic: &str, id: &EventId) -> String {
        format!(
            "event:{}:{:016x}:{:08x}:{:016x}",
            topic,
            id.timestamp.wall_time(),
            id.timestamp.logical(),
            id.sequence
        )
    }

    fn prefix(topic: &str) -> String {
        format!("event:{}:", topic)
    }
}

impl EventLog for LmdbEventLog {
    fn append(&self, topic: &str, payload: &[u8]) -> Result<EventId> {
        if topic.is_empty() {
            return Err(ActantError::Config("event topic must not be empty".into()));
        }

        let timestamp = self.clock.tick();
        // 取号只在分片锁内做一次原子自增，随后立刻释放：提交要落盘（Sync 模式
        // 下是 fsync），若仍持着分片锁，同一分片内**其他 topic** 的 append 会被
        // 这次磁盘 IO 阻塞。
        let counter = self
            .sequences
            .entry(topic.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone();
        let sequence = counter.fetch_add(1, Ordering::SeqCst) + 1;
        let id = EventId {
            timestamp,
            sequence,
        };
        let entry = LogEntry {
            id,
            topic: topic.to_string(),
            payload: payload.to_vec(),
        };
        let key = Self::key(topic, &id);
        let value =
            postcard::to_allocvec(&entry).map_err(|e| ActantError::Serialization(e.to_string()))?;
        // meta 写「当前已分配的最大序号」而非本次序号：并发下较小的序号可能后
        // 提交，若写自己的序号会让 meta 回退，重启后重复分配。写入已分配上界后
        // meta 单调不减；崩溃时最多留下序号空洞（不冲突，读取按 key 前缀扫描）。
        // 上界在**提交锁内**重读计数器：此时所有更早分配的序号已计数在案，而
        // 更晚分配的序号必然在本提交之后落盘——meta 因此严格单调，不受并发
        // 提交乱序影响。
        let commit_lock = self
            .commit_locks
            .entry(topic.to_string())
            .or_default()
            .clone();
        let _commit = commit_lock.lock();
        let allocated_floor = counter.load(Ordering::SeqCst).max(sequence);
        // 事件条目与序号元数据在同一事务提交：崩溃时不会出现事件已落盘
        // 但序号未推进，重启后重复分配同一 sequence。
        self.store.put_batch(&[
            (key, value),
            (Self::seq_key(topic), allocated_floor.to_le_bytes().to_vec()),
        ])?;
        Ok(id)
    }

    fn append_batch(&self, topic: &str, payloads: &[Vec<u8>]) -> Result<Vec<EventId>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        if topic.is_empty() {
            return Err(ActantError::Config("event topic must not be empty".into()));
        }

        let counter = self
            .sequences
            .entry(topic.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone();
        // 一次性预留整批序号；失败最多留下序号空洞，不会重复分配。
        let first = counter.fetch_add(payloads.len() as u64, Ordering::SeqCst) + 1;
        let mut ids = Vec::with_capacity(payloads.len());
        let mut batch = Vec::with_capacity(payloads.len() + 1);
        for (offset, payload) in payloads.iter().enumerate() {
            let timestamp = self.clock.tick();
            let sequence = first + offset as u64;
            let id = EventId {
                timestamp,
                sequence,
            };
            let entry = LogEntry {
                id,
                topic: topic.to_string(),
                payload: payload.clone(),
            };
            let value = postcard::to_allocvec(&entry)
                .map_err(|e| ActantError::Serialization(e.to_string()))?;
            batch.push((Self::key(topic, &id), value));
            ids.push(id);
        }
        // 与 append 相同：提交锁内重读计数器作为 meta 上界，保证并发提交
        // 乱序时 meta 单调不减；meta 条目必须在锁内、用重算后的上界构造。
        let commit_lock = self
            .commit_locks
            .entry(topic.to_string())
            .or_default()
            .clone();
        let _commit = commit_lock.lock();
        let allocated_floor =
            (first + payloads.len() as u64 - 1).max(counter.load(Ordering::SeqCst));
        batch.push((Self::seq_key(topic), allocated_floor.to_le_bytes().to_vec()));
        // 整批事件与序号元数据同事务提交：崩溃后不存在“部分事件可见”。
        self.store.put_batch(&batch)?;
        Ok(ids)
    }

    fn read_after(&self, topic: &str, after: Option<&EventId>) -> Result<Vec<LogEntry>> {
        let prefix = Self::prefix(topic);
        let mut cursor = after.map(|id| Self::key(topic, id));
        let mut result = Vec::new();
        loop {
            let page = self
                .store
                .scan_prefix_page(&prefix, cursor.as_deref(), EVENT_PAGE_SIZE)?;
            let Some((last_key, _)) = page.last() else {
                break;
            };
            cursor = Some(last_key.clone());

            for (_, value) in &page {
                let entry: LogEntry = postcard::from_bytes(value)
                    .map_err(|e| ActantError::Serialization(e.to_string()))?;
                result.push(entry);
            }
            if page.len() < EVENT_PAGE_SIZE {
                break;
            }
        }
        Ok(result)
    }

    fn latest(&self, topic: &str) -> Result<Option<LogEntry>> {
        let prefix = Self::prefix(topic);
        match self.store.scan_prefix_last(&prefix)? {
            Some((_, value)) => {
                let entry: LogEntry = postcard::from_bytes(&value)
                    .map_err(|e| ActantError::Serialization(e.to_string()))?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    fn count(&self, topic: &str) -> Result<usize> {
        let prefix = Self::prefix(topic);
        let mut cursor: Option<String> = None;
        let mut count = 0usize;
        loop {
            let page = self
                .store
                .scan_prefix_page(&prefix, cursor.as_deref(), EVENT_PAGE_SIZE)?;
            let Some((last_key, _)) = page.last() else {
                break;
            };
            cursor = Some(last_key.clone());
            count += page.len();
            if page.len() < EVENT_PAGE_SIZE {
                break;
            }
        }
        Ok(count)
    }

    fn list_topics(&self) -> Result<Vec<String>> {
        let mut topics: Vec<String> = self
            .store
            .scan_prefix(SEQ_KEY_PREFIX)?
            .into_iter()
            .filter_map(|(key, _)| key.strip_prefix(SEQ_KEY_PREFIX).map(str::to_string))
            .collect();
        topics.sort();
        topics.dedup();
        Ok(topics)
    }

    fn trim_absorbed(&self, topic: &str, up_to: &EventId, keep: usize) -> Result<usize> {
        if keep == 0 {
            return Ok(0);
        }
        let prefix = Self::prefix(topic);
        let up_to_key = Self::key(topic, up_to);

        // 只收集被吸收的 key（不加载 payload），并在越过水位后立即停止。
        let mut absorbed: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;
        'outer: loop {
            let page = self
                .store
                .scan_prefix_page(&prefix, cursor.as_deref(), EVENT_PAGE_SIZE)?;
            let Some((last_key, _)) = page.last() else {
                break;
            };
            let page_len = page.len();
            cursor = Some(last_key.clone());
            for (key, _) in page {
                if key.as_str() > up_to_key.as_str() {
                    break 'outer;
                }
                absorbed.push(key);
            }
            if page_len < EVENT_PAGE_SIZE {
                break;
            }
        }

        let excess = absorbed.len().saturating_sub(keep);
        if excess == 0 {
            return Ok(0);
        }
        // 区间删除是单事务提交：逐 key delete 会为每条记录触发一次 fsync。
        let cutoff = &absorbed[excess - 1];
        self.store.delete_range(&prefix, cutoff)
    }

    fn hide_topic(&self, topic: &str) -> Result<()> {
        // list_topics 从 SEQ 键扫描，把该键**移入** hidden 命名空间即可让
        // topic 退出恢复期的扫描结果；事件条目全部保留，审计面仍可按 topic
        // 直读。不能直接删除 seq 键：重启恢复的快路径只装载 seq 元数据，
        // 删除会让同 id 重用时序号从 1 重新计数（违背既有恢复契约），且
        // 时钟下界不再覆盖该 topic 的历史事件——回拨时钟时新事件键会排进
        // 旧代事件中间。先写 hidden 键再删正常键：两步之间崩溃时两键并存，
        // 恢复装载以正常键优先，序号不丢。同 id 重用的 append 重写正常
        // seq 键后，残留的 hidden 键被恢复装载的优先级自然覆盖，无害。
        let sequence = self
            .sequences
            .get(topic)
            .map(|counter| counter.load(Ordering::SeqCst))
            .unwrap_or(0);
        self.store
            .put_batch(&[(Self::hidden_seq_key(topic), sequence.to_le_bytes().to_vec())])?;
        self.store.delete(&Self::seq_key(topic))?;
        Ok(())
    }
}

/// 内存实现的 EventLog，仅用于测试。
#[derive(Default)]
pub struct MemoryEventLog {
    inner: std::sync::Arc<parking_lot::Mutex<HashMap<String, Vec<LogEntry>>>>,
    clock: HybridLogicalClock,
    sequences: std::sync::Arc<parking_lot::Mutex<HashMap<String, u64>>>,
    /// 已淘汰 topic：仍有事件数据，但不再出现在 `list_topics` 中。
    hidden: parking_lot::Mutex<HashSet<String>>,
}

impl EventLog for MemoryEventLog {
    fn append(&self, topic: &str, payload: &[u8]) -> Result<EventId> {
        if topic.is_empty() {
            return Err(ActantError::Config("event topic must not be empty".into()));
        }
        // 与 LMDB 实现对齐：追加会重新写出 topic 的序号元数据，因此同一
        // workflow_id 被重用时 topic 必须重新出现在 list_topics 中。
        self.hidden.lock().remove(topic);
        let timestamp = self.clock.tick();
        let sequence = {
            let mut seqs = self.sequences.lock();
            let next = seqs.get(topic).copied().unwrap_or(0) + 1;
            seqs.insert(topic.to_string(), next);
            next
        };
        let id = EventId {
            timestamp,
            sequence,
        };
        let entry = LogEntry {
            id,
            topic: topic.to_string(),
            payload: payload.to_vec(),
        };
        let mut inner = self.inner.lock();
        inner.entry(topic.to_string()).or_default().push(entry);
        Ok(id)
    }

    fn read_after(&self, topic: &str, after: Option<&EventId>) -> Result<Vec<LogEntry>> {
        let inner = self.inner.lock();
        let entries = inner.get(topic).cloned().unwrap_or_default();
        Ok(match after {
            Some(after_id) => entries.into_iter().filter(|e| e.id > *after_id).collect(),
            None => entries,
        })
    }

    fn append_batch(&self, topic: &str, payloads: &[Vec<u8>]) -> Result<Vec<EventId>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        if topic.is_empty() {
            return Err(ActantError::Config("event topic must not be empty".into()));
        }
        self.hidden.lock().remove(topic);
        let mut seqs = self.sequences.lock();
        let mut next = seqs.get(topic).copied().unwrap_or(0);
        let mut ids = Vec::with_capacity(payloads.len());
        let mut inner = self.inner.lock();
        let entries = inner.entry(topic.to_string()).or_default();
        for payload in payloads {
            next += 1;
            let id = EventId {
                timestamp: self.clock.tick(),
                sequence: next,
            };
            entries.push(LogEntry {
                id,
                topic: topic.to_string(),
                payload: payload.clone(),
            });
            ids.push(id);
        }
        seqs.insert(topic.to_string(), next);
        Ok(ids)
    }

    fn latest(&self, topic: &str) -> Result<Option<LogEntry>> {
        let inner = self.inner.lock();
        Ok(inner.get(topic).and_then(|v| v.last().cloned()))
    }

    fn count(&self, topic: &str) -> Result<usize> {
        let inner = self.inner.lock();
        Ok(inner.get(topic).map(|v| v.len()).unwrap_or(0))
    }

    fn list_topics(&self) -> Result<Vec<String>> {
        let hidden = self.hidden.lock();
        let mut topics: Vec<String> = self
            .inner
            .lock()
            .keys()
            .filter(|topic| !hidden.contains(topic.as_str()))
            .cloned()
            .collect();
        topics.sort();
        Ok(topics)
    }

    fn trim_absorbed(&self, topic: &str, up_to: &EventId, keep: usize) -> Result<usize> {
        if keep == 0 {
            return Ok(0);
        }
        let mut inner = self.inner.lock();
        let Some(entries) = inner.get_mut(topic) else {
            return Ok(0);
        };
        // 追加顺序即升序，前面的最旧。
        let absorbed = entries.iter().filter(|e| e.id <= *up_to).count();
        let excess = absorbed.saturating_sub(keep);
        if excess == 0 {
            return Ok(0);
        }
        entries.drain(..excess);
        Ok(excess)
    }

    fn hide_topic(&self, topic: &str) -> Result<()> {
        self.hidden.lock().insert(topic.to_string());
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../../../tests/rust/unit/runtime/state/event_log.rs"]
mod tests;
