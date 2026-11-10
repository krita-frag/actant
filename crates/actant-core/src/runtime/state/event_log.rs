//! EventLog 抽象 — State 四盒之一的事件溯源层。
//!
//! 提供按 topic 分区的 append-only 事件流，支持：
//! - 追加事件（带 HLC 时间戳与 topic 内序列号）
//! - 按时间戳/序列号读取后续事件
//! - 读取最新事件
//!
//! 底层基于 `LmdbStore`（LMDB），事件 ID 全局唯一且可排序。

use std::collections::HashMap;
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
}

/// 基于 LMDB `LmdbStore` 的 EventLog 实现。
pub struct LmdbEventLog {
    store: LmdbStore,
    clock: HybridLogicalClock,
    /// 每个 topic 的**已分配**序列号下界。值用原子计数器承载，取号只做一次
    /// 原子自增，不与 LMDB 提交共享临界区。
    sequences: DashMap<String, Arc<AtomicU64>>,
}

impl LmdbEventLog {
    /// 打开或创建基于给定 `LmdbStore` 的 EventLog。
    ///
    /// 启动时扫描已持久化的事件，恢复每个 topic 的最大序列号，
    /// 防止跨重启追加同 topic 事件时 sequence 回绕导致 EventId 冲突。
    pub(crate) fn new(store: LmdbStore) -> Self {
        let sequences = Self::recover_sequences(&store);
        Self {
            store,
            clock: HybridLogicalClock::new(),
            sequences,
        }
    }

    /// 扫描已持久化的序号元数据，恢复每个 topic 的最大序列号。
    ///
    /// 序号与事件条目同事务写入 `meta:event_seq:{topic}`，因此正常路径
    /// 只需 O(topic 数) 次读取。存量库没有这些元数据时，退回一次全量
    /// 事件扫描并补写，后续启动即走快路径。
    fn recover_sequences(store: &LmdbStore) -> DashMap<String, Arc<AtomicU64>> {
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

        if sequences.is_empty() {
            Self::migrate_legacy_sequences(store, &sequences);
        }
        sequences
    }

    /// 为没有序号元数据的存量库做一次性迁移。
    ///
    /// 迁移在一个写事务内补写全部 topic 的序号，避免逐个 topic 提交。
    fn migrate_legacy_sequences(store: &LmdbStore, sequences: &DashMap<String, Arc<AtomicU64>>) {
        let entries = match store.scan_prefix("event:") {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("failed to scan legacy event log: {e}, starting from 0");
                return;
            }
        };

        let mut max_by_topic: HashMap<String, u64> = HashMap::new();
        for (_, value) in entries {
            match postcard::from_bytes::<LogEntry>(&value) {
                Ok(entry) => {
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

        if max_by_topic.is_empty() {
            return;
        }

        let batch: Vec<(String, Vec<u8>)> = max_by_topic
            .iter()
            .map(|(topic, seq)| (Self::seq_key(topic), seq.to_le_bytes().to_vec()))
            .collect();
        if let Err(e) = store.put_batch(&batch) {
            // 迁移失败不阻止启动：内存中的序号仍然正确，下次启动会重试。
            tracing::warn!(error = %e, "failed to persist migrated event sequence metadata");
        }
        for (topic, seq) in max_by_topic {
            sequences.insert(topic, Arc::new(AtomicU64::new(seq)));
        }
        tracing::info!(
            topics = sequences.len(),
            "migrated event sequence metadata from existing event log"
        );
    }

    fn seq_key(topic: &str) -> String {
        format!("{SEQ_KEY_PREFIX}{topic}")
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
        // 元数据写「当前已分配的最大序号」而非本次序号：并发下较小的序号可能后
        // 提交，若写自己的序号会让 meta 回退，重启后重复分配。写入已分配上界后
        // meta 单调不减；崩溃时最多留下序号空洞（不冲突，读取按 key 前缀扫描）。
        let allocated_floor = counter.load(Ordering::SeqCst).max(sequence);
        // 事件条目与序号元数据在同一事务提交：崩溃时不会出现事件已落盘
        // 但序号未推进，重启后重复分配同一 sequence。
        self.store.put_batch(&[
            (key, value),
            (Self::seq_key(topic), allocated_floor.to_le_bytes().to_vec()),
        ])?;
        Ok(id)
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
}

/// 内存实现的 EventLog，仅用于测试。
#[derive(Default)]
pub struct MemoryEventLog {
    inner: std::sync::Arc<parking_lot::Mutex<HashMap<String, Vec<LogEntry>>>>,
    clock: HybridLogicalClock,
    sequences: std::sync::Arc<parking_lot::Mutex<HashMap<String, u64>>>,
}

impl EventLog for MemoryEventLog {
    fn append(&self, topic: &str, payload: &[u8]) -> Result<EventId> {
        if topic.is_empty() {
            return Err(ActantError::Config("event topic must not be empty".into()));
        }
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

    fn latest(&self, topic: &str) -> Result<Option<LogEntry>> {
        let inner = self.inner.lock();
        Ok(inner.get(topic).and_then(|v| v.last().cloned()))
    }

    fn count(&self, topic: &str) -> Result<usize> {
        let inner = self.inner.lock();
        Ok(inner.get(topic).map(|v| v.len()).unwrap_or(0))
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
}

#[cfg(test)]
#[path = "../../../../../tests/rust/unit/runtime/state/event_log.rs"]
mod tests;
