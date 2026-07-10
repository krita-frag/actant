//! EventLog 抽象 — State 四盒之一的事件溯源层。
//!
//! 提供按 topic 分区的 append-only 事件流，支持：
//! - 追加事件（带 HLC 时间戳与 topic 内序列号）
//! - 按时间戳/序列号读取后续事件
//! - 读取最新事件
//!
//! 底层基于 `LmdbStore`（LMDB），事件 ID 全局唯一且可排序。

use std::collections::HashMap;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::common::{ActantError, Result};
use crate::runtime::state::{HybridLogicalClock, LmdbStore};

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
    fn append(&self, topic: &str, payload: &[u8]) -> Result<EventId>;

    /// 读取指定 topic 在 `after` 之后的所有事件。
    ///
    /// `after = None` 表示从头读取。
    fn read_after(&self, topic: &str, after: Option<&EventId>) -> Result<Vec<LogEntry>>;

    /// 读取指定 topic 的最新一条事件。
    fn latest(&self, topic: &str) -> Result<Option<LogEntry>>;

    /// 返回指定 topic 的事件总数（仅用于测试/调试）。
    fn count(&self, topic: &str) -> Result<usize>;
}

/// 基于 LMDB `LmdbStore` 的 EventLog 实现。
pub struct LmdbEventLog {
    store: LmdbStore,
    clock: HybridLogicalClock,
    sequences: DashMap<String, u64>,
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

    /// 扫描已持久化的事件，恢复每个 topic 的最大序列号。
    fn recover_sequences(store: &LmdbStore) -> DashMap<String, u64> {
        let sequences = DashMap::new();
        match store.scan_prefix("event:") {
            Ok(entries) => {
                for (_, value) in entries {
                    if let Ok(entry) = postcard::from_bytes::<LogEntry>(&value) {
                        let mut seq = sequences.entry(entry.topic).or_insert(0);
                        if entry.id.sequence > *seq {
                            *seq = entry.id.sequence;
                        }
                    }
                }
                if !sequences.is_empty() {
                    tracing::info!(
                        "recovered event log sequences for {} topic(s)",
                        sequences.len()
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    "failed to recover event log sequences: {e}, starting from 0"
                );
            }
        }
        sequences
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
        let sequence = {
            let mut entry = self.sequences.entry(topic.to_string()).or_insert(0);
            *entry += 1;
            *entry
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
        let key = Self::key(topic, &id);
        let value =
            postcard::to_allocvec(&entry).map_err(|e| ActantError::Serialization(e.to_string()))?;
        self.store.put(&key, &value)?;
        Ok(id)
    }

    fn read_after(&self, topic: &str, after: Option<&EventId>) -> Result<Vec<LogEntry>> {
        let prefix = Self::prefix(topic);
        let entries = self.store.scan_prefix(&prefix)?;
        let mut result = Vec::with_capacity(entries.len());
        for (_, value) in entries {
            let entry: LogEntry = postcard::from_bytes(&value)
                .map_err(|e| ActantError::Serialization(e.to_string()))?;
            if let Some(after_id) = after {
                if entry.id <= *after_id {
                    continue;
                }
            }
            result.push(entry);
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
        let entries = self.store.scan_prefix(&prefix)?;
        Ok(entries.len())
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn lmdb_event_log_append_and_read() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let log = LmdbEventLog::new(store);

        let id1 = log.append("workflow", b"e1").unwrap();
        let id2 = log.append("workflow", b"e2").unwrap();

        assert!(id2 > id1);
        let entries = log.read_after("workflow", None).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].payload, b"e1");
        assert_eq!(entries[1].payload, b"e2");
    }

    #[test]
    fn lmdb_event_log_read_after_filters() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let log = LmdbEventLog::new(store);

        let id1 = log.append("task", b"a").unwrap();
        log.append("task", b"b").unwrap();
        log.append("task", b"c").unwrap();

        let entries = log.read_after("task", Some(&id1)).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].payload, b"b");
        assert_eq!(entries[1].payload, b"c");
    }

    #[test]
    fn lmdb_event_log_latest() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let log = LmdbEventLog::new(store);

        assert!(log.latest("empty").unwrap().is_none());
        log.append("empty", b"x").unwrap();
        let latest = log.latest("empty").unwrap().unwrap();
        assert_eq!(latest.payload, b"x");
    }

    #[test]
    fn lmdb_event_log_topics_isolated() {
        let dir = tempdir().unwrap();
        let store = LmdbStore::open(dir.path()).unwrap();
        let log = LmdbEventLog::new(store);

        log.append("a", b"1").unwrap();
        log.append("b", b"2").unwrap();

        assert_eq!(log.count("a").unwrap(), 1);
        assert_eq!(log.count("b").unwrap(), 1);
        assert_eq!(log.count("c").unwrap(), 0);
    }

    #[test]
    fn memory_event_log_basic() {
        let log = MemoryEventLog::default();
        let id = log.append("x", b"y").unwrap();
        assert_eq!(log.latest("x").unwrap().unwrap().id, id);
        assert_eq!(log.read_after("x", None).unwrap().len(), 1);
    }

    #[test]
    fn lmdb_event_log_sequence_recovers_across_reopen() {
        // 跨重启追加同 topic 事件时，sequence 必须从已持久化的最大值继续，
        // 而非从 0 重新计数，否则 EventId 可能与已有事件冲突。
        let dir = tempdir().unwrap();

        // 第一次打开：追加 3 条事件到 topic "wf"
        let store = LmdbStore::open(dir.path()).unwrap();
        let log = LmdbEventLog::new(store);
        let id1 = log.append("wf", b"e1").unwrap();
        let id2 = log.append("wf", b"e2").unwrap();
        let id3 = log.append("wf", b"e3").unwrap();
        assert_eq!(id1.sequence, 1);
        assert_eq!(id2.sequence, 2);
        assert_eq!(id3.sequence, 3);

        // 显式释放第一个 LMDB env，否则同路径重开会报 EnvAlreadyOpened
        drop(log);

        // 重新打开同一存储：sequence 应从 4 开始
        let store2 = LmdbStore::open(dir.path()).unwrap();
        let log2 = LmdbEventLog::new(store2);
        let id4 = log2.append("wf", b"e4").unwrap();
        assert_eq!(
            id4.sequence, 4,
            "sequence must continue from persisted max, not reset to 1"
        );

        // read_after(id3) 应只返回新追加的事件
        let entries = log2.read_after("wf", Some(&id3)).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].payload, b"e4");
    }
}
