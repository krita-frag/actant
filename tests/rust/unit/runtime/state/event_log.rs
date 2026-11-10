//! Unit tests extracted from `src/runtime/state/event_log.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

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

// ───────────────────────── 序号元数据与迁移 ─────────────────────────

#[test]
fn lmdb_event_log_persists_sequence_metadata_atomically() {
    let dir = tempdir().unwrap();
    let store = LmdbStore::open(dir.path()).unwrap();
    let log = LmdbEventLog::new(store.clone());

    let id = log.append("wf", b"e1").unwrap();
    let raw = store
        .get(&LmdbEventLog::seq_key("wf"))
        .unwrap()
        .expect("sequence metadata must be written together with the event");
    assert_eq!(raw, id.sequence.to_le_bytes().to_vec());
}

#[test]
fn lmdb_event_log_migrates_legacy_events_without_meta() {
    let dir = tempdir().unwrap();
    let store = LmdbStore::open(dir.path()).unwrap();

    // 模拟旧版本写入：只有事件条目，没有 meta:event_seq。
    for sequence in 1..=3u64 {
        let entry = LogEntry {
            id: EventId {
                timestamp: crate::runtime::state::HlcTimestamp::zero(),
                sequence,
            },
            topic: "legacy".into(),
            payload: vec![sequence as u8],
        };
        let value = postcard::to_allocvec(&entry).unwrap();
        store
            .put(&LmdbEventLog::key("legacy", &entry.id), &value)
            .unwrap();
    }
    assert!(store
        .get(&LmdbEventLog::seq_key("legacy"))
        .unwrap()
        .is_none());

    let log = LmdbEventLog::new(store.clone());
    let next = log.append("legacy", b"next").unwrap();
    assert_eq!(next.sequence, 4, "legacy events must be recovered by scan");
    assert!(
        store
            .get(&LmdbEventLog::seq_key("legacy"))
            .unwrap()
            .is_some(),
        "migration must persist sequence metadata for future starts"
    );
}

#[test]
fn lmdb_event_log_read_after_spans_pages() {
    let dir = tempdir().unwrap();
    let config = crate::common::StoreConfig {
        data_dir: None,
        sync_mode: crate::common::SyncMode::NoSync,
        ..Default::default()
    };
    let store = LmdbStore::open_with_config(dir.path(), &config).unwrap();
    let log = LmdbEventLog::new(store);

    let total = EVENT_PAGE_SIZE + 5;
    for i in 0..total {
        log.append("paged", format!("e{i}").as_bytes()).unwrap();
    }

    let entries = log.read_after("paged", None).unwrap();
    assert_eq!(entries.len(), total);
    assert_eq!(entries[0].payload, b"e0");
    assert_eq!(
        entries[total - 1].payload,
        format!("e{}", total - 1).into_bytes()
    );
    assert_eq!(log.count("paged").unwrap(), total);
}

#[test]
fn lmdb_event_log_trim_keeps_watermark_tail() {
    let dir = tempdir().unwrap();
    let config = crate::common::StoreConfig {
        data_dir: None,
        sync_mode: crate::common::SyncMode::NoSync,
        ..Default::default()
    };
    let store = LmdbStore::open_with_config(dir.path(), &config).unwrap();
    let log = LmdbEventLog::new(store);

    let ids: Vec<_> = (0..10)
        .map(|i| log.append("trim", format!("e{i}").as_bytes()).unwrap())
        .collect();
    let up_to = ids[5];
    let removed = log.trim_absorbed("trim", &up_to, 2).unwrap();
    assert_eq!(
        removed, 4,
        "only the excess oldest absorbed events are removed"
    );
    assert_eq!(log.count("trim").unwrap(), 6);

    let remaining = log.read_after("trim", None).unwrap();
    assert_eq!(remaining[0].payload, b"e4");
    assert_eq!(remaining.len(), 6);

    // 水位之后的条目永不裁剪。
    assert_eq!(log.latest("trim").unwrap().unwrap().payload, b"e9");
}
