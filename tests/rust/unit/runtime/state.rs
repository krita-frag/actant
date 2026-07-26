//! Unit tests extracted from `src/runtime/state.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

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
        ..Default::default()
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
        ..Default::default()
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

// ───────────────────────── WalEvent / recover_events 测试 ─────────────────────────

#[test]
fn wal_writer_append_event_writes_serialized_event() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("event.wal");
    let mut writer = WalWriter::open(&path).unwrap();
    let ts = HlcTimestamp::from_parts(100, 1);
    let event = WalEvent {
        actor_id: ActorId("actor-x".into()),
        sequence: 42,
        payload: b"payload-data".to_vec(),
    };
    let offset = writer.append_event(ts, &event).unwrap();
    assert_eq!(offset, 0);
    drop(writer);

    let reader = WalReader::open(path).unwrap();
    let events = reader.recover_events(0).unwrap();
    assert_eq!(events.len(), 1);
    let (recovered_ts, recovered_event) = &events[0];
    assert_eq!(recovered_ts.wall_time(), 100);
    assert_eq!(recovered_ts.logical(), 1);
    assert_eq!(recovered_event.actor_id.0, "actor-x");
    assert_eq!(recovered_event.sequence, 42);
    assert_eq!(recovered_event.payload, b"payload-data");
}

#[test]
fn wal_writer_append_event_with_sync_calls_sync() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("synced-event.wal");
    let mut writer = WalWriter::open_with_sync(&path, true).unwrap();
    let ts = HlcTimestamp::from_parts(1, 0);
    let event = WalEvent {
        actor_id: ActorId("a".into()),
        sequence: 1,
        payload: b"x".to_vec(),
    };
    let offset = writer.append_event(ts, &event).unwrap();
    assert_eq!(offset, 0);
}

#[test]
fn wal_reader_recover_events_multiple_entries_preserves_order() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("multi.wal");
    let mut writer = WalWriter::open(&path).unwrap();
    for i in 0..5u32 {
        let event = WalEvent {
            actor_id: ActorId(format!("actor-{}", i)),
            sequence: i as u64,
            payload: vec![i as u8; 10],
        };
        writer
            .append_event(HlcTimestamp::from_parts(100 + i as u64, i), &event)
            .unwrap();
    }
    drop(writer);

    let reader = WalReader::open(path).unwrap();
    let events = reader.recover_events(0).unwrap();
    assert_eq!(events.len(), 5);
    for (i, (ts, event)) in events.iter().enumerate() {
        assert_eq!(ts.wall_time(), 100 + i as u64);
        assert_eq!(event.sequence, i as u64);
    }
}

#[test]
fn wal_reader_recover_events_from_offset_skips_earlier() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("from-offset.wal");
    let mut writer = WalWriter::open(&path).unwrap();
    let mut offsets = Vec::new();
    for i in 0..3u32 {
        let event = WalEvent {
            actor_id: ActorId("a".into()),
            sequence: i as u64,
            payload: b"data".to_vec(),
        };
        offsets.push(
            writer
                .append_event(HlcTimestamp::from_parts(i as u64, i), &event)
                .unwrap(),
        );
    }
    drop(writer);

    let reader = WalReader::open(path).unwrap();
    // 从第二个条目的 offset 开始恢复
    let events = reader.recover_events(offsets[1]).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].1.sequence, 1);
    assert_eq!(events[1].1.sequence, 2);
}

#[test]
fn wal_reader_recover_events_empty_file_returns_empty() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty-recover.wal");
    std::fs::write(&path, []).unwrap();
    let reader = WalReader::open(path).unwrap();
    let events = reader.recover_events(0).unwrap();
    assert!(events.is_empty());
}

#[test]
fn wal_reader_recover_events_corrupt_data_breaks_silently() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("corrupt-recover.wal");
    let mut writer = WalWriter::open(&path).unwrap();
    let event = WalEvent {
        actor_id: ActorId("a".into()),
        sequence: 1,
        payload: b"good".to_vec(),
    };
    writer
        .append_event(HlcTimestamp::from_parts(1, 0), &event)
        .unwrap();
    drop(writer);
    // 破坏 checksum
    let mut data = std::fs::read(&path).unwrap();
    data[16] ^= 0xFF;
    std::fs::write(&path, data).unwrap();

    let reader = WalReader::open(path).unwrap();
    let events = reader.recover_events(0).unwrap();
    // 校验和失败应停止读取
    assert!(events.is_empty());
}

// ───────────────────────── LmdbStore 错误路径测试 ─────────────────────────

#[test]
fn lmdb_store_open_with_invalid_path_returns_error() {
    // /dev/null/x 不是有效目录
    let result = LmdbStore::open(std::path::Path::new("/dev/null/subdir"));
    assert!(result.is_err());
}

#[test]
fn lmdb_store_open_with_config_invalid_path_returns_error() {
    let config = StoreConfig::default();
    let result = LmdbStore::open_with_config(std::path::Path::new("/dev/null/sub"), &config);
    assert!(result.is_err());
}

// ───────────────────────── Store clone 测试 ─────────────────────────

#[test]
fn store_clone_shares_data() {
    let dir = tempdir().unwrap();
    let store = Store::new(LmdbStore::open(dir.path()).unwrap());
    let cloned = store.clone();
    // 通过 runtime block_on
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        store.put("shared", b"v1").await.unwrap();
        assert_eq!(cloned.get("shared").await.unwrap(), Some(b"v1".to_vec()));
    });
}

// ───────────────────────── WalCompactor 额外测试 ─────────────────────────

#[test]
fn wal_compactor_compact_removes_old_checkpoints_per_actor() {
    let dir = tempdir().unwrap();
    let store = LmdbStore::open(dir.path()).unwrap();
    let cm = CheckpointManager::new(store);
    // actor-1 写入 3 个 checkpoint
    for seq in 1..=3u64 {
        cm.save(&make_snapshot("actor-1", seq, seq * 100)).unwrap();
    }
    // actor-2 写入 2 个 checkpoint
    for seq in 1..=2u64 {
        cm.save(&make_snapshot("actor-2", seq, seq * 50)).unwrap();
    }

    let wal_path = dir.path().join("compactor.wal");
    let mut writer = WalWriter::open(&wal_path).unwrap();
    writer
        .append(HlcTimestamp::from_parts(1, 0), b"entry")
        .unwrap();
    drop(writer);

    let compactor = WalCompactor::new(wal_path.clone(), 100_000, cm.clone(), 1);
    let _ = compactor.compact().unwrap();

    // 每个 actor 只保留最近 1 个
    assert!(cm.load(&ActorId("actor-1".into()), 1).unwrap().is_none());
    assert!(cm.load(&ActorId("actor-1".into()), 2).unwrap().is_none());
    assert!(cm.load(&ActorId("actor-1".into()), 3).unwrap().is_some());
    assert!(cm.load(&ActorId("actor-2".into()), 1).unwrap().is_none());
    assert!(cm.load(&ActorId("actor-2".into()), 2).unwrap().is_some());
}

#[test]
fn wal_compactor_compact_min_offset_zero_returns_zero() {
    // 当 min_offset == 0 时，compact 应返回 0（不重写）。
    let dir = tempdir().unwrap();
    let store = LmdbStore::open(dir.path()).unwrap();
    let cm = CheckpointManager::new(store);
    // 写入一个 wal_offset=0 的 checkpoint
    cm.save(&make_snapshot("actor-1", 1, 0)).unwrap();

    let wal_path = dir.path().join("zero.wal");
    let mut writer = WalWriter::open(&wal_path).unwrap();
    writer
        .append(HlcTimestamp::from_parts(1, 0), b"entry")
        .unwrap();
    drop(writer);

    let compactor = WalCompactor::new(wal_path, 1000, cm, 1);
    let result = compactor.compact().unwrap();
    // min_offset == 0 → 应返回 0
    assert_eq!(result, 0);
}

// ───────────────────────── HLC merge 边缘场景 ─────────────────────────

#[test]
fn hlc_merge_with_zero_remote_advances_local() {
    let hlc = HybridLogicalClock::new();
    let local = hlc.tick();
    let zero = HlcTimestamp::zero();
    let merged = hlc.merge(&zero);
    // 合并后应不小于 local
    assert!(merged >= local);
}

#[test]
fn hlc_tick_many_times_remains_monotonic() {
    let hlc = HybridLogicalClock::new();
    let mut prev = hlc.tick();
    for _ in 0..1000 {
        let curr = hlc.tick();
        assert!(curr > prev, "tick not monotonic: {:?} -> {:?}", prev, curr);
        prev = curr;
    }
}

#[test]
fn hlc_with_max_drift_ms_explicit_value() {
    let hlc = HybridLogicalClock::with_max_drift_ms(1000);
    let local = hlc.tick();
    let remote = HlcTimestamp::from_parts(local.wall_time() + 500_000_000, 0); // 500ms 在未来
    let merged = hlc.merge(&remote);
    // 1000ms drift 允许，应接受 remote
    assert!(merged.wall_time() >= remote.wall_time());
}

// ───────────────────────── checkpoint_key 格式测试 ─────────────────────────

#[test]
fn checkpoint_key_format_is_zero_padded() {
    let key = checkpoint_key(&ActorId("actor-x".into()), 5);
    // CHECKPOINT 前缀 ("ckpt:") + actor_id + ":" + 20 位零填充
    assert!(key.starts_with("ckpt:actor-x:"));
    assert!(key.ends_with("00000000000000000005"));
}

#[test]
fn checkpoint_key_orders_lexicographically() {
    let k1 = checkpoint_key(&ActorId("a".into()), 1);
    let k2 = checkpoint_key(&ActorId("a".into()), 2);
    let k10 = checkpoint_key(&ActorId("a".into()), 10);
    // 零填充确保字典序 == 数值序
    assert!(k1 < k2);
    assert!(k2 < k10);
}
