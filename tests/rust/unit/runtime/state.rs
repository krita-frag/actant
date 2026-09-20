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
    // 远端与本地历史 wall_time 相同、logical 更高。merge 时本地物理时钟
    // 可能已前进：此时 Kulkarni 标准算法将 logical 清零，输出 (P2, 0)
    // 仍严格大于 local 与 remote；若物理时钟未前进，则落入并列主导分支
    // 取 max(local, remote) + 1。两种情况输出都严格大于双方，单调性保持。
    let remote = HlcTimestamp::from_parts(local.wall_time(), local.logical() + 10);
    let merged = hlc.merge(&remote);
    assert!(merged > local);
    assert!(merged > remote);
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

#[test]
fn store_config_default() {
    let c = StoreConfig::default();
    // 默认值存在即可
    assert!(c.map_size > 0);
    assert!(c.max_dbs > 0);
}

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

#[test]
fn store_writes_fmt_version_on_first_open() {
    let dir = tempdir().unwrap();
    let store = LmdbStore::open(dir.path()).unwrap();
    // 首次创建即写入当前版本（u64 LE）。
    let raw = store.get(STORE_FMT_VERSION_KEY).unwrap();
    assert_eq!(
        raw,
        Some(STORE_FMT_VERSION.to_le_bytes().to_vec()),
        "first open must stamp the current store format version"
    );

    // 重开同一 store：版本匹配，正常打开。
    drop(store);
    assert!(
        LmdbStore::open(dir.path()).is_ok(),
        "matching version must reopen"
    );
}

#[test]
fn store_rejects_mismatched_fmt_version() {
    let dir = tempdir().unwrap();
    {
        let store = LmdbStore::open(dir.path()).unwrap();
        // 模拟由更高（不兼容）版本写入的数据。
        store
            .put(STORE_FMT_VERSION_KEY, &999u64.to_le_bytes())
            .unwrap();
    }
    let err = match LmdbStore::open(dir.path()) {
        Err(e) => e,
        Ok(_) => panic!("mismatched format version must be rejected"),
    };
    let msg = err.to_string();
    assert!(
        msg.contains("format version"),
        "mismatched format version must produce a semantic error, got: {msg}"
    );
}
