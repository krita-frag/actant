use std::path::Path;
use std::sync::Arc;

use heed::types::{Bytes, Str};
use heed::{Database, Env, EnvOpenOptions};

use crate::common::{Result, StoreConfig};

pub struct Store {
    env: Arc<Env>,
    default_db: Database<Str, Bytes>,
}

impl Store {
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
    /// 当只需最新记录时，比 `scan_prefix` 更高效。
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

    pub fn read_txn(&self) -> Result<heed::RoTxn<'_, heed::WithTls>> {
        self.env.read_txn().map_err(Into::into)
    }

    pub fn write_txn(&self) -> Result<heed::RwTxn<'_>> {
        self.env.write_txn().map_err(Into::into)
    }

    /// 启动 LMDB 环境的优雅关闭。
    ///
    /// 调用 `heed::Env::prepare_for_closing()`，表示应在所有 `Arc<Env>` 引用释放后关闭环境。
    /// 返回的 `EnvClosingEvent` 可用于等待实际关闭完成。
    ///
    /// 这对于释放 LMDB 锁文件（`lock.mdb`）以允许另一进程打开同一数据目录至关重要。
    pub fn prepare_close(&self) -> heed::EnvClosingEvent {
        // prepare_for_closing 接管 Env（self）所有权，因此克隆 Arc 并解引用获得 owned Env。
        let env = Arc::clone(&self.env);
        Arc::try_unwrap(env)
            .unwrap_or_else(|arc| (*arc).clone())
            .prepare_for_closing()
    }
}

impl Clone for Store {
    fn clone(&self) -> Self {
        Self {
            env: self.env.clone(),
            default_db: self.default_db,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn put_and_get() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.put("key1", b"value1").unwrap();
        let val = store.get("key1").unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[test]
    fn delete_key() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.put("key1", b"value1").unwrap();
        assert!(store.delete("key1").unwrap());
        assert_eq!(store.get("key1").unwrap(), None);
    }

    #[test]
    fn scan_prefix() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.put("a:1", b"one").unwrap();
        store.put("a:2", b"two").unwrap();
        store.put("b:1", b"three").unwrap();
        let results = store.scan_prefix("a:").unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn custom_config() {
        let dir = tempdir().unwrap();
        let config = StoreConfig {
            data_dir: None,
            map_size: 64 * 1024 * 1024,
            max_dbs: 8,
        };
        let store = Store::open_with_config(dir.path(), &config).unwrap();
        store.put("key1", b"value1").unwrap();
        let val = store.get("key1").unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[test]
    fn get_nonexistent_key_returns_none() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get("missing").unwrap(), None);
    }

    #[test]
    fn delete_nonexistent_key_returns_false() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        // delete 返回 bool 表示 key 是否存在；不存在时返回 false（非错误）
        let existed = store.delete("missing").unwrap();
        assert!(!existed);
    }

    #[test]
    fn delete_existing_key_returns_true() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.put("key1", b"val").unwrap();
        let existed = store.delete("key1").unwrap();
        assert!(existed);
        assert_eq!(store.get("key1").unwrap(), None);
    }

    #[test]
    fn scan_prefix_empty_when_no_matches() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.put("other", b"val").unwrap();
        let results = store.scan_prefix("prefix:").unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn scan_prefix_returns_sorted_entries() {
        // LMDB 按键序排列，scan_prefix 应返回排序结果。
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.put("a:3", b"three").unwrap();
        store.put("a:1", b"one").unwrap();
        store.put("a:2", b"two").unwrap();
        let results = store.scan_prefix("a:").unwrap();
        let keys: Vec<_> = results.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["a:1", "a:2", "a:3"]);
    }

    #[test]
    fn scan_prefix_last_returns_highest_key() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.put("seq:001", b"first").unwrap();
        store.put("seq:003", b"third").unwrap();
        store.put("seq:002", b"second").unwrap();
        let last = store.scan_prefix_last("seq:").unwrap().unwrap();
        assert_eq!(last.0, "seq:003");
        assert_eq!(last.1, b"third");
    }

    #[test]
    fn scan_prefix_last_none_when_no_matches() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.scan_prefix_last("empty:").unwrap(), None);
    }

    #[test]
    fn put_overwrites_existing_value() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.put("key1", b"old").unwrap();
        store.put("key1", b"new").unwrap();
        assert_eq!(store.get("key1").unwrap(), Some(b"new".to_vec()));
    }

    #[test]
    fn put_batch_writes_all_entries_atomically() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
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
    fn put_batch_empty_is_noop() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.put_batch(&[]).unwrap();
        // 空 batch 不应出错，store 应仍可正常使用
        store.put("after", b"val").unwrap();
        assert_eq!(store.get("after").unwrap(), Some(b"val".to_vec()));
    }

    #[test]
    fn store_clone_shares_underlying_env() {
        // Store::clone 共享 Arc<Env>，两个 clone 操作同一底层数据库。
        let dir = tempdir().unwrap();
        let store1 = Store::open(dir.path()).unwrap();
        let store2 = store1.clone();
        store1.put("shared", b"from-store1").unwrap();
        assert_eq!(store2.get("shared").unwrap(), Some(b"from-store1".to_vec()));
    }

    #[test]
    fn large_value_persisted_correctly() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let large: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();
        store.put("large", &large).unwrap();
        assert_eq!(store.get("large").unwrap(), Some(large));
    }
}
