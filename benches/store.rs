//! LMDB 存储基准测试。
//!
//! 测量热点路径：
//! - `Store::put` / `get` — 单键读写（含事务提交）
//! - `Store::put_batch` — 批量写入（DAG 持久化路径）
//! - `Store::delete`
//!
//! 这是 orchestrator submit 持久化与 worker 状态保存的关键路径。
//!
//! 运行：`cargo bench --bench store`

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use actant::common::StoreConfig;
use actant::store::Store;

/// 创建临时 LMDB 目录，返回 (Store, 临时目录路径)。
/// 临时目录在基准测试进程退出时由系统清理。
fn open_temp_store() -> (Store, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open_with_config(dir.path(), &StoreConfig::default()).unwrap();
    (store, dir)
}

fn bench_put_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("store/put_single");
    // 单键 put 每次都开启+提交独立事务（含 fsync），成本高。
    // 使用较小 n 避免单次迭代过长导致 criterion 采样不足。
    for &n in &[100usize, 1_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let (store, _dir) = open_temp_store();
            let value = vec![0u8; 128];
            b.iter(|| {
                for i in 0..n {
                    let key = format!("key-{i}");
                    store.put(black_box(&key), &value).unwrap();
                }
            });
        });
    }
    group.finish();
}

fn bench_get_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("store/get_single");
    for &n in &[1_000usize, 10_000] {
        let (store, _dir) = open_temp_store();
        let value = vec![0u8; 128];
        for i in 0..n {
            store.put(&format!("key-{i}"), &value).unwrap();
        }
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                for i in 0..n {
                    let _ = store.get(black_box(&format!("key-{i}"))).unwrap();
                }
            });
        });
    }
    group.finish();
}

fn bench_put_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("store/put_batch");
    for &batch_size in &[10usize, 100, 1_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &bs| {
                let (store, _dir) = open_temp_store();
                let value = vec![0u8; 128];
                b.iter_batched(
                    || {
                        (0..bs)
                            .map(|i| (format!("key-{i}"), value.clone()))
                            .collect::<Vec<_>>()
                    },
                    |entries| {
                        store.put_batch(black_box(&entries)).unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_delete_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("store/delete_single");
    let (store, _dir) = open_temp_store();
    for &n in &[100usize, 1_000] {
        let value = vec![0u8; 128];
        for i in 0..n {
            store.put(&format!("key-{i}"), &value).unwrap();
        }
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                for i in 0..n {
                    let _ = store.delete(black_box(&format!("key-{i}"))).unwrap();
                }
            });
        });
    }
    group.finish();
}

criterion_group!(
    store_benches,
    bench_put_single,
    bench_get_single,
    bench_put_batch,
    bench_delete_single,
);
criterion_main!(store_benches);
