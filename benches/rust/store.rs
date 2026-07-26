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
use tokio::runtime::Runtime;

use actant::common::{StoreConfig, SyncMode};
use actant::runtime::state::Store;

/// 创建临时 LMDB 目录，返回 (Store, 临时目录路径)。
/// 临时目录在基准测试进程退出时由系统清理。
fn open_temp_store(rt: &Runtime) -> (Store, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = rt
        .block_on(Store::open_with_config(dir.path(), StoreConfig::default()))
        .unwrap();
    (store, dir)
}

/// 创建启用 GroupCommit 的临时 Store。
fn open_group_commit_store(rt: &Runtime, flush_interval_ms: u64) -> (Store, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig {
        sync_mode: SyncMode::GroupCommit(flush_interval_ms),
        ..StoreConfig::default()
    };
    let store = rt
        .block_on(Store::open_with_config(dir.path(), config))
        .unwrap();
    (store, dir)
}

fn bench_put_single(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("store/put_single");
    // 单键 put 每次都开启+提交独立事务（含 fsync），成本高（~2.88ms/次）。
    // n=1000 时单次迭代 ~2.88s，默认 sample_size=100 会导致单组耗时 ~5min。
    // 降低 sample_size 与 measurement_time 使整组在 ~30s 内完成，仍保持统计有效性。
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    for &n in &[100usize, 1_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let (store, _dir) = open_temp_store(&rt);
            let value = vec![0u8; 128];
            b.iter(|| {
                for i in 0..n {
                    let key = format!("key-{i}");
                    rt.block_on(store.put(black_box(&key), &value)).unwrap();
                }
            });
        });
    }
    group.finish();
}

fn bench_get_single(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("store/get_single");
    for &n in &[1_000usize, 10_000] {
        let (store, _dir) = open_temp_store(&rt);
        let value = vec![0u8; 128];
        for i in 0..n {
            rt.block_on(store.put(&format!("key-{i}"), &value)).unwrap();
        }
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                for i in 0..n {
                    let _ = rt
                        .block_on(store.get(black_box(&format!("key-{i}"))))
                        .unwrap();
                }
            });
        });
    }
    group.finish();
}

fn bench_put_batch(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("store/put_batch");
    // put_batch 单次事务含 fsync，但批量写入摊薄了开销。
    // batch_size=1000 单次迭代 ~3ms，默认配置可行但仍适度降低以保持一致性。
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    for &batch_size in &[10usize, 100, 1_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &bs| {
                let (store, _dir) = open_temp_store(&rt);
                let value = vec![0u8; 128];
                b.iter_batched(
                    || {
                        (0..bs)
                            .map(|i| (format!("key-{i}"), value.clone()))
                            .collect::<Vec<_>>()
                    },
                    |entries| {
                        rt.block_on(store.put_batch(black_box(&entries))).unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_delete_single(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("store/delete_single");
    // delete 同 put，每次独立事务含 fsync，降低 sample_size 避免长耗时。
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    let (store, _dir) = open_temp_store(&rt);
    for &n in &[100usize, 1_000] {
        let value = vec![0u8; 128];
        for i in 0..n {
            rt.block_on(store.put(&format!("key-{i}"), &value)).unwrap();
        }
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                for i in 0..n {
                    let _ = rt
                        .block_on(store.delete(black_box(&format!("key-{i}"))))
                        .unwrap();
                }
            });
        });
    }
    group.finish();
}

/// GroupCommit 模式单 key put 吞吐量，对比 `bench_put_single`（Sync 模式）。
/// `flush` 确保每次迭代写入在测量窗口内持久化，不跨迭代累积。
fn bench_put_group_commit(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("store/put_group_commit");
    for &n in &[1_000usize, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let (store, _dir) = open_group_commit_store(&rt, 10);
            let value = vec![0u8; 128];
            b.iter(|| {
                for i in 0..n {
                    let key = format!("key-{i}");
                    rt.block_on(store.put(black_box(&key), &value)).unwrap();
                }
                rt.block_on(store.flush()).unwrap();
            });
        });
    }
    group.finish();
}

/// 固定 1000 次 put，对比 Sync 与 GroupCommit 总耗时，量化 write coalescing 收益。
fn bench_put_sync_vs_group_commit(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("store/put_sync_vs_group_commit");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));

    const N: usize = 1_000;
    let value = vec![0u8; 128];

    // Sync 模式基线。
    group.bench_function("sync_1000", |b| {
        b.iter(|| {
            let (store, _dir) = open_temp_store(&rt);
            for i in 0..N {
                let key = format!("key-{i}");
                rt.block_on(store.put(black_box(&key), &value)).unwrap();
            }
        });
    });

    // GroupCommit 模式。
    group.bench_function("group_commit_1000", |b| {
        b.iter(|| {
            let (store, _dir) = open_group_commit_store(&rt, 10);
            for i in 0..N {
                let key = format!("key-{i}");
                rt.block_on(store.put(black_box(&key), &value)).unwrap();
            }
            rt.block_on(store.flush()).unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    store_benches,
    bench_put_single,
    bench_get_single,
    bench_put_batch,
    bench_delete_single,
    bench_put_group_commit,
    bench_put_sync_vs_group_commit,
);
criterion_main!(store_benches);
