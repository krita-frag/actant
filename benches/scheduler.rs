//! 调度器基准测试。
//!
//! 测量热点路径：
//! - `PriorityScheduler::enqueue` / `dequeue` — BTreeMap + VecDeque 操作
//! - `FifoScheduler::enqueue` / `dequeue` — VecDeque 操作
//! - `enqueue_batch` — 批量入队
//! - `dequeue_batch` — 批量出队（优先级排序）
//!
//! 调度器为异步 API，使用 tokio runtime 驱动。
//!
//! 运行：`cargo bench --bench scheduler`

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use tokio::runtime::Runtime;

use actant::common::{RetryPolicy, TaskDefinition, TaskId, WorkflowId};
use actant::orchestrator::{FifoScheduler, PriorityScheduler, Scheduler};

fn make_task(idx: usize, priority: i32) -> TaskDefinition {
    TaskDefinition {
        id: TaskId::new(format!("task-{idx}")),
        name: format!("task-{idx}"),
        payload: vec![0u8; 64],
        workflow_id: Some(WorkflowId::new("wf-bench".into())),
        target_node: None,
        origin_node: None,
        retry_policy: Some(RetryPolicy::default()),
        priority,
        timeout_ms: Some(30_000),
        attempt: 0,
        enqueued_at_ms: 0,
        target_endpoint_addr: None,
        origin_endpoint_addr: None,
    }
}

fn bench_enqueue_priority(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("scheduler/priority/enqueue");
    for &n in &[1_000usize, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let sched: Arc<dyn Scheduler> = Arc::new(PriorityScheduler::new());
                rt.block_on(async {
                    for i in 0..n {
                        let _ = sched.enqueue(make_task(i, (i % 10) as i32 - 5)).await;
                    }
                    black_box(&sched);
                });
            });
        });
    }
    group.finish();
}

fn bench_dequeue_priority(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("scheduler/priority/dequeue");
    for &n in &[1_000usize, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let sched: Arc<dyn Scheduler> = Arc::new(PriorityScheduler::new());
                    rt.block_on(async {
                        for i in 0..n {
                            let _ = sched.enqueue(make_task(i, (i % 10) as i32 - 5)).await;
                        }
                    });
                    sched
                },
                |sched| {
                    rt.block_on(async {
                        let mut count = 0;
                        while sched.dequeue().await.is_some() {
                            count += 1;
                        }
                        black_box(count);
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_enqueue_batch_priority(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("scheduler/priority/enqueue_batch");
    for &n in &[1_000usize, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let tasks: Vec<TaskDefinition> =
                        (0..n).map(|i| make_task(i, (i % 10) as i32 - 5)).collect();
                    tasks
                },
                |tasks| {
                    let sched: Arc<dyn Scheduler> = Arc::new(PriorityScheduler::new());
                    rt.block_on(async {
                        let _ = sched.enqueue_batch(tasks).await;
                        black_box(&sched);
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_enqueue_fifo(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("scheduler/fifo/enqueue");
    for &n in &[1_000usize, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let sched: Arc<dyn Scheduler> = Arc::new(FifoScheduler::new());
                rt.block_on(async {
                    for i in 0..n {
                        let _ = sched.enqueue(make_task(i, 0)).await;
                    }
                    black_box(&sched);
                });
            });
        });
    }
    group.finish();
}

fn bench_dequeue_batch_priority(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("scheduler/priority/dequeue_batch_256");
    for &n in &[1_000usize, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let sched: Arc<dyn Scheduler> = Arc::new(PriorityScheduler::new());
                    rt.block_on(async {
                        for i in 0..n {
                            let _ = sched.enqueue(make_task(i, (i % 10) as i32 - 5)).await;
                        }
                    });
                    sched
                },
                |sched| {
                    rt.block_on(async {
                        let mut total = 0;
                        loop {
                            let batch = sched.dequeue_batch(256).await;
                            if batch.is_empty() {
                                break;
                            }
                            total += batch.len();
                        }
                        black_box(total);
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    scheduler_benches,
    bench_enqueue_priority,
    bench_dequeue_priority,
    bench_enqueue_batch_priority,
    bench_enqueue_fifo,
    bench_dequeue_batch_priority,
);
criterion_main!(scheduler_benches);
