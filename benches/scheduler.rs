//! 调度器基准测试。
//!
//! 测量热点路径（通过 ActorScheduler → SchedulerActor 端到端）：
//! - `enqueue` / `dequeue` — 优先级与 FIFO 两种 SchedulerActor
//! - `enqueue_batch` — 批量入队
//! - `dequeue_batch` — 批量出队
//!
//! P4 Actor 化重构后，调度状态由 SchedulerActor 持有，客户端通过 ActorSystem
//! 消息协议转发请求。本基准测量端到端开销（含消息编解码与 actor 调度），
//! 反映真实生产路径。
//!
//! 运行：`cargo bench --bench scheduler`

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use tokio::runtime::Runtime;

use actant::common::{ActorId, RetryPolicy, TaskDefinition, TaskId, WorkflowId};
use actant::runtime::actor::ActorSystem;
use actant::runtime::workflow::{
    fifo_scheduler_actor, priority_scheduler_actor, ActorScheduler, Scheduler,
};

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

/// 启动一个 priority SchedulerActor 并返回客户端。
fn priority_client(rt: &Runtime) -> Arc<dyn Scheduler> {
    let actor_system = Arc::new(ActorSystem::new());
    let actor_id = ActorId::new("sched-priority-bench".to_string());
    rt.block_on(actor_system.spawn(actor_id.clone(), priority_scheduler_actor()))
        .unwrap();
    Arc::new(ActorScheduler::new(actor_id, actor_system))
}

/// 启动一个 fifo SchedulerActor 并返回客户端。
fn fifo_client(rt: &Runtime) -> Arc<dyn Scheduler> {
    let actor_system = Arc::new(ActorSystem::new());
    let actor_id = ActorId::new("sched-fifo-bench".to_string());
    rt.block_on(actor_system.spawn(actor_id.clone(), fifo_scheduler_actor()))
        .unwrap();
    Arc::new(ActorScheduler::new(actor_id, actor_system))
}

fn bench_enqueue_priority(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("scheduler/priority/enqueue");
    for &n in &[1_000usize, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let sched = priority_client(&rt);
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
                    let sched = priority_client(&rt);
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
                    (0..n)
                        .map(|i| make_task(i, (i % 10) as i32 - 5))
                        .collect::<Vec<_>>()
                },
                |tasks| {
                    let sched = priority_client(&rt);
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
                let sched = fifo_client(&rt);
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
                    let sched = priority_client(&rt);
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
