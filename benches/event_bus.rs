//! 事件总线基准测试。
//!
//! 测量内部事件总线的核心路径：
//! - `publish` BestEffort 吞吐量（可克隆事件，单订阅者）
//! - `publish` Reliable 吞吐量（可克隆事件，单订阅者）
//! - 多订阅者 fan-out 吞吐量
//!
//! 事件总线是 worker 生命周期、任务完成通知、集群心跳等
//! 内部解耦通信的中枢，其吞吐量直接影响系统可观测性和事件传播延迟。
//!
//! 运行：`cargo bench --bench event_bus`

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use tokio::runtime::Runtime;

use actant::common::{NodeId, TaskCompletion, TaskId, WorkflowId};
use actant::runtime::event_bus::{BusEvent, EventBus, Topic};

/// 创建一个 NodeHeartbeat 事件用于基准测试。
fn make_heartbeat() -> BusEvent {
    BusEvent::Heartbeat(actant::common::NodeHeartbeat {
        node_id: NodeId::new("bench-node".into()),
        active_workflows: vec![],
        timestamp_ms: 0,
        available_slots: 4,
        max_slots: 4,
        endpoint_addr: None,
    })
}

/// 创建一个 TaskCompleted 事件用于基准测试。
fn make_task_completed() -> BusEvent {
    BusEvent::TaskCompleted(TaskCompletion::Completed {
        workflow_id: WorkflowId::new("wf-bench".into()),
        task_id: TaskId::new("task-bench".into()),
        task_name: "task-bench".into(),
        result: vec![0u8; 64],
        target_node: None,
    })
}

/// BestEffort publish 吞吐量（单订阅者，消费速度跟上）。
fn bench_publish_best_effort(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let bus = EventBus::new();
    let mut rx = bus.subscribe(Topic::ClusterHeartbeat);

    // 启动消费者
    rt.spawn(async move { while rx.recv().await.is_some() {} });

    let mut group = c.benchmark_group("event_bus/publish_best_effort");
    for &n in &[1_000usize, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    for _ in 0..n {
                        bus.publish(make_heartbeat()).await;
                    }
                    black_box(());
                });
            });
        });
    }
    group.finish();
}

/// Reliable publish 吞吐量（单订阅者，消费速度跟上）。
fn bench_publish_reliable(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let bus = EventBus::new();
    let mut rx = bus.subscribe(Topic::TaskCompleted);

    // 启动消费者
    rt.spawn(async move { while rx.recv().await.is_some() {} });

    let mut group = c.benchmark_group("event_bus/publish_reliable");
    for &n in &[1_000usize, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    for _ in 0..n {
                        bus.publish(make_task_completed()).await;
                    }
                    black_box(());
                });
            });
        });
    }
    group.finish();
}

/// 多订阅者 fan-out 吞吐量（4 个订阅者）。
fn bench_publish_fanout(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let bus = EventBus::new();

    // 4 个订阅者，各启动消费者
    for _ in 0..4 {
        let mut rx = bus.subscribe(Topic::ClusterHeartbeat);
        rt.spawn(async move { while rx.recv().await.is_some() {} });
    }

    let mut group = c.benchmark_group("event_bus/publish_fanout_4");
    for &n in &[1_000usize, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    for _ in 0..n {
                        bus.publish(make_heartbeat()).await;
                    }
                    black_box(());
                });
            });
        });
    }
    group.finish();
}

criterion_group!(
    event_bus_benches,
    bench_publish_best_effort,
    bench_publish_reliable,
    bench_publish_fanout,
);
criterion_main!(event_bus_benches);
