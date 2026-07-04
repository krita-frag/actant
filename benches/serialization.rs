//! 序列化基准测试。
//!
//! 测量热点路径：
//! - `serialize_rkyv` / `deserialize_rkyv_value` — DAG 与 TaskDefinition 的 rkyv 零拷贝序列化
//! - 这是持久化（Store put_batch）与网络（gossip 复制）的关键路径。
//!
//! 运行：`cargo bench --bench serialization`

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use actant::common::{
    deserialize_rkyv_value, serialize_rkyv, RetryPolicy, TaskDefinition, TaskId, WorkflowId,
};
use actant::orchestrator::{Dag, DagNode};

fn make_task(idx: usize) -> TaskDefinition {
    TaskDefinition {
        id: TaskId::new(format!("task-{idx}")),
        name: format!("task-{idx}"),
        payload: vec![0u8; 64],
        workflow_id: Some(WorkflowId::new("wf-bench".into())),
        target_node: None,
        origin_node: None,
        retry_policy: Some(RetryPolicy::default()),
        priority: 0,
        timeout_ms: Some(30_000),
        attempt: 0,
        enqueued_at_ms: 0,
        target_endpoint_addr: None,
        origin_endpoint_addr: None,
    }
}

fn make_node(idx: usize) -> DagNode {
    DagNode {
        task_id: TaskId::new(format!("task-{idx}")),
        name: format!("task-{idx}"),
        payload: vec![0u8; 64],
        retry_policy: Some(RetryPolicy::default()),
        timeout_ms: Some(30_000),
        priority: 0,
        metadata: std::collections::HashMap::new(),
    }
}

fn build_chain_dag(n: usize) -> Dag {
    let mut dag = Dag::new();
    for i in 0..n {
        dag.add_node(make_node(i)).unwrap();
    }
    for i in 0..n.saturating_sub(1) {
        dag.add_edge(
            TaskId::new(format!("task-{i}")),
            TaskId::new(format!("task-{}", i + 1)),
        )
        .unwrap();
    }
    dag
}

fn bench_serialize_dag(c: &mut Criterion) {
    let mut group = c.benchmark_group("rkyv/serialize_dag");
    for &n in &[10usize, 100, 1_000] {
        let dag = build_chain_dag(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &dag, |b, dag| {
            b.iter(|| {
                let bytes = serialize_rkyv(black_box(dag)).unwrap();
                black_box(bytes);
            });
        });
    }
    group.finish();
}

fn bench_deserialize_dag(c: &mut Criterion) {
    let mut group = c.benchmark_group("rkyv/deserialize_dag");
    for &n in &[10usize, 100, 1_000] {
        let dag = build_chain_dag(n);
        let bytes = serialize_rkyv(&dag).unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(n), &bytes, |b, bytes| {
            b.iter(|| {
                let dag: Dag = deserialize_rkyv_value(black_box(bytes)).unwrap();
                black_box(dag);
            });
        });
    }
    group.finish();
}

fn bench_serialize_task(c: &mut Criterion) {
    let mut group = c.benchmark_group("rkyv/serialize_task");
    for &n in &[1usize, 100, 1_000] {
        let tasks: Vec<TaskDefinition> = (0..n).map(make_task).collect();
        group.bench_with_input(BenchmarkId::from_parameter(n), &tasks, |b, tasks| {
            b.iter(|| {
                for t in tasks {
                    let _ = serialize_rkyv(black_box(t)).unwrap();
                }
            });
        });
    }
    group.finish();
}

fn bench_serialize_dag_round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("rkyv/dag_round_trip");
    for &n in &[10usize, 100, 1_000] {
        let dag = build_chain_dag(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &dag, |b, dag| {
            b.iter(|| {
                let bytes = serialize_rkyv(dag).unwrap();
                let dag2: Dag = deserialize_rkyv_value(&bytes).unwrap();
                black_box(dag2);
            });
        });
    }
    group.finish();
}

criterion_group!(
    serialization_benches,
    bench_serialize_dag,
    bench_deserialize_dag,
    bench_serialize_task,
    bench_serialize_dag_round_trip,
);
criterion_main!(serialization_benches);
