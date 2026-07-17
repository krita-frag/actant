//! Orchestrator DAG 拓扑计算基准测试。
//!
//! 测量编排器的纯计算热点路径：
//! - `Dag::topological_sort` — 任务调度前的拓扑排序
//! - `Dag::roots` / `Dag::sinks` — 调度入口/出口节点查询
//! - `Dag::predecessors_of` / `Dag::successors_of` — 任务完成后的后继查找
//!
//! 这些路径在每次任务完成回调、调度决策、条件边求值时都会执行，
//! 其延迟直接影响编排器的吞吐量。
//!
//! 运行：`cargo bench --bench orchestrator`

use std::collections::HashMap;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use actant::common::TaskId;
use actant::runtime::workflow::{Dag, DagNode};

/// 构造一个 N 节点的线性 DAG：n0 → n1 → n2 → ... → n(N-1)。
///
/// 线性 DAG 是拓扑排序最坏情况（每次只能释放一个节点），
/// 用于测量排序算法的常数因子。
fn build_linear_dag(n: usize) -> Dag {
    let mut dag = Dag::new();
    for i in 0..n {
        let id: TaskId = format!("task-{i}").into();
        dag.add_node(DagNode {
            task_id: id.clone(),
            name: format!("task-{i}"),
            payload: vec![0u8; 32],
            retry_policy: None,
            timeout_ms: None,
            priority: 0,
            metadata: HashMap::new(),
        })
        .unwrap();
        if i > 0 {
            let prev: TaskId = format!("task-{}", i - 1).into();
            dag.add_edge(prev, id).unwrap();
        }
    }
    dag
}

/// 构造一个 N 节点的宽 DAG：1 root → (N-2) middle → 1 sink。
///
/// 宽 DAG 模拟并行 fan-out / fan-in 工作流，测量大量节点同时就绪时
/// 拓扑排序的队列管理开销。
fn build_wide_dag(n: usize) -> Dag {
    assert!(n >= 3);
    let mut dag = Dag::new();
    let root: TaskId = "root".into();
    let sink: TaskId = "sink".into();
    dag.add_node(DagNode {
        task_id: root.clone(),
        name: "root".into(),
        payload: vec![0u8; 32],
        retry_policy: None,
        timeout_ms: None,
        priority: 0,
        metadata: HashMap::new(),
    })
    .unwrap();
    dag.add_node(DagNode {
        task_id: sink.clone(),
        name: "sink".into(),
        payload: vec![0u8; 32],
        retry_policy: None,
        timeout_ms: None,
        priority: 0,
        metadata: HashMap::new(),
    })
    .unwrap();
    for i in 0..(n - 2) {
        let mid: TaskId = format!("mid-{i}").into();
        dag.add_node(DagNode {
            task_id: mid.clone(),
            name: format!("mid-{i}"),
            payload: vec![0u8; 32],
            retry_policy: None,
            timeout_ms: None,
            priority: 0,
            metadata: HashMap::new(),
        })
        .unwrap();
        dag.add_edge(root.clone(), mid.clone()).unwrap();
        dag.add_edge(mid, sink.clone()).unwrap();
    }
    dag
}

/// 拓扑排序基准：线性 DAG。
fn bench_topological_sort_linear(c: &mut Criterion) {
    let mut group = c.benchmark_group("orchestrator/topological_sort/linear");
    for &n in &[100usize, 1_000, 10_000] {
        let dag = build_linear_dag(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &dag, |b, dag| {
            b.iter(|| {
                let sorted = dag.topological_sort().unwrap();
                black_box(sorted);
            });
        });
    }
    group.finish();
}

/// 拓扑排序基准：宽 DAG（fan-out / fan-in）。
fn bench_topological_sort_wide(c: &mut Criterion) {
    let mut group = c.benchmark_group("orchestrator/topological_sort/wide");
    for &n in &[100usize, 1_000, 10_000] {
        let dag = build_wide_dag(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &dag, |b, dag| {
            b.iter(|| {
                let sorted = dag.topological_sort().unwrap();
                black_box(sorted);
            });
        });
    }
    group.finish();
}

/// roots / sinks 查询：调度器在每轮决策时调用。
fn bench_roots_sinks(c: &mut Criterion) {
    let mut group = c.benchmark_group("orchestrator/roots_sinks");
    for &n in &[100usize, 1_000, 10_000] {
        let dag = build_wide_dag(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &dag, |b, dag| {
            b.iter(|| {
                let roots = dag.roots();
                let sinks = dag.sinks();
                black_box(roots);
                black_box(sinks);
            });
        });
    }
    group.finish();
}

/// 后继节点查询：任务完成回调中触发后继任务就绪检查的高频路径。
fn bench_successors_of(c: &mut Criterion) {
    let mut group = c.benchmark_group("orchestrator/successors_of");
    for &n in &[100usize, 1_000, 10_000] {
        let dag = build_wide_dag(n);
        let root_id: TaskId = "root".into();
        group.bench_with_input(BenchmarkId::from_parameter(n), &root_id, |b, id| {
            b.iter(|| {
                let succs = dag.successors_of(id);
                black_box(succs);
            });
        });
    }
    group.finish();
}

criterion_group!(
    orchestrator_benches,
    bench_topological_sort_linear,
    bench_topological_sort_wide,
    bench_roots_sinks,
    bench_successors_of,
);
criterion_main!(orchestrator_benches);
