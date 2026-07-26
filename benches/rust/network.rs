//! 网络层 wire 协议编解码基准测试。
//!
//! 测量跨节点消息的热点路径：
//! - `WireEnvelope::wrap` — 发送方封装（含 UUID v4 生成 + postcard 序列化）
//! - `WireEnvelope::decode` — 接收方解码（含版本校验 + postcard 反序列化）
//!
//! 这两条路径在每个跨节点消息上都会执行：
//! - TaskDispatch：任务分发到远端 worker
//! - NodeHeartbeat：节点心跳广播（failover 检测依据）
//! - DagStateUpdate：DAG 状态同步
//!
//! 其延迟直接影响跨节点消息的端到端延迟，进而影响 P2P 拓扑下的
//! 任务分发吞吐量与故障检测响应时间。
//!
//! 运行：`cargo bench --bench network`

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

// wire 模块本身是 pub(crate)，类型通过 common 的 #[doc(hidden)] re-export 暴露
// 供测试与基准测试使用。
use actant::common::{NodeHeartbeat, WireEnvelope, WireMessage};
use actant::common::{NodeId, TaskDefinition, TaskId, WorkflowId};

/// 构造一个典型大小的 TaskDispatch 消息（payload 256 字节）。
fn make_task_dispatch() -> WireMessage {
    WireMessage::TaskDispatch(TaskDefinition {
        id: TaskId::from("task-bench-0001"),
        name: "bench_task".into(),
        payload: vec![0u8; 256],
        workflow_id: Some(WorkflowId::from("wf-bench-0001")),
        target_node: Some(NodeId::from("node-bench-target")),
        origin_node: Some(NodeId::from("node-bench-origin")),
        retry_policy: None,
        priority: 0,
        timeout_ms: Some(30_000),
        attempt: 0,
        enqueued_at_ms: 0,
        target_endpoint_addr: None,
        origin_endpoint_addr: None,
    })
}

/// 构造一个含 10 个活跃 workflow 的 NodeHeartbeat（典型规模）。
fn make_node_heartbeat() -> WireMessage {
    let active_workflows = (0..10)
        .map(|i| WorkflowId::from(format!("wf-bench-{i:04}").as_str()))
        .collect();
    WireMessage::NodeHeartbeat(NodeHeartbeat {
        node_id: NodeId::from("node-bench-self"),
        active_workflows,
        timestamp_ms: 1_700_000_000_000,
        available_slots: 8,
        max_slots: 16,
        endpoint_addr: Some("endpoint-bench-addr".into()),
    })
}

/// `WireEnvelope::wrap`：发送方封装（UUID + postcard 序列化）。
fn bench_wrap(c: &mut Criterion) {
    let dispatch = make_task_dispatch();
    let heartbeat = make_node_heartbeat();

    let mut group = c.benchmark_group("network/wrap");
    group.bench_with_input(
        BenchmarkId::new("task_dispatch", "256B"),
        &dispatch,
        |b, msg| {
            b.iter(|| {
                let env = WireEnvelope::wrap(black_box(msg.clone()));
                black_box(env);
            });
        },
    );
    group.bench_with_input(
        BenchmarkId::new("node_heartbeat", "10wf"),
        &heartbeat,
        |b, msg| {
            b.iter(|| {
                let env = WireEnvelope::wrap(black_box(msg.clone()));
                black_box(env);
            });
        },
    );
    group.finish();
}

/// `WireEnvelope::decode`：接收方解码（版本校验 + postcard 反序列化）。
///
/// wrap → decode 是 round-trip 路径，decode 单独测量反序列化开销。
fn bench_decode(c: &mut Criterion) {
    let dispatch = make_task_dispatch();
    let heartbeat = make_node_heartbeat();
    let dispatch_bytes = postcard::to_allocvec(&WireEnvelope::wrap(dispatch)).unwrap();
    let heartbeat_bytes = postcard::to_allocvec(&WireEnvelope::wrap(heartbeat)).unwrap();

    let mut group = c.benchmark_group("network/decode");
    group.bench_with_input(
        BenchmarkId::new("task_dispatch", format!("{}B", dispatch_bytes.len())),
        &dispatch_bytes,
        |b, bytes| {
            b.iter(|| {
                let decoded = WireEnvelope::decode(black_box(bytes));
                black_box(decoded);
            });
        },
    );
    group.bench_with_input(
        BenchmarkId::new("node_heartbeat", format!("{}B", heartbeat_bytes.len())),
        &heartbeat_bytes,
        |b, bytes| {
            b.iter(|| {
                let decoded = WireEnvelope::decode(black_box(bytes));
                black_box(decoded);
            });
        },
    );
    group.finish();
}

/// wrap → decode round-trip：测量完整跨节点消息处理路径的纯开销
/// （不含网络 IO，仅协议层）。
fn bench_round_trip(c: &mut Criterion) {
    let dispatch = make_task_dispatch();

    let mut group = c.benchmark_group("network/round_trip");
    group.bench_with_input(
        BenchmarkId::new("task_dispatch", "256B"),
        &dispatch,
        |b, msg| {
            b.iter(|| {
                let env = WireEnvelope::wrap(black_box(msg.clone()));
                let bytes = postcard::to_allocvec(&env).unwrap();
                let decoded = WireEnvelope::decode(black_box(&bytes));
                black_box(decoded);
            });
        },
    );
    group.finish();
}

criterion_group!(network_benches, bench_wrap, bench_decode, bench_round_trip,);
criterion_main!(network_benches);
