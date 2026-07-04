//! 堆内存分配跟踪。
//!
//! 使用 dhat 全局 ProfilingAllocator 测量各热点路径的堆分配。
//! dhat 只支持单一活跃 profile，因此所有热点路径在同一 profile 下顺序执行，
//! 进程退出时自动输出 dhat-heap.json（含 per-call-site 分配明细）与 stderr 摘要。
//!
//! 运行：
//!   cargo test --bench mem_profile --features dhat-heap -- --nocapture
//!
//! 启用 `dhat-heap` feature 后，dhat 自动注册全局分配器；
//! 退出时输出 dhat-heap.json 与按分配点排序的摘要。

use std::hint::black_box;
use std::sync::Arc;

use actant::common::{serialize_rkyv, RetryPolicy, TaskDefinition, TaskId, WorkflowId};
use actant::orchestrator::{Dag, DagNode, FifoScheduler, PriorityScheduler, Scheduler};

// dhat 全局分配器 — 仅在此 bench 二进制中注册，启用 dhat-heap feature 时生效。
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn build_chain_dag(n: usize) -> Dag {
    let mut dag = Dag::new();
    for i in 0..n {
        dag.add_node(DagNode {
            task_id: TaskId::new(format!("task-{i}")),
            name: format!("task-{i}"),
            payload: vec![0u8; 64],
            retry_policy: Some(RetryPolicy::default()),
            timeout_ms: Some(30_000),
            priority: 0,
            metadata: std::collections::HashMap::new(),
        })
        .unwrap();
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

fn make_task(idx: usize) -> TaskDefinition {
    TaskDefinition {
        id: TaskId::new(format!("task-{idx}")),
        name: format!("task-{idx}"),
        payload: vec![0u8; 64],
        workflow_id: Some(WorkflowId::new("wf".into())),
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

fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    // 单一 profile 覆盖全部热点路径。
    // dhat 在进程退出时输出 dhat-heap.json + stderr 摘要（按分配点排序）。
    let _prof = dhat::Profiler::new_heap();

    // 1. DAG 构建（1000 节点链）
    let dag = build_chain_dag(1000);
    black_box(&dag);
    eprintln!("[mem] 1/5 dag build (1000 nodes) done");

    // 2. rkyv 序列化 1000 节点 DAG
    let dag_bytes = serialize_rkyv(&dag).unwrap();
    eprintln!(
        "[mem] 2/5 rkyv serialize dag(1000): serialized_size={} bytes",
        dag_bytes.len()
    );
    black_box(&dag_bytes);

    // 3. PriorityScheduler 入队 10000 任务
    let sched_p: Arc<dyn Scheduler> = Arc::new(PriorityScheduler::new());
    rt.block_on(async {
        for i in 0..10_000 {
            let _ = sched_p.enqueue(make_task(i)).await;
        }
    });
    eprintln!("[mem] 3/5 priority enqueue 10000 done");

    // 4. FifoScheduler 入队 10000 任务
    let sched_f: Arc<dyn Scheduler> = Arc::new(FifoScheduler::new());
    rt.block_on(async {
        for i in 0..10_000 {
            let _ = sched_f.enqueue(make_task(i)).await;
        }
    });
    eprintln!("[mem] 4/5 fifo enqueue 10000 done");

    // 5. Store put_batch 1000 键
    let dir = tempfile::tempdir().unwrap();
    let store =
        actant::store::Store::open_with_config(dir.path(), &actant::common::StoreConfig::default())
            .unwrap();
    let entries: Vec<(String, Vec<u8>)> = (0..1000)
        .map(|i| (format!("key-{i}"), vec![0u8; 128]))
        .collect();
    store.put_batch(&entries).unwrap();
    eprintln!("[mem] 5/5 store put_batch 1000 done");

    eprintln!("[mem] memory profiling complete — see dhat-heap.json for full per-site report");
    // _prof 在此处 drop，触发 dhat 输出
}
