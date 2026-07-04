//! 内置 Capability 的默认 handler 实现。
//!
//! 这些 handler 提供开箱即用的最小可行实现，用户可通过注册自定义 Layer 覆盖。
//!
//! # 设计
//!
//! - `LocalRouter`：Routing 默认实现，总是返回本地节点
//! - `FifoScheduler`：Scheduling 默认实现，FIFO 顺序
//! - `DefaultRetryPolicy`：RetryPolicy 默认实现，按 max_retries 判断
//! - `PassthroughSerializer`：Serialization 默认实现，透传 bytes（实际序列化在 Python 侧）
//! - `NoopTransport`：Transport 默认实现，空操作（单节点模式）
//! - `MemoryStore`：Store 默认实现，内存 HashMap
//! - `LifecycleLogger`：Lifecycle 默认实现，tracing 日志

use std::collections::HashMap;

use async_trait::async_trait;
use parking_lot::Mutex;
use tracing::info;

use crate::capability::builtins::{
    Execute, ExecuteCtx, ExecuteOutcome, NodeEvent, RetryCtx, RouteCtx, Routing,
    ScheduleCtx, Scheduling, Serialization, SerializationReq, Store, StoreReq, TaskEvent,
    TaskLifecycle, Transport, TransportReq, WorkflowEvent, WorkflowLifecycle,
    NodeLifecycle, RetryPolicy,
};
use crate::capability::{Handler, Layer};

// ============================================================================
// Routing 默认实现
// ============================================================================

/// 默认路由器：总是返回本地节点。
///
/// 适用于单节点模式或作为 fallback（chain 末尾）。
pub struct LocalRouter;

#[async_trait]
impl Handler<Routing> for LocalRouter {
    async fn handle(&self, req: RouteCtx) -> Option<Option<crate::common::NodeId>> {
        Some(Some(req.local_node))
    }
}

/// Tag 亲和路由器：选择第一个匹配 tag 的 peer。
///
/// 若无匹配，返回 `None`（放弃决策，由后续 handler 处理）。
pub struct TagAffinityRouter {
    /// 节点 tag 映射（node_id_str -> tags）。
    pub node_tags: HashMap<String, Vec<String>>,
}

impl TagAffinityRouter {
    pub fn new(node_tags: HashMap<String, Vec<String>>) -> Self {
        Self { node_tags }
    }
}

#[async_trait]
impl Handler<Routing> for TagAffinityRouter {
    async fn handle(&self, req: RouteCtx) -> Option<Option<crate::common::NodeId>> {
        if req.tags.is_empty() {
            return None;
        }
        for peer in &req.peers {
            let peer_str = peer.to_string();
            if let Some(peer_tags) = self.node_tags.get(&peer_str) {
                if req.tags.iter().all(|t| peer_tags.contains(t)) {
                    return Some(Some(peer.clone()));
                }
            }
        }
        None
    }
}

// ============================================================================
// Scheduling 默认实现
// ============================================================================

/// FIFO 调度器：返回 pending 列表的第一个任务。
pub struct FifoScheduler;

#[async_trait]
impl Handler<Scheduling> for FifoScheduler {
    async fn handle(&self, req: ScheduleCtx) -> Option<Option<crate::common::TaskId>> {
        req.pending.first().cloned().map(Some)
    }
}

/// 优先级调度器：按 priority 字段排序（需 ctx 扩展，暂用 FIFO 兜底）。
pub struct PriorityScheduler;

#[async_trait]
impl Handler<Scheduling> for PriorityScheduler {
    async fn handle(&self, req: ScheduleCtx) -> Option<Option<crate::common::TaskId>> {
        // TODO: 按 priority 排序；当前 FIFO 兜底
        req.pending.first().cloned().map(Some)
    }
}

// ============================================================================
// RetryPolicy 默认实现
// ============================================================================

/// 默认重试策略：attempt < max_retries 时返回 `true`。
pub struct DefaultRetryPolicy;

#[async_trait]
impl Handler<RetryPolicy> for DefaultRetryPolicy {
    async fn handle(&self, req: RetryCtx) -> Option<Option<bool>> {
        Some(Some(req.attempt < req.max_retries))
    }
}

// ============================================================================
// Serialization 默认实现
// ============================================================================

/// 透传序列化器：直接返回 bytes（实际序列化由 Python 侧 cloudpickle 处理）。
///
/// Rust 侧不解析 payload 语义，仅做透传。
pub struct PassthroughSerializer;

#[async_trait]
impl Handler<Serialization> for PassthroughSerializer {
    async fn handle(&self, req: SerializationReq) -> Option<Result<Vec<u8>, String>> {
        match req {
            SerializationReq::Dump { payload } => Some(Ok(payload)),
            SerializationReq::Load { data } => Some(Ok(data)),
        }
    }
}

// ============================================================================
// Transport 默认实现
// ============================================================================

/// 空操作传输层：单节点模式下不发送任何网络消息。
///
/// 多节点模式应注册 iroh-backed handler 覆盖。
pub struct NoopTransport;

#[async_trait]
impl Handler<Transport> for NoopTransport {
    async fn handle(&self, _req: TransportReq) -> Option<Result<(), String>> {
        Some(Ok(()))
    }
}

// ============================================================================
// Store 默认实现
// ============================================================================

/// 内存存储：用 `HashMap<Vec<u8>, Vec<u8>>` 存储。
///
/// 适用于测试与单节点临时模式。生产环境应注册 LMDB-backed handler。
pub struct MemoryStore {
    inner: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Handler<Store> for MemoryStore {
    async fn handle(&self, req: StoreReq) -> Option<Result<Option<Vec<u8>>, String>> {
        let mut map = self.inner.lock();
        match req {
            StoreReq::Put { key, value } => {
                map.insert(key, value);
                Some(Ok(None))
            }
            StoreReq::Get { key } => Some(Ok(map.get(&key).cloned())),
            StoreReq::Delete { key } => {
                map.remove(&key);
                Some(Ok(None))
            }
        }
    }
}

// ============================================================================
// Execute 默认实现
// ============================================================================

/// 默认执行器：透传 payload（实际执行由 Python 侧 worker 处理）。
///
/// Rust 侧不解析 task payload 语义。
pub struct PassthroughExecutor;

#[async_trait]
impl Handler<Execute> for PassthroughExecutor {
    async fn handle(&self, req: ExecuteCtx) -> Option<Result<ExecuteOutcome, String>> {
        // Rust 侧无法执行 Python callable，返回 payload 作为结果（占位）
        // 真实执行由 Python 侧 worker 通过 PyO3 注册的 handler 完成
        Some(Ok(ExecuteOutcome {
            task_id: req.task_id,
            result_payload: req.payload,
        }))
    }
}

// ============================================================================
// Lifecycle 默认实现（emit）
// ============================================================================

/// 任务生命周期日志记录器。
pub struct TaskLifecycleLogger;

#[async_trait]
impl Handler<TaskLifecycle> for TaskLifecycleLogger {
    async fn handle(&self, event: TaskEvent) -> Option<()> {
        info!(event = ?event, "task lifecycle event");
        Some(())
    }
}

/// 工作流生命周期日志记录器。
pub struct WorkflowLifecycleLogger;

#[async_trait]
impl Handler<WorkflowLifecycle> for WorkflowLifecycleLogger {
    async fn handle(&self, event: WorkflowEvent) -> Option<()> {
        info!(event = ?event, "workflow lifecycle event");
        Some(())
    }
}

/// 节点生命周期日志记录器。
pub struct NodeLifecycleLogger;

#[async_trait]
impl Handler<NodeLifecycle> for NodeLifecycleLogger {
    async fn handle(&self, event: NodeEvent) -> Option<()> {
        info!(event = ?event, "node lifecycle event");
        Some(())
    }
}

// ============================================================================
// 默认 Layer 工厂
// ============================================================================

use crate::capability::{erase_handler, EffectKind};

/// 将所有默认 handler 注册到 Runtime。
///
/// 用户可在注册后通过 `runtime.chain::<Routing>(custom_handler)` 追加自定义 handler，
/// 自定义 handler 优先级更高（chain 顺序决定 ask 的决策顺序）。
pub fn register_defaults(runtime: &crate::capability::Runtime) {
    runtime.register(
        Layer::<Routing>::new(Routing::meta()).chain_erased(erase_handler(LocalRouter)),
    );
    runtime.register(
        Layer::<Scheduling>::new(Scheduling::meta())
            .chain_erased(erase_handler(FifoScheduler)),
    );
    runtime.register(
        Layer::<RetryPolicy>::new(RetryPolicy::meta())
            .chain_erased(erase_handler(DefaultRetryPolicy)),
    );
    runtime.register(
        Layer::<Serialization>::new(Serialization::meta())
            .chain_erased(erase_handler(PassthroughSerializer)),
    );
    runtime.register(
        Layer::<Transport>::new(Transport::meta())
            .chain_erased(erase_handler(NoopTransport)),
    );
    runtime.register(
        Layer::<Store>::new(Store::meta())
            .chain_erased(erase_handler(MemoryStore::new())),
    );
    runtime.register(
        Layer::<Execute>::new(Execute::meta())
            .chain_erased(erase_handler(PassthroughExecutor)),
    );
    runtime.register(
        Layer::<TaskLifecycle>::new(TaskLifecycle::meta())
            .chain_erased(erase_handler(TaskLifecycleLogger)),
    );
    runtime.register(
        Layer::<WorkflowLifecycle>::new(WorkflowLifecycle::meta())
            .chain_erased(erase_handler(WorkflowLifecycleLogger)),
    );
    runtime.register(
        Layer::<NodeLifecycle>::new(NodeLifecycle::meta())
            .chain_erased(erase_handler(NodeLifecycleLogger)),
    );
}

/// 返回默认 handler 的元信息（用于 PyO3 反射与文档）。
pub fn default_handler_metas() -> Vec<(&'static str, EffectKind)> {
    vec![
        ("Routing", EffectKind::Ask),
        ("Scheduling", EffectKind::Ask),
        ("RetryPolicy", EffectKind::Ask),
        ("Serialization", EffectKind::Perform),
        ("Transport", EffectKind::Perform),
        ("Store", EffectKind::Perform),
        ("Execute", EffectKind::Perform),
        ("TaskLifecycle", EffectKind::Emit),
        ("WorkflowLifecycle", EffectKind::Emit),
        ("NodeLifecycle", EffectKind::Emit),
    ]
}
