//! 内置 Capability 定义。
//!
//! 每个 `Capability` 声明"能做什么"，但不包含实现。具体实现由各模块的 handler 提供。
//!
//! # Capability 命名约定
//!
//! - 决策型（`EffectKind::Ask`）：返回 `Option<T>`，第一个 `Some` 决定结果
//! - 副作用型（`EffectKind::Perform`）：返回具体 `Result<T, E>`
//! - 反应型（`EffectKind::Emit`）：返回 `()`，所有 handler 顺序执行

use crate::capability::{Capability, CapabilityMeta, EffectKind};
use crate::common::{NodeId, TaskId, WorkflowId};

// ============================================================================
// 决策型 Capability
// ============================================================================

/// 决策型：任务路由到哪个节点。
///
/// # 请求
/// - `RouteCtx`：包含任务名、已知 peer、能力标签等上下文
///
/// # 响应
/// - `Option<NodeId>`：选中的目标节点，`None` 表示放弃决策
pub struct Routing;

impl Capability for Routing {
    type Request = RouteCtx;
    type Response = Option<NodeId>;
}

impl Routing {
    /// 返回该 capability 的元数据。
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("Routing", EffectKind::Ask)
    }
}

/// `Routing` capability 的请求上下文。
#[derive(Debug, Clone)]
pub struct RouteCtx {
    /// 任务名。
    pub task_name: String,
    /// 已知 peer 节点 ID 列表。
    pub peers: Vec<NodeId>,
    /// 任务能力标签（如 `["gpu"]`）。
    pub tags: Vec<String>,
    /// 本地节点 ID（用于 fallback）。
    pub local_node: NodeId,
}

// ============================================================================

/// 决策型：从任务队列选下一个要执行的任务。
pub struct Scheduling;

impl Capability for Scheduling {
    type Request = ScheduleCtx;
    type Response = Option<TaskId>;
}

impl Scheduling {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("Scheduling", EffectKind::Ask)
    }
}

/// `Scheduling` capability 的请求上下文。
#[derive(Debug, Clone)]
pub struct ScheduleCtx {
    /// 工作流 ID。
    pub workflow_id: WorkflowId,
    /// 待调度的任务 ID 列表（按优先级或 FIFO 排序）。
    pub pending: Vec<TaskId>,
    /// 当前 worker 的最大并发数。
    pub max_concurrent: usize,
}

// ============================================================================

/// 决策型：决定是否重试失败任务。
pub struct RetryPolicy;

impl Capability for RetryPolicy {
    type Request = RetryCtx;
    type Response = Option<bool>;
}

impl RetryPolicy {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("RetryPolicy", EffectKind::Ask)
    }
}

/// `RetryPolicy` capability 的请求上下文。
#[derive(Debug, Clone)]
pub struct RetryCtx {
    pub task_id: TaskId,
    pub attempt: u32,
    pub last_error: String,
    pub max_retries: u32,
}

// ============================================================================
// 副作用型 Capability
// ============================================================================

/// 副作用型：序列化对象为字节（替代旧 `PayloadSerializer`）。
pub struct Serialization;

impl Capability for Serialization {
    type Request = SerializationReq;
    type Response = Result<Vec<u8>, String>;
}

impl Serialization {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("Serialization", EffectKind::Perform)
    }
}

/// `Serialization` capability 的请求。
#[derive(Debug, Clone)]
pub enum SerializationReq {
    /// 序列化 Python 对象（已通过 PyO3 转换为不透明 bytes）。
    Dump { payload: Vec<u8> },
    /// 反序列化。
    Load { data: Vec<u8> },
}

// ============================================================================

/// 副作用型：网络传输（替代旧 `Transport` trait）。
///
/// 0.2.0 的内置实现包装 iroh `NetworkManager`。
pub struct Transport;

impl Capability for Transport {
    type Request = TransportReq;
    type Response = Result<(), String>;
}

impl Transport {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("Transport", EffectKind::Perform)
    }
}

/// `Transport` capability 的请求。
#[derive(Debug, Clone)]
pub enum TransportReq {
    /// 发送任务到目标节点。
    SendTask { target: NodeId, payload: Vec<u8> },
    /// 发送 Actor 消息到目标节点。
    SendActorMessage { target: NodeId, payload: Vec<u8> },
    /// 广播心跳。
    BroadcastHeartbeat { payload: Vec<u8> },
}

// ============================================================================

/// 副作用型：持久化存储（替代旧 `Store` trait）。
pub struct Store;

impl Capability for Store {
    type Request = StoreReq;
    type Response = Result<Option<Vec<u8>>, String>;
}

impl Store {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("Store", EffectKind::Perform)
    }
}

/// `Store` capability 的请求。
#[derive(Debug, Clone)]
pub enum StoreReq {
    /// 写入 key-value。
    Put { key: Vec<u8>, value: Vec<u8> },
    /// 读取 key。
    Get { key: Vec<u8> },
    /// 删除 key。
    Delete { key: Vec<u8> },
}

// ============================================================================

/// 副作用型：执行任务（worker 调度入口）。
pub struct Execute;

impl Capability for Execute {
    type Request = ExecuteCtx;
    type Response = Result<ExecuteOutcome, String>;
}

impl Execute {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("Execute", EffectKind::Perform)
    }
}

/// `Execute` capability 的请求上下文。
#[derive(Debug, Clone)]
pub struct ExecuteCtx {
    pub task_id: TaskId,
    pub workflow_id: WorkflowId,
    /// 已序列化的任务 payload（cloudpickle bytes）。
    pub payload: Vec<u8>,
    /// 超时（毫秒）。
    pub timeout_ms: u64,
}

/// `Execute` capability 的执行结果。
#[derive(Debug, Clone)]
pub struct ExecuteOutcome {
    pub task_id: TaskId,
    /// 已序列化的结果 payload。
    pub result_payload: Vec<u8>,
}

// ============================================================================
// 反应型 Capability（事件订阅）
// ============================================================================

/// 反应型：任务生命周期事件（替代旧 `actant.on(TaskEvent)`）。
pub struct TaskLifecycle;

impl Capability for TaskLifecycle {
    type Request = TaskEvent;
    type Response = ();
}

impl TaskLifecycle {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("TaskLifecycle", EffectKind::Emit)
    }
}

/// `TaskLifecycle` capability 的事件类型。
#[derive(Debug, Clone)]
pub enum TaskEvent {
    Started { task_id: TaskId, workflow_id: WorkflowId },
    Completed { task_id: TaskId, result_payload: Vec<u8> },
    Failed { task_id: TaskId, error: String, attempt: u32 },
    Retried { task_id: TaskId, next_attempt: u32 },
    Cancelled { task_id: TaskId },
}

// ============================================================================

/// 反应型：工作流生命周期事件。
pub struct WorkflowLifecycle;

impl Capability for WorkflowLifecycle {
    type Request = WorkflowEvent;
    type Response = ();
}

impl WorkflowLifecycle {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("WorkflowLifecycle", EffectKind::Emit)
    }
}

/// `WorkflowLifecycle` capability 的事件类型。
#[derive(Debug, Clone)]
pub enum WorkflowEvent {
    Submitted { workflow_id: WorkflowId },
    Started { workflow_id: WorkflowId },
    Completed { workflow_id: WorkflowId },
    Failed { workflow_id: WorkflowId, error: String },
    Cancelled { workflow_id: WorkflowId },
}

// ============================================================================

/// 反应型：节点生命周期事件。
pub struct NodeLifecycle;

impl Capability for NodeLifecycle {
    type Request = NodeEvent;
    type Response = ();
}

impl NodeLifecycle {
    pub fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("NodeLifecycle", EffectKind::Emit)
    }
}

/// `NodeLifecycle` capability 的事件类型。
#[derive(Debug, Clone)]
pub enum NodeEvent {
    Started { node_id: NodeId },
    Stopped { node_id: NodeId },
    PeerJoined { peer_id: NodeId },
    PeerLeft { peer_id: NodeId },
    Heartbeat { node_id: NodeId, timestamp_ms: u64 },
}

// ============================================================================

/// 内置 Capability 注册表（用于 Runtime 启动时自动注册空 layer）。
///
/// 用户调用 `Runtime::start()` 时，未显式提供 handler 的内置 capability
/// 会使用默认内置实现（如 `PriorityScheduler`、`IrohTransport`）。
pub fn builtin_capabilities() -> Vec<CapabilityMeta> {
    vec![
        Routing::meta(),
        Scheduling::meta(),
        RetryPolicy::meta(),
        Serialization::meta(),
        Transport::meta(),
        Store::meta(),
        Execute::meta(),
        TaskLifecycle::meta(),
        WorkflowLifecycle::meta(),
        NodeLifecycle::meta(),
    ]
}
