use std::fmt;

use serde::{Deserialize, Serialize};

use super::model::{
    ActorId, ActorMessageResult, MessageId, NodeId, TaskDefinition, TaskId, WorkflowId,
};
use crate::runtime::state::HlcTimestamp;

/// 网络协议常量 — 所有魔法字符串与数值限制的唯一真相来源。
///
/// 集中管理可避免跨模块漂移，使协议变更可在一处审计。
pub mod constants {
    /// 当前协议版本。协议格式发生破坏性变更时递增。
    pub const WIRE_PROTOCOL_VERSION: u8 = 1;

    // --- Gossip 话题前缀与名称 ---

    pub const TOPIC_TASK_PREFIX: &str = "actant:task:";
    pub const TOPIC_DAG_STATE: &str = "actant:dag-state";
    pub const TOPIC_HEARTBEAT: &str = "actant:heartbeat";
    pub const TOPIC_FAILOVER: &str = "actant:failover";
    pub const TOPIC_HEADS: &str = "actant:heads";
    pub const TOPIC_ACTOR_PREFIX: &str = "actant:actor:";
    pub const TOPIC_ACTOR_REPLY_PREFIX: &str = "actant:actor-reply:";
    pub const TOPIC_WORKFLOW_STATE_REQ: &str = "actant:wf-state-req:";
    pub const TOPIC_WORKFLOW_STATE_RESP_PREFIX: &str = "actant:wf-state-resp:";
    /// 跨节点任务取消广播话题。
    pub const TOPIC_CANCEL: &str = "actant:cancel";
    /// Capability gossip 话题。
    ///
    /// 注意：与其他 `actant:` 前缀的 gossip topic 不同，此 topic 使用 `actant://` 前缀。
    /// 这是因为 capability gossip 走 `network.gossip_broadcast` 路径（直传字符串），不经过
    /// `Topic` 构造器；保持稳定字符串便于跨版本兼容性审计。
    pub const TOPIC_CAPABILITY_GOSSIP: &str = "actant://capability/gossip";

    // --- LMDB 持久化存储键前缀 ---

    pub mod store_keys {
        pub const DAG: &str = "orch:dag:";
        pub const EXEC: &str = "orch:exec:";
        pub const PENDING: &str = "orch:pending:";
        pub const RESULT: &str = "orch:result:";
        pub const LEASE: &str = "lease:";
        pub const CHECKPOINT: &str = "ckpt:";
    }

    // --- 限制 ---

    /// 话题字符串最大长度。超过此长度的话题将被拒绝，防止畸形 gossip 消息导致内存无限增长。
    pub const MAX_TOPIC_LEN: usize = 256;
}

// 在模块根重新导出常量，供单名导入使用（`crate::common::WIRE_PROTOCOL_VERSION` 等）。
// 仅重新导出在 `wire.rs` 外部实际使用的常量。话题前缀常量保持内部可见：
// 所有话题构造必须通过 `Topic::task(...)`、`Topic::actor(...)` 等构造器进行。
pub use constants::{
    store_keys::{
        DAG as STORE_KEY_DAG, EXEC as STORE_KEY_EXEC, LEASE as STORE_KEY_LEASE,
        PENDING as STORE_KEY_PENDING, RESULT as STORE_KEY_RESULT,
    },
    TOPIC_CANCEL, TOPIC_DAG_STATE, TOPIC_FAILOVER, TOPIC_HEADS, TOPIC_HEARTBEAT,
    TOPIC_WORKFLOW_STATE_REQ, TOPIC_WORKFLOW_STATE_RESP_PREFIX, WIRE_PROTOCOL_VERSION,
};

/// Gossip 话题标识符。
///
/// 包装 `String` 的新类型，集中话题构造，避免调用方手工拼接话题字符串。
/// 使用 `Topic::task(node)`、`Topic::actor(node)` 等构造器，而非 `format!(...)`。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Topic(pub String);

impl Topic {
    /// 构造节点作用域的任务话题：`actant:task:<node>`。
    pub fn task(node: &NodeId) -> Self {
        Self(format!("{}{}", constants::TOPIC_TASK_PREFIX, node.as_str()))
    }

    /// 构造节点作用域的 Actor 话题：`actant:actor:<node>`。
    pub fn actor(node: &NodeId) -> Self {
        Self(format!(
            "{}{}",
            constants::TOPIC_ACTOR_PREFIX,
            node.as_str()
        ))
    }

    /// 构造节点作用域的 Actor 回复话题：`actant:actor-reply:<node>`。
    pub fn actor_reply(node: &NodeId) -> Self {
        Self(format!(
            "{}{}",
            constants::TOPIC_ACTOR_REPLY_PREFIX,
            node.as_str()
        ))
    }

    /// 构造 DAG 状态话题：`actant:dag-state`。
    pub fn dag_state() -> Self {
        Self(constants::TOPIC_DAG_STATE.to_string())
    }

    /// 构造心跳话题：`actant:heartbeat`。
    pub fn heartbeat() -> Self {
        Self(constants::TOPIC_HEARTBEAT.to_string())
    }

    /// 构造故障转移话题：`actant:failover`。
    pub fn failover() -> Self {
        Self(constants::TOPIC_FAILOVER.to_string())
    }

    /// 构造 heads 话题：`actant:heads`。
    pub fn heads() -> Self {
        Self(constants::TOPIC_HEADS.to_string())
    }

    /// 构造节点作用域的工作流状态请求话题：`actant:wf-state-req:<node>`。
    pub fn workflow_state_req(node: &NodeId) -> Self {
        Self(format!(
            "{}{}",
            constants::TOPIC_WORKFLOW_STATE_REQ,
            node.as_str()
        ))
    }

    /// 构造节点作用域的工作流状态响应话题：`actant:wf-state-resp:<node>`。
    pub fn workflow_state_resp(node: &NodeId) -> Self {
        Self(format!(
            "{}{}",
            constants::TOPIC_WORKFLOW_STATE_RESP_PREFIX,
            node.as_str()
        ))
    }

    /// 判断此话题是否以给定前缀开头。
    pub fn starts_with(&self, prefix: &str) -> bool {
        self.0.starts_with(prefix)
    }

    /// 查看底层字符串。
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// 将此话题分类为 [`TopicRoute`] 用于分发。
    ///
    /// 用单一可穷举匹配的枚举替代路由器中零散的 `starts_with` 链，
    /// 使分发表显式化，确保新话题都能被处理。
    pub fn classify(&self) -> TopicRoute {
        let s = self.as_str();
        if let Some(node) = s.strip_prefix(constants::TOPIC_TASK_PREFIX) {
            TopicRoute::Task(node.to_string())
        } else if let Some(node) = s.strip_prefix(constants::TOPIC_ACTOR_PREFIX) {
            TopicRoute::Actor(node.to_string())
        } else if let Some(node) = s.strip_prefix(constants::TOPIC_ACTOR_REPLY_PREFIX) {
            TopicRoute::ActorReply(node.to_string())
        } else if let Some(node) = s.strip_prefix(constants::TOPIC_WORKFLOW_STATE_REQ) {
            TopicRoute::WorkflowStateReq(node.to_string())
        } else if let Some(node) = s.strip_prefix(constants::TOPIC_WORKFLOW_STATE_RESP_PREFIX) {
            TopicRoute::WorkflowStateResp(node.to_string())
        } else {
            match s {
                constants::TOPIC_DAG_STATE => TopicRoute::DagState,
                constants::TOPIC_HEARTBEAT => TopicRoute::Heartbeat,
                constants::TOPIC_FAILOVER => TopicRoute::Failover,
                constants::TOPIC_HEADS => TopicRoute::Heads,
                constants::TOPIC_CANCEL => TopicRoute::Cancel,
                _ => TopicRoute::Unknown,
            }
        }
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<Topic> for String {
    fn from(t: Topic) -> String {
        t.0
    }
}

impl From<&str> for Topic {
    fn from(s: &str) -> Self {
        // 防御性边界：及早截断超长话题字符串，防止单个畸形 gossip 消息
        // 触发下游订阅者的无限分配。使用字符边界安全截断，避免在 UTF-8
        // 多字节字符中间切割导致 panic。
        if s.len() > constants::MAX_TOPIC_LEN {
            tracing::warn!(
                len = s.len(),
                max = constants::MAX_TOPIC_LEN,
                "truncating oversized topic string"
            );
            // 回退到最近的 char 边界，确保不在多字节字符中间切割
            let mut end = constants::MAX_TOPIC_LEN;
            while !s.is_char_boundary(end) {
                end -= 1;
            }
            Self(s[..end].to_string())
        } else {
            Self(s.to_string())
        }
    }
}

/// Gossip 话题路由分类。
///
/// 由 `Topic::classify` 产生。路由器代码匹配此枚举而非做字符串前缀比较，
/// 使分发表显式且可穷举。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TopicRoute {
    /// 节点作用域的任务分发话题。
    Task(String),
    /// 节点作用域的 Actor 请求话题。
    Actor(String),
    /// 节点作用域的 Actor 回复话题。
    ActorReply(String),
    /// 节点作用域的工作流状态请求话题。
    WorkflowStateReq(String),
    /// 节点作用域的工作流状态响应话题。
    WorkflowStateResp(String),
    /// DAG 状态更新广播话题。
    DagState,
    /// 节点心跳广播话题。
    Heartbeat,
    /// 故障转移/租约声明广播话题。
    Failover,
    /// Heads 交换（工作流进度）话题。
    Heads,
    /// 跨节点任务取消广播话题。
    Cancel,
    /// 未识别话题 — 记录日志后丢弃。
    Unknown,
}

/// 远程 Actor 回复路由共享注册表。以 correlation_id（MessageId）为键，值为 oneshot 发送端。
pub type ReplyRegistry =
    dashmap::DashMap<MessageId, tokio::sync::oneshot::Sender<ActorMessageResult>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireEnvelope {
    pub version: u8,
    pub message: WireMessage,
    /// 跨节点 trace 关联 ID。
    ///
    /// 发送方在 [`WireEnvelope::wrap`] 时生成（UUID v4），接收方在
    /// [`WireEnvelope::decode`] 后用于创建 `wire.recv` 子 span，使跨节点
    /// 消息流可在日志中通过该 ID 串联。
    ///
    /// 该字段不参与协议版本协商，旧版本节点发送的消息反序列化时为 `None`
    /// （`#[serde(default)]`），新版本节点发送给旧版本节点的消息会被
    /// 旧版本因 version 不匹配而丢弃，无前向兼容问题。
    #[serde(default)]
    pub trace_id: Option<String>,
}

impl WireEnvelope {
    /// 用当前协议版本封装 [`WireMessage`]，并注入跨节点 trace 关联 ID。
    ///
    /// 生成的 `trace_id` 同时通过 `tracing::Span::current()` 记录到发送方
    /// 当前 span 的 `wire.trace_id` field，便于在发送方日志中按 ID 检索。
    pub fn wrap(msg: WireMessage) -> Self {
        let trace_id = uuid::Uuid::new_v4().to_string();
        tracing::Span::current().record("wire.trace_id", &trace_id);
        Self {
            version: constants::WIRE_PROTOCOL_VERSION,
            message: msg,
            trace_id: Some(trace_id),
        }
    }

    /// 从原始字节解码并校验协议封装。
    ///
    /// 这是反序列化 gossip 消息的唯一入口：集中执行协议版本校验，
    /// 调用方无需重复版本比较样板代码。
    ///
    /// 反序列化失败或协议版本不兼容时返回 `None`（并记录告警）。
    ///
    /// 返回的元组包含消息本体与可选的跨节点 trace 关联 ID；调用方应使用
    /// 该 ID 创建 `wire.recv` 子 span 以串联跨节点日志。
    pub fn decode(payload: &[u8]) -> Option<(WireMessage, Option<String>)> {
        // 远端 gossip 输入：先校验大小上限，避免恶意嵌套结构 OOM。
        let envelope = match crate::common::decode_postcard::<WireEnvelope>(payload) {
            Ok(env) => env,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    payload_len = payload.len(),
                    "dropping message: failed to deserialize WireEnvelope"
                );
                return None;
            }
        };
        if envelope.version != constants::WIRE_PROTOCOL_VERSION {
            tracing::warn!(
                "dropping message with incompatible protocol version: got {}, expected {}",
                envelope.version,
                constants::WIRE_PROTOCOL_VERSION
            );
            return None;
        }
        Some((envelope.message, envelope.trace_id))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireMessage {
    TaskDispatch(TaskDefinition),
    DagStateUpdate(WireDagStateUpdate),
    NodeHeartbeat(NodeHeartbeat),
    OrchestratorClaim(OrchestratorClaim),
    HeadsExchange(HeadsExchange),
    RemoteActorReply(RemoteActorReply),
    WorkflowStateRequest(WorkflowStateRequest),
    WorkflowStateResponse(WorkflowStateResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHeartbeat {
    pub node_id: NodeId,
    pub active_workflows: Vec<WorkflowId>,
    /// UNIX 纪元起的墙钟时间戳（毫秒）。用于故障检测（与 failure_timeout_ms 比较）。
    pub timestamp_ms: u64,
    /// 本节点可用任务槽位。
    #[serde(default)]
    pub available_slots: u32,
    /// 本节点最大任务槽位。
    #[serde(default)]
    pub max_slots: u32,
    /// Iroh endpoint ID（公钥），用于直连。
    #[serde(default)]
    pub endpoint_addr: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorClaim {
    pub node_id: NodeId,
    pub workflow_id: WorkflowId,
    /// UNIX 纪元起的墙钟时间戳（毫秒）。用于租约过期计算。
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireTaskResult {
    pub workflow_id: WorkflowId,
    pub task_id: TaskId,
    pub task_name: String,
    pub outcome: WireTaskOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireTaskOutcome {
    Completed(Vec<u8>),
    Failed(String),
    Cancelled,
    Skipped,
}

/// 跨节点任务取消广播消息。
///
/// 由节点通过 gossip topic ``actant:cancel`` 广播，接收方根据 task_id/workflow_id
/// 定位本地正在执行的任务并触发取消。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelBroadcast {
    pub task_id: TaskId,
    pub workflow_id: WorkflowId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireDagStateUpdate {
    pub workflow_id: WorkflowId,
    pub task_id: TaskId,
    pub task_state: WireTaskState,
    /// 混合逻辑时钟时间戳。wall_time 为 UNIX 纪元起的纳秒。
    /// 用于跨分布式节点的 CRDT 式冲突解决。
    pub hlc_timestamp: HlcTimestamp,
    pub origin_node: NodeId,
}

/// [`WireTaskState`] 变体的稳定字符串标识符。
///
/// 这是 Rust↔Python 状态映射的唯一真相来源。
/// `as_str()` 和 `from_python_str()` 均引用这些常量，确保双向不漂移。
///
/// 注意：此处无 `PENDING` 常量，因为 `WireTaskState` 仅携带分发后状态。
/// Pending 由 `WireTaskState` 条目缺失表示，并在编排器侧以 `Phase::Pending` 呈现。
pub mod state_str {
    pub const RUNNING: &str = "Running";
    pub const COMPLETED: &str = "Completed";
    pub const FAILED: &str = "Failed";
    pub const CANCELLED: &str = "Cancelled";
    pub const SKIPPED: &str = "Skipped";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireTaskState {
    Running,
    Completed { result: Vec<u8> },
    Failed { error: String },
    Cancelled,
    Skipped,
}

impl WireTaskState {
    /// PyO3 边界使用的稳定字符串表示。
    ///
    /// 返回 `state_str` 常量之一 — 绝不返回字面量。
    pub fn as_str(&self) -> &'static str {
        match self {
            WireTaskState::Running => state_str::RUNNING,
            WireTaskState::Completed { .. } => state_str::COMPLETED,
            WireTaskState::Failed { .. } => state_str::FAILED,
            WireTaskState::Cancelled => state_str::CANCELLED,
            WireTaskState::Skipped => state_str::SKIPPED,
        }
    }

    /// 将 Python 层的状态字符串解析为 `WireTaskState`。
    ///
    /// 与 `state_str` 常量（小写形式）做大小写不敏感比较，
    /// 以容忍异构 gossip 来源。无法识别的状态字符串返回 `None`。
    pub fn from_python_str(state: &str, data: Vec<u8>) -> Option<Self> {
        // 与规范常量做大小写不敏感比较。
        let lower = state.to_ascii_lowercase();
        let ok = |c: &'static str| lower == c.to_ascii_lowercase();
        if ok(state_str::COMPLETED) {
            Some(WireTaskState::Completed { result: data })
        } else if ok(state_str::FAILED) {
            Some(WireTaskState::Failed {
                error: String::from_utf8_lossy(&data).into_owned(),
            })
        } else if ok(state_str::RUNNING) {
            Some(WireTaskState::Running)
        } else if ok(state_str::CANCELLED) {
            Some(WireTaskState::Cancelled)
        } else if ok(state_str::SKIPPED) {
            Some(WireTaskState::Skipped)
        } else {
            None
        }
    }
}

impl WireTaskOutcome {
    /// PyO3 边界使用的稳定字符串表示。
    pub fn as_str(&self) -> &'static str {
        match self {
            WireTaskOutcome::Completed(_) => state_str::COMPLETED,
            WireTaskOutcome::Failed(_) => state_str::FAILED,
            WireTaskOutcome::Cancelled => state_str::CANCELLED,
            WireTaskOutcome::Skipped => state_str::SKIPPED,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteActorRequest {
    pub target: ActorId,
    pub method: String,
    pub payload: Vec<u8>,
    /// 若存在，源节点应通过 `actant:actor-reply:{origin_node}` 话题接收回复，
    /// 并以此 correlation_id 关联。
    pub reply_to: Option<RemoteReplyAddress>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteReplyAddress {
    pub node_id: NodeId,
    pub correlation_id: MessageId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteActorReply {
    pub correlation_id: MessageId,
    pub result: ActorMessageResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStateRequest {
    pub workflow_id: WorkflowId,
    pub requesting_node: NodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStateResponse {
    pub workflow_id: WorkflowId,
    pub dag: Option<Vec<u8>>,
    pub execution: Option<Vec<u8>>,
    pub pending: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowHead {
    pub workflow_id: WorkflowId,
    /// 已成功完成的任务数。
    pub succeeded_count: usize,
    pub total_count: usize,
    pub hlc_timestamp: HlcTimestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeadsExchange {
    pub node_id: NodeId,
    pub heads: Vec<WorkflowHead>,
}

#[cfg(test)]
#[path = "../../tests/rust/unit/common/wire.rs"]
mod tests;
