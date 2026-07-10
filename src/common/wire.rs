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
    TOPIC_DAG_STATE, TOPIC_FAILOVER, TOPIC_HEADS, TOPIC_HEARTBEAT, TOPIC_WORKFLOW_STATE_REQ,
    TOPIC_WORKFLOW_STATE_RESP_PREFIX, WIRE_PROTOCOL_VERSION,
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
/// 由 [`Topic::classify`] 产生。路由器代码匹配此枚举而非做字符串前缀比较，
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
}

impl WireEnvelope {
    /// 用当前协议版本封装 [`WireMessage`]。
    pub fn wrap(msg: WireMessage) -> Self {
        Self {
            version: constants::WIRE_PROTOCOL_VERSION,
            message: msg,
        }
    }

    /// 从原始字节解码并校验协议封装。
    ///
    /// 这是反序列化 gossip 消息的唯一入口：集中执行协议版本校验，
    /// 调用方无需重复版本比较样板代码。
    ///
    /// 反序列化失败或协议版本不兼容时返回 `None`（并记录告警）。
    pub fn decode(payload: &[u8]) -> Option<WireMessage> {
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
        Some(envelope.message)
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
    /// 返回 [`state_str`] 常量之一 — 绝不返回字面量。
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
    /// 与 [`state_str`] 常量（小写形式）做大小写不敏感比较，
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
mod tests {
    use super::*;

    fn node(s: &str) -> NodeId {
        NodeId::from(s.to_string())
    }

    // --- Topic 构造器 ---

    #[test]
    fn topic_task_uses_correct_prefix() {
        let t = Topic::task(&node("worker-1"));
        assert_eq!(t.as_str(), "actant:task:worker-1");
        assert!(t.starts_with(constants::TOPIC_TASK_PREFIX));
    }

    #[test]
    fn topic_actor_uses_correct_prefix() {
        let t = Topic::actor(&node("node-a"));
        assert_eq!(t.as_str(), "actant:actor:node-a");
    }

    #[test]
    fn topic_actor_reply_distinct_from_actor() {
        let n = node("n1");
        assert_ne!(Topic::actor(&n), Topic::actor_reply(&n));
        assert_eq!(Topic::actor_reply(&n).as_str(), "actant:actor-reply:n1");
    }

    #[test]
    fn topic_dag_state_is_constant() {
        assert_eq!(Topic::dag_state().as_str(), constants::TOPIC_DAG_STATE);
    }

    #[test]
    fn topic_heartbeat_is_constant() {
        assert_eq!(Topic::heartbeat().as_str(), constants::TOPIC_HEARTBEAT);
    }

    #[test]
    fn topic_failover_is_constant() {
        assert_eq!(Topic::failover().as_str(), constants::TOPIC_FAILOVER);
    }

    #[test]
    fn topic_heads_is_constant() {
        assert_eq!(Topic::heads().as_str(), constants::TOPIC_HEADS);
    }

    #[test]
    fn topic_workflow_state_req_resp_are_distinct() {
        let n = node("worker-9");
        let req = Topic::workflow_state_req(&n);
        let resp = Topic::workflow_state_resp(&n);
        assert_ne!(req, resp);
        assert!(req
            .as_str()
            .starts_with(constants::TOPIC_WORKFLOW_STATE_REQ));
        assert!(resp
            .as_str()
            .starts_with(constants::TOPIC_WORKFLOW_STATE_RESP_PREFIX));
    }

    // --- Topic::classify 路由分发（关键路径） ---

    #[test]
    fn classify_task_extracts_node() {
        assert_eq!(
            Topic::task(&node("w1")).classify(),
            TopicRoute::Task("w1".to_string())
        );
    }

    #[test]
    fn classify_actor_extracts_node() {
        assert_eq!(
            Topic::actor(&node("a1")).classify(),
            TopicRoute::Actor("a1".to_string())
        );
    }

    #[test]
    fn classify_actor_reply_extracts_node() {
        assert_eq!(
            Topic::actor_reply(&node("r1")).classify(),
            TopicRoute::ActorReply("r1".to_string())
        );
    }

    #[test]
    fn classify_workflow_state_req_resp() {
        assert_eq!(
            Topic::workflow_state_req(&node("n")).classify(),
            TopicRoute::WorkflowStateReq("n".to_string())
        );
        assert_eq!(
            Topic::workflow_state_resp(&node("n")).classify(),
            TopicRoute::WorkflowStateResp("n".to_string())
        );
    }

    #[test]
    fn classify_dag_state_heartbeat_failover_heads() {
        assert_eq!(Topic::dag_state().classify(), TopicRoute::DagState);
        assert_eq!(Topic::heartbeat().classify(), TopicRoute::Heartbeat);
        assert_eq!(Topic::failover().classify(), TopicRoute::Failover);
        assert_eq!(Topic::heads().classify(), TopicRoute::Heads);
    }

    #[test]
    fn classify_unknown_for_unrecognized_topic() {
        assert_eq!(Topic::from("garbage").classify(), TopicRoute::Unknown);
        assert_eq!(Topic::from("").classify(), TopicRoute::Unknown);
    }

    #[test]
    fn classify_actor_prefix_does_not_match_task_prefix() {
        // 关键：prefix 不能互相包含，否则路由错误
        let actor_topic = Topic::actor(&node("task"));
        assert_eq!(
            actor_topic.classify(),
            TopicRoute::Actor("task".to_string())
        );
        // 反向：task topic 不应被分类为 actor
        let task_topic = Topic::task(&node("actor"));
        assert_eq!(task_topic.classify(), TopicRoute::Task("actor".to_string()));
    }

    // --- Topic::from 边界保护 ---

    #[test]
    fn from_truncates_oversized_topic() {
        let huge = "x".repeat(constants::MAX_TOPIC_LEN + 10);
        let t = Topic::from(huge.as_str());
        assert_eq!(t.as_str().len(), constants::MAX_TOPIC_LEN);
    }

    #[test]
    fn from_preserves_normal_length_topic() {
        let s = "actant:task:worker-1";
        let t = Topic::from(s);
        assert_eq!(t.as_str(), s);
    }

    /// 回归测试：UTF-8 多字节字符在 MAX_TOPIC_LEN 边界处截断不应 panic。
    ///
    /// 曾用 `s[..MAX_TOPIC_LEN]` 按字节切片，截断点落在多字节 UTF-8 字符内部
    /// 会导致 panic。已改为字符边界安全截断。
    #[test]
    fn from_truncation_at_utf8_boundary_should_not_panic() {
        // 构造：255 个 ASCII + 1 个 3 字节中文字符 = 258 字节
        // 截断点 256 会切到中文字符的第二个字节
        let mut s = "a".repeat(255);
        s.push('中'); // U+4E2D, 3 字节 UTF-8
        assert!(
            s.len() > constants::MAX_TOPIC_LEN,
            "test string must exceed MAX_TOPIC_LEN"
        );

        // 修复后应成功返回，截断到 char 边界
        let t = Topic::from(s.as_str());
        // 应截断到 255 字节（中文字符完整被移除）
        assert_eq!(t.as_str().len(), 255);
        assert!(t.as_str().is_char_boundary(t.as_str().len()));
    }

    // --- Display / From<Topic> for String ---

    #[test]
    fn display_matches_as_str() {
        let t = Topic::task(&node("worker-1"));
        assert_eq!(format!("{}", t), t.as_str());
    }

    #[test]
    fn into_string_consumes_inner() {
        let t = Topic::heartbeat();
        let s: String = t.into();
        assert_eq!(s, constants::TOPIC_HEARTBEAT);
    }

    // --- WireEnvelope::decode 错误处理 ---

    /// 捕获 tracing 输出到 `Vec<u8>` 的 `MakeWriter`，用于断言 `decode` 发出 WARN。
    #[derive(Clone)]
    struct CapturingWriter {
        buf: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    }

    impl CapturingWriter {
        fn new() -> Self {
            Self {
                buf: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }
        fn captured(&self) -> String {
            String::from_utf8_lossy(&self.buf.lock().unwrap()).into_owned()
        }
    }

    impl std::io::Write for CapturingWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.buf.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturingWriter {
        type Writer = CapturingWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn test_decode_returns_none_on_invalid_payload() {
        assert!(WireEnvelope::decode(b"definitely-not-valid-postcard").is_none());
    }

    #[test]
    fn test_decode_logs_warning_on_invalid_payload() {
        // P1-B1：反序列化失败必须发出 warn，而非静默丢弃。
        let writer = CapturingWriter::new();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        let dispatch = tracing::dispatcher::Dispatch::new(subscriber);

        let result = tracing::dispatcher::with_default(&dispatch, || {
            WireEnvelope::decode(b"definitely-not-valid-postcard")
        });

        assert!(
            result.is_none(),
            "decode should return None on invalid payload"
        );
        let captured = writer.captured();
        assert!(
            captured.contains("WARN"),
            "decode should log a WARN on deserialization failure, got: {captured}"
        );
    }

    #[test]
    fn test_decode_logs_warning_on_version_mismatch() {
        // 版本不兼容路径已有 warn，此测试确保不回归。
        let envelope = WireEnvelope {
            version: 255, // 不存在的版本
            message: WireMessage::NodeHeartbeat(NodeHeartbeat {
                node_id: node("n"),
                active_workflows: vec![],
                timestamp_ms: 0,
                available_slots: 0,
                max_slots: 0,
                endpoint_addr: None,
            }),
        };
        let payload = postcard::to_allocvec::<WireEnvelope>(&envelope).unwrap();

        let writer = CapturingWriter::new();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        let dispatch = tracing::dispatcher::Dispatch::new(subscriber);

        let result =
            tracing::dispatcher::with_default(&dispatch, || WireEnvelope::decode(&payload));

        assert!(result.is_none());
        let captured = writer.captured();
        assert!(
            captured.contains("WARN"),
            "version mismatch should log WARN, got: {captured}"
        );
    }
}
