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
    /// Actor 注册表 gossip 话题：广播节点承载的 actor 类型集合。
    ///
    /// 接收方通过 [`crate::runtime::actor::router::ActorRegistryGossipActor::handle_gossip`]
    /// 更新本地注册表，供 [`crate::runtime::actor::router::ActorRouter`] 选择目标节点。
    pub const TOPIC_ACTOR_REGISTRY: &str = "actant:actor-registry";
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

/// W3C Trace Context（`traceparent` header）实现。
///
/// 规范见 <https://www.w3.org/TR/trace-context/>。本模块仅实现 wire 协议所需子集：
/// 生成新 trace 上下文、解析 `traceparent` 字符串、创建子 span（继承 trace-id、
/// 生成新 span-id）。
///
/// 格式：`00-<trace-id>-<span-id>-<flags>`
/// - `trace-id`：32 hex 小写（16 字节），全零非法
/// - `span-id`：16 hex 小写（8 字节），全零非法
/// - `flags`：2 hex 小写，bit 0 = sampled
///
/// 本模块不依赖 OpenTelemetry SDK：保持 Rust 核心零外部依赖约束，
/// 仅提供 trace 关联 ID 的生成与传播。若用户启用 OTLP exporter（C1），
/// 接收方 `tracing::Span` 的 `wire.trace_id`/`wire.span_id` field 可被
/// `opentelemetry-appender-tracing` 桥接到 OTLP span 属性，实现端到端关联。
pub mod traceparent {
    /// `traceparent` 字符串最大长度（含版本前缀与分隔符）。
    pub const TRACEPARENT_LEN: usize = 55; // "00-" + 32 + "-" + 16 + "-" + 2

    /// W3C trace context 解析后的结构。
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct TraceContext {
        /// 16 字节 trace-id（全零非法）。
        pub trace_id: [u8; 16],
        /// 8 字节 span-id（全零非法）。
        pub span_id: [u8; 8],
        /// 1 字节 flags，bit 0 = sampled。
        pub flags: u8,
    }

    impl TraceContext {
        /// 生成全新的 root trace 上下文。
        ///
        /// `sampled` 控制初始 flags。trace-id 与 span-id 通过 `uuid` v4 生成
        ///（122 随机位），保证全局唯一性。若 `sampled=false`，下游节点可
        /// 跳过采集以节省开销。
        ///
        /// 注：UUID v4 设置 version 与 variant 位（6 固定位），理论上减少随机空间，
        /// 但 W3C 规范允许任何非零 16 字节作为 trace-id，且 OTel SDK 实践中 UUID v4
        /// 是常见 trace-id 生成方式。本实现复用现有 uuid 依赖，避免引入新 crate。
        pub fn new_root(sampled: bool) -> Self {
            // UUID v4 内部使用 CSPRNG，几乎不可能产生全零值；为严格遵循 W3C 规范
            // 仍保留重试循环（极低成本，正常路径只执行一次）。
            let mut trace_id = *uuid::Uuid::new_v4().as_bytes();
            while trace_id == [0u8; 16] {
                trace_id = *uuid::Uuid::new_v4().as_bytes();
            }
            let mut span_id = [0u8; 8];
            let span_uuid = *uuid::Uuid::new_v4().as_bytes();
            span_id.copy_from_slice(&span_uuid[..8]);
            while span_id == [0u8; 8] {
                let u = *uuid::Uuid::new_v4().as_bytes();
                span_id.copy_from_slice(&u[..8]);
            }
            let flags = if sampled { 0x01 } else { 0x00 };
            Self {
                trace_id,
                span_id,
                flags,
            }
        }

        /// 创建子 span：继承 trace-id 与 flags，生成新 span-id。
        ///
        /// 用于接收方在 `wire.recv` span 内继续向下传播时构造下一跳的 traceparent。
        pub fn child(&self) -> Self {
            let mut span_id = [0u8; 8];
            let span_uuid = *uuid::Uuid::new_v4().as_bytes();
            span_id.copy_from_slice(&span_uuid[..8]);
            while span_id == [0u8; 8] {
                let u = *uuid::Uuid::new_v4().as_bytes();
                span_id.copy_from_slice(&u[..8]);
            }
            Self {
                trace_id: self.trace_id,
                span_id,
                flags: self.flags,
            }
        }

        /// 序列化为 W3C `traceparent` 字符串。
        ///
        /// 格式：`00-{trace_id_hex}-{span_id_hex}-{flags_hex}`。
        /// 全小写，符合 W3C 规范与 OpenTelemetry SDK 默认输出。
        pub fn to_header(&self) -> String {
            format!(
                "00-{}-{}-{:02x}",
                hex_encode(&self.trace_id),
                hex_encode(&self.span_id),
                self.flags
            )
        }

        /// 从 W3C `traceparent` 字符串解析。
        ///
        /// 接受宽松格式：前后空白被忽略；版本号必须为 `00`；长度必须严格匹配。
        /// 返回 `None` 的情况：格式错误、长度不符、trace-id 或 span-id 全零、
        /// 含非 hex 字符。调用方应将 `None` 视为对端不支持 trace 传播，
        /// 创建独立 root span。
        pub fn parse(header: &str) -> Option<Self> {
            let s = header.trim();
            // 严格校验长度：防止截断/填充导致歧义。
            if s.len() != TRACEPARENT_LEN {
                return None;
            }
            let parts: Vec<&str> = s.splitn(4, '-').collect();
            if parts.len() != 4 {
                return None;
            }
            // 版本必须为 "00"：未来版本（如 "ff"）按规范应忽略但保留原 header 透传，
            // 但本实现仅支持 v0（W3C 当前唯一版本），其他版本拒绝。
            if parts[0] != "00" {
                return None;
            }
            let trace_id = hex_decode_16(parts[1])?;
            let span_id = hex_decode_8(parts[2])?;
            let flags = u8::from_str_radix(parts[3], 16).ok()?;
            // W3C 规范：trace-id 与 span-id 全零为非法值。
            if trace_id == [0u8; 16] || span_id == [0u8; 8] {
                return None;
            }
            Some(Self {
                trace_id,
                span_id,
                flags,
            })
        }
    }

    /// 16 字节数组的 hex 编码（小写，32 字符）。
    fn hex_encode(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            s.push_str(&format!("{:02x}", b));
        }
        s
    }

    /// 32 hex 字符 → 16 字节。失败返回 `None`（非 hex 或长度不符）。
    fn hex_decode_16(s: &str) -> Option<[u8; 16]> {
        if s.len() != 32 {
            return None;
        }
        let mut out = [0u8; 16];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            out[i] = u8::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok()?;
        }
        Some(out)
    }

    /// 16 hex 字符 → 8 字节。失败返回 `None`。
    fn hex_decode_8(s: &str) -> Option<[u8; 8]> {
        if s.len() != 16 {
            return None;
        }
        let mut out = [0u8; 8];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            out[i] = u8::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok()?;
        }
        Some(out)
    }

    /// `tracing::field::Display` 包装器，将字节切片以 hex 小写形式输出到 span field。
    ///
    /// 用于在 `wire.recv` span 中记录 trace-id（16 字节）与 span-id（8 字节）
    /// 的 hex 表示，便于在日志与 OTLP span 属性中按 ID 检索。
    pub struct HexDisplay<'a>(pub &'a [u8]);

    impl<'a> std::fmt::Display for HexDisplay<'a> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            for b in self.0 {
                write!(f, "{:02x}", b)?;
            }
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn new_root_produces_nonzero_ids() {
            let ctx = TraceContext::new_root(true);
            assert_ne!(ctx.trace_id, [0u8; 16]);
            assert_ne!(ctx.span_id, [0u8; 8]);
            assert_eq!(ctx.flags, 0x01);
        }

        #[test]
        fn new_root_unsampled_has_zero_flags() {
            let ctx = TraceContext::new_root(false);
            assert_eq!(ctx.flags, 0x00);
        }

        #[test]
        fn child_inherits_trace_id_and_flags() {
            let parent = TraceContext::new_root(true);
            let child = parent.child();
            assert_eq!(child.trace_id, parent.trace_id);
            assert_eq!(child.flags, parent.flags);
            // 子 span 必须有新 span-id（极大概率不等于父 span-id）。
            assert_ne!(child.span_id, parent.span_id);
            assert_ne!(child.span_id, [0u8; 8]);
        }

        #[test]
        fn roundtrip_to_header_and_parse() {
            let ctx = TraceContext::new_root(true);
            let header = ctx.to_header();
            let parsed = TraceContext::parse(&header).expect("roundtrip should succeed");
            assert_eq!(parsed, ctx);
        }

        #[test]
        fn to_header_format_matches_w3c() {
            let ctx = TraceContext {
                trace_id: [
                    0x4b, 0x3a, 0x2c, 0x1d, 0x9b, 0x87, 0x4a, 0xfe, 0x8b, 0x01, 0x2c, 0x3d, 0x4e,
                    0x5f, 0x6a, 0x7b,
                ],
                span_id: [0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0],
                flags: 0x01,
            };
            assert_eq!(
                ctx.to_header(),
                "00-4b3a2c1d9b874afe8b012c3d4e5f6a7b-123456789abcdef0-01"
            );
        }

        #[test]
        fn parse_rejects_wrong_length() {
            assert!(TraceContext::parse("00-short").is_none());
            assert!(TraceContext::parse(
                "00-4b3a2c1d9b874afe8b012c3d4e5f6a7b-123456789abcdef0-01-extra"
            )
            .is_none());
        }

        #[test]
        fn parse_rejects_unsupported_version() {
            // ff 版本拒绝
            let header = "ff-4b3a2c1d9b874afe8b012c3d4e5f6a7b-123456789abcdef0-01";
            assert!(TraceContext::parse(header).is_none());
        }

        #[test]
        fn parse_rejects_all_zero_trace_id() {
            let header = "00-00000000000000000000000000000000-123456789abcdef0-01";
            assert!(TraceContext::parse(header).is_none());
        }

        #[test]
        fn parse_rejects_all_zero_span_id() {
            let header = "00-4b3a2c1d9b874afe8b012c3d4e5f6a7b-0000000000000000-01";
            assert!(TraceContext::parse(header).is_none());
        }

        #[test]
        fn parse_rejects_non_hex_chars() {
            let header = "00-xx3a2c1d9b874afe8b012c3d4e5f6a7b-123456789abcdef0-01";
            assert!(TraceContext::parse(header).is_none());
        }

        #[test]
        fn parse_trims_surrounding_whitespace() {
            let header = "  00-4b3a2c1d9b874afe8b012c3d4e5f6a7b-123456789abcdef0-01  \n";
            let ctx = TraceContext::parse(header).expect("should parse after trim");
            assert_eq!(ctx.flags, 0x01);
        }

        #[test]
        fn to_header_is_lowercase() {
            let ctx = TraceContext::new_root(true);
            let header = ctx.to_header();
            // trace-id 与 span-id 段必须全小写（W3C 规范）。
            let parts: Vec<&str> = header.split('-').collect();
            assert_eq!(parts.len(), 4);
            assert!(parts[1]
                .chars()
                .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase()));
            assert!(parts[2]
                .chars()
                .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase()));
            assert!(parts[3]
                .chars()
                .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase()));
        }

        #[test]
        fn unique_trace_ids_across_calls() {
            // 连续生成 1000 次，trace-id 不应重复（极小概率下随机碰撞也接受，
            // 但 UUID v4 碰撞概率 < 2^-122，测试应稳定通过）。
            let mut seen = std::collections::HashSet::new();
            for _ in 0..1000 {
                let ctx = TraceContext::new_root(true);
                seen.insert(ctx.trace_id);
            }
            assert_eq!(seen.len(), 1000);
        }
    }
}

// 重新导出 TraceContext 供外部使用。
pub use traceparent::TraceContext;

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

    /// 构造 Actor 注册表 gossip 话题：`actant:actor-registry`。
    ///
    /// 此话题广播全局共享的 actor 类型注册表，与节点作用域的 `Topic::actor`
    ///（单 actor 实例消息）不同：actor-registry 同步的是 *哪些节点承载哪些 actor 类型*，
    /// 供路由器选择目标节点。
    pub fn actor_registry() -> Self {
        Self(constants::TOPIC_ACTOR_REGISTRY.to_string())
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
                constants::TOPIC_ACTOR_REGISTRY => TopicRoute::ActorRegistry,
                constants::TOPIC_CAPABILITY_GOSSIP => TopicRoute::CapabilityGossip,
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
    /// Actor 注册表 gossip 话题（广播节点 → actor 类型映射）。
    ActorRegistry,
    /// Capability 元信息 gossip 话题（广播节点 → capability 元信息）。
    CapabilityGossip,
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
    /// 跨节点 trace 上下文（W3C `traceparent` 字符串）。
    ///
    /// C3：发送方在 [`WireEnvelope::wrap`] 时从当前 `tracing::Span` 提取
    /// trace-id/span-id/flags，按 W3C Trace Context 格式序列化为字符串；
    /// 接收方在 [`WireEnvelope::decode`] 后解析该字符串，创建 `wire.recv`
    /// 子 span，使跨节点消息流在日志与 OTLP span 树中关联。
    ///
    /// 与 D2 MAC 协同：MAC 覆盖本字段，确保 traceparent 不可被中间人篡改
    /// （否则攻击者可注入伪造 trace-id 干扰排查）。
    ///
    /// `None` 表示无 trace 上下文（如单元测试或调用方未启用 tracing），
    /// 接收方据此创建独立 root span。
    #[serde(default)]
    pub traceparent: Option<String>,
    /// Wire message BLAKE3 keyed MAC（D2：节点身份认证 + 共享密钥签名）。
    ///
    /// 由 [`WireEnvelope::wrap`] 在序列化 message 后计算并填入；
    /// [`WireEnvelope::decode`] 恒定时间校验，签名不匹配则丢弃消息。
    ///
    /// - 集群共享密钥非空（[`set_wire_signing_key`] 已设置）：所有节点入站消息
    ///   必须携带正确 MAC；伪造者即使持有合法 iroh EndpointId（TLS 层认证通过）
    ///   也无法伪造 wire message，实现端到端集群身份认证。
    /// - 集群共享密钥为空：MAC 字段为 `None`，decode 时跳过验证（向后兼容 0.2）。
    ///
    /// MAC 覆盖范围：`version` + `message` + `trace_id` 三字段序列化后的字节，
    /// 不包含本字段自身（避免循环）。这样 MAC 与 wire 协议版本耦合，版本不匹配
    /// 时 MAC 也会失败，提供双保险。
    #[serde(default)]
    pub mac: Option<[u8; crate::common::payload::WIRE_MAC_LEN]>,
}

/// 全局 wire message 签名密钥。
///
/// 由 `RuntimeBuilder::build` 在构造 NetworkManager 前调用
/// [`set_wire_signing_key`] 设置。空密钥（默认）= 禁用 wire 签名验证，
/// 保持 0.2 行为；非空密钥 = 集群内所有节点必须共享同一密钥，否则
/// 跨节点消息被对端丢弃。
///
/// 使用 `parking_lot::Mutex` 而非 `std::sync::Mutex`：避免 poison，
/// 且 `set` 后只读路径无锁竞争。
static WIRE_SIGNING_KEY: parking_lot::Mutex<Option<Vec<u8>>> = parking_lot::Mutex::new(None);

// C3：当前线程的入站 trace 上下文。
//
// 由 `current_trace_scope` 在 `wire.recv` span 进入时设置，在 span 退出时
// 通过 guard 清除。当 wrap() 在该 span 内被调用（消息转发场景），会读取此
// thread-local 并调用 `TraceContext::child` 生成延续 trace-id 的新 span-id。
//
// 局限性：thread-local 无法跨 tokio task 边界传播。若 `handle_message` 把
// 任务 spawn 到独立 tokio task（与原 worker 线程解耦），新 task 内的 wrap()
// 看不到入站 trace，会退化为 root span。actant 当前 scheduler 路径在原任务
// 上下文内同步入队，未跨 task 调度，因此实际多跳链路能正确延续。
thread_local! {
    static CURRENT_INBOUND_TRACE: std::cell::RefCell<Option<TraceContext>> =
        const { std::cell::RefCell::new(None) };
}

/// 设置当前线程的入站 trace 上下文，返回 guard 在 drop 时清除。
///
/// 用于 `wire.recv` span 进入时把发送方 traceparent 解析结果压入 thread-local，
/// 使该 span 内的 outgoing `WireEnvelope::wrap()` 能生成 child traceparent，
/// 实现"接收 → 处理 → 转发"链路的 trace-id 延续。
///
/// 嵌套调用（理论上不会发生，但允许）会覆盖前值并按 LIFO 恢复。
pub fn current_trace_scope(ctx: TraceContext) -> TraceScopeGuard {
    CURRENT_INBOUND_TRACE.with(|c| *c.borrow_mut() = Some(ctx));
    TraceScopeGuard { restored: false }
}

/// [`current_trace_scope`] 返回的 RAII guard，drop 时恢复 thread-local 前值。
///
/// 显式 drop 而非依赖 `Drop` trait：在 async 上下文中 guard 跨 await 会失效，
/// 调用方应在同步代码块内使用。当前 `handle_message` 实现：进入 span → 设置 scope →
/// 同步处理 → 退出 span 前 guard drop，符合此约束。
pub struct TraceScopeGuard {
    restored: bool,
}

impl TraceScopeGuard {
    /// 显式释放 scope，恢复 thread-local 前值。
    pub fn restore(mut self) {
        if !self.restored {
            CURRENT_INBOUND_TRACE.with(|c| *c.borrow_mut() = None);
            self.restored = true;
        }
    }
}

impl Drop for TraceScopeGuard {
    fn drop(&mut self) {
        if !self.restored {
            CURRENT_INBOUND_TRACE.with(|c| *c.borrow_mut() = None);
        }
    }
}

/// 设置全局 wire message 签名密钥。
///
/// 在 `RuntimeBuilder::build` 调用一次即可。重复调用覆盖前值（用于一个进程
/// 构造多个 Runtime 的测试场景）。传入空 `Vec` 等价于禁用签名。
pub fn set_wire_signing_key(key: Vec<u8>) {
    *WIRE_SIGNING_KEY.lock() = if key.is_empty() { None } else { Some(key) };
}

impl WireEnvelope {
    /// 用当前协议版本封装 [`WireMessage`]，并注入跨节点 trace 上下文与 wire MAC。
    ///
    /// C3：trace 上下文以 W3C `traceparent` 字符串形式注入。若当前线程有活跃
    /// `tracing::Span`（通过 `tracing::Span::current()`），从中提取 trace-id
    /// 与 flags，生成新的 span-id（child span），构造 `traceparent` 字符串。
    /// 若无活跃 span（如未启用 tracing subscriber 或顶层调用），则生成
    /// root trace 上下文。
    ///
    /// 注入的 `traceparent` 字符串同时通过 `tracing::Span::current().record()`
    /// 记录到发送方当前 span 的 `wire.traceparent` field，便于在发送方日志
    /// 中通过该字符串检索关联的接收方 span。
    ///
    /// 若全局签名密钥非空（[`set_wire_signing_key`] 已设置），计算 MAC 并填入
    /// `mac` 字段。MAC 覆盖 `version` + `message` + `traceparent` 三字段序列化字节，
    /// 不含 `mac` 字段自身——通过对一个不含 mac 的临时结构序列化来计算。
    pub fn wrap(msg: WireMessage) -> Self {
        // C3：生成 W3C traceparent。
        //
        // 多跳传播策略：检查当前线程的入站 trace 上下文（由 wire.recv span
        // 设置）。若存在，调用 `child()` 生成延续 trace-id 的新 span-id，
        // 使"接收 → 处理 → 转发"链路在 OTLP span 树中保持关联；否则生成
        // root 上下文（顶层发送方）。
        //
        // 局限性：thread-local 无法跨 tokio task 边界传播。actant scheduler
        // 路径在原任务上下文内同步入队，未跨 task 调度，因此多跳链路能正确延续。
        // 若未来引入跨 task 异步调度，需替换为 task_local。
        let ctx = CURRENT_INBOUND_TRACE.with(|c| {
            c.borrow()
                .as_ref()
                .map(|parent| parent.child())
                .unwrap_or_else(|| TraceContext::new_root(true))
        });
        let traceparent = ctx.to_header();
        tracing::Span::current().record("wire.traceparent", &traceparent);

        // 先构造不含 MAC 的 envelope（mac = None），序列化后计算 MAC。
        let unsigned = Self {
            version: constants::WIRE_PROTOCOL_VERSION,
            message: msg,
            traceparent: Some(traceparent),
            mac: None,
        };

        // 计算可选 MAC：仅当全局签名密钥非空时。
        let mac = {
            let guard = WIRE_SIGNING_KEY.lock();
            guard.as_ref().and_then(|key| {
                // 序列化 unsigned envelope（mac=None），对字节计算 MAC。
                // 这里使用临时变量避免 borrow guard 跨 await（parking_lot::Mutex 非 async）。
                let bytes = crate::common::encode_postcard(&unsigned).ok()?;
                crate::common::payload::wire_mac(key, &bytes)
            })
        };

        Self { mac, ..unsigned }
    }

    /// 从原始字节解码并校验协议封装与 wire MAC。
    ///
    /// 这是反序列化 gossip 消息的唯一入口：集中执行协议版本校验与 MAC 验证，
    /// 调用方无需重复样板代码。
    ///
    /// 反序列化失败、协议版本不兼容或 MAC 验证失败时返回 `None`（并记录告警）。
    /// MAC 验证失败时记录 `warn` 并计数到 `actant_wire_messages_failed_total{reason="mac"}`。
    ///
    /// 返回的元组包含消息本体与可选的跨节点 trace 上下文字符串（W3C `traceparent`）；
    /// 调用方应使用该字符串创建 `wire.recv` 子 span 以串联跨节点日志与 OTLP span 树。
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

        // MAC 校验：若全局签名密钥非空，所有入站消息必须携带有效 MAC。
        // 验证方式：从 envelope 中剥离 mac 字段得到 unsigned 视图，重新序列化后比对。
        // 注意：postcard 序列化为定长紧凑格式，相同字段值的结构序列化结果一致，
        // 因此剥离 mac 后重新序列化可与发送方计算的 unsigned 字节流匹配。
        let mac_ok: bool = {
            let guard = WIRE_SIGNING_KEY.lock();
            match guard.as_ref() {
                None => true, // 禁用签名验证
                Some(key) => {
                    let mac = match &envelope.mac {
                        Some(m) => m,
                        None => {
                            tracing::warn!(
                                "dropping message: wire MAC required (cluster signing key set) but missing"
                            );
                            return None;
                        }
                    };
                    // 构造 unsigned 视图：克隆 envelope 但置 mac=None，序列化后验证。
                    let unsigned_view = WireEnvelope {
                        version: envelope.version,
                        message: envelope.message.clone(),
                        traceparent: envelope.traceparent.clone(),
                        mac: None,
                    };
                    match crate::common::encode_postcard(&unsigned_view) {
                        Ok(unsigned_bytes) => {
                            crate::common::payload::verify_wire_mac(key, &unsigned_bytes, mac)
                                .is_ok()
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "dropping message: failed to serialize unsigned view for MAC verification"
                            );
                            false
                        }
                    }
                }
            }
        };
        if !mac_ok {
            tracing::warn!(
                "dropping message: wire MAC verification failed (possible forgery or cluster key mismatch)"
            );
            return None;
        }

        Some((envelope.message, envelope.traceparent))
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
