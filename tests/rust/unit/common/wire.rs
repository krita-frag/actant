//! Unit tests extracted from `src/common/wire.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

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
    // 反序列化失败必须发出 warn，而非静默丢弃。
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
        trace_id: None,
    };
    let payload = postcard::to_allocvec::<WireEnvelope>(&envelope).unwrap();

    let writer = CapturingWriter::new();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(writer.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .finish();
    let dispatch = tracing::dispatcher::Dispatch::new(subscriber);

    let result = tracing::dispatcher::with_default(&dispatch, || WireEnvelope::decode(&payload));

    assert!(result.is_none());
    let captured = writer.captured();
    assert!(
        captured.contains("WARN"),
        "version mismatch should log WARN, got: {captured}"
    );
}

// --- WireTaskState / WireTaskOutcome 字符串映射 ---

#[test]
fn wire_task_state_as_str_returns_canonical_values() {
    assert_eq!(WireTaskState::Running.as_str(), state_str::RUNNING);
    assert_eq!(
        WireTaskState::Completed { result: vec![] }.as_str(),
        state_str::COMPLETED
    );
    assert_eq!(
        WireTaskState::Failed {
            error: "e".to_string()
        }
        .as_str(),
        state_str::FAILED
    );
    assert_eq!(WireTaskState::Cancelled.as_str(), state_str::CANCELLED);
    assert_eq!(WireTaskState::Skipped.as_str(), state_str::SKIPPED);
}

#[test]
fn wire_task_state_from_python_str_parses_all_states() {
    assert!(matches!(
        WireTaskState::from_python_str("Running", vec![]),
        Some(WireTaskState::Running)
    ));
    assert!(matches!(
        WireTaskState::from_python_str("completed", vec![1]),
        Some(WireTaskState::Completed { result }) if result == vec![1]
    ));
    assert!(matches!(
        WireTaskState::from_python_str("FAILED", b"oops".to_vec()),
        Some(WireTaskState::Failed { error }) if error == "oops"
    ));
    assert!(matches!(
        WireTaskState::from_python_str("CANCELLED", vec![]),
        Some(WireTaskState::Cancelled)
    ));
    assert!(matches!(
        WireTaskState::from_python_str("skipped", vec![]),
        Some(WireTaskState::Skipped)
    ));
    assert!(WireTaskState::from_python_str("Unknown", vec![]).is_none());
}

#[test]
fn wire_task_outcome_as_str_returns_canonical_values() {
    assert_eq!(
        WireTaskOutcome::Completed(vec![1]).as_str(),
        state_str::COMPLETED
    );
    assert_eq!(
        WireTaskOutcome::Failed("e".to_string()).as_str(),
        state_str::FAILED
    );
    assert_eq!(WireTaskOutcome::Cancelled.as_str(), state_str::CANCELLED);
    assert_eq!(WireTaskOutcome::Skipped.as_str(), state_str::SKIPPED);
}
