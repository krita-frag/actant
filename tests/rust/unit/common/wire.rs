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
fn classify_task_prefix_is_exact() {
    // 关键：prefix 不能互相包含，否则路由错误。
    // task topic 不应被误分类为其他节点作用域话题。
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
/// 截断实现使用字符边界安全切片（`floor_char_boundary`），避免按字节
/// 切片落在多字节 UTF-8 字符内部导致 panic。
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
        traceparent: None,
        mac: None,
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

// --- C3：W3C Traceparent 跨节点传播 ---

/// C3 集成测试：`wrap()` 在无 thread-local scope 时生成 root traceparent，
/// 接收方 `decode()` 后能解析出该 traceparent。
#[test]
fn wire_envelope_wrap_injects_w3c_traceparent() {
    let msg = WireMessage::NodeHeartbeat(NodeHeartbeat {
        node_id: node("n1"),
        active_workflows: vec![],
        timestamp_ms: 0,
        available_slots: 0,
        max_slots: 0,
        endpoint_addr: None,
    });
    let envelope = WireEnvelope::wrap(msg);
    // traceparent 字段必须非空，且为合法 W3C 格式。
    let tp = envelope
        .traceparent
        .as_ref()
        .expect("wrap() must inject traceparent");
    let ctx = TraceContext::parse(tp).expect("traceparent must be valid W3C format");
    // root 上下文 sampled=true。
    assert_eq!(ctx.flags & 0x01, 0x01);
}

/// C3 集成测试：wrap → encode → decode 链路中 traceparent 保持不变
///（端到端 wire 协议不丢失 trace 上下文）。
#[test]
fn wire_envelope_roundtrip_preserves_traceparent() {
    let msg = WireMessage::NodeHeartbeat(NodeHeartbeat {
        node_id: node("n2"),
        active_workflows: vec![],
        timestamp_ms: 0,
        available_slots: 0,
        max_slots: 0,
        endpoint_addr: None,
    });
    let envelope = WireEnvelope::wrap(msg);
    let original_tp = envelope.traceparent.clone().unwrap();
    let bytes = crate::common::encode_postcard(&envelope).unwrap();
    let (decoded_msg, decoded_tp) = WireEnvelope::decode(&bytes).expect("decode should succeed");
    // 消息本体保留。
    assert!(matches!(decoded_msg, WireMessage::NodeHeartbeat(_)));
    // traceparent 保留。
    assert_eq!(decoded_tp.as_deref(), Some(original_tp.as_str()));
}

/// C3 集成测试：在 `current_trace_scope` 内调用 wrap() 时，生成的 traceparent
/// 必须是 child（trace-id 与父一致，span-id 不同）。
#[test]
fn wire_envelope_wrap_within_scope_produces_child_traceparent() {
    let parent = TraceContext::new_root(true);
    let _scope = current_trace_scope(parent.clone());

    let msg = WireMessage::NodeHeartbeat(NodeHeartbeat {
        node_id: node("n3"),
        active_workflows: vec![],
        timestamp_ms: 0,
        available_slots: 0,
        max_slots: 0,
        endpoint_addr: None,
    });
    let envelope = WireEnvelope::wrap(msg);
    let child_tp = envelope.traceparent.as_ref().unwrap();
    let child = TraceContext::parse(child_tp).unwrap();

    // trace-id 必须延续父。
    assert_eq!(child.trace_id, parent.trace_id);
    // flags 必须延续父。
    assert_eq!(child.flags, parent.flags);
    // span-id 必须与父不同（child 生成了新 span-id）。
    assert_ne!(child.span_id, parent.span_id);
}

/// C3 集成测试：scope 退出（guard drop）后，wrap() 退化为生成 root traceparent。
#[test]
fn wire_envelope_wrap_after_scope_drop_returns_to_root() {
    let parent = TraceContext::new_root(true);
    let parent_trace_id = parent.trace_id;

    // 在 scope 内调用 wrap，得到 child trace。
    let child_tp = {
        let _scope = current_trace_scope(parent);
        let envelope = WireEnvelope::wrap(WireMessage::NodeHeartbeat(NodeHeartbeat {
            node_id: node("n4"),
            active_workflows: vec![],
            timestamp_ms: 0,
            available_slots: 0,
            max_slots: 0,
            endpoint_addr: None,
        }));
        envelope.traceparent.unwrap()
    };

    // scope 退出后，wrap 应生成新的 root trace。
    let envelope = WireEnvelope::wrap(WireMessage::NodeHeartbeat(NodeHeartbeat {
        node_id: node("n4"),
        active_workflows: vec![],
        timestamp_ms: 0,
        available_slots: 0,
        max_slots: 0,
        endpoint_addr: None,
    }));
    let root_tp = envelope.traceparent.unwrap();
    let root = TraceContext::parse(&root_tp).unwrap();
    let child = TraceContext::parse(&child_tp).unwrap();

    // root 的 trace-id 应不同于 child 的（独立 trace）。
    assert_ne!(root.trace_id, parent_trace_id);
    assert_ne!(root.trace_id, child.trace_id);
}

/// C3 集成测试：MAC 与 traceparent 协同——一旦设置签名密钥，
/// 篡改 traceparent 字段后 decode 必须失败。
#[test]
fn wire_mac_protects_traceparent_field() {
    // 复用模块级 MAC_TEST_LOCK，与 D2 测试串行执行避免全局密钥污染。
    let _guard = MAC_TEST_LOCK.lock().unwrap();

    crate::common::set_wire_signing_key(b"test-key-c3".to_vec());

    let envelope = WireEnvelope::wrap(WireMessage::NodeHeartbeat(NodeHeartbeat {
        node_id: node("n5"),
        active_workflows: vec![],
        timestamp_ms: 0,
        available_slots: 0,
        max_slots: 0,
        endpoint_addr: None,
    }));
    let original_tp = envelope.traceparent.clone().unwrap();
    let mut bytes = crate::common::encode_postcard(&envelope).unwrap();

    // 构造篡改后的 envelope：替换 traceparent 为攻击者伪造值。
    let forged_ctx = TraceContext::new_root(true);
    let forged_tp = forged_ctx.to_header();
    let forged_envelope = WireEnvelope {
        version: envelope.version,
        message: envelope.message.clone(),
        traceparent: Some(forged_tp.clone()),
        mac: envelope.mac, // 攻击者无法重算 MAC（无密钥）
    };
    let forged_bytes = crate::common::encode_postcard(&forged_envelope).unwrap();

    // 用篡改字节覆盖（确保长度一致——postcard 序列化使 traceparent 字符串
    // 长度变化会导致字节流变化，但 decode 仍应因 MAC 不匹配而失败）。
    let _ = std::mem::replace(&mut bytes, forged_bytes);

    // decode 必须失败（MAC 不匹配）。
    let result = WireEnvelope::decode(&bytes);
    assert!(
        result.is_none(),
        "MAC verification must catch traceparent tampering"
    );

    // 原始 envelope decode 应该成功，且 traceparent 与原始一致。
    let original_bytes = crate::common::encode_postcard(&envelope).unwrap();
    let (_, decoded_tp) = WireEnvelope::decode(&original_bytes).unwrap();
    assert_eq!(decoded_tp.as_deref(), Some(original_tp.as_str()));

    // 清理全局状态。
    crate::common::set_wire_signing_key(Vec::new());
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

// --- WireEnvelope MAC（D2：节点身份认证 + 共享密钥签名） ---

/// 串行化 MAC 相关测试的全局锁。
///
/// 这些测试修改全局 `WIRE_SIGNING_KEY`，并行执行会互相污染。
/// 通过 `let _guard = MAC_TEST_LOCK.lock().unwrap();` 在测试开头获取锁，
/// 保证 MAC 测试组串行执行。锁在 guard drop 时自动释放。
static MAC_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn heartbeat_msg(n: &str) -> WireMessage {
    WireMessage::NodeHeartbeat(NodeHeartbeat {
        node_id: node(n),
        active_workflows: vec![],
        timestamp_ms: 42_000,
        available_slots: 1,
        max_slots: 4,
        endpoint_addr: Some("test-addr".to_string()),
    })
}

/// 回归测试：禁用签名（默认全局状态）时 wrap→decode 应成功往返。
#[test]
fn envelope_roundtrip_without_signing() {
    let _guard = MAC_TEST_LOCK.lock().unwrap();
    // 确保全局密钥为空（其他测试可能设置过）。
    set_wire_signing_key(Vec::new());

    let env = WireEnvelope::wrap(heartbeat_msg("n1"));
    // 禁用签名时 mac 为 None。
    assert!(env.mac.is_none(), "mac must be None when key is empty");

    let bytes = postcard::to_allocvec(&env).unwrap();
    let (decoded, _trace) = WireEnvelope::decode(&bytes).expect("roundtrip should succeed");
    if let WireMessage::NodeHeartbeat(hb) = decoded {
        assert_eq!(hb.node_id.as_str(), "n1");
    } else {
        panic!("expected NodeHeartbeat");
    }
}

/// D2 关键测试：启用签名后 wrap 自动计算 MAC，decode 通过校验。
#[test]
fn envelope_wrap_with_key_produces_mac() {
    let _guard = MAC_TEST_LOCK.lock().unwrap();
    set_wire_signing_key(b"cluster-secret-123".to_vec());

    let env = WireEnvelope::wrap(heartbeat_msg("n2"));
    assert!(
        env.mac.is_some(),
        "mac must be Some when signing key is set"
    );

    let bytes = postcard::to_allocvec(&env).unwrap();
    let (decoded, _) = WireEnvelope::decode(&bytes).expect("valid MAC should decode");

    // 清理：恢复禁用状态避免影响后续测试。
    set_wire_signing_key(Vec::new());

    if let WireMessage::NodeHeartbeat(hb) = decoded {
        assert_eq!(hb.node_id.as_str(), "n2");
    } else {
        panic!("expected NodeHeartbeat");
    }
}

/// D2 关键测试：发送方与接收方密钥不匹配时，decode 必须丢弃消息。
#[test]
fn envelope_decode_rejects_mismatched_key() {
    let _guard = MAC_TEST_LOCK.lock().unwrap();
    set_wire_signing_key(b"key-sender".to_vec());

    let env = WireEnvelope::wrap(heartbeat_msg("n3"));
    let bytes = postcard::to_allocvec(&env).unwrap();

    // 接收方使用不同密钥。
    set_wire_signing_key(b"key-receiver".to_vec());
    let result = WireEnvelope::decode(&bytes);

    set_wire_signing_key(Vec::new()); // 清理

    assert!(
        result.is_none(),
        "decode must drop message with mismatched MAC key"
    );
}

/// D2 关键测试：启用签名后，未携带 MAC 的消息（如伪造者跳过签名）被丢弃。
#[test]
fn envelope_decode_rejects_unsigned_message_when_key_set() {
    let _guard = MAC_TEST_LOCK.lock().unwrap();
    set_wire_signing_key(Vec::new());
    // 在禁用签名的状态下 wrap（不会产生 MAC）。
    let unsigned_env = WireEnvelope::wrap(heartbeat_msg("n4"));
    assert!(unsigned_env.mac.is_none());
    let bytes = postcard::to_allocvec(&unsigned_env).unwrap();

    // 接收方启用签名验证：缺少 MAC 的消息必须被丢弃。
    set_wire_signing_key(b"cluster-key".to_vec());
    let result = WireEnvelope::decode(&bytes);

    set_wire_signing_key(Vec::new()); // 清理

    assert!(
        result.is_none(),
        "decode must drop unsigned message when cluster key is set"
    );
}

/// D2 关键测试：篡改序列化字节后 MAC 校验失败。
#[test]
fn envelope_decode_rejects_tampered_bytes() {
    let _guard = MAC_TEST_LOCK.lock().unwrap();
    set_wire_signing_key(b"cluster-key".to_vec());
    let env = WireEnvelope::wrap(heartbeat_msg("n5"));
    let mut bytes = postcard::to_allocvec(&env).unwrap();

    // 翻转最后一个字节（属于 payload 区，非 MAC 本身）。
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;

    let result = WireEnvelope::decode(&bytes);

    set_wire_signing_key(Vec::new()); // 清理

    assert!(
        result.is_none(),
        "decode must drop tampered message (MAC mismatch)"
    );
}

/// C3 集成测试：嵌套 scope 退出后恢复**进入前的旧值**（LIFO），而非清空为无 trace。
///
/// 回归测试：TraceScopeGuard 历史上 drop 时无条件清空 thread-local，嵌套场景下
/// 外层 scope 的 trace 上下文会被内层 guard 的退出误删。
#[test]
fn trace_scope_guard_restore_recovers_previous_scope() {
    let outer = TraceContext::new_root(true);
    let inner = TraceContext::new_root(true);
    assert_ne!(outer.trace_id, inner.trace_id);

    let wrap_under_outer = || {
        WireEnvelope::wrap(WireMessage::NodeHeartbeat(NodeHeartbeat {
            node_id: node("nested"),
            active_workflows: vec![],
            timestamp_ms: 0,
            available_slots: 0,
            max_slots: 0,
            endpoint_addr: None,
        }))
        .traceparent
        .unwrap()
    };

    let outer_scope = current_trace_scope(outer.clone());
    let tp_within_outer = wrap_under_outer();

    let inner_scope = current_trace_scope(inner.clone());
    let tp_within_inner = wrap_under_outer();
    // 显式 restore 与 drop 两条恢复路径都验证。
    inner_scope.restore();
    let tp_after_inner = wrap_under_outer();
    drop(outer_scope);
    let tp_after_all = wrap_under_outer();

    // 内层 scope 内：child of inner。
    let child_of_inner = TraceContext::parse(&tp_within_inner).unwrap();
    assert_eq!(child_of_inner.trace_id, inner.trace_id);
    // 内层退出后：恢复为 child of outer（而非 root）。
    let child_of_outer = TraceContext::parse(&tp_after_inner).unwrap();
    assert_eq!(child_of_outer.trace_id, outer.trace_id);
    // 外层 scope 进入时的 wrap 也是 child of outer（基线）。
    let baseline = TraceContext::parse(&tp_within_outer).unwrap();
    assert_eq!(baseline.trace_id, outer.trace_id);
    // 全部退出后：退化为 root（独立 trace）。
    let root = TraceContext::parse(&tp_after_all).unwrap();
    assert_ne!(root.trace_id, outer.trace_id);
    assert_ne!(root.trace_id, inner.trace_id);
}

/// 兼容性红线测试：MAC 覆盖字节的分段组装（`mac_input_bytes`）必须与
/// 「序列化整个 `mac: None` 的 unsigned envelope」逐字节一致。
///
/// postcard 格式演进（字段增删、编码规则变化）若破坏该不变量，此测试先行
/// 失败，防止跨节点 MAC 校验静默失效。
#[test]
fn mac_input_segments_match_unsigned_envelope_serialization() {
    let heartbeat = heartbeat_msg("mac-segments");
    let unsigned = WireEnvelope {
        version: constants::WIRE_PROTOCOL_VERSION,
        message: heartbeat.clone(),
        traceparent: Some(TraceContext::new_root(true).to_header()),
        mac: None,
    };
    let whole = crate::common::encode_postcard(&unsigned).unwrap();
    let segments =
        mac_input_bytes(unsigned.version, &unsigned.message, &unsigned.traceparent).unwrap();
    assert_eq!(
        segments, whole,
        "MAC input segments must be byte-identical to unsigned envelope serialization"
    );

    // traceparent 为 None 时同样成立。
    let unsigned_no_tp = WireEnvelope {
        traceparent: None,
        ..unsigned
    };
    let whole_no_tp = crate::common::encode_postcard(&unsigned_no_tp).unwrap();
    let segments_no_tp =
        mac_input_bytes(unsigned_no_tp.version, &unsigned_no_tp.message, &None).unwrap();
    assert_eq!(segments_no_tp, whole_no_tp);
}

/// H6.2：同进程两个不同密钥 Runtime 互不干扰。
///
/// `register_wire_signing_key` 按 node 隔离：rt-b 注册新密钥后，rt-a 的出站
/// 消息仍以 key-a 签名（旧的全局单密钥行为会被覆盖）；接收侧尝试全部已注册
/// 密钥，两个 Runtime 各自集群的入站消息均可验证；未注册密钥签名的消息被拒绝。
#[test]
fn two_runtimes_with_different_keys_do_not_interfere() {
    let _guard = MAC_TEST_LOCK.lock().unwrap();
    let node_a = node("rt-a");
    let node_b = node("rt-b");
    crate::common::register_wire_signing_key(&node_a, b"cluster-key-a".to_vec());
    let env_a_before = WireEnvelope::wrap(heartbeat_msg("rt-a"));
    assert!(env_a_before.mac.is_some());

    // 另一 Runtime 以不同密钥注册（旧全局单密钥行为会在此覆盖 rt-a 的密钥）。
    crate::common::register_wire_signing_key(&node_b, b"cluster-key-b".to_vec());

    // rt-a 出站仍用 key-a 签名：以 key-a 手工重算 MAC 必须一致。
    let env_a_after = WireEnvelope::wrap(heartbeat_msg("rt-a"));
    let bytes_a = postcard::to_allocvec(&env_a_after).unwrap();
    let mac_a = env_a_after.mac.expect("rt-a envelope must be signed");
    let unsigned_a = WireEnvelope {
        mac: None,
        ..env_a_after
    };
    let expected_a = crate::common::payload::wire_mac(
        b"cluster-key-a",
        &crate::common::encode_postcard(&unsigned_a).unwrap(),
    )
    .unwrap();
    assert_eq!(
        mac_a, expected_a,
        "rt-a outbound must still be signed with key-a after rt-b registered key-b"
    );

    // rt-b 出站用 key-b 签名。
    let env_b = WireEnvelope::wrap(heartbeat_msg("rt-b"));
    let bytes_b = postcard::to_allocvec(&env_b).unwrap();
    let mac_b = env_b.mac.expect("rt-b envelope must be signed");
    let unsigned_b = WireEnvelope { mac: None, ..env_b };
    let expected_b = crate::common::payload::wire_mac(
        b"cluster-key-b",
        &crate::common::encode_postcard(&unsigned_b).unwrap(),
    )
    .unwrap();
    assert_eq!(mac_b, expected_b);

    // 接收侧：两个 Runtime 的消息均可验证。
    assert!(
        WireEnvelope::decode(&bytes_a).is_some(),
        "rt-a message must decode after rt-b registration"
    );
    assert!(WireEnvelope::decode(&bytes_b).is_some());

    // 未注册密钥（key-c）签名的消息仍被拒绝。
    let rogue_unsigned = WireEnvelope {
        version: constants::WIRE_PROTOCOL_VERSION,
        message: heartbeat_msg("rt-c"),
        traceparent: Some(TraceContext::new_root(true).to_header()),
        mac: None,
    };
    let rogue_mac = crate::common::payload::wire_mac(
        b"cluster-key-c",
        &crate::common::encode_postcard(&rogue_unsigned).unwrap(),
    )
    .unwrap();
    let rogue = WireEnvelope {
        mac: Some(rogue_mac),
        ..rogue_unsigned
    };
    let rogue_bytes = postcard::to_allocvec(&rogue).unwrap();
    assert!(
        WireEnvelope::decode(&rogue_bytes).is_none(),
        "message signed with unregistered key must be rejected"
    );

    // 清理全局注册表，避免污染其他 MAC 测试。
    crate::common::register_wire_signing_key(&node_a, Vec::new());
    crate::common::register_wire_signing_key(&node_b, Vec::new());
    set_wire_signing_key(Vec::new());
}

/// 兼容性红线测试：wrap() 用分段字节计算的 MAC 必须与对完整 unsigned envelope
/// 一次性计算的结果一致（发送方/接收方任一路径单独演进都不允许漂移）。
#[test]
fn wire_mac_matches_oneshot_unsigned_serialization() {
    let _guard = MAC_TEST_LOCK.lock().unwrap();
    let key = b"segment-compat-key".to_vec();
    set_wire_signing_key(key.clone());

    let env = WireEnvelope::wrap(heartbeat_msg("mac-oneshot"));
    let wrapped_mac = env.mac.expect("mac must be present with signing key");
    set_wire_signing_key(Vec::new());

    let unsigned = WireEnvelope { mac: None, ..env };
    let whole = crate::common::encode_postcard(&unsigned).unwrap();
    let oneshot = crate::common::payload::wire_mac(&key, &whole).unwrap();
    assert_eq!(
        wrapped_mac, oneshot,
        "segment-assembled MAC input must equal one-shot unsigned serialization"
    );
}
