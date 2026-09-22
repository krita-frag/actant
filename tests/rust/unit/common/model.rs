//! Unit tests extracted from `src/common/model.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;

// --- 访问器与构造器 ---

#[test]
fn test_id_as_str_returns_inner() {
    let id = TaskId::new("task-123".to_string());
    assert_eq!(id.as_str(), "task-123");
}

#[test]
fn test_id_new_constructs_correctly() {
    let id = ActorId::new("actor-abc".to_string());
    assert_eq!(id.as_str(), "actor-abc");
}

#[test]
fn test_id_generate_returns_nonempty_uuid() {
    let id = WorkflowId::generate();
    assert!(!id.as_str().is_empty());
    // UUID v4 格式：8-4-4-4-12
    assert_eq!(id.as_str().len(), 36);
}

#[test]
fn test_id_into_inner_consumes() {
    let id = NodeId::new("node-1".to_string());
    let inner: String = id.into_inner();
    assert_eq!(inner, "node-1");
}

// --- From / FromStr / Display ---

#[test]
fn test_id_from_string() {
    let id = MessageId::from("msg-1".to_string());
    assert_eq!(id.as_str(), "msg-1");
}

#[test]
fn test_id_from_str_ref() {
    let id = TaskId::from("task-x");
    assert_eq!(id.as_str(), "task-x");
}

#[test]
fn test_id_fromstr_trait() {
    let id: TaskId = "task-parse".parse().unwrap();
    assert_eq!(id.as_str(), "task-parse");
}

#[test]
fn test_id_display_matches_as_str() {
    let id = ActorId::new("actor-d".to_string());
    assert_eq!(format!("{}", id), id.as_str());
}

#[test]
fn test_id_as_ref_str() {
    let id = NodeId::new("node-r".to_string());
    let s: &str = id.as_ref();
    assert_eq!(s, "node-r");
}

// --- 序列化往返（serde + postcard）---

#[test]
fn test_id_postcard_roundtrip() {
    let original = TaskId::new("task-postcard".to_string());
    let bytes = postcard::to_allocvec::<TaskId>(&original).unwrap();
    let decoded: TaskId = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn test_id_serde_json_roundtrip() {
    let original = WorkflowId::new("wf-json".to_string());
    let json = serde_json::to_string(&original).unwrap();
    // transparent 序列化为纯字符串，而非 {"0": "..."}
    assert_eq!(json, "\"wf-json\"");
    let decoded: WorkflowId = serde_json::from_str(&json).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn test_id_serde_transparent_format_unchanged() {
    // 关键：transparent 保证序列化格式与 pub String 时期一致（纯字符串）。
    let id = ActorId::new("actor-fmt".to_string());
    let json = serde_json::to_string(&id).unwrap();
    assert_eq!(json, "\"actor-fmt\"");
}

// --- rkyv 往返 ---

#[test]
fn test_id_rkyv_roundtrip() {
    let original = NodeId::new("node-rkyv".to_string());
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&original).unwrap();
    let decoded: NodeId = rkyv::from_bytes::<NodeId, rkyv::rancor::Error>(&bytes).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn test_id_rkyv_archived_hash_eq() {
    // rkyv(derive(Hash, Eq, PartialEq)) 保证归档形式可比较。
    let a = MessageId::new("msg-1".to_string());
    let b = MessageId::new("msg-1".to_string());
    let bytes_a = rkyv::to_bytes::<rkyv::rancor::Error>(&a).unwrap();
    let bytes_b = rkyv::to_bytes::<rkyv::rancor::Error>(&b).unwrap();
    let arch_a: &rkyv::Archived<MessageId> =
        rkyv::access::<rkyv::Archived<MessageId>, rkyv::rancor::Error>(&bytes_a).unwrap();
    let arch_b: &rkyv::Archived<MessageId> =
        rkyv::access::<rkyv::Archived<MessageId>, rkyv::rancor::Error>(&bytes_b).unwrap();
    assert_eq!(arch_a, arch_b);
}

// --- 相等性与 Hash ---

#[test]
fn test_id_eq_and_hash() {
    let a = TaskId::new("t1".to_string());
    let b = TaskId::new("t1".to_string());
    let c = TaskId::new("t2".to_string());
    assert_eq!(a, b);
    assert_ne!(a, c);
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut ha = DefaultHasher::new();
    let mut hb = DefaultHasher::new();
    a.hash(&mut ha);
    b.hash(&mut hb);
    assert_eq!(ha.finish(), hb.finish());
}

// --- ActorId helpers ---

#[test]
fn test_actor_id_helpers() {
    let node = NodeId::from("node-1");
    assert_eq!(ActorId::workflow(&node).as_str(), "workflow-node-1");
    assert_eq!(ActorId::scheduler(&node).as_str(), "scheduler-node-1");
    assert_eq!(ActorId::failover(&node).as_str(), "failover-node-1");
    assert_eq!(ActorId::dag_gossip(&node).as_str(), "dag-gossip-node-1");
    assert_eq!(ActorId::capability("Exec").as_str(), "capability-Exec");
}

// --- ActorStatus ---

#[test]
fn test_actor_status_as_str() {
    assert_eq!(ActorStatus::Created.as_str(), "Created");
    assert_eq!(ActorStatus::Running.as_str(), "Running");
    assert_eq!(ActorStatus::Stopped.as_str(), "Stopped");
    assert_eq!(ActorStatus::Failed.as_str(), "Failed");
}

// --- RetryPolicy ---

#[test]
fn test_retry_policy_default() {
    let rp = RetryPolicy::default();
    assert_eq!(rp.max_retries, RetryPolicy::DEFAULT_MAX_RETRIES);
    assert_eq!(rp.delay_ms, RetryPolicy::DEFAULT_DELAY_MS);
    assert_eq!(
        rp.backoff_multiplier,
        RetryPolicy::DEFAULT_BACKOFF_MULTIPLIER
    );
    assert_eq!(rp.max_delay_ms, RetryPolicy::DEFAULT_MAX_DELAY_MS);
}

// --- ActorMessage ---

#[test]
fn test_actor_message_new_and_with_reply() {
    let target = ActorId::from("a");
    let mut msg = ActorMessage::new(target.clone(), "m".to_string(), vec![1, 2]);
    assert_eq!(msg.target, target);
    assert_eq!(msg.method, "m");
    assert_eq!(msg.payload, vec![1, 2]);
    assert!(msg.take_reply_tx().is_none());

    let (mut msg2, _rx) = ActorMessage::with_reply(target, "m2".to_string(), vec![]);
    assert!(msg2.take_reply_tx().is_some());
    assert!(msg2.take_reply_tx().is_none());
}

// --- TaskCompletion ---

#[test]
fn test_task_completion_accessors() {
    let wf = WorkflowId::from("wf-1");
    let task = TaskId::from("t-1");
    let node = NodeId::from("n-1");
    let tc = TaskCompletion::Completed {
        workflow_id: wf.clone(),
        task_id: task.clone(),
        task_name: "tn".to_string(),
        result: vec![1],
        target_node: Some(node.clone()),
    };
    assert_eq!(tc.as_str(), "Completed");
    assert_eq!(tc.workflow_id(), &wf);
    assert_eq!(tc.task_id(), &task);
    assert_eq!(tc.task_name(), "tn");
    assert_eq!(tc.target_node(), Some(&node));
}

#[test]
fn test_task_completion_to_wire_result_all_variants() {
    use crate::common::{WireTaskOutcome, WireTaskResult};

    let wf = WorkflowId::from("wf-1");
    let task = TaskId::from("t-1");

    let completed = TaskCompletion::Completed {
        workflow_id: wf.clone(),
        task_id: task.clone(),
        task_name: "tc".to_string(),
        result: vec![1, 2],
        target_node: None,
    };
    let wire = completed.to_wire_result(wf.clone());
    assert!(
        matches!(wire, WireTaskResult { ref task_id, ref task_name, ref workflow_id, outcome: WireTaskOutcome::Completed(ref r) } if task_id == &task && task_name == "tc" && workflow_id == &wf && r == &vec![1, 2])
    );

    let failed = TaskCompletion::Failed {
        workflow_id: wf.clone(),
        task_id: task.clone(),
        task_name: "tf".to_string(),
        error: "boom".to_string(),
        target_node: None,
    };
    let wire = failed.to_wire_result(wf.clone());
    assert!(
        matches!(wire, WireTaskResult { outcome: WireTaskOutcome::Failed(ref e), .. } if e == "boom")
    );

    let cancelled = TaskCompletion::Cancelled {
        workflow_id: wf.clone(),
        task_id: task.clone(),
        task_name: "tcx".to_string(),
        target_node: None,
    };
    let wire = cancelled.to_wire_result(wf.clone());
    assert!(matches!(
        wire,
        WireTaskResult {
            outcome: WireTaskOutcome::Cancelled,
            ..
        }
    ));

    let skipped = TaskCompletion::Skipped {
        workflow_id: wf.clone(),
        task_id: task.clone(),
        task_name: "ts".to_string(),
        target_node: None,
    };
    let wire = skipped.to_wire_result(wf.clone());
    assert!(matches!(
        wire,
        WireTaskResult {
            outcome: WireTaskOutcome::Skipped,
            ..
        }
    ));
}
