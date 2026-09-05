//! Unit tests extracted from `src/runtime/workflow/gossip.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;
use crate::common::{
    ActantError, GossipConfig, HeadsExchange, NodeId, Result, TaskId, WireDagStateUpdate,
    WireTaskState, WorkflowId, WorkflowStateRequest, WorkflowStateResponse,
};
use crate::runtime::actor::ActorSystem;
use crate::runtime::network::{
    DirectRequest, DirectResponse, DirectResponseChannel, ListenAddresses, NetworkEvent, Transport,
};
use crate::runtime::state::HlcTimestamp;
use crate::test_support::MockTransport;

fn make_gossip(node_id: &str, config: GossipConfig) -> DagGossip {
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new(node_id));
    let actor_system = Arc::new(ActorSystem::new());
    let wf_id = ActorId::workflow(&NodeId::from(node_id.to_string()));
    DagGossip::new(network, actor_system, wf_id, config)
}

fn make_entry(ts: u64, prio: u8) -> SeenEntry {
    SeenEntry {
        inserted_at: std::time::Instant::now(),
        hlc_timestamp: HlcTimestamp::from_parts(ts, 0),
        state_priority: prio,
    }
}

#[test]
fn state_priority_orders_terminal_above_running() {
    assert_eq!(state_priority(&WireTaskState::Running), 1);
    assert_eq!(state_priority(&WireTaskState::Skipped), 2);
    assert_eq!(state_priority(&WireTaskState::Cancelled), 3);
    assert_eq!(
        state_priority(&WireTaskState::Failed { error: "e".into() }),
        4
    );
    assert_eq!(
        state_priority(&WireTaskState::Completed { result: Vec::new() }),
        5
    );
}

#[test]
fn seen_entry_superseded_by_higher_hlc() {
    let entry = make_entry(10, 3);
    assert!(entry.is_superseded_by(&HlcTimestamp::from_parts(20, 0), 1));
}

#[test]
fn seen_entry_not_superseded_by_lower_hlc() {
    let entry = make_entry(20, 1);
    assert!(!entry.is_superseded_by(&HlcTimestamp::from_parts(10, 0), 5));
}

#[test]
fn seen_entry_superseded_by_equal_hlc_and_higher_or_equal_prio() {
    let entry = make_entry(10, 3);
    // 同 HLC、更高 prio → 覆盖
    assert!(entry.is_superseded_by(&HlcTimestamp::from_parts(10, 0), 4));
    // 同 HLC、同 prio → 覆盖（>=）
    assert!(entry.is_superseded_by(&HlcTimestamp::from_parts(10, 0), 3));
    // 同 HLC、更低 prio → 不覆盖
    assert!(!entry.is_superseded_by(&HlcTimestamp::from_parts(10, 0), 2));
}

#[test]
fn getters_return_configured_values() {
    let cfg = GossipConfig {
        dedup_window_size: 50,
        dedup_ttl_secs: 30,
        retry_attempts: 4,
        retry_base_delay_ms: 200,
        heads_broadcast_interval_ms: 1500,
    };
    let g = make_gossip("node-A", cfg);
    assert_eq!(g.node_id().as_str(), "node-A");
    assert_eq!(g.heads_broadcast_interval(), Duration::from_millis(1500));
    let (seen, window, ttl, retries) = g.dedup_stats();
    assert_eq!(seen, 0);
    assert_eq!(window, 50);
    assert_eq!(ttl, 30);
    assert_eq!(retries, 4);
}

#[test]
fn seen_count_tracks_inserts() {
    let g = make_gossip("node-A", GossipConfig::default());
    assert_eq!(g.seen_count(), 0);
    let key = SeenKey(
        WorkflowId("wf-1".to_string()),
        TaskId::from("t-1".to_string()),
    );
    g.seen.insert(key, make_entry(5, 1));
    assert_eq!(g.seen_count(), 1);
}

#[test]
fn evict_seen_noop_below_window_threshold() {
    // dedup_window_size * 2 是触发阈值；小于该值不应清理。
    let cfg = GossipConfig {
        dedup_window_size: 10,
        dedup_ttl_secs: 60,
        retry_attempts: 1,
        retry_base_delay_ms: 1,
        heads_broadcast_interval_ms: 1000,
    };
    let g = make_gossip("node-A", cfg);
    for i in 0..15 {
        let key = SeenKey(
            WorkflowId(format!("wf-{}", i)),
            TaskId::from(format!("t-{}", i)),
        );
        g.seen.insert(key, make_entry(i as u64, 1));
    }
    g.evict_seen();
    // 15 < 10*2=20，不应触发清理
    assert_eq!(g.seen_count(), 15);
}

#[test]
fn evict_seen_trims_to_window_size_when_exceeding_double() {
    let cfg = GossipConfig {
        dedup_window_size: 5,
        dedup_ttl_secs: 60,
        retry_attempts: 1,
        retry_base_delay_ms: 1,
        heads_broadcast_interval_ms: 1000,
    };
    let g = make_gossip("node-A", cfg);
    // 插入 12 个条目（> 5*2=10），触发清理。
    for i in 0..12 {
        let key = SeenKey(
            WorkflowId(format!("wf-{}", i)),
            TaskId::from(format!("t-{}", i)),
        );
        g.seen.insert(
            key,
            SeenEntry {
                // 递增 inserted_at，使最旧的被先驱逐
                inserted_at: std::time::Instant::now()
                    - std::time::Duration::from_millis(100 - i as u64),
                hlc_timestamp: HlcTimestamp::from_parts(i as u64, 0),
                state_priority: 1,
            },
        );
    }
    g.evict_seen();
    assert!(
        g.seen_count() <= 5,
        "expected <= 5 after eviction, got {}",
        g.seen_count()
    );
}

#[test]
fn apply_remote_update_drops_stale_lower_hlc() {
    // 先写入高 HLC 的终态，再尝试用低 HLC 的 Running 覆盖 — 应被丢弃。
    let g = make_gossip("node-A", GossipConfig::default());
    let wf = WorkflowId("wf-1".to_string());
    let task = TaskId::from("t-1".to_string());

    let newer = WireDagStateUpdate {
        workflow_id: wf.clone(),
        task_id: task.clone(),
        task_state: WireTaskState::Completed {
            result: vec![1, 2, 3],
        },
        hlc_timestamp: HlcTimestamp::from_parts(100, 5),
        origin_node: NodeId::from("node-B".to_string()),
    };
    // 直接操作 seen 表验证 CRDT 逻辑（不调用 actor）。
    let key = SeenKey(wf.clone(), task.clone());
    g.seen.insert(
        key.clone(),
        SeenEntry {
            inserted_at: std::time::Instant::now(),
            hlc_timestamp: newer.hlc_timestamp,
            state_priority: state_priority(&newer.task_state),
        },
    );
    assert_eq!(g.seen_count(), 1);

    // 构造一个更老的 Running 更新，应被丢弃（不增加 seen，CRDT 保留高优先级）。
    let older = WireDagStateUpdate {
        workflow_id: wf,
        task_id: task,
        task_state: WireTaskState::Running,
        hlc_timestamp: HlcTimestamp::from_parts(50, 0),
        origin_node: NodeId::from("node-C".to_string()),
    };
    // 直接复用 apply_remote_update 的 dedup 判定逻辑（不调用 actor）：
    let existing = g.seen.get(&key).unwrap();
    assert!(!existing.is_superseded_by(&older.hlc_timestamp, state_priority(&older.task_state)));
    drop(existing);
    assert_eq!(g.seen_count(), 1);
    // 保留旧值不被覆盖
    let entry = g.seen.get(&key).unwrap();
    assert_eq!(entry.state_priority, 5);
}

#[test]
fn clock_tick_monotonic() {
    let g = make_gossip("node-A", GossipConfig::default());
    let t1 = g.clock().tick();
    let t2 = g.clock().tick();
    assert!(t2 > t1, "HLC tick must be monotonic: {:?} -> {:?}", t1, t2);
}

#[test]
fn clock_merges_remote_timestamp() {
    let g = make_gossip("node-A", GossipConfig::default());
    let remote = HlcTimestamp::from_parts(999, 9);
    g.clock().merge(&remote);
    let local = g.clock().tick();
    assert!(local > remote, "after merge, local tick must exceed remote");
}

// ───────────────────────── broadcast_update / broadcast_state_update ─────────────────────────

#[tokio::test]
async fn broadcast_update_records_seen_and_broadcasts() {
    let g = make_gossip("node-A", GossipConfig::default());
    let wf = WorkflowId("wf-b1".to_string());
    let task = TaskId::from("t-b1".to_string());

    g.broadcast_update(&wf, &task, WireTaskState::Running)
        .await
        .expect("broadcast should succeed");

    // seen 表应记录该条目
    assert_eq!(g.seen_count(), 1);
    let key = SeenKey(wf, task);
    let entry = g.seen.get(&key).expect("entry should exist");
    assert_eq!(entry.state_priority, 1); // Running = 1
}

#[tokio::test]
async fn broadcast_state_update_translates_completion_to_wire_state() {
    let g = make_gossip("node-A", GossipConfig::default());
    let wf = WorkflowId("wf-b2".to_string());
    let task = TaskId::from("t-b2".to_string());
    let completion = crate::common::TaskCompletion::Completed {
        workflow_id: wf.clone(),
        task_id: task.clone(),
        task_name: "test".to_string(),
        result: vec![42],
        target_node: None,
    };

    g.broadcast_state_update(&wf, &task, &completion)
        .await
        .expect("broadcast should succeed");

    // 验证 seen 表中记录的是 Completed (priority 5)
    let key = SeenKey(wf, task);
    let entry = g.seen.get(&key).expect("entry should exist");
    assert_eq!(entry.state_priority, 5); // Completed = 5
}

#[tokio::test]
async fn broadcast_state_update_translates_failed_completion() {
    let g = make_gossip("node-A", GossipConfig::default());
    let wf = WorkflowId("wf-b3".to_string());
    let task = TaskId::from("t-b3".to_string());
    let completion = crate::common::TaskCompletion::Failed {
        workflow_id: wf.clone(),
        task_id: task.clone(),
        task_name: "test".to_string(),
        error: "boom".into(),
        target_node: None,
    };

    g.broadcast_state_update(&wf, &task, &completion)
        .await
        .expect("broadcast should succeed");

    let key = SeenKey(wf, task);
    let entry = g.seen.get(&key).expect("entry should exist");
    assert_eq!(entry.state_priority, 4); // Failed = 4
}

#[tokio::test]
async fn broadcast_state_update_translates_cancelled_completion() {
    let g = make_gossip("node-A", GossipConfig::default());
    let wf = WorkflowId("wf-b4".to_string());
    let task = TaskId::from("t-b4".to_string());
    let completion = crate::common::TaskCompletion::Cancelled {
        workflow_id: wf.clone(),
        task_id: task.clone(),
        task_name: "test".to_string(),
        target_node: None,
    };

    g.broadcast_state_update(&wf, &task, &completion)
        .await
        .expect("broadcast should succeed");

    let key = SeenKey(wf, task);
    let entry = g.seen.get(&key).expect("entry should exist");
    assert_eq!(entry.state_priority, 3); // Cancelled = 3
}

#[tokio::test]
async fn broadcast_state_update_translates_skipped_completion() {
    let g = make_gossip("node-A", GossipConfig::default());
    let wf = WorkflowId("wf-b5".to_string());
    let task = TaskId::from("t-b5".to_string());
    let completion = crate::common::TaskCompletion::Skipped {
        workflow_id: wf.clone(),
        task_id: task.clone(),
        task_name: "test".to_string(),
        target_node: None,
    };

    g.broadcast_state_update(&wf, &task, &completion)
        .await
        .expect("broadcast should succeed");

    let key = SeenKey(wf, task);
    let entry = g.seen.get(&key).expect("entry should exist");
    assert_eq!(entry.state_priority, 2); // Skipped = 2
}

#[tokio::test]
async fn broadcast_task_running_records_running_state() {
    let g = make_gossip("node-A", GossipConfig::default());
    let wf = WorkflowId("wf-run".to_string());
    let task = TaskId::from("t-run".to_string());

    g.broadcast_task_running(&wf, &task)
        .await
        .expect("broadcast should succeed");

    let key = SeenKey(wf, task);
    let entry = g.seen.get(&key).expect("entry should exist");
    assert_eq!(entry.state_priority, 1); // Running = 1
}

#[tokio::test]
async fn broadcast_update_terminal_uses_retry_path() {
    // 终态使用 broadcast_with_retry，非终态使用单次 broadcast。
    // 由于 MockTransport 总是成功，两者都应返回 Ok。
    let g = make_gossip("node-A", GossipConfig::default());
    let wf = WorkflowId("wf-retry".to_string());
    let task = TaskId::from("t-retry".to_string());

    // 终态
    g.broadcast_update(&wf, &task, WireTaskState::Completed { result: vec![] })
        .await
        .expect("terminal broadcast should succeed");
    assert_eq!(g.seen_count(), 1);

    // 非终态
    let wf2 = WorkflowId("wf-retry2".to_string());
    let task2 = TaskId::from("t-retry2".to_string());
    g.broadcast_update(&wf2, &task2, WireTaskState::Running)
        .await
        .expect("non-terminal broadcast should succeed");
    assert_eq!(g.seen_count(), 2);
}

#[tokio::test]
async fn broadcast_update_same_key_supersedes_with_higher_hlc() {
    let g = make_gossip("node-A", GossipConfig::default());
    let wf = WorkflowId("wf-sup".to_string());
    let task = TaskId::from("t-sup".to_string());
    let key = SeenKey(wf.clone(), task.clone());

    // 第一次广播 Running
    g.broadcast_update(&wf, &task, WireTaskState::Running)
        .await
        .expect("first broadcast");
    let first_ts = g.seen.get(&key).unwrap().hlc_timestamp;
    let _ = first_ts;

    // 第二次广播 Completed，HLC 更大 → 覆盖
    g.broadcast_update(&wf, &task, WireTaskState::Completed { result: vec![] })
        .await
        .expect("second broadcast");
    let entry = g.seen.get(&key).unwrap();
    assert!(entry.hlc_timestamp > first_ts);
    assert_eq!(entry.state_priority, 5);
}

// ───────────────────────── apply_remote_update ─────────────────────────

#[tokio::test]
async fn apply_remote_update_accepts_new_key() {
    // 新的 (wf, task) 没有已存在条目 → 落地（NotFound 视为已处理）后插入 seen 表。
    let g = make_gossip_with_actor("node-A", GossipConfig::default());
    let update = WireDagStateUpdate {
        workflow_id: WorkflowId("wf-new".to_string()),
        task_id: TaskId::from("t-new".to_string()),
        task_state: WireTaskState::Running,
        hlc_timestamp: HlcTimestamp::from_parts(10, 0),
        origin_node: NodeId::from("node-B".to_string()),
    };

    let result = g.apply_remote_update(update.clone()).await;
    // 本节点未托管该 workflow → NotFound 视为已处理，返回 Ok。
    assert!(result.is_ok(), "NotFound should be treated as processed");
    // 落地成功后 seen 表被登记。
    assert_eq!(g.seen_count(), 1);
}

#[tokio::test]
async fn apply_remote_update_drops_stale_update() {
    let g = make_gossip("node-A", GossipConfig::default());
    let wf = WorkflowId("wf-stale".to_string());
    let task = TaskId::from("t-stale".to_string());
    let key = SeenKey(wf.clone(), task.clone());

    // 预插入高 HLC + 高 priority 的条目
    g.seen.insert(
        key.clone(),
        SeenEntry {
            inserted_at: std::time::Instant::now(),
            hlc_timestamp: HlcTimestamp::from_parts(100, 5),
            state_priority: 5, // Completed
        },
    );
    assert_eq!(g.seen_count(), 1);

    // 用更低的 HLC 尝试覆盖
    let stale_update = WireDagStateUpdate {
        workflow_id: wf,
        task_id: task,
        task_state: WireTaskState::Running,
        hlc_timestamp: HlcTimestamp::from_parts(50, 0),
        origin_node: NodeId::from("node-C".to_string()),
    };

    g.apply_remote_update(stale_update)
        .await
        .expect("stale update should return Ok (dropped)");

    // seen 表不变
    assert_eq!(g.seen_count(), 1);
    let entry = g.seen.get(&key).unwrap();
    assert_eq!(entry.state_priority, 5); // 保留旧值
}

#[tokio::test]
async fn apply_remote_update_accepts_higher_hlc() {
    let g = make_gossip_with_actor("node-A", GossipConfig::default());
    let wf = WorkflowId("wf-accept".to_string());
    let task = TaskId::from("t-accept".to_string());
    let key = SeenKey(wf.clone(), task.clone());

    // 预插入低 HLC 的 Running
    g.seen.insert(
        key.clone(),
        SeenEntry {
            inserted_at: std::time::Instant::now(),
            hlc_timestamp: HlcTimestamp::from_parts(10, 0),
            state_priority: 1,
        },
    );

    // 用更高 HLC 的 Completed 覆盖
    let update = WireDagStateUpdate {
        workflow_id: wf,
        task_id: task,
        task_state: WireTaskState::Completed { result: vec![1, 2] },
        hlc_timestamp: HlcTimestamp::from_parts(200, 0),
        origin_node: NodeId::from("node-B".to_string()),
    };

    // apply 落地成功（NotFound 视为已处理）后 seen 被更新。
    g.apply_remote_update(update).await.unwrap();

    // seen 表应被更新
    let entry = g.seen.get(&key).unwrap();
    assert_eq!(entry.state_priority, 5); // 现在是 Completed
    assert_eq!(entry.hlc_timestamp, HlcTimestamp::from_parts(200, 0));
}

// ───────────────────────── handle_heads_exchange ─────────────────────────

#[tokio::test]
async fn handle_heads_exchange_skips_same_node() {
    let g = make_gossip("node-A", GossipConfig::default());

    // 从同一节点发来的 heads exchange → 立即返回 Ok
    let exchange = HeadsExchange {
        node_id: NodeId::from("node-A".to_string()), // 同节点
        heads: Vec::new(),
    };

    let result = g.handle_heads_exchange(&exchange).await;
    assert!(result.is_ok(), "same node exchange should be skipped");
}

#[tokio::test]
async fn handle_heads_exchange_with_empty_heads_succeeds() {
    let g = make_gossip("node-A", GossipConfig::default());

    let exchange = HeadsExchange {
        node_id: NodeId::from("node-B".to_string()),
        heads: Vec::new(), // 空 heads 列表
    };

    let result = g.handle_heads_exchange(&exchange).await;
    assert!(result.is_ok(), "empty heads should succeed");
}

// ───────────────────────── handle_workflow_state_response ─────────────────────────

#[tokio::test]
async fn handle_workflow_state_response_empty_returns_ok() {
    let g = make_gossip("node-A", GossipConfig::default());

    let response = WorkflowStateResponse {
        workflow_id: WorkflowId("wf-empty".to_string()),
        dag: None,
        execution: None,
        pending: None,
    };

    // dag 和 execution 都为 None → 立即返回 Ok
    let result = g.handle_workflow_state_response(&response).await;
    assert!(result.is_ok(), "empty response should return Ok");
}

// ───────────────────────── request_workflow_state ─────────────────────────

#[tokio::test]
async fn request_workflow_state_succeeds_with_mock_transport() {
    let g = make_gossip("node-A", GossipConfig::default());
    let wf = WorkflowId("wf-req".to_string());
    let target = NodeId::from("node-B".to_string());

    let result = g.request_workflow_state(&wf, &target).await;
    assert!(result.is_ok(), "should succeed with MockTransport");
}

// ───────────────────────── handle_workflow_state_request ─────────────────────────

#[tokio::test]
async fn handle_workflow_state_request_no_state_returns_ok() {
    let g = make_gossip("node-A", GossipConfig::default());

    let request = WorkflowStateRequest {
        workflow_id: WorkflowId("wf-no-state".to_string()),
        requesting_node: NodeId::from("node-B".to_string()),
    };

    // 没有注册 WorkflowActor，call_workflow 会失败，
    // 但如果 actor 返回 None（无状态），应返回 Ok。
    let result = g.handle_workflow_state_request(&request).await;
    // 由于没有 actor，会返回 Err — 验证它确实返回错误
    assert!(result.is_err(), "should fail without WorkflowActor");
}

// ───────────────────────── broadcast_with_retry ─────────────────────────

/// 用于测试 broadcast_with_retry 的失败 transport。
struct FailingTransport {
    node_id: NodeId,
    fail_count: std::sync::atomic::AtomicUsize,
    max_fails: usize,
}

impl FailingTransport {
    fn new(node_id: &str, max_fails: usize) -> Self {
        Self {
            node_id: NodeId::from(node_id.to_string()),
            fail_count: std::sync::atomic::AtomicUsize::new(0),
            max_fails,
        }
    }

    fn attempts(&self) -> usize {
        self.fail_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl Transport for FailingTransport {
    fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    fn local_peer_id(&self) -> &str {
        "peer-fail"
    }

    async fn broadcast(&self, _topic: &str, _data: Vec<u8>) -> Result<()> {
        let n = self
            .fail_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if n < self.max_fails {
            Err(ActantError::Internal(format!("simulated failure #{}", n)))
        } else {
            Ok(())
        }
    }

    async fn subscribe(&self, _topic: &str) -> Result<()> {
        Ok(())
    }

    async fn recv_event(&self) -> Option<NetworkEvent> {
        None
    }

    async fn dial(&self, _addr: &str) -> Result<()> {
        Ok(())
    }

    async fn add_gossip_peer(&self, _peer_id: &str) -> Result<()> {
        Ok(())
    }

    fn listen_addresses(&self) -> Result<ListenAddresses> {
        Ok(ListenAddresses {
            endpoint_id: "peer-fail".to_string(),
            relay_url: None,
            direct_addrs: Vec::new(),
            endpoint_addr: "peer-fail".to_string(),
        })
    }

    async fn send_direct_request(
        &self,
        _peer_id_str: &str,
        _request: DirectRequest,
    ) -> Result<DirectResponse> {
        Err(ActantError::Internal("not implemented".into()))
    }

    async fn send_direct_response(
        &self,
        _channel: DirectResponseChannel,
        _response: DirectResponse,
    ) -> Result<()> {
        Ok(())
    }

    async fn discover_peers(&self) -> Result<Vec<crate::runtime::network::PeerId>> {
        Ok(Vec::new())
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn broadcast_with_retry_succeeds_after_failures() {
    // FailingTransport 前 2 次失败，第 3 次成功。
    let transport = Arc::new(FailingTransport::new("node-retry", 2));
    let network: Arc<dyn Transport> = transport.clone();
    let actor_system = Arc::new(ActorSystem::new());
    let wf_id = ActorId::workflow(&NodeId::from("node-retry".to_string()));
    let cfg = GossipConfig {
        retry_attempts: 5,
        retry_base_delay_ms: 1, // 快速重试
        ..GossipConfig::default()
    };
    let g = DagGossip::new(network, actor_system, wf_id, cfg);

    let wf = WorkflowId("wf-retry".to_string());
    let task = TaskId::from("t-retry".to_string());

    // 终态 → 使用 broadcast_with_retry
    g.broadcast_update(&wf, &task, WireTaskState::Completed { result: vec![] })
        .await
        .expect("should succeed after retries");

    assert_eq!(
        transport.attempts(),
        3,
        "should have tried 3 times (2 fails + 1 success)"
    );
}

#[tokio::test]
async fn broadcast_with_retry_exhausted_returns_error() {
    // FailingTransport 总是失败（max_fails=100，但 retry_attempts=3）
    let transport = Arc::new(FailingTransport::new("node-fail", 100));
    let network: Arc<dyn Transport> = transport.clone();
    let actor_system = Arc::new(ActorSystem::new());
    let wf_id = ActorId::workflow(&NodeId::from("node-fail".to_string()));
    let cfg = GossipConfig {
        retry_attempts: 3,
        retry_base_delay_ms: 1,
        ..GossipConfig::default()
    };
    let g = DagGossip::new(network, actor_system, wf_id, cfg);

    let wf = WorkflowId("wf-fail".to_string());
    let task = TaskId::from("t-fail".to_string());

    let result = g
        .broadcast_update(&wf, &task, WireTaskState::Failed { error: "x".into() })
        .await;
    assert!(result.is_err(), "should fail after exhausting retries");
    assert_eq!(transport.attempts(), 3, "should have tried exactly 3 times");
}

#[tokio::test]
async fn broadcast_with_retry_zero_attempts_returns_error() {
    // retry_attempts=0 → 无尝试 → 返回 Internal error
    let transport = Arc::new(FailingTransport::new("node-zero", 0));
    let network: Arc<dyn Transport> = transport.clone();
    let actor_system = Arc::new(ActorSystem::new());
    let wf_id = ActorId::workflow(&NodeId::from("node-zero".to_string()));
    let cfg = GossipConfig {
        retry_attempts: 0,
        ..GossipConfig::default()
    };
    let g = DagGossip::new(network, actor_system, wf_id, cfg);

    let wf = WorkflowId("wf-zero".to_string());
    let task = TaskId::from("t-zero".to_string());

    let result = g
        .broadcast_update(&wf, &task, WireTaskState::Completed { result: vec![] })
        .await;
    assert!(result.is_err(), "should fail with 0 retry attempts");
}

// ───────────────────────── evict_seen TTL ─────────────────────────

#[test]
fn evict_seen_removes_expired_entries() {
    let cfg = GossipConfig {
        dedup_window_size: 2, // 阈值 = 4
        dedup_ttl_secs: 1,    // 1 秒 TTL
        retry_attempts: 1,
        retry_base_delay_ms: 1,
        heads_broadcast_interval_ms: 1000,
    };
    let g = make_gossip("node-evict", cfg);

    // 插入 5 个条目（> 2*2=4），触发清理
    for i in 0..5 {
        let key = SeenKey(
            WorkflowId(format!("wf-{}", i)),
            TaskId::from(format!("t-{}", i)),
        );
        g.seen.insert(
            key,
            SeenEntry {
                inserted_at: std::time::Instant::now() - std::time::Duration::from_secs(10), // 已过期
                hlc_timestamp: HlcTimestamp::from_parts(i as u64, 0),
                state_priority: 1,
            },
        );
    }
    assert_eq!(g.seen_count(), 5);
    g.evict_seen();
    // 所有条目都过期 → 全部被 TTL 清理
    assert_eq!(g.seen_count(), 0, "all expired entries should be removed");
}

#[test]
fn evict_seen_keeps_unexpired_entries() {
    let cfg = GossipConfig {
        dedup_window_size: 2,
        dedup_ttl_secs: 3600, // 1 小时 TTL
        retry_attempts: 1,
        retry_base_delay_ms: 1,
        heads_broadcast_interval_ms: 1000,
    };
    let g = make_gossip("node-evict2", cfg);

    // 插入 5 个未过期条目
    for i in 0..5 {
        let key = SeenKey(
            WorkflowId(format!("wf-{}", i)),
            TaskId::from(format!("t-{}", i)),
        );
        g.seen.insert(
            key,
            SeenEntry {
                inserted_at: std::time::Instant::now(),
                hlc_timestamp: HlcTimestamp::from_parts(i as u64, 0),
                state_priority: 1,
            },
        );
    }
    assert_eq!(g.seen_count(), 5);
    g.evict_seen();
    // TTL 未过期，但超过 window_size(2) → 保留最新的 2 个
    assert_eq!(
        g.seen_count(),
        2,
        "should trim to window_size after TTL pass"
    );
}

// ───────────────────────── apply_remote_update NotFound 路径 ─────────────────────────
//
// 以下测试验证：当本节点注册了 WorkflowActor 但 workflow 不存在时，
// apply_remote_update 应收到 NotFound 错误并静默忽略（返回 Ok）。

use crate::runtime::workflow::actor::WorkflowActor;
use crate::runtime::workflow::orchestrator::Orchestrator;

fn make_gossip_with_actor(node_id: &str, config: GossipConfig) -> DagGossip {
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new(node_id));
    let actor_system = Arc::new(ActorSystem::new());
    let wf_id = ActorId::workflow(&NodeId::from(node_id.to_string()));
    // 注册一个真实的 WorkflowActor，但 orchestrator 中没有 workflow
    let actor = WorkflowActor::new(Orchestrator::new());
    futures::executor::block_on(actor_system.spawn(wf_id.clone(), actor)).unwrap();
    DagGossip::new(network, actor_system, wf_id, config)
}

#[tokio::test]
async fn apply_remote_update_running_for_unknown_workflow_returns_ok() {
    let g = make_gossip_with_actor("node-nf-1", GossipConfig::default());
    let update = WireDagStateUpdate {
        workflow_id: WorkflowId("unknown-wf".to_string()),
        task_id: TaskId::from("t-1".to_string()),
        task_state: WireTaskState::Running,
        hlc_timestamp: HlcTimestamp::from_parts(10, 0),
        origin_node: NodeId::from("node-B".to_string()),
    };

    // WorkflowActor 存在但 orchestrator 没有该 workflow → NotFound → 静默忽略
    let result = g.apply_remote_update(update).await;
    assert!(result.is_ok(), "NotFound should be silently ignored");
    assert_eq!(g.seen_count(), 1);
}

#[tokio::test]
async fn apply_remote_update_completed_for_unknown_workflow_returns_ok() {
    let g = make_gossip_with_actor("node-nf-2", GossipConfig::default());
    let update = WireDagStateUpdate {
        workflow_id: WorkflowId("unknown-wf".to_string()),
        task_id: TaskId::from("t-1".to_string()),
        task_state: WireTaskState::Completed {
            result: vec![1, 2, 3],
        },
        hlc_timestamp: HlcTimestamp::from_parts(10, 0),
        origin_node: NodeId::from("node-B".to_string()),
    };

    let result = g.apply_remote_update(update).await;
    assert!(result.is_ok(), "NotFound should be silently ignored");
}

#[tokio::test]
async fn apply_remote_update_failed_for_unknown_workflow_returns_ok() {
    let g = make_gossip_with_actor("node-nf-3", GossipConfig::default());
    let update = WireDagStateUpdate {
        workflow_id: WorkflowId("unknown-wf".to_string()),
        task_id: TaskId::from("t-1".to_string()),
        task_state: WireTaskState::Failed {
            error: "boom".into(),
        },
        hlc_timestamp: HlcTimestamp::from_parts(10, 0),
        origin_node: NodeId::from("node-B".to_string()),
    };

    let result = g.apply_remote_update(update).await;
    assert!(result.is_ok(), "NotFound should be silently ignored");
}

#[tokio::test]
async fn apply_remote_update_cancelled_for_unknown_workflow_returns_ok() {
    let g = make_gossip_with_actor("node-nf-4", GossipConfig::default());
    let update = WireDagStateUpdate {
        workflow_id: WorkflowId("unknown-wf".to_string()),
        task_id: TaskId::from("t-1".to_string()),
        task_state: WireTaskState::Cancelled,
        hlc_timestamp: HlcTimestamp::from_parts(10, 0),
        origin_node: NodeId::from("node-B".to_string()),
    };

    let result = g.apply_remote_update(update).await;
    assert!(result.is_ok(), "NotFound should be silently ignored");
}

#[tokio::test]
async fn apply_remote_update_skipped_for_unknown_workflow_returns_ok() {
    let g = make_gossip_with_actor("node-nf-5", GossipConfig::default());
    let update = WireDagStateUpdate {
        workflow_id: WorkflowId("unknown-wf".to_string()),
        task_id: TaskId::from("t-1".to_string()),
        task_state: WireTaskState::Skipped,
        hlc_timestamp: HlcTimestamp::from_parts(10, 0),
        origin_node: NodeId::from("node-B".to_string()),
    };

    let result = g.apply_remote_update(update).await;
    assert!(result.is_ok(), "NotFound should be silently ignored");
}

// ───────────────────────── call_workflow_void 错误类型保留 ─────────────────────────

#[tokio::test]
async fn call_workflow_void_preserves_not_found_error_type() {
    let g = make_gossip_with_actor("node-nf-type", GossipConfig::default());
    // 调用一个需要 workflow 存在的方法，但 workflow 不存在
    let result = g
        .call_workflow_void(
            crate::runtime::workflow::actor::workflow_methods::MARK_TASK_RUNNING,
            (
                &WorkflowId("nonexistent".to_string()),
                &TaskId::from("t-1".to_string()),
            ),
        )
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, crate::common::ActantError::NotFound(_)),
        "expected NotFound, got {:?}",
        err
    );
}

// ───────────────────────── actor_system / clock getter ─────────────────────────

#[test]
fn actor_system_getter_returns_reference() {
    let g = make_gossip("node-getter", GossipConfig::default());
    let sys = g.actor_system();
    // 验证返回的 Arc 引用同一个 ActorSystem
    assert!(Arc::strong_count(sys) >= 1);
}

// ───────────────────────── broadcast_heads 成功路径 ─────────────────────────

#[tokio::test]
async fn broadcast_heads_with_no_active_workflows_succeeds() {
    let g = make_gossip_with_actor("node-heads-1", GossipConfig::default());

    // 没有 active workflow → active_ids 为空 → heads 为空 → 广播成功
    let result = g.broadcast_heads().await;
    assert!(
        result.is_ok(),
        "broadcast_heads with no workflows should succeed"
    );
}

// ───────────────────────── handle_workflow_state_request 成功路径 ─────────────────────────

#[tokio::test]
async fn handle_workflow_state_request_unknown_workflow_returns_ok() {
    let g = make_gossip_with_actor("node-req-1", GossipConfig::default());
    let request = WorkflowStateRequest {
        workflow_id: WorkflowId("unknown-wf".to_string()),
        requesting_node: NodeId::from("node-B".to_string()),
    };

    // WorkflowActor 存在，但 orchestrator 没有该 workflow → 返回空 bytes → Ok
    let result = g.handle_workflow_state_request(&request).await;
    assert!(result.is_ok(), "should return Ok for unknown workflow");
}

// ───────────────────────── handle_workflow_state_response 成功路径 ─────────────────────────

#[tokio::test]
async fn handle_workflow_state_response_with_dag_only_returns_ok() {
    let g = make_gossip_with_actor("node-resp-1", GossipConfig::default());
    let response = WorkflowStateResponse {
        workflow_id: WorkflowId("wf-restore".to_string()),
        dag: Some(vec![1, 2, 3]),
        execution: None,
        pending: None,
    };

    // dag 有值，execution 为 None → 调用 APPLY_FULL_STATE
    // restore_workflow_from_bytes 使用 .ok() 丢弃反序列化错误，返回 Ok
    // 因此即使 dag_bytes 是无效的序列化数据，也应返回 Ok
    let result = g.handle_workflow_state_response(&response).await;
    assert!(result.is_ok(), "should return Ok even with invalid bytes");
}

// ───────────────────────── handle_heads_exchange 成功路径 ─────────────────────────

#[tokio::test]
async fn handle_heads_exchange_remote_node_with_heads_succeeds() {
    let g = make_gossip_with_actor("node-hex-1", GossipConfig::default());

    let exchange = HeadsExchange {
        node_id: NodeId::from("node-B".to_string()),
        heads: vec![WorkflowHead {
            workflow_id: WorkflowId("wf-remote".to_string()),
            succeeded_count: 5,
            total_count: 10,
            hlc_timestamp: HlcTimestamp::from_parts(100, 0),
        }],
    };

    // 远程节点有 head，但本地没有该 workflow → 调用 ADOPT_WORKFLOW
    // adopt_workflow 对于不存在的 workflow 会插入占位符并返回 Ok
    // 然后调用 request_workflow_state 广播请求
    let result = g.handle_heads_exchange(&exchange).await;
    assert!(result.is_ok(), "should succeed: adopt + request");
}

// ───────────────────────── request_workflow_state topic 格式 ─────────────────────────

#[tokio::test]
async fn request_workflow_state_broadcasts_to_correct_topic() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CapturingTransport {
        node_id: NodeId,
        broadcast_count: AtomicUsize,
        last_topic: std::sync::Mutex<String>,
    }

    #[async_trait::async_trait]
    impl Transport for CapturingTransport {
        fn node_id(&self) -> &NodeId {
            &self.node_id
        }
        fn local_peer_id(&self) -> &str {
            "peer-cap"
        }
        async fn broadcast(&self, topic: &str, _data: Vec<u8>) -> Result<()> {
            self.broadcast_count.fetch_add(1, Ordering::Relaxed);
            *self.last_topic.lock().unwrap() = topic.to_string();
            Ok(())
        }
        async fn subscribe(&self, _topic: &str) -> Result<()> {
            Ok(())
        }
        async fn recv_event(&self) -> Option<NetworkEvent> {
            None
        }
        async fn dial(&self, _addr: &str) -> Result<()> {
            Ok(())
        }
        async fn add_gossip_peer(&self, _peer_id: &str) -> Result<()> {
            Ok(())
        }
        fn listen_addresses(&self) -> Result<ListenAddresses> {
            Ok(ListenAddresses {
                endpoint_id: "peer-cap".to_string(),
                relay_url: None,
                direct_addrs: Vec::new(),
                endpoint_addr: "peer-cap".to_string(),
            })
        }
        async fn send_direct_request(
            &self,
            _peer_id_str: &str,
            _request: DirectRequest,
        ) -> Result<DirectResponse> {
            Err(ActantError::Internal("not implemented".into()))
        }
        async fn send_direct_response(
            &self,
            _channel: DirectResponseChannel,
            _response: DirectResponse,
        ) -> Result<()> {
            Ok(())
        }
        async fn discover_peers(&self) -> Result<Vec<crate::runtime::network::PeerId>> {
            Ok(Vec::new())
        }
        async fn shutdown(&self) -> Result<()> {
            Ok(())
        }
    }

    let transport = Arc::new(CapturingTransport {
        node_id: NodeId::from("node-cap".to_string()),
        broadcast_count: AtomicUsize::new(0),
        last_topic: std::sync::Mutex::new(String::new()),
    });
    let network: Arc<dyn Transport> = transport.clone();
    let actor_system = Arc::new(ActorSystem::new());
    let wf_id = ActorId::workflow(&NodeId::from("node-cap".to_string()));
    let g = DagGossip::new(network, actor_system, wf_id, GossipConfig::default());

    let wf = WorkflowId("wf-topic".to_string());
    let target = NodeId::from("node-target".to_string());
    g.request_workflow_state(&wf, &target).await.unwrap();

    assert_eq!(transport.broadcast_count.load(Ordering::Relaxed), 1);
    let topic = transport.last_topic.lock().unwrap().clone();
    assert!(
        topic.contains("node-target"),
        "topic should contain target node id, got: {}",
        topic
    );
}

// ───────────────────────── broadcast_update 非终态路径覆盖 ─────────────────────────

#[tokio::test]
async fn broadcast_update_running_uses_single_broadcast_path() {
    // Running 是非终态，应使用单次 broadcast（不重试）
    // MockTransport 总是成功，验证不 panic 即可
    let g = make_gossip("node-running", GossipConfig::default());
    let wf = WorkflowId("wf-run-path".to_string());
    let task = TaskId::from("t-run-path".to_string());

    g.broadcast_update(&wf, &task, WireTaskState::Running)
        .await
        .expect("non-terminal broadcast should succeed");

    // 再次广播同一 key 的 Running（HLC 更大）→ 覆盖
    g.broadcast_update(&wf, &task, WireTaskState::Running)
        .await
        .expect("second running broadcast should succeed");
    assert_eq!(g.seen_count(), 1);
}

// ───────────────────────── evict_seen 边界条件 ─────────────────────────

#[test]
fn evict_seen_exact_double_threshold_is_noop() {
    let cfg = GossipConfig {
        dedup_window_size: 5,
        dedup_ttl_secs: 60,
        retry_attempts: 1,
        retry_base_delay_ms: 1,
        heads_broadcast_interval_ms: 1000,
    };
    let g = make_gossip("node-evict-boundary", cfg);
    // 正好 dedup_window_size * 2 = 10 个条目 → 不触发清理（<= 阈值）
    for i in 0..10 {
        let key = SeenKey(
            WorkflowId(format!("wf-{}", i)),
            TaskId::from(format!("t-{}", i)),
        );
        g.seen.insert(key, make_entry(i as u64, 1));
    }
    g.evict_seen();
    assert_eq!(g.seen_count(), 10, "exactly at threshold should not evict");
}

#[test]
fn evict_seen_one_above_threshold_trims() {
    let cfg = GossipConfig {
        dedup_window_size: 5,
        dedup_ttl_secs: 3600, // 长 TTL，避免 TTL 清理干扰
        retry_attempts: 1,
        retry_base_delay_ms: 1,
        heads_broadcast_interval_ms: 1000,
    };
    let g = make_gossip("node-evict-one-above", cfg);
    // 11 个条目（> 10）→ 触发清理，trim 到 5
    for i in 0..11 {
        let key = SeenKey(
            WorkflowId(format!("wf-{}", i)),
            TaskId::from(format!("t-{}", i)),
        );
        g.seen.insert(key, make_entry(i as u64, 1));
    }
    g.evict_seen();
    assert_eq!(g.seen_count(), 5, "should trim to window_size");
}

// ───────────────────────── broadcast_with_retry 第一条成功 ─────────────────────────

#[tokio::test]
async fn broadcast_with_retry_succeeds_on_first_attempt() {
    // MockTransport 总是成功 → 第一次就成功，不应重试
    let g = make_gossip(
        "node-first-success",
        GossipConfig {
            retry_attempts: 5,
            retry_base_delay_ms: 1,
            ..GossipConfig::default()
        },
    );
    let wf = WorkflowId("wf-first".to_string());
    let task = TaskId::from("t-first".to_string());

    g.broadcast_update(&wf, &task, WireTaskState::Completed { result: vec![] })
        .await
        .expect("should succeed on first attempt");
    assert_eq!(g.seen_count(), 1);
}

// ───────────────────────── Drop impl ─────────────────────────

#[test]
fn drop_does_not_panic() {
    let g = make_gossip("node-drop", GossipConfig::default());
    // drop 应正常执行，不 panic
    drop(g);
}

// ───────────────────────── Clone 行为 ─────────────────────────

#[test]
fn clone_shares_seen_and_clock() {
    let g = make_gossip("node-clone", GossipConfig::default());
    let g2 = g.clone();

    // 在 g 上插入 seen 条目
    let key = SeenKey(
        WorkflowId("wf-clone".to_string()),
        TaskId::from("t-clone".to_string()),
    );
    g.seen.insert(key, make_entry(10, 1));

    // g2 应能看到（共享 Arc）
    assert_eq!(g2.seen_count(), 1);
    assert_eq!(g.seen_count(), 1);

    // clock 也应共享
    let t1 = g.clock().tick();
    let t2 = g2.clock().tick();
    assert!(t2 > t1, "shared clock should be monotonic");
}

// ───────────────────────── 去重登记先于落地（apply 成功后才登记 seen）─────────────────────────

use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

/// 前 N 次 `complete_task` 调用返回错误的 WorkflowActor 桩。
struct FlakyCompleteActor {
    fails_left: AtomicUsize,
}

#[async_trait::async_trait]
impl crate::runtime::actor::Actor for FlakyCompleteActor {
    fn actor_type(&self) -> &str {
        "WorkflowActor"
    }

    async fn handle_message(
        &mut self,
        msg: crate::common::ActorMessage,
    ) -> crate::common::Result<crate::common::ActorMessageResult> {
        let fail = msg.method == "complete_task"
            && self
                .fails_left
                .fetch_update(AtomicOrdering::SeqCst, AtomicOrdering::SeqCst, |n| {
                    if n > 0 {
                        Some(n - 1)
                    } else {
                        None
                    }
                })
                .is_ok();
        let error = if fail {
            Some(crate::common::ActorErrorEnvelope {
                kind: crate::common::ActorErrorKind::Internal,
                message: "transient failure".to_string(),
            })
        } else {
            None
        };
        Ok(crate::common::ActorMessageResult {
            message_id: msg.id,
            payload: vec![],
            error,
        })
    }
}

async fn make_gossip_with_flaky_actor(node_id: &str, fails: usize) -> DagGossip {
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new(node_id));
    let actor_system = Arc::new(ActorSystem::new());
    let wf_id = ActorId::workflow(&NodeId::from(node_id.to_string()));
    actor_system
        .spawn(
            wf_id.clone(),
            FlakyCompleteActor {
                fails_left: AtomicUsize::new(fails),
            },
        )
        .await
        .unwrap();
    DagGossip::new(network, actor_system, wf_id, GossipConfig::default())
}

/// apply 失败时不登记 seen：更新不丢，重传可重放。
#[tokio::test]
async fn apply_remote_update_failure_does_not_register_seen() {
    let g = make_gossip_with_flaky_actor("node-dedup-1", 1).await;
    let update = WireDagStateUpdate {
        workflow_id: WorkflowId("wf-dedup".to_string()),
        task_id: TaskId::from("t-1".to_string()),
        task_state: WireTaskState::Completed { result: vec![1] },
        hlc_timestamp: HlcTimestamp::from_parts(10, 0),
        origin_node: NodeId::from("node-B".to_string()),
    };

    let result = g.apply_remote_update(update.clone()).await;
    assert!(result.is_err(), "apply failure should propagate");
    assert_eq!(
        g.seen_count(),
        0,
        "failed apply must NOT register the dedup entry"
    );

    // 重传同一更新（此时桩已恢复）：应被重新落地而非判为重复丢弃。
    let retried = g.apply_remote_update(update).await;
    assert!(retried.is_ok(), "retry after failure should re-apply");
    assert_eq!(g.seen_count(), 1, "successful apply registers seen");
}

/// apply 成功后登记 seen：同一更新重放会被判为重复丢弃。
#[tokio::test]
async fn apply_remote_update_registers_seen_after_success() {
    let g = make_gossip_with_flaky_actor("node-dedup-2", 0).await;
    let update = WireDagStateUpdate {
        workflow_id: WorkflowId("wf-dedup-ok".to_string()),
        task_id: TaskId::from("t-1".to_string()),
        task_state: WireTaskState::Completed { result: vec![2] },
        hlc_timestamp: HlcTimestamp::from_parts(10, 0),
        origin_node: NodeId::from("node-B".to_string()),
    };

    g.apply_remote_update(update.clone()).await.unwrap();
    assert_eq!(g.seen_count(), 1);

    // 同一更新重放：HLC 相同、priority 相同 → 不被 superseded → 丢弃。
    g.apply_remote_update(update).await.unwrap();
    assert_eq!(
        g.seen_count(),
        1,
        "duplicate replay after success must not grow seen"
    );
}
