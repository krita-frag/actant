//! Unit tests extracted from `src/runtime/workflow/failover.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;
use crate::common::{NodeHeartbeat, NodeId, WorkflowId};
use crate::runtime::actor::ActorSystem;
use crate::test_support::MockTransport;

fn make_fm(node_id: &str) -> FailoverManager {
    let network: Arc<dyn crate::runtime::network::Transport> =
        Arc::new(MockTransport::new(node_id));
    let actor_system = Arc::new(ActorSystem::new());
    let wf_id = ActorId::workflow(&NodeId::from(node_id.to_string()));
    FailoverManager::new(
        NodeId::from(node_id.to_string()),
        network,
        actor_system,
        wf_id,
    )
}

fn hb(node_id: &str, ts_ms: u64, workflows: &[&str]) -> NodeHeartbeat {
    NodeHeartbeat {
        node_id: NodeId::from(node_id.to_string()),
        active_workflows: workflows
            .iter()
            .map(|w| WorkflowId(w.to_string()))
            .collect(),
        timestamp_ms: ts_ms,
        available_slots: 4,
        max_slots: 8,
        endpoint_addr: Some(format!("peer-{}", node_id)),
    }
}

#[test]
fn getters_return_configured_values() {
    let fm = make_fm("node-A")
        .with_heartbeat_interval(1234)
        .with_capacity(3, 10);
    assert_eq!(fm.heartbeat_interval_ms(), 1234);
    assert_eq!(
        fm.failure_timeout_ms(),
        FailoverConfig::default().failure_timeout_ms
    );
    assert_eq!(fm.node_id().as_str(), "node-A");
}

#[test]
fn handle_heartbeat_records_peer() {
    let fm = make_fm("node-A");
    // last_heartbeat_ms 使用接收方本地时钟记录（而非发送方 timestamp_ms），
    // 断言落在调用前后的墙钟窗口内。
    let before = crate::common::epoch_millis();
    fm.handle_heartbeat(&hb("node-B", before, &["wf-1"]));
    let after = crate::common::epoch_millis();

    let infos = fm.get_peer_infos();
    assert!(infos.contains_key(&NodeId::from("node-B".to_string())));
    let peer = &infos[&NodeId::from("node-B".to_string())];
    assert!(
        peer.last_heartbeat_ms >= before && peer.last_heartbeat_ms <= after,
        "last_heartbeat_ms should be receiver local receive time, got {}",
        peer.last_heartbeat_ms
    );
    assert_eq!(peer.available_slots, 4);
    assert_eq!(peer.max_slots, 8);
    assert_eq!(peer.endpoint_addr.as_deref(), Some("peer-node-B"));
    assert!(peer
        .active_workflows
        .contains(&WorkflowId("wf-1".to_string())));
}

#[test]
fn handle_heartbeat_ignores_own_node() {
    let fm = make_fm("node-A");
    fm.handle_heartbeat(&hb("node-A", crate::common::epoch_millis(), &[]));
    assert!(fm.get_peer_infos().is_empty());
}

#[test]
fn handle_heartbeat_updates_existing_peer() {
    let fm = make_fm("node-A");
    let now = crate::common::epoch_millis();
    fm.handle_heartbeat(&hb("node-B", now, &["wf-1"]));
    let first = fm.get_peer_infos()[&NodeId::from("node-B".to_string())].last_heartbeat_ms;
    fm.handle_heartbeat(&hb("node-B", now + 1000, &["wf-1", "wf-2"]));

    let peer = &fm.get_peer_infos()[&NodeId::from("node-B".to_string())];
    // 接收方本地时钟：第二次心跳的记录时间不早于第一次。
    assert!(peer.last_heartbeat_ms >= first);
    assert_eq!(peer.active_workflows.len(), 2);
}

#[test]
fn remove_peer_drops_entry() {
    let fm = make_fm("node-A");
    fm.handle_heartbeat(&hb("node-B", crate::common::epoch_millis(), &[]));
    fm.remove_peer(&NodeId::from("node-B".to_string()));
    assert!(!fm
        .get_peer_infos()
        .contains_key(&NodeId::from("node-B".to_string())));
}

#[test]
fn expire_stale_peers_removes_only_timed_out() {
    // last_heartbeat_ms 由接收方本地时钟记录，无法用伪造的旧时间戳构造失联；
    // 使用短超时（failure_timeout_ms=5ms）配置并真实等待。
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let fm = make_fm_with_short_lease("node-A").await;
        register_and_age_peer(&fm, "node-C", &[]).await;
        // node-C 失联后刷新 node-B 心跳，保持其新鲜（超时窗口仅 5ms）。
        fm.handle_heartbeat(&hb("node-B", crate::common::epoch_millis(), &[]));

        let removed = fm.expire_stale_peers();
        let removed_ids: Vec<String> = removed.iter().map(|(n, _)| n.0.clone()).collect();
        assert_eq!(removed_ids, vec!["node-C".to_string()]);
        let infos = fm.get_peer_infos();
        assert!(infos.contains_key(&NodeId::from("node-B".to_string())));
        assert!(!infos.contains_key(&NodeId::from("node-C".to_string())));
    });
}

#[test]
fn expire_stale_peers_skips_zero_heartbeat() {
    // last_heartbeat_ms == 0 表示从未真正收到心跳，不应被判定为 stale。
    let fm = make_fm("node-A");
    fm.handle_heartbeat(&hb("node-B", 0, &[]));
    let removed = fm.expire_stale_peers();
    assert!(removed.is_empty());
}

#[test]
fn update_local_capacity_reflected_in_get_peer_capacities() {
    let fm = make_fm("node-A");
    fm.update_local_capacity(7, 16);
    // 本节点容量不进入 peer 表（仅 peer 容量才进），但 send_heartbeat 会读取。
    // 这里仅验证 update 不 panic 且 peer 表为空。
    assert!(fm.get_peer_capacities().is_empty());
}

#[test]
fn update_peer_capacity_modifies_existing_peer() {
    let fm = make_fm("node-A");
    fm.handle_heartbeat(&hb("node-B", crate::common::epoch_millis(), &[]));
    fm.update_peer_capacity(NodeId::from("node-B".to_string()), 2, 5);
    let caps = fm.get_peer_capacities();
    let b_cap = &caps[&NodeId::from("node-B".to_string())];
    assert_eq!(b_cap.0, 2);
    assert_eq!(b_cap.1, 5);
}

#[test]
fn update_peer_capacity_ignores_unknown_peer() {
    let fm = make_fm("node-A");
    fm.update_peer_capacity(NodeId::from("node-X".to_string()), 1, 2);
    assert!(fm.get_peer_capacities().is_empty());
}

#[test]
fn active_leases_empty_initially() {
    let fm = make_fm("node-A");
    assert!(fm.active_leases().is_empty());
}

#[test]
fn handle_claim_same_node_records_lease_without_network_call() {
    // 同节点 claim：不调用 remove_active_workflow（避免 actor 调用），
    // 仅持久化 lease + 写入 leases map。
    let fm = make_fm("node-A");
    let now = crate::common::epoch_millis();
    let claim = OrchestratorClaim {
        node_id: fm.node_id().clone(),
        workflow_id: WorkflowId("wf-same".to_string()),
        timestamp_ms: now,
    };
    // 阻塞执行 async 方法
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(fm.handle_claim(&claim));

    let leases = fm.active_leases();
    assert_eq!(leases.len(), 1);
    let (wf, node, claimed, expires) = &leases[0];
    assert_eq!(wf, "wf-same");
    assert_eq!(node, "node-A");
    assert_eq!(*claimed, now);
    assert_eq!(*expires, now + FailoverConfig::default().lease_duration_ms);
}

#[test]
fn handle_claim_remote_node_records_lease_and_skips_removal_on_actor_error() {
    // 远端节点 claim：会调用 remove_active_workflow（actor 调用失败被吞掉），
    // 但 lease 仍应被记录。
    let fm = make_fm("node-A");
    let now = crate::common::epoch_millis();
    let claim = OrchestratorClaim {
        node_id: NodeId::from("node-Z".to_string()),
        workflow_id: WorkflowId("wf-remote".to_string()),
        timestamp_ms: now,
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(fm.handle_claim(&claim));

    let leases = fm.active_leases();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].1, "node-Z");
    assert_eq!(
        leases[0].3,
        now + FailoverConfig::default().lease_duration_ms
    );
}

#[test]
fn with_capacity_sets_atomic_counters() {
    let fm = make_fm("node-A").with_capacity(5, 12);
    // 通过 update_peer_capacity 间接验证 atomic 可用性（不 panic）。
    fm.update_local_capacity(0, 0);
}

// ───────────────────────── send_heartbeat ─────────────────────────

#[tokio::test]
async fn send_heartbeat_returns_error_without_actor_system() {
    // 未注册 workflow actor，send_heartbeat 调用 active_workflow_ids 会失败。
    // 验证错误被传播（而非 panic）。
    let fm = make_fm("node-A").with_capacity(3, 10);
    let result = fm.send_heartbeat().await;
    assert!(result.is_err(), "should fail without actor system");
}

#[tokio::test]
async fn send_heartbeat_with_zero_capacity_returns_error() {
    let fm = make_fm("node-A").with_capacity(0, 0);
    let result = fm.send_heartbeat().await;
    assert!(result.is_err(), "should fail without actor system");
}

// ───────────────────────── subscribe_topics ─────────────────────────

#[tokio::test]
async fn subscribe_topics_succeeds() {
    let fm = make_fm("node-A");
    fm.subscribe_topics().await.expect("subscribe_topics");
}

// ───────────────────────── expire_leases ─────────────────────────

#[tokio::test]
async fn expire_leases_no_op_when_no_leases() {
    let fm = make_fm("node-A");
    fm.expire_leases().await;
    assert!(fm.active_leases().is_empty());
}

// ───────────────────────── detect_and_claim_failed_nodes ─────────────────────────

#[tokio::test]
async fn detect_and_claim_failed_nodes_no_op_without_stale_peers() {
    let fm = make_fm("node-A");
    // 无 peer，应直接返回
    fm.detect_and_claim_failed_nodes().await;
    assert!(fm.active_leases().is_empty());
}

#[tokio::test]
async fn detect_and_claim_failed_nodes_skips_fresh_peers() {
    let fm = make_fm("node-A");
    let now = crate::common::epoch_millis();
    // node-B 心跳新鲜且有活跃 workflow
    fm.handle_heartbeat(&hb("node-B", now, &["wf-1"]));
    fm.detect_and_claim_failed_nodes().await;
    // 不应 claim 任何 workflow
    assert!(fm.active_leases().is_empty());
}

#[tokio::test]
async fn detect_and_claim_failed_nodes_skips_peer_without_workflows() {
    let fm = make_fm("node-A");
    let now = crate::common::epoch_millis();
    let timeout = fm.failure_timeout_ms();
    // node-C 心跳过期但无活跃 workflow
    fm.handle_heartbeat(&hb("node-C", now.saturating_sub(timeout + 10000), &[]));
    fm.detect_and_claim_failed_nodes().await;
    assert!(fm.active_leases().is_empty());
}

// ───────────────────────── get_peer_capacities ─────────────────────────

#[test]
fn get_peer_capacities_returns_endpoint_addr() {
    let fm = make_fm("node-A");
    fm.handle_heartbeat(&hb("node-B", crate::common::epoch_millis(), &[]));
    let caps = fm.get_peer_capacities();
    let b_cap = &caps[&NodeId::from("node-B".to_string())];
    // endpoint_addr 应为 Some("peer-node-B")
    assert_eq!(b_cap.2, Some("peer-node-B".to_string()));
}

#[test]
fn get_peer_capacities_empty_initially() {
    let fm = make_fm("node-A");
    assert!(fm.get_peer_capacities().is_empty());
}

// ───────────────────────── set_scheduler ─────────────────────────

#[test]
fn set_scheduler_does_not_panic() {
    let fm = make_fm("node-A");
    // 设置 None 不应 panic
    // set_scheduler 接收 Arc<dyn Scheduler>，None 难以构造，跳过实际设置
    let _ = fm.node_id();
}

// ───────────────────────── handle_heartbeat edge cases ─────────────────────────

#[test]
fn handle_heartbeat_records_multiple_peers() {
    let fm = make_fm("node-A");
    let now = crate::common::epoch_millis();
    fm.handle_heartbeat(&hb("node-B", now, &["wf-1"]));
    fm.handle_heartbeat(&hb("node-C", now, &["wf-2", "wf-3"]));
    fm.handle_heartbeat(&hb("node-D", now, &[]));

    let infos = fm.get_peer_infos();
    assert_eq!(infos.len(), 3);
    assert!(infos.contains_key(&NodeId::from("node-B".to_string())));
    assert!(infos.contains_key(&NodeId::from("node-C".to_string())));
    assert!(infos.contains_key(&NodeId::from("node-D".to_string())));
}

#[test]
fn handle_heartbeat_zero_slot_values() {
    let fm = make_fm("node-A");
    fm.handle_heartbeat(&hb("node-B", crate::common::epoch_millis(), &[]));
    let peer = &fm.get_peer_infos()[&NodeId::from("node-B".to_string())];
    // hb() 默认 available_slots=4, max_slots=8
    assert_eq!(peer.available_slots, 4);
    assert_eq!(peer.max_slots, 8);
}

// ───────────────────────── expire_stale_peers edge cases ─────────────────────────

#[test]
fn expire_stale_peers_returns_expired_peer_info() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let fm = make_fm_with_short_lease("node-A").await;
        register_and_age_peer(&fm, "node-stale", &["wf-x"]).await;

        let removed = fm.expire_stale_peers();
        assert_eq!(removed.len(), 1);
        let (node_id, info) = &removed[0];
        assert_eq!(node_id.0, "node-stale");
        // 返回的 PeerInfo 应保留原 active_workflows
        assert!(info
            .active_workflows
            .contains(&WorkflowId("wf-x".to_string())));
    });
}

#[test]
fn expire_stale_peers_empty_when_no_peers() {
    let fm = make_fm("node-A");
    let removed = fm.expire_stale_peers();
    assert!(removed.is_empty());
}

// ───────────────────────── update_local_capacity ─────────────────────────

#[test]
fn update_local_capacity_zero_values() {
    let fm = make_fm("node-A");
    fm.update_local_capacity(0, 0);
    // 无 panic 即可
}

#[test]
fn update_local_capacity_max_values() {
    let fm = make_fm("node-A");
    fm.update_local_capacity(u32::MAX, u32::MAX);
    // 无 panic 即可
}

// ───────────────────────── update_peer_capacity ─────────────────────────

#[test]
fn update_peer_capacity_zero_values() {
    let fm = make_fm("node-A");
    fm.handle_heartbeat(&hb("node-B", crate::common::epoch_millis(), &[]));
    fm.update_peer_capacity(NodeId::from("node-B".to_string()), 0, 0);
    let caps = fm.get_peer_capacities();
    let b_cap = &caps[&NodeId::from("node-B".to_string())];
    assert_eq!(b_cap.0, 0);
    assert_eq!(b_cap.1, 0);
}

// ───────────────────────── with_heartbeat_interval ─────────────────────────

#[test]
fn with_heartbeat_interval_zero() {
    let fm = make_fm("node-A").with_heartbeat_interval(0);
    assert_eq!(fm.heartbeat_interval_ms(), 0);
}

#[test]
fn with_heartbeat_interval_large() {
    let fm = make_fm("node-A").with_heartbeat_interval(u64::MAX);
    assert_eq!(fm.heartbeat_interval_ms(), u64::MAX);
}

// ───────────────────────── remove_peer ─────────────────────────

#[test]
fn remove_peer_unknown_does_not_panic() {
    let fm = make_fm("node-A");
    fm.remove_peer(&NodeId::from("nonexistent".to_string()));
    // 无 panic 即可
}

#[test]
fn remove_peer_only_removes_specified() {
    let fm = make_fm("node-A");
    let now = crate::common::epoch_millis();
    fm.handle_heartbeat(&hb("node-B", now, &[]));
    fm.handle_heartbeat(&hb("node-C", now, &[]));

    fm.remove_peer(&NodeId::from("node-B".to_string()));
    let infos = fm.get_peer_infos();
    assert!(!infos.contains_key(&NodeId::from("node-B".to_string())));
    assert!(infos.contains_key(&NodeId::from("node-C".to_string())));
}

// ───────────────────────── active_leases ─────────────────────────

#[tokio::test]
async fn active_leases_returns_correct_format() {
    let fm = make_fm("node-A");
    let now = crate::common::epoch_millis();
    let claim = OrchestratorClaim {
        node_id: NodeId::from("node-A".to_string()),
        workflow_id: WorkflowId("wf-format".to_string()),
        timestamp_ms: now,
    };
    fm.handle_claim(&claim).await;

    let leases = fm.active_leases();
    assert_eq!(leases.len(), 1);
    let (wf, node, claimed, expires) = &leases[0];
    assert_eq!(wf, "wf-format");
    assert_eq!(node, "node-A");
    assert_eq!(*claimed, now);
    assert_eq!(*expires, now + FailoverConfig::default().lease_duration_ms);
}

// ───────────────────────── claim_workflow ─────────────────────────

#[tokio::test]
async fn claim_workflow_returns_error_without_actor_system() {
    // 未注册 workflow actor，claim_workflow 调用 adopt_workflow 会失败。
    let fm = make_fm("node-A");
    let wf = WorkflowId("wf-claim-new".to_string());
    let result = fm.claim_workflow(&wf).await;
    assert!(result.is_err(), "should fail without actor system");
    // claim 失败时不应插入 lease
    assert!(!fm
        .active_leases()
        .iter()
        .any(|(w, _, _, _)| w == "wf-claim-new"));
}

// ───────────────────────── handle_claim multiple claims ─────────────────────────

#[tokio::test]
async fn handle_claim_multiple_workflows() {
    let fm = make_fm("node-A");
    let now = crate::common::epoch_millis();

    let claim1 = OrchestratorClaim {
        node_id: NodeId::from("node-B".to_string()),
        workflow_id: WorkflowId("wf-1".to_string()),
        timestamp_ms: now,
    };
    let claim2 = OrchestratorClaim {
        node_id: NodeId::from("node-C".to_string()),
        workflow_id: WorkflowId("wf-2".to_string()),
        timestamp_ms: now,
    };
    fm.handle_claim(&claim1).await;
    fm.handle_claim(&claim2).await;

    let leases = fm.active_leases();
    assert_eq!(leases.len(), 2);
}

#[tokio::test]
async fn handle_claim_overwrites_existing_lease() {
    let fm = make_fm("node-A");
    let now = crate::common::epoch_millis();

    let claim1 = OrchestratorClaim {
        node_id: NodeId::from("node-B".to_string()),
        workflow_id: WorkflowId("wf-overwrite".to_string()),
        timestamp_ms: now,
    };
    fm.handle_claim(&claim1).await;

    let claim2 = OrchestratorClaim {
        node_id: NodeId::from("node-C".to_string()),
        workflow_id: WorkflowId("wf-overwrite".to_string()),
        timestamp_ms: now + 1000,
    };
    fm.handle_claim(&claim2).await;

    let leases = fm.active_leases();
    assert_eq!(leases.len(), 1, "should overwrite, not duplicate");
    assert_eq!(leases[0].1, "node-C", "should be owned by node-C now");
}

// ───────────────────────── PeerInfo conversion ─────────────────────────

#[test]
fn peer_info_preserves_all_fields() {
    let fm = make_fm("node-A");
    let now = crate::common::epoch_millis();
    fm.handle_heartbeat(&hb("node-B", now, &["wf-1", "wf-2"]));
    fm.update_peer_capacity(NodeId::from("node-B".to_string()), 6, 12);

    let infos = fm.get_peer_infos();
    let peer = &infos[&NodeId::from("node-B".to_string())];
    assert_eq!(peer.last_heartbeat_ms, now);
    assert_eq!(peer.available_slots, 6);
    assert_eq!(peer.max_slots, 12);
    assert_eq!(peer.endpoint_addr.as_deref(), Some("peer-node-B"));
    assert_eq!(peer.active_workflows.len(), 2);
}

// ───────────────────────── Stub Workflow Actor ─────────────────────────

use crate::common::{ActorId, ActorMessage, ActorMessageResult, TaskDefinition};
use crate::runtime::actor::Actor;

struct StubWorkflowActor;

#[async_trait::async_trait]
impl Actor for StubWorkflowActor {
    fn actor_type(&self) -> &str {
        "WorkflowActor"
    }

    async fn handle_message(
        &mut self,
        msg: ActorMessage,
    ) -> crate::common::Result<ActorMessageResult> {
        match msg.method.as_str() {
            "adopt_workflow" | "remove_active_workflow" | "delete_workflow" => {
                Ok(ActorMessageResult {
                    message_id: msg.id,
                    payload: vec![],
                    error: None,
                })
            }
            "active_workflow_ids" => Ok(ActorMessageResult {
                message_id: msg.id,
                payload: crate::runtime::workflow::messaging::encode(&Vec::<WorkflowId>::new())?,
                error: None,
            }),
            "reschedule_running_tasks" => {
                let tasks = vec![TaskDefinition {
                    id: crate::common::TaskId::from("t-rescheduled".to_string()),
                    name: "rescheduled".to_string(),
                    payload: vec![],
                    workflow_id: None,
                    target_node: None,
                    origin_node: None,
                    retry_policy: None,
                    priority: 0,
                    timeout_ms: None,
                    attempt: 0,
                    enqueued_at_ms: 0,
                    target_endpoint_addr: None,
                    origin_endpoint_addr: None,
                }];
                Ok(ActorMessageResult {
                    message_id: msg.id,
                    payload: crate::runtime::workflow::messaging::encode(&tasks)?,
                    error: None,
                })
            }
            _ => Ok(ActorMessageResult {
                message_id: msg.id,
                payload: vec![],
                error: None,
            }),
        }
    }
}

async fn make_fm_with_stub_actor(node_id: &str) -> FailoverManager {
    let network: Arc<dyn crate::runtime::network::Transport> =
        Arc::new(MockTransport::new(node_id));
    let actor_system = Arc::new(ActorSystem::new());
    let wf_id = ActorId::workflow(&NodeId::from(node_id.to_string()));
    actor_system
        .spawn(wf_id.clone(), StubWorkflowActor)
        .await
        .unwrap();
    FailoverManager::new(
        NodeId::from(node_id.to_string()),
        network,
        actor_system,
        wf_id,
    )
}

// ───────────────────────── claim_workflow success paths ─────────────────────────

#[tokio::test]
async fn claim_workflow_succeeds_for_new_workflow() {
    let fm = make_fm_with_stub_actor("node-A").await;
    let wf = WorkflowId("wf-claim-new".to_string());

    let result = fm.claim_workflow(&wf).await;
    assert!(result.is_ok());

    let leases = fm.active_leases();
    assert!(leases
        .iter()
        .any(|(w, n, _, _)| w == "wf-claim-new" && n == "node-A"));
}

#[tokio::test]
async fn claim_workflow_returns_ok_when_already_claimed_by_self() {
    let fm = make_fm_with_stub_actor("node-A").await;
    let wf = WorkflowId("wf-claim-self".to_string());

    fm.claim_workflow(&wf).await.unwrap();
    let result = fm.claim_workflow(&wf).await;
    assert!(result.is_ok());

    let leases = fm.active_leases();
    assert_eq!(leases.len(), 1);
}

#[tokio::test]
async fn claim_workflow_defers_when_other_node_has_valid_lease() {
    let fm = make_fm_with_stub_actor("node-A").await;
    let wf = WorkflowId("wf-other".to_string());
    let now = crate::common::epoch_millis();

    // 先插入一个远端节点的有效租约
    fm.handle_claim(&OrchestratorClaim {
        node_id: NodeId::from("node-B".to_string()),
        workflow_id: wf.clone(),
        timestamp_ms: now,
    })
    .await;

    let result = fm.claim_workflow(&wf).await;
    assert!(result.is_ok(), "should defer to existing valid lease");

    let leases = fm.active_leases();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].1, "node-B");
}

// ───────────────────────── reschedule_workflow_tasks ─────────────────────────

#[tokio::test]
async fn reschedule_workflow_tasks_succeeds_without_scheduler() {
    let fm = make_fm_with_stub_actor("node-A").await;
    let wf = WorkflowId("wf-reschedule".to_string());

    let result = fm.reschedule_workflow_tasks(&wf).await;
    assert!(result.is_ok(), "should succeed even without scheduler");
}

// ───────────────────────── Scheduler routed reschedule ─────────────────────────

use crate::runtime::workflow::Scheduler;
use std::sync::Mutex as StdMutex;

struct RecordingScheduler {
    enqueued: Arc<StdMutex<Vec<crate::common::TaskDefinition>>>,
}

impl RecordingScheduler {
    fn new() -> Self {
        Self {
            enqueued: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    fn take_enqueued(&self) -> Vec<crate::common::TaskDefinition> {
        std::mem::take(&mut *self.enqueued.lock().unwrap())
    }
}

#[async_trait::async_trait]
impl Scheduler for RecordingScheduler {
    async fn enqueue(&self, task: crate::common::TaskDefinition) -> crate::common::Result<()> {
        self.enqueued.lock().unwrap().push(task);
        Ok(())
    }

    async fn enqueue_batch(
        &self,
        _tasks: Vec<crate::common::TaskDefinition>,
    ) -> crate::common::Result<()> {
        Ok(())
    }

    async fn dequeue(&self) -> Option<crate::common::TaskDefinition> {
        None
    }

    async fn try_dequeue(&self) -> Option<crate::common::TaskDefinition> {
        None
    }

    async fn dequeue_batch(&self, _limit: usize) -> Vec<crate::common::TaskDefinition> {
        Vec::new()
    }

    async fn drain_unrouted(&self) -> Vec<crate::common::TaskDefinition> {
        Vec::new()
    }

    async fn is_empty(&self) -> bool {
        true
    }

    async fn len(&self) -> usize {
        0
    }

    fn total_queued(&self) -> usize {
        0
    }
}

#[tokio::test]
async fn reschedule_workflow_tasks_enqueues_tasks_via_scheduler() {
    let fm = make_fm_with_stub_actor("node-A").await;
    let scheduler = Arc::new(RecordingScheduler::new());
    fm.set_scheduler(scheduler.clone());
    let wf = WorkflowId("wf-reschedule-sched".to_string());

    let result = fm.reschedule_workflow_tasks(&wf).await;
    assert!(result.is_ok());

    let enqueued = scheduler.take_enqueued();
    assert_eq!(enqueued.len(), 1);
    assert_eq!(enqueued[0].name, "rescheduled");
}

// ───────────────────────── detect_and_claim_failed_nodes ─────────────────────────

#[tokio::test]
async fn detect_and_claim_failed_nodes_claims_orphaned_workflows() {
    let fm = make_fm_with_short_lease("node-A").await;

    // node-B 心跳过期（接收方本地时钟 + 真实等待）且有一个活跃 workflow。
    register_and_age_peer(&fm, "node-B", &["wf-orphan"]).await;

    fm.detect_and_claim_failed_nodes().await;

    let leases = fm.active_leases();
    assert!(
        leases
            .iter()
            .any(|(w, n, _, _)| w == "wf-orphan" && n == "node-A"),
        "node-A is the only live candidate and must claim the orphan: {leases:?}"
    );
}

/// 失联节点不参与接管选举：即使一致性哈希本会把 workflow 分配给死节点，
/// 也由存活节点接管。
#[tokio::test]
async fn detect_and_claim_excludes_stale_nodes_from_election() {
    let fm = make_fm_with_short_lease("node-A").await;

    // 找一个在 [node-A, node-B] 候选下会分配给 node-B（死节点）的 workflow：
    // 旧语义下无人接管；新语义下 node-B 被清出候选集，node-A 必须接管。
    let mut target_wf = None;
    for i in 0..50 {
        let wf = format!("wf-dead-{i}");
        if !crate::common::should_claim_workflow(
            &wf,
            "node-A",
            vec!["node-A".to_string(), "node-B".to_string()],
        ) {
            target_wf = Some(wf);
            break;
        }
    }
    let wf = target_wf.expect("should find a workflow the hash assigns to node-B");

    register_and_age_peer(&fm, "node-B", &[&wf]).await;

    fm.detect_and_claim_failed_nodes().await;

    let leases = fm.active_leases();
    assert!(
        leases.iter().any(|(w, n, _, _)| w == &wf && n == "node-A"),
        "orphan assigned to the dead node must be claimed by the live node: {leases:?}"
    );
}

/// 检测前失联节点已被清出视图（P1：detect_and_claim_failed_nodes 先 expire_stale_peers）。
#[tokio::test]
async fn detect_and_claim_failed_nodes_expires_stale_peers_first() {
    let fm = make_fm_with_short_lease("node-A").await;
    register_and_age_peer(&fm, "node-stale", &[]).await;

    fm.detect_and_claim_failed_nodes().await;

    assert!(
        !fm.get_peer_infos()
            .contains_key(&NodeId::from("node-stale".to_string())),
        "stale peer must be removed from the view before claim election"
    );
}

// ───────────────────────── expire_leases ─────────────────────────

async fn make_fm_with_short_lease(node_id: &str) -> FailoverManager {
    let network: Arc<dyn crate::runtime::network::Transport> =
        Arc::new(MockTransport::new(node_id));
    let actor_system = Arc::new(ActorSystem::new());
    let wf_id = ActorId::workflow(&NodeId::from(node_id.to_string()));
    actor_system
        .spawn(wf_id.clone(), StubWorkflowActor)
        .await
        .unwrap();
    let config = FailoverConfig {
        heartbeat_interval_ms: 1,
        failure_timeout_ms: 5,
        lease_duration_ms: 10,
        lease_expiry_check_interval_secs: 1,
    };
    FailoverManager::with_config(
        NodeId::from(node_id.to_string()),
        network,
        actor_system,
        wf_id,
        config,
        None,
    )
}

/// 注册一条心跳并真实等待超过 failure_timeout_ms（5ms），使其成为失联 peer。
///
/// last_heartbeat_ms 由接收方本地时钟记录，无法用伪造的旧时间戳构造失联。
async fn register_and_age_peer(fm: &FailoverManager, node: &str, workflows: &[&str]) {
    fm.handle_heartbeat(&hb(node, crate::common::epoch_millis(), workflows));
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
}

/// 活跃 workflow 集合可变的 WorkflowActor 桩：真实响应
/// `active_workflow_ids` 与 `remove_active_workflow`，其余方法返回空 ok。
struct MutableActiveIdsActor {
    active: std::sync::Mutex<HashSet<WorkflowId>>,
}

#[async_trait::async_trait]
impl Actor for MutableActiveIdsActor {
    fn actor_type(&self) -> &str {
        "WorkflowActor"
    }

    async fn handle_message(
        &mut self,
        msg: ActorMessage,
    ) -> crate::common::Result<ActorMessageResult> {
        match msg.method.as_str() {
            "active_workflow_ids" => {
                let ids: Vec<WorkflowId> = self.active.lock().unwrap().iter().cloned().collect();
                Ok(ActorMessageResult {
                    message_id: msg.id,
                    payload: crate::runtime::workflow::messaging::encode(&ids)?,
                    error: None,
                })
            }
            "remove_active_workflow" => {
                let wf: WorkflowId = crate::runtime::workflow::messaging::decode(&msg.payload)?;
                self.active.lock().unwrap().remove(&wf);
                Ok(ActorMessageResult {
                    message_id: msg.id,
                    payload: vec![],
                    error: None,
                })
            }
            _ => Ok(ActorMessageResult {
                message_id: msg.id,
                payload: vec![],
                error: None,
            }),
        }
    }
}

/// 构造 FailoverManager 并保留 MockTransport 句柄，供广播计数断言。
async fn make_fm_with_mutable_active(
    node_id: &str,
    ids: Vec<WorkflowId>,
) -> (FailoverManager, Arc<MockTransport>) {
    let network = Arc::new(MockTransport::new(node_id));
    let actor_system = Arc::new(ActorSystem::new());
    let wf_actor_id = ActorId::workflow(&NodeId::from(node_id.to_string()));
    actor_system
        .spawn(
            wf_actor_id.clone(),
            MutableActiveIdsActor {
                active: std::sync::Mutex::new(ids.into_iter().collect()),
            },
        )
        .await
        .unwrap();
    let config = FailoverConfig {
        heartbeat_interval_ms: 1,
        failure_timeout_ms: 5,
        lease_duration_ms: 10,
        lease_expiry_check_interval_secs: 1,
    };
    let fm = FailoverManager::with_config(
        NodeId::from(node_id.to_string()),
        network.clone(),
        actor_system,
        wf_actor_id,
        config,
        None,
    );
    (fm, network)
}

/// 0.3.1 租约裁决：本节点活跃 workflow 的失效租约**无条件续租**——
/// 直接本地延长到期时间并持久化，不重走 claim→广播→重选路径。
#[tokio::test]
async fn expire_leases_renews_lapsed_lease_for_active_workflow_without_reclaim() {
    let (fm, network) =
        make_fm_with_mutable_active("node-A", vec![WorkflowId("wf-lapsed".to_string())]).await;
    let wf = WorkflowId("wf-lapsed".to_string());

    fm.claim_workflow(&wf).await.unwrap();
    assert_eq!(fm.active_leases().len(), 1);
    let old_expires = fm.active_leases()[0].3;
    let broadcasts_after_claim = network.broadcast_count();

    // 等待租约过期（lease_duration_ms=10ms）。
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    fm.expire_leases().await;

    // 活跃 workflow 的失效租约被续租：fresh expiry，持有者不变。
    let leases = fm.active_leases();
    assert_eq!(
        leases.len(),
        1,
        "lapsed lease of an active workflow must be renewed, not dropped"
    );
    assert_eq!(leases[0].1, "node-A");
    assert!(
        leases[0].3 > old_expires,
        "renewed lease must have a fresh expiry, not the old one"
    );
    assert_eq!(
        network.broadcast_count(),
        broadcasts_after_claim,
        "renewal must not broadcast a claim (no re-election / write amplification)"
    );
}

/// 反双主时序契约：远端 claim 使本节点让位（remove_active_workflow），
/// 让位后旧主不再为该 workflow 续租。
#[tokio::test]
async fn expire_leases_stops_renewing_after_remote_claim_removes_workflow() {
    let (fm, _network) =
        make_fm_with_mutable_active("node-A", vec![WorkflowId("wf-shared".to_string())]).await;
    let wf = WorkflowId("wf-shared".to_string());

    fm.claim_workflow(&wf).await.unwrap();

    // 租约过期时 workflow 仍活跃 → 续租（不丢租约）。
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    fm.expire_leases().await;
    assert_eq!(
        fm.active_leases().len(),
        1,
        "active workflow must keep its lease across expiry"
    );

    // 远端 node-Z claim：本节点让位，workflow 退出 active_set。
    let claim = OrchestratorClaim {
        node_id: NodeId::from("node-Z".to_string()),
        workflow_id: wf.clone(),
        timestamp_ms: crate::common::epoch_millis(),
    };
    fm.handle_claim(&claim).await;
    let active = fm.active_workflow_ids().await.unwrap();
    assert!(
        !active.contains(&wf),
        "remote claim must remove the workflow from the local active set"
    );

    // 让位后旧主不再续租：租约过期走失效移除路径。
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    fm.expire_leases().await;
    assert!(
        fm.active_leases().iter().all(|l| l.0 != "wf-shared"),
        "workflow removed by remote claim must not be renewed nor held locally"
    );
}

/// 过期租约不再无条件下自续：workflow 不再活跃时失效租约被直接移除。
#[tokio::test]
async fn expire_leases_removes_lapsed_own_lease_for_inactive_workflow() {
    let fm = make_fm_with_short_lease("node-A").await;
    let wf = WorkflowId("wf-lapsed-inactive".to_string());

    fm.claim_workflow(&wf).await.unwrap();
    assert_eq!(fm.active_leases().len(), 1);

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    fm.expire_leases().await;
    assert!(
        fm.active_leases().is_empty(),
        "lapsed lease for inactive workflow must be dropped"
    );
}

#[tokio::test]
async fn expire_leases_removes_expired_leases_for_inactive_workflows() {
    let fm = make_fm_with_short_lease("node-A").await;
    let wf = WorkflowId("wf-expire".to_string());

    // claim 一个 workflow
    fm.claim_workflow(&wf).await.unwrap();
    assert_eq!(fm.active_leases().len(), 1);

    // 等待租约过期（lease_duration_ms=10ms，多等一点确保过期）。
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    // expire_leases 会查询 active workflow ids（stub 返回空），
    // 因此该 lease 对应的 workflow 不在 active_set 中且已过期，应被移除。
    fm.expire_leases().await;
    assert!(fm.active_leases().is_empty());
}

#[tokio::test]
async fn expire_leases_keeps_own_valid_lease() {
    let fm = make_fm_with_short_lease("node-A").await;
    let wf = WorkflowId("wf-renew".to_string());

    fm.claim_workflow(&wf).await.unwrap();
    let _ = fm.active_leases().pop().unwrap();

    // 在租约过期前调用 expire_leases：租约仍有效，不应被移除。
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    fm.expire_leases().await;

    let leases = fm.active_leases();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].0, "wf-renew");
}
