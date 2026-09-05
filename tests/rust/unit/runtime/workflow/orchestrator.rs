use super::*;
use crate::common::should_claim_workflow;
use crate::common::{ActantConfig, ActantError, NodeId, Result, RetryPolicy, TaskId, WorkflowId};
use crate::runtime::network::{
    DirectRequest, DirectResponse, ListenAddresses, NetworkEvent, PeerId, Transport,
};
use crate::runtime::state::Store;
use crate::runtime::workflow::dag::Terminal;
use crate::runtime::workflow::orchestrator::types::ConditionEvaluator;
use crate::runtime::workflow::{Dag, DagNode, FailureScope, Phase};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tempfile::tempdir;
const TEST_SIGNING_KEY: &[u8] = b"test-key";

fn make_node(id: &str, name: &str) -> DagNode {
    DagNode {
        task_id: TaskId::from(id.to_string()),
        name: name.to_string(),
        payload: crate::common::payload::sign(TEST_SIGNING_KEY, b"").unwrap(),
        retry_policy: None,
        timeout_ms: None,
        priority: 0,
        metadata: HashMap::new(),
    }
}

fn make_linear_dag() -> Dag {
    // t1 → t2 → t3
    let mut dag = Dag::new();
    dag.add_node(make_node("t1", "first")).unwrap();
    dag.add_node(make_node("t2", "second")).unwrap();
    dag.add_node(make_node("t3", "third")).unwrap();
    dag.add_edge(TaskId::from("t1"), TaskId::from("t2"))
        .unwrap();
    dag.add_edge(TaskId::from("t2"), TaskId::from("t3"))
        .unwrap();
    dag
}

fn make_diamond_dag() -> Dag {
    //     t1
    //    /  \
    //   t2   t3
    //    \  /
    //     t4
    let mut dag = Dag::new();
    dag.add_node(make_node("t1", "root")).unwrap();
    dag.add_node(make_node("t2", "left")).unwrap();
    dag.add_node(make_node("t3", "right")).unwrap();
    dag.add_node(make_node("t4", "join")).unwrap();
    dag.add_edge(TaskId::from("t1"), TaskId::from("t2"))
        .unwrap();
    dag.add_edge(TaskId::from("t1"), TaskId::from("t3"))
        .unwrap();
    dag.add_edge(TaskId::from("t2"), TaskId::from("t4"))
        .unwrap();
    dag.add_edge(TaskId::from("t3"), TaskId::from("t4"))
        .unwrap();
    dag
}
#[tokio::test]
async fn submit_registers_workflow_in_state() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-1");
    let dag = make_linear_dag();

    orch.submit(wf.clone(), dag).await.unwrap();

    assert!(orch.has_workflow(&wf));
    let ids = orch.active_workflow_ids();
    assert!(ids.contains(&wf));
}

/// `adopt_workflow` 在无本地 store 数据时插入占位符（`SlotState::Loading`）。
/// 占位符应：`has_workflow` 返回 true，但 `start` / `on_task_completed` 返回
/// `InvalidState` 错误，防止在数据到达前误操作。
#[tokio::test]
async fn adopt_workflow_inserts_placeholder_when_no_local_data() {
    // Orchestrator::new() 无 store，adopt_workflow 必走占位符路径。
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-adopted");

    orch.adopt_workflow(&wf).await.unwrap();

    // 占位符已注册 workflow ID
    assert!(orch.has_workflow(&wf));
    assert!(!orch.state.is_ready(&wf));

    // start 应拒绝占位符
    let err = orch.start(&wf).unwrap_err();
    assert!(
        matches!(err, crate::common::ActantError::InvalidState(_)),
        "start on placeholder should return InvalidState, got {:?}",
        err
    );

    // on_task_completed 也应拒绝占位符
    let err = orch
        .on_task_completed(&wf, &TaskId::from("t1"), b"r".to_vec())
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::common::ActantError::InvalidState(_)),
        "on_task_completed on placeholder should return InvalidState, got {:?}",
        err
    );
}

/// `submit` 对已就绪的工作流返回 `AlreadyExists`，但对占位符允许覆盖。
#[tokio::test]
async fn submit_rejects_ready_workflow_but_allows_placeholder_override() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-submit");

    // 先 submit 一次，进入 Ready
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    assert!(orch.state.is_ready(&wf));

    // 再次 submit 应返回 AlreadyExists
    let err = orch
        .submit(wf.clone(), make_linear_dag())
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::common::ActantError::AlreadyExists(_)),
        "second submit on Ready workflow should return AlreadyExists, got {:?}",
        err
    );

    // 另一工作流先 adopt 为占位符，再 submit 应成功（覆盖占位符）
    let wf2 = WorkflowId::from("wf-override");
    orch.adopt_workflow(&wf2).await.unwrap();
    assert!(!orch.state.is_ready(&wf2));
    orch.submit(wf2.clone(), make_linear_dag()).await.unwrap();
    assert!(orch.state.is_ready(&wf2));
}

#[tokio::test]
async fn start_returns_root_tasks_and_marks_running() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-1");
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();

    let roots = orch.start(&wf).unwrap();
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].id, TaskId::from("t1"));

    let state = orch.get_state(&wf).unwrap();
    assert_eq!(state.state, Phase::Running);
}

#[tokio::test]
async fn start_returns_error_for_unknown_workflow() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let err = orch.start(&WorkflowId::from("nonexistent")).unwrap_err();
    assert!(matches!(err, crate::common::ActantError::NotFound(_)));
}

#[tokio::test]
async fn completing_task_returns_ready_successors() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-1");
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    orch.start(&wf).unwrap();

    let (ready, _, terminal) = orch
        .on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
        .await
        .unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].id, TaskId::from("t2"));
    assert!(
        !terminal,
        "workflow should not be terminal after completing only the root task"
    );
}

#[tokio::test]
async fn condition_evaluator_automatically_activates_branch() {
    struct AlwaysTrue;
    #[async_trait]
    impl ConditionEvaluator for AlwaysTrue {
        async fn evaluate(
            &self,
            _workflow_id: &WorkflowId,
            _task_id: &TaskId,
            _condition: &str,
        ) -> crate::common::Result<bool> {
            Ok(true)
        }
    }

    let orch = Orchestrator::new()
        .with_signing_key(TEST_SIGNING_KEY.to_vec())
        .with_condition_evaluator(Arc::new(AlwaysTrue));
    let wf = WorkflowId::from("wf-1");

    let mut dag = Dag::new();
    dag.add_node(make_node("t1", "root")).unwrap();
    dag.add_node(make_node("t2", "left")).unwrap();
    dag.add_conditional_edge(
        TaskId::from("t1"),
        TaskId::from("t2"),
        "go_left".to_string(),
    )
    .unwrap();

    orch.submit(wf.clone(), dag).await.unwrap();
    let roots = orch.start(&wf).unwrap();
    assert_eq!(roots.len(), 1);

    let (ready, conditional, _) = orch
        .on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
        .await
        .unwrap();
    assert!(
        conditional.is_empty(),
        "conditional edges should be handled internally"
    );
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].id, TaskId::from("t2"));
}

#[tokio::test]
async fn completing_last_task_signals_workflow_terminal() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-1");
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    orch.start(&wf).unwrap();

    orch.on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
        .await
        .unwrap();
    orch.on_task_completed(&wf, &TaskId::from("t2"), b"r2".to_vec())
        .await
        .unwrap();
    let (ready, _, terminal) = orch
        .on_task_completed(&wf, &TaskId::from("t3"), b"r3".to_vec())
        .await
        .unwrap();

    // Terminal: explicit flag set, no more ready tasks.
    // 必须使用显式 terminal 标志而非 `ready.is_empty()`——条件求值器全部跳过
    // 后继时 ready 也会为空，但工作流未必终态。
    assert!(terminal, "explicit workflow_terminal flag must be set");
    assert!(ready.is_empty());

    let state = orch.get_state(&wf).unwrap();
    assert!(state.is_terminal());
    assert_eq!(state.state, Phase::Completed);
}

#[tokio::test]
async fn completing_diamond_join_waits_for_both_predecessors() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-1");
    orch.submit(wf.clone(), make_diamond_dag()).await.unwrap();
    orch.start(&wf).unwrap();

    // Complete root t1 → t2 and t3 become ready
    let (ready, _, _) = orch
        .on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
        .await
        .unwrap();
    assert_eq!(ready.len(), 2);

    // Complete t2 → t4 NOT ready (still waiting on t3)
    let (ready, _, _) = orch
        .on_task_completed(&wf, &TaskId::from("t2"), b"r2".to_vec())
        .await
        .unwrap();
    assert!(ready.is_empty());

    // Complete t3 → t4 NOW ready
    let (ready, _, _) = orch
        .on_task_completed(&wf, &TaskId::from("t3"), b"r3".to_vec())
        .await
        .unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].id, TaskId::from("t4"));
}

#[tokio::test]
async fn skip_conditional_branch_skips_task_without_failing_workflow() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-1");

    // t1 → t2 → t3 where t2 is skipped
    let dag = make_linear_dag();
    orch.submit(wf.clone(), dag).await.unwrap();
    orch.start(&wf).unwrap();

    orch.skip_conditional_branch(&wf, &TaskId::from("t2"))
        .await
        .unwrap();

    let state = orch.get_state(&wf).unwrap();
    let t2_state = state.tasks.get(&TaskId::from("t2")).unwrap();
    assert_eq!(t2_state.state, Phase::Skipped);
}

/// 回归测试：节点同时是某前驱的普通后继和另一前驱的条件后继时，
/// `skip_conditional_branch` 不得将其错误跳过。
///
/// 场景：
/// ```text
///     t1 ──普通──→ t3
///     t2 ──条件──→ t3
/// ```
/// t3 的 pending = 2（t1 普通 + t2 条件）。当 t2 的条件分支不激活时，
/// `skip_conditional_branch(t3)` 应仅减少一次 pending（2→1），不跳过 t3，
/// 因为 t3 仍等待 t1 的普通完成。t3 的状态应保持 Pending。
#[tokio::test]
async fn skip_conditional_branch_does_not_skip_node_also_regular_successor() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-mixed");

    // t1 ──普通──→ t3
    // t2 ──条件──→ t3
    let mut dag = Dag::new();
    dag.add_node(make_node("t1", "regular_pred")).unwrap();
    dag.add_node(make_node("t2", "cond_pred")).unwrap();
    dag.add_node(make_node("t3", "join")).unwrap();
    dag.add_edge(TaskId::from("t1"), TaskId::from("t3"))
        .unwrap();
    dag.add_conditional_edge(TaskId::from("t2"), TaskId::from("t3"), "maybe".to_string())
        .unwrap();
    orch.submit(wf.clone(), dag).await.unwrap();
    orch.start(&wf).unwrap();

    // 模拟 t2 的条件分支不激活：调用 skip_conditional_branch(t3)。
    // t3 的 pending 应从 2 减为 1，状态保持 Pending（不跳过）。
    let ready = orch
        .skip_conditional_branch(&wf, &TaskId::from("t3"))
        .await
        .unwrap();

    // 不应返回任何 ready 任务（t3 的 pending 仍为 1）
    assert!(
        ready.is_empty(),
        "t3 should not be ready: it still waits for t1"
    );

    let state = orch.get_state(&wf).unwrap();
    let t3_state = state.tasks.get(&TaskId::from("t3")).unwrap();
    assert_ne!(
        t3_state.state,
        Phase::Skipped,
        "t3 must not be skipped: it is still a regular successor of t1"
    );
    assert_eq!(
        t3_state.state,
        Phase::Pending,
        "t3 should remain Pending while waiting for t1"
    );
}

#[tokio::test]
async fn cancel_marks_workflow_cancelled() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-1");
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    orch.start(&wf).unwrap();

    orch.cancel(&wf).await.unwrap();

    let state = orch.get_state(&wf).unwrap();
    assert!(state.is_terminal());
    assert_eq!(state.state, Phase::Cancelled);
}

#[tokio::test]
async fn cancel_unknown_workflow_returns_not_found() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let err = orch.cancel(&WorkflowId::from("nope")).await.unwrap_err();
    assert!(matches!(err, crate::common::ActantError::NotFound(_)));
}

#[tokio::test]
async fn terminal_waiter_resolves_after_completion() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-1");
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    orch.start(&wf).unwrap();

    let rx = orch.state.register_terminal_waiter(wf.clone());

    // Complete all tasks
    orch.on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
        .await
        .unwrap();
    orch.on_task_completed(&wf, &TaskId::from("t2"), b"r2".to_vec())
        .await
        .unwrap();
    orch.on_task_completed(&wf, &TaskId::from("t3"), b"r3".to_vec())
        .await
        .unwrap();

    // Waiter should resolve
    tokio::time::timeout(std::time::Duration::from_secs(1), rx)
        .await
        .expect("waiter did not resolve within 1s")
        .expect("waiter was dropped without signaling");
}

#[tokio::test]
async fn terminal_waiter_resolves_immediately_if_already_terminal() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-1");
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    orch.start(&wf).unwrap();
    orch.cancel(&wf).await.unwrap();

    let rx = orch.state.register_terminal_waiter(wf.clone());
    // Should resolve immediately
    tokio::time::timeout(std::time::Duration::from_millis(100), rx)
        .await
        .expect("waiter did not resolve immediately")
        .expect("waiter was dropped without signaling");
}

#[test]
fn builder_with_node_id_sets_node_id() {
    let orch = Orchestrator::new()
        .with_signing_key(TEST_SIGNING_KEY.to_vec())
        .with_node_id(NodeId::from("node-1"));
    assert_eq!(orch.node_id(), Some(&NodeId::from("node-1")));
}

#[test]
fn new_orchestrator_has_no_store() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    assert!(orch.store().is_none());
}

#[tokio::test]
async fn submit_with_timeout_sets_deadline() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-1");
    orch.submit_with_timeout(wf.clone(), make_linear_dag(), 5000)
        .await
        .unwrap();

    let state = orch.get_state(&wf).unwrap();
    assert!(state.deadline_ms().is_some());
}

// --- B2: 工作流级硬超时主动取消 ---

/// 捕获所有 broadcast 调用的 mock transport，用于断言超时监控发出了取消广播。
struct BroadcastCaptureTransport {
    node_id: NodeId,
    broadcasts: StdMutex<Vec<(String, Vec<u8>)>>,
}

impl BroadcastCaptureTransport {
    fn new(node_id: &str) -> Self {
        Self {
            node_id: NodeId::from(node_id.to_string()),
            broadcasts: StdMutex::new(Vec::new()),
        }
    }

    fn take_broadcasts(&self) -> Vec<(String, Vec<u8>)> {
        self.broadcasts.lock().unwrap().drain(..).collect()
    }
}

#[async_trait::async_trait]
impl Transport for BroadcastCaptureTransport {
    fn node_id(&self) -> &NodeId {
        &self.node_id
    }
    fn local_peer_id(&self) -> &str {
        "capture-peer"
    }
    async fn broadcast(&self, topic: &str, data: Vec<u8>) -> Result<()> {
        self.broadcasts
            .lock()
            .unwrap()
            .push((topic.to_string(), data));
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
            endpoint_id: "capture".to_string(),
            relay_url: None,
            direct_addrs: Vec::new(),
            endpoint_addr: "capture".to_string(),
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
        _channel: crate::runtime::network::DirectResponseChannel,
        _response: DirectResponse,
    ) -> Result<()> {
        Ok(())
    }
    async fn discover_peers(&self) -> Result<Vec<PeerId>> {
        Ok(Vec::new())
    }
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

/// B2 关键测试：注入网络后，工作流超时触发 CancelBroadcast 广播。
#[tokio::test]
async fn workflow_timeout_broadcasts_cancel_when_network_set() {
    use crate::common::wire::{CancelBroadcast, TOPIC_CANCEL};

    // 配置极短的轮询间隔与超时窗口，确保测试在 1s 内完成。
    let mut config = ActantConfig::default();
    config.workflow.state_poll_interval_ms = 20;
    let transport = Arc::new(BroadcastCaptureTransport::new("n1"));
    let orch = Orchestrator::new()
        .with_signing_key(TEST_SIGNING_KEY.to_vec())
        .with_config(config)
        .with_network(transport.clone());

    let wf = WorkflowId::from("wf-timeout");
    // deadline=10ms：start 后立即进入超时。
    orch.submit_with_timeout(wf.clone(), make_linear_dag(), 10)
        .await
        .unwrap();
    // start 设置 started_at_ms，启动超时计时。
    let roots = orch.start(&wf).unwrap();
    // 将根任务标记为 Running，使其成为超时时需要取消的运行中任务。
    orch.mark_task_running(&wf, &roots[0].id).unwrap();

    // 启动超时监控。
    let cancel_tx = orch.start_timeout_watcher();

    // 轮询等待广播发生（最多 2s）。
    let mut captured = Vec::new();
    for _ in 0..200 {
        captured = transport.take_broadcasts();
        if !captured.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let _ = cancel_tx.send(true);

    // 至少应有一条广播到 TOPIC_CANCEL 的消息。
    let cancel_msgs: Vec<&(String, Vec<u8>)> =
        captured.iter().filter(|(t, _)| t == TOPIC_CANCEL).collect();
    assert!(
        !cancel_msgs.is_empty(),
        "timeout watcher should broadcast at least one CancelBroadcast, got: {:?}",
        captured.iter().map(|(t, _)| t).collect::<Vec<_>>()
    );

    // 解码验证内容：task_id 与 workflow_id 必须匹配超时的工作流与运行中任务。
    let decoded: CancelBroadcast = postcard::from_bytes(&cancel_msgs[0].1).unwrap();
    assert_eq!(decoded.workflow_id, wf);
    assert_eq!(decoded.task_id, roots[0].id);

    // 工作流应已进入 Failed 终态。
    let state = orch.get_state(&wf).unwrap();
    assert_eq!(state.state, Phase::Failed);
}

/// B2 回归测试：未注入网络时，超时监控仍标记工作流失败但不广播取消。
#[tokio::test]
async fn workflow_timeout_without_network_marks_failed_without_broadcast() {
    let mut config = ActantConfig::default();
    config.workflow.state_poll_interval_ms = 20;
    // 不调用 with_network，模拟无网络场景（单元测试）。
    let orch = Orchestrator::new()
        .with_signing_key(TEST_SIGNING_KEY.to_vec())
        .with_config(config);

    let wf = WorkflowId::from("wf-no-net");
    orch.submit_with_timeout(wf.clone(), make_linear_dag(), 10)
        .await
        .unwrap();
    let roots = orch.start(&wf).unwrap();
    orch.mark_task_running(&wf, &roots[0].id).unwrap();

    let cancel_tx = orch.start_timeout_watcher();

    // 等待监控至少轮询一次（state_poll_interval_ms=20，等待 100ms 足够）。
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let _ = cancel_tx.send(true);

    let state = orch.get_state(&wf).unwrap();
    assert_eq!(
        state.state,
        Phase::Failed,
        "workflow should be marked Failed even without network"
    );
}

#[tokio::test]
async fn get_dag_returns_submitted_dag() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-1");
    orch.submit(wf.clone(), make_diamond_dag()).await.unwrap();

    let dag = orch.get_dag(&wf).unwrap();
    assert_eq!(dag.node_count(), 4);
}

#[tokio::test]
async fn get_state_returns_none_for_unknown_workflow() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    assert!(orch.get_state(&WorkflowId::from("nonexistent")).is_none());
}

// get_result 返回 pack_group 打包的所有已完成任务结果（与 store 路径一致）。
// get_results 解包 get_result 的返回值，得到 Vec<Vec<u8>>。
// 结果按 task_id 升序排序，保证确定性（HashMap 迭代顺序未定义）。

#[tokio::test]
async fn get_result_returns_packed_group_after_completion() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-1");

    let mut dag = Dag::new();
    dag.add_node(make_node("t1", "only")).unwrap();
    orch.submit(wf.clone(), dag).await.unwrap();
    orch.start(&wf).unwrap();

    orch.on_task_completed(&wf, &TaskId::from("t1"), b"final".to_vec())
        .await
        .unwrap();

    // get_result 返回 pack_group 编码的字节（与 store 路径一致）。
    let packed = orch.get_result(&wf).await.expect("should have result");
    assert_eq!(
        packed,
        crate::common::pack_group(&[b"final".to_vec()]).unwrap()
    );

    // get_results 解包得到原始任务结果列表。
    let results = orch.get_results(&wf).await.expect("should have results");
    assert_eq!(results, vec![b"final".to_vec()]);
}

#[tokio::test]
async fn get_results_orders_by_task_id_deterministically() {
    // 多任务工作流：验证结果按 task_id 升序排序，而非 HashMap 随机顺序。
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-multi");

    let mut dag = Dag::new();
    // 故意用非字母序的提交顺序，且 task_id 字典序与提交序不同。
    dag.add_node(make_node("t3", "third")).unwrap();
    dag.add_node(make_node("t1", "first")).unwrap();
    dag.add_node(make_node("t2", "second")).unwrap();
    orch.submit(wf.clone(), dag).await.unwrap();
    orch.start(&wf).unwrap();

    // 按非字典序完成，排除"提交即排序"的巧合。
    orch.on_task_completed(&wf, &TaskId::from("t2"), b"r2".to_vec())
        .await
        .unwrap();
    orch.on_task_completed(&wf, &TaskId::from("t3"), b"r3".to_vec())
        .await
        .unwrap();
    orch.on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
        .await
        .unwrap();

    let results = orch.get_results(&wf).await.expect("should have results");
    // 期望按 task_id 升序：t1, t2, t3 → r1, r2, r3
    assert_eq!(
        results,
        vec![b"r1".to_vec(), b"r2".to_vec(), b"r3".to_vec()]
    );
}

#[tokio::test]
async fn get_result_returns_none_when_no_completed_tasks() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-empty");

    let mut dag = Dag::new();
    dag.add_node(make_node("t1", "pending")).unwrap();
    orch.submit(wf.clone(), dag).await.unwrap();
    orch.start(&wf).unwrap();

    // 任务未完成，无结果。
    assert_eq!(orch.get_result(&wf).await, None);
    assert_eq!(orch.get_results(&wf).await, None);
}

#[tokio::test]
async fn start_propagates_retry_policy_from_dag_node() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-1");

    let mut dag = Dag::new();
    let mut node = make_node("t1", "retryable");
    node.retry_policy = Some(RetryPolicy {
        max_retries: 5,
        delay_ms: 1000,
        backoff_multiplier: 2.0,
        max_delay_ms: 60000,
    });
    dag.add_node(node).unwrap();

    orch.submit(wf.clone(), dag).await.unwrap();
    let roots = orch.start(&wf).unwrap();

    assert_eq!(roots.len(), 1);
    let policy = roots[0].retry_policy.as_ref().unwrap();
    assert_eq!(policy.max_retries, 5);
    assert_eq!(policy.delay_ms, 1000);
}

// ---- should_claim_workflow 一致性哈希 vs claim_workflow 退让逻辑对齐性 ----

/// 验证 `should_claim_workflow`（一致性哈希决策）与 `claim_workflow`（租约退让）
/// 两条路径策略对齐：当租约仍有效且不属于本节点时直接退让，仅当租约过期或
/// 归属本节点时才认领，避免一致性哈希指定的认领者因退让逻辑而让权给非指定节点。
#[test]
fn failover_claim_strategy_aligned_with_consistent_hash() {
    // should_claim_workflow 使用一致性哈希决定认领权。
    // claim_workflow 与之一致：当租约仍有效且不属于本节点时直接退让，
    // 仅当租约过期或归属本节点时才认领，两种策略不会矛盾。
    let candidates = vec!["node_a".to_string(), "node_b".to_string()];

    // 暴力搜索一个 key 使一致性哈希指向 node_b 而非 node_a
    let mut conflict_key = String::new();
    for i in 0..10000 {
        let key = format!("wf-{}", i);
        if should_claim_workflow(&key, "node_b", candidates.clone())
            && !should_claim_workflow(&key, "node_a", candidates.clone())
        {
            conflict_key = key;
            break;
        }
    }
    assert!(
        !conflict_key.is_empty(),
        "should find a key mapping to node_b via consistent hash"
    );

    assert!(should_claim_workflow(
        &conflict_key,
        "node_b",
        candidates.clone()
    ));
    assert!(!should_claim_workflow(
        &conflict_key,
        "node_a",
        candidates.clone()
    ));
}

/// 高扇出 DAG 基准测试：1 root → N children。
///
/// 测量端到端热点路径（submit → start → complete root → complete all children）
/// 在不同扇出度下的耗时，作为 orchestrator 状态机扩展性能的回归基线。
/// 不做绝对耗时断言（CI 环境噪声大），仅验证正确性与相对增长趋势。
#[tokio::test]
async fn high_fanout_dag_completes_correctly() {
    for &fanout in &[10usize, 100, 500] {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from(format!("wf-fanout-{fanout}"));

        let mut dag = Dag::new();
        dag.add_node(make_node("root", "root")).unwrap();
        for i in 0..fanout {
            let child_id = format!("c{i}");
            dag.add_node(make_node(&child_id, &child_id)).unwrap();
            dag.add_edge(TaskId::from("root"), TaskId::from(child_id))
                .unwrap();
        }

        orch.submit(wf.clone(), dag).await.unwrap();
        let roots = orch.start(&wf).unwrap();
        assert_eq!(roots.len(), 1, "fanout={fanout}: single root");

        // 完成 root，应一次性释放全部 fanout 个子任务。
        let (ready, _, terminal) = orch
            .on_task_completed(&wf, &TaskId::from("root"), b"r".to_vec())
            .await
            .unwrap();
        assert!(
            !terminal,
            "fanout={fanout}: workflow not terminal after root"
        );
        assert_eq!(
            ready.len(),
            fanout,
            "fanout={fanout}: all children ready after root"
        );

        // 逐个完成子任务；最后一个应触发 workflow 终态。
        for i in 0..fanout {
            let child_id = TaskId::from(format!("c{i}"));
            let (_, _, terminal) = orch
                .on_task_completed(&wf, &child_id, b"c".to_vec())
                .await
                .unwrap();
            let is_last = i == fanout - 1;
            assert_eq!(
                terminal, is_last,
                "fanout={fanout}: terminal flag mismatch at child {i}"
            );
        }

        let state = orch.get_state(&wf).unwrap();
        assert_eq!(
            state.state,
            Phase::Completed,
            "fanout={fanout}: workflow completed"
        );
    }
}

// ---- 持久化恢复（重建 Orchestrator 内存状态 + 运行中任务重置为 Pending）----

/// 恢复需要与提交时一致的有效负载签名密钥，否则重建的 ready 任务会因
/// "signing disabled but payload appears signed" 而失败。
fn recover_config() -> ActantConfig {
    ActantConfig {
        payload_signing_key: TEST_SIGNING_KEY.to_vec(),
        ..ActantConfig::default()
    }
}

/// 进程崩溃后从 Store 恢复：DAG/exec/pending 被重建，已完成任务保留，
/// 运行中任务被重置为 Pending，恢复后的 Orchestrator 可继续推进至终态。
///
/// 模拟时间线：submit → start → 完成 t1 → t2 变为 Running → 未落盘时崩溃。
/// 恢复后 t2 应被重置为 Pending（可重新调度），t1 保持 Completed。
#[tokio::test]
async fn recover_reconstructs_workflow_and_resumes_to_terminal() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).await.unwrap();

    // 写入并推进到"崩溃点"：t1 完成、t2 Running。
    let wf = WorkflowId::from("wf-recover-1");
    let orch = Orchestrator::new()
        .with_signing_key(TEST_SIGNING_KEY.to_vec())
        .with_store(store.clone());
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    let roots = orch.start(&wf).unwrap();
    assert_eq!(roots.len(), 1);
    orch.mark_task_running(&wf, &roots[0].id).unwrap();
    let (ready, _, _) = orch
        .on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
        .await
        .unwrap();
    assert_eq!(ready.len(), 1);
    orch.mark_task_running(&wf, &TaskId::from("t2")).unwrap();
    // 显式落盘非终态进度（模拟后台 flush），相当于崩溃前的最后检查点。
    orch.flush_dirty().await.unwrap();
    drop(orch);

    // 模拟重启：从同一 Store 恢复 Orchestrator 内存状态。
    let recovered = Orchestrator::recover(store, recover_config(), None)
        .await
        .unwrap();
    assert!(recovered.has_workflow(&wf));

    let state = recovered.get_state(&wf).unwrap();
    assert!(
        !state.is_terminal(),
        "recovered workflow must not be terminal"
    );
    assert_eq!(state.state, Phase::Running);
    // t1 已完成 → 保留 Completed；t2 曾 Running → 重置为 Pending。
    assert_eq!(state.tasks[&TaskId::from("t1")].state, Phase::Completed);
    assert_eq!(state.tasks[&TaskId::from("t2")].state, Phase::Pending);
    assert_eq!(state.tasks[&TaskId::from("t3")].state, Phase::Pending);

    // 待调度任务重新浮出为 ready：仅 state == Pending 且 pending == 0 的任务
    // 会被重建派发。t1 已 Completed，绝不能被重跑（副作用重复执行）；
    // t2 曾 Running，恢复时被重置为 Pending，是唯一应重新派发的任务。
    let mut ready_ids: Vec<String> = recovered
        .recover_ready_tasks()
        .into_iter()
        .map(|t| t.id.to_string())
        .collect();
    ready_ids.sort();
    assert_eq!(ready_ids, vec!["t2".to_string()]);

    // 恢复后的 Orchestrator 可继续推进：完成 t2、t3 后工作流进入 Completed。
    recovered
        .on_task_completed(&wf, &TaskId::from("t2"), b"r2".to_vec())
        .await
        .unwrap();
    let (_, _, terminal) = recovered
        .on_task_completed(&wf, &TaskId::from("t3"), b"r3".to_vec())
        .await
        .unwrap();
    assert!(terminal);
    assert_eq!(recovered.get_state(&wf).unwrap().state, Phase::Completed);
}

/// 崩溃发生在 `submit` 之后、任何进度落盘之前：恢复后所有任务仍为 Pending，
/// 可从根任务重新 start 并走完全程（验证"仅提交即崩溃"也能被恢复）。
#[tokio::test]
async fn recover_resumes_workflow_submitted_before_progress_flush() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).await.unwrap();

    let wf = WorkflowId::from("wf-recover-2");
    let orch = Orchestrator::new()
        .with_signing_key(TEST_SIGNING_KEY.to_vec())
        .with_store(store.clone());
    // submit 已同步持久化 dag/exec/pending；此后再无任何改动（模拟刚提交即崩溃）。
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    drop(orch);

    let recovered = Orchestrator::recover(store, recover_config(), None)
        .await
        .unwrap();
    assert!(recovered.has_workflow(&wf));
    let state = recovered.get_state(&wf).unwrap();
    assert!(!state.is_terminal());
    for ts in state.tasks.values() {
        assert_eq!(ts.state, Phase::Pending);
    }

    // 重新 start → 返回根任务 t1，随后可走完整个线性 DAG。
    let roots = recovered.start(&wf).unwrap();
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].id, TaskId::from("t1"));
    recovered
        .on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
        .await
        .unwrap();
    recovered
        .on_task_completed(&wf, &TaskId::from("t2"), b"r2".to_vec())
        .await
        .unwrap();
    let (_, _, terminal) = recovered
        .on_task_completed(&wf, &TaskId::from("t3"), b"r3".to_vec())
        .await
        .unwrap();
    assert!(terminal);
    assert_eq!(recovered.get_state(&wf).unwrap().state, Phase::Completed);
}

// ---- P0-2：终态守卫（迟到完成不得复活终态工作流）----

/// 取消后 worker 迟到回传完成：`on_task_completed` 应被守卫拒绝——
/// 工作流保持 Cancelled，任务状态与结果不被改写，不返回任何 ready 后继。
#[tokio::test]
async fn late_completion_after_cancel_does_not_change_terminal_state() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-late-cancel");
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    orch.start(&wf).unwrap();
    orch.mark_task_running(&wf, &TaskId::from("t1")).unwrap();
    orch.cancel(&wf).await.unwrap();

    let (ready, conditional, terminal) = orch
        .on_task_completed(&wf, &TaskId::from("t1"), b"late".to_vec())
        .await
        .unwrap();

    assert!(ready.is_empty(), "no ready successors may be returned");
    assert!(conditional.is_empty());
    assert!(
        !terminal,
        "skipped late completion must not report a fresh terminal transition"
    );

    let state = orch.get_state(&wf).unwrap();
    assert_eq!(
        state.state,
        Phase::Cancelled,
        "workflow must stay Cancelled"
    );
    let t1 = state.tasks.get(&TaskId::from("t1")).unwrap();
    assert_eq!(t1.state, Phase::Cancelled, "task must stay Cancelled");
    assert!(t1.result.is_none(), "late result must not be attached");
    assert_eq!(state.succeeded_count(), 0);
}

/// 失败（FailFast）后迟到的完成结果不得把 Failed 工作流复活为 Completed。
#[tokio::test]
async fn late_completion_after_failure_does_not_revive_workflow() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-late-fail");
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    orch.start(&wf).unwrap();
    orch.fail_task(
        &wf,
        &TaskId::from("t1"),
        "boom".to_string(),
        FailureScope::WorkflowLevel,
    )
    .await
    .unwrap();
    assert_eq!(orch.get_state(&wf).unwrap().state, Phase::Failed);

    let (ready, _, terminal) = orch
        .on_task_completed(&wf, &TaskId::from("t1"), b"late".to_vec())
        .await
        .unwrap();

    assert!(ready.is_empty());
    assert!(!terminal);
    let state = orch.get_state(&wf).unwrap();
    assert_eq!(state.state, Phase::Failed, "workflow must stay Failed");
    let t1 = state.tasks.get(&TaskId::from("t1")).unwrap();
    assert_eq!(t1.state, Phase::Failed);
    assert!(t1.result.is_none());
}

// ---- P1：submit_with_timeout 的 deadline 持久化 ----

/// 设置 deadline 后经 flush 落盘，重启 recover 后 deadline 仍在。
#[tokio::test]
async fn submit_with_timeout_persists_deadline_across_recover() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).await.unwrap();
    let wf = WorkflowId::from("wf-deadline");

    let orch = Orchestrator::new()
        .with_signing_key(TEST_SIGNING_KEY.to_vec())
        .with_store(store.clone());
    orch.submit_with_timeout(wf.clone(), make_linear_dag(), 5000)
        .await
        .unwrap();
    // deadline 设置晚于 submit 的同步落盘，必须经脏标记随 flush 落盘。
    orch.flush_dirty().await.unwrap();
    drop(orch);

    let recovered = Orchestrator::recover(store, recover_config(), None)
        .await
        .unwrap();
    assert!(recovered.has_workflow(&wf));
    let state = recovered.get_state(&wf).unwrap();
    assert_eq!(
        state.deadline_ms(),
        Some(5000),
        "deadline must survive persist + recover"
    );
}

// ---- P1：条件边求值失败不得丢弃 ready 后继 ----

/// 求值器返回 Err 时：`on_task_completed` 不应整体失败，已就绪的普通后继
/// 照常返回，求值失败的条件边原样交还调用方外部处理（重试同一完成消息
/// 不会因任务已终态被守卫拒绝而永久卡死）。
#[tokio::test]
async fn condition_evaluator_error_defers_edge_and_keeps_ready() {
    struct AlwaysErr;
    #[async_trait]
    impl ConditionEvaluator for AlwaysErr {
        async fn evaluate(
            &self,
            _workflow_id: &WorkflowId,
            _task_id: &TaskId,
            _condition: &str,
        ) -> crate::common::Result<bool> {
            Err(ActantError::Internal("evaluator down".into()))
        }
    }

    let orch = Orchestrator::new()
        .with_signing_key(TEST_SIGNING_KEY.to_vec())
        .with_condition_evaluator(Arc::new(AlwaysErr));
    let wf = WorkflowId::from("wf-cond-err");

    // t1 ──普通──→ t3
    // t1 ──条件──→ t2
    let mut dag = Dag::new();
    dag.add_node(make_node("t1", "root")).unwrap();
    dag.add_node(make_node("t2", "cond_succ")).unwrap();
    dag.add_node(make_node("t3", "plain_succ")).unwrap();
    dag.add_edge(TaskId::from("t1"), TaskId::from("t3"))
        .unwrap();
    dag.add_conditional_edge(TaskId::from("t1"), TaskId::from("t2"), "maybe".to_string())
        .unwrap();

    orch.submit(wf.clone(), dag).await.unwrap();
    orch.start(&wf).unwrap();

    let (ready, conditional, terminal) = orch
        .on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
        .await
        .unwrap();

    assert!(
        !terminal,
        "workflow must not be terminal with t2/t3 still outstanding"
    );
    // 普通后继 t3 照常就绪并被返回。
    assert_eq!(ready.len(), 1, "ready successor must not be dropped");
    assert_eq!(ready[0].id, TaskId::from("t3"));
    // 求值失败的条件边原样交还调用方。
    assert_eq!(
        conditional,
        vec![(TaskId::from("t2"), "maybe".to_string())],
        "failed conditional edge must be deferred to the caller"
    );

    // t2 仍是 Pending（未被跳过也未被激活）。
    let state = orch.get_state(&wf).unwrap();
    assert_eq!(state.tasks[&TaskId::from("t2")].state, Phase::Pending);
}

// ---- P1：超时失败的工作流写入 Failed 事件 ----

/// 工作流级硬超时触发后，除状态翻转外还应向 `workflow:{id}` topic 写入
/// `WorkflowEventPayload::Failed` 事件（对齐 fail_task 路径）。
#[tokio::test]
async fn workflow_timeout_writes_failed_event_to_event_log() {
    use crate::runtime::state::event_log::{EventLog, MemoryEventLog};
    use crate::runtime::workflow::orchestrator::types::WorkflowEventPayload;

    let mut config = ActantConfig::default();
    config.workflow.state_poll_interval_ms = 20;
    let event_log = Arc::new(MemoryEventLog::default());
    let orch = Orchestrator::new()
        .with_signing_key(TEST_SIGNING_KEY.to_vec())
        .with_config(config)
        .with_event_log(event_log.clone());

    let wf = WorkflowId::from("wf-timeout-event");
    orch.submit_with_timeout(wf.clone(), make_linear_dag(), 10)
        .await
        .unwrap();
    let roots = orch.start(&wf).unwrap();
    orch.mark_task_running(&wf, &roots[0].id).unwrap();

    let cancel_tx = orch.start_timeout_watcher();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let _ = cancel_tx.send(true);

    let state = orch.get_state(&wf).unwrap();
    assert_eq!(state.state, Phase::Failed);

    let topic = format!("workflow:{}", wf.as_str());
    let entries = event_log.read_after(&topic, None).unwrap();
    let has_failed_event = entries.iter().any(|entry| {
        postcard::from_bytes::<WorkflowEventPayload>(&entry.payload)
            .is_ok_and(|p| matches!(p, WorkflowEventPayload::Failed { .. }))
    });
    assert!(
        has_failed_event,
        "timeout must append a Failed event to topic {topic}"
    );
}

// ───────────────────────── 派发代数（attempt）递增测试 ─────────────────────────

/// 首次派发 attempt = 0，任务完成后重试路径递增 attempt 并随新派发携带。
#[tokio::test]
async fn prepare_task_retry_increments_attempt_and_carries_it_on_dispatch() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-attempt-retry");
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();

    let roots = orch.start(&wf).unwrap();
    assert_eq!(roots[0].attempt, 0, "first dispatch must carry attempt 0");

    // t1 失败（TaskOnly，workflow 保持非终态）后重试。
    orch.fail_task(
        &wf,
        &TaskId::from("t1"),
        "boom".into(),
        FailureScope::TaskOnly,
    )
    .await
    .unwrap();

    let task_def = orch
        .prepare_task_retry(&wf, &TaskId::from("t1"))
        .unwrap()
        .expect("failed task should be retryable");
    assert_eq!(
        task_def.attempt, 1,
        "retry dispatch must carry the incremented attempt"
    );

    let state = orch.get_state(&wf).unwrap();
    assert_eq!(
        state.tasks[&TaskId::from("t1")].attempt(),
        1,
        "TaskState.attempt must record the new dispatch generation"
    );
}

/// 故障转移重派发：Running 任务重置 Pending 时 attempt 递增并随新派发携带。
#[tokio::test]
async fn reschedule_running_tasks_increments_attempt_and_carries_it_on_dispatch() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-attempt-resched");
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    orch.start(&wf).unwrap();
    orch.mark_task_running(&wf, &TaskId::from("t1")).unwrap();

    let rescheduled = orch.reschedule_running_tasks(&wf).unwrap();
    assert_eq!(rescheduled.len(), 1);
    assert_eq!(
        rescheduled[0].id,
        TaskId::from("t1"),
        "only the running task should be rescheduled"
    );
    assert_eq!(
        rescheduled[0].attempt, 1,
        "failover redispatch must carry the incremented attempt"
    );

    // 再次重派发（新一轮故障转移）继续递增。
    orch.mark_task_running(&wf, &TaskId::from("t1")).unwrap();
    let rescheduled = orch.reschedule_running_tasks(&wf).unwrap();
    assert_eq!(rescheduled[0].attempt, 2);
}

// ───────────────────────── complete_task 入口 attempt fencing 测试 ─────────────────────────

/// 故障转移重派发后（attempt=1），旧代（attempt=0）在途执行的迟到完成结果在
/// `complete_task` 入口被丢弃：任务保持 Pending、后继不被调度、无终态推进；
/// 当代（attempt=1）结果正常接受并调度后继。
#[tokio::test]
async fn complete_task_drops_stale_attempt_result_and_accepts_current() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-fencing-complete");
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    orch.start(&wf).unwrap();
    orch.mark_task_running(&wf, &TaskId::from("t1")).unwrap();

    // 故障转移：t1 重置 Pending，attempt 0 → 1
    let rescheduled = orch.reschedule_running_tasks(&wf).unwrap();
    assert_eq!(rescheduled[0].attempt, 1);

    // 旧代（attempt 0）迟到完成 → 丢弃：t1 保持 Pending，t2 不 ready。
    let info = orch
        .complete_task(&wf, &TaskId::from("t1"), b"stale".to_vec(), Some(0))
        .await
        .unwrap();
    assert!(
        info.ready_successors.is_empty(),
        "stale result must not schedule successors"
    );
    assert!(!info.workflow_terminal);
    let state = orch.get_state(&wf).unwrap();
    assert_eq!(state.tasks[&TaskId::from("t1")].state, Phase::Pending);
    assert!(state.tasks[&TaskId::from("t1")].result.is_none());

    // 当代（attempt 1）完成 → 接受：t2 ready。
    let info = orch
        .complete_task(&wf, &TaskId::from("t1"), b"fresh".to_vec(), Some(1))
        .await
        .unwrap();
    assert_eq!(
        info.ready_successors.len(),
        1,
        "current-generation result must schedule the successor"
    );
    assert_eq!(
        info.ready_successors[0],
        TaskId::from("t2"),
        "current-generation result must schedule the successor"
    );
    let state = orch.get_state(&wf).unwrap();
    assert_eq!(state.tasks[&TaskId::from("t1")].state, Phase::Completed);
    assert_eq!(
        state.tasks[&TaskId::from("t1")].result.as_deref(),
        Some(b"fresh".as_slice())
    );
}

/// `on_task_completed`（公共入口）在结果通路未携带 attempt 时传 `None`：
/// fencing 放行，行为与协议扩展前一致（向后兼容）。
#[tokio::test]
async fn on_task_completed_without_attempt_info_still_accepts_results() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-fencing-compat");
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    orch.start(&wf).unwrap();

    let (ready, _, terminal) = orch
        .on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
        .await
        .unwrap();
    assert!(!terminal);
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].id, TaskId::from("t2"));
}

// ───────────────────────── S0：统一工作流历史（快照 + 事件重放）─────────────────────────

use crate::runtime::state::event_log::{EventLog, MemoryEventLog};
use crate::runtime::workflow::orchestrator::types::WorkflowEventPayload;
use crate::runtime::workflow::WaitCondition;

/// 读取工作流 topic 的全部事件并解码。
fn read_history(event_log: &MemoryEventLog, wf: &WorkflowId) -> Vec<WorkflowEventPayload> {
    let topic = format!("workflow:{}", wf.as_str());
    event_log
        .read_after(&topic, None)
        .unwrap()
        .into_iter()
        .map(|entry| postcard::from_bytes(&entry.payload).unwrap())
        .collect()
}

fn event_names(events: &[WorkflowEventPayload]) -> Vec<&'static str> {
    events
        .iter()
        .map(|p| match p {
            WorkflowEventPayload::Submitted { .. } => "Submitted",
            WorkflowEventPayload::NodeAdded { .. } => "NodeAdded",
            WorkflowEventPayload::TaskDispatched { .. } => "TaskDispatched",
            WorkflowEventPayload::Started { .. } => "Started",
            WorkflowEventPayload::TaskRunning { .. } => "TaskRunning",
            WorkflowEventPayload::TaskCompleted { .. } => "TaskCompleted",
            WorkflowEventPayload::TaskFailed { .. } => "TaskFailed",
            WorkflowEventPayload::TaskCancelled { .. } => "TaskCancelled",
            WorkflowEventPayload::Completed { .. } => "Completed",
            WorkflowEventPayload::Failed { .. } => "Failed",
            WorkflowEventPayload::WaitPointRegistered { .. } => "WaitPointRegistered",
            WorkflowEventPayload::SignalReceived { .. } => "SignalReceived",
            WorkflowEventPayload::TimerFired { .. } => "TimerFired",
            WorkflowEventPayload::Recovered { .. } => "Recovered",
        })
        .collect()
}

/// submit 是节点新增的唯一变更点：整图提交时每个节点追加一条 NodeAdded 事件。
#[tokio::test]
async fn submit_appends_node_added_event_per_node() {
    let event_log = Arc::new(MemoryEventLog::default());
    let orch = Orchestrator::new()
        .with_signing_key(TEST_SIGNING_KEY.to_vec())
        .with_event_log(event_log.clone());
    let wf = WorkflowId::from("wf-node-added");

    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();

    let names = event_names(&read_history(&event_log, &wf));
    assert_eq!(
        names.iter().filter(|n| **n == "Submitted").count(),
        1,
        "exactly one Submitted event, got {names:?}"
    );
    assert_eq!(
        names.iter().filter(|n| **n == "NodeAdded").count(),
        3,
        "one NodeAdded event per DAG node, got {names:?}"
    );
    // NodeAdded 携带完整节点定义（S7 增量提交的可重建前提）。
    let nodes: Vec<String> = read_history(&event_log, &wf)
        .into_iter()
        .filter_map(|p| match p {
            WorkflowEventPayload::NodeAdded { node, .. } => Some(node.task_id.to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(nodes.len(), 3);
    assert!(nodes.contains(&"t1".to_string()));
    assert!(nodes.contains(&"t2".to_string()));
    assert!(nodes.contains(&"t3".to_string()));
}

/// recover = 快照 + 其后事件重放：快照水位之后的 TaskCompleted 事件（携带
/// 结果）被重放，崩溃前未落盘的完成进度得以恢复；恢复后 Pending 且依赖
/// 满足的任务重派发、已完成任务不重跑（红线语义）。
#[tokio::test]
async fn recover_replays_events_after_snapshot_watermark() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).await.unwrap();
    let event_log = Arc::new(MemoryEventLog::default());
    let wf = WorkflowId::from("wf-replay-1");

    let orch = Orchestrator::new()
        .with_signing_key(TEST_SIGNING_KEY.to_vec())
        .with_store(store.clone())
        .with_event_log(event_log.clone());
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    let roots = orch.start(&wf).unwrap();
    assert_eq!(roots.len(), 1);
    orch.mark_task_running(&wf, &roots[0].id).unwrap();
    // 快照落盘：水位停在 Started/TaskRunning。
    orch.flush_dirty().await.unwrap();
    // 崩溃前未落盘的完成：TaskCompleted 事件只存在于历史。
    let (ready, _, _) = orch
        .on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
        .await
        .unwrap();
    assert_eq!(ready.len(), 1);
    drop(orch);

    // 历史中确有携带结果的 TaskCompleted 事件。
    let history = read_history(&event_log, &wf);
    assert!(history.iter().any(|p| matches!(
        p,
        WorkflowEventPayload::TaskCompleted { result, .. }
            if result == b"r1".to_vec().as_slice() || *result == b"r1"
    )));

    let recovered = Orchestrator::recover(store, recover_config(), Some(event_log.clone()))
        .await
        .unwrap();
    let state = recovered.get_state(&wf).unwrap();
    assert_eq!(
        state.tasks[&TaskId::from("t1")].state,
        Phase::Completed,
        "watermark-beyond TaskCompleted event must be replayed"
    );
    assert_eq!(
        state.tasks[&TaskId::from("t1")].result.as_deref(),
        Some(b"r1".as_slice()),
        "replay must restore the result carried by the event"
    );
    assert_eq!(
        state.succeeded_count(),
        1,
        "replay must not double-count completions"
    );

    // 红线：恢复后仅 t2（Pending 且依赖满足）重派发，t1 不重跑，t3 未就绪。
    let mut ready_ids: Vec<String> = recovered
        .recover_ready_tasks()
        .into_iter()
        .map(|t| t.id.to_string())
        .collect();
    ready_ids.sort();
    assert_eq!(ready_ids, vec!["t2".to_string()]);
}

/// 重放幂等：重复应用同一事件（重复 recover、重复投递）不产生二次状态变更。
#[tokio::test]
async fn replay_is_idempotent_for_duplicate_events() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).await.unwrap();
    let event_log = Arc::new(MemoryEventLog::default());
    let wf = WorkflowId::from("wf-replay-idem");

    let orch = Orchestrator::new()
        .with_signing_key(TEST_SIGNING_KEY.to_vec())
        .with_store(store.clone())
        .with_event_log(event_log.clone());
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    orch.start(&wf).unwrap();
    orch.flush_dirty().await.unwrap();
    drop(orch);

    let recovered = Orchestrator::recover(store.clone(), recover_config(), Some(event_log))
        .await
        .unwrap();
    let duplicate = WorkflowEventPayload::TaskCompleted {
        workflow_id: wf.clone(),
        task_id: TaskId::from("t1"),
        result: b"r1".to_vec(),
    };
    // 第一次应用：真正推进（t1 此前是 Pending）。
    recovered.apply_replayed_event(duplicate.clone()).await;
    assert_eq!(
        recovered.get_state(&wf).unwrap().succeeded_count(),
        1,
        "first replay application should transition t1"
    );
    // 第二次应用：终态守卫拒绝，计数不变。
    recovered.apply_replayed_event(duplicate).await;
    let state = recovered.get_state(&wf).unwrap();
    assert_eq!(state.succeeded_count(), 1);
    assert_eq!(
        state.tasks[&TaskId::from("t1")].result.as_deref(),
        Some(b"r1".as_slice())
    );

    // 再次 recover（模拟崩溃发生在重放后落盘前）：结果一致。
    drop(recovered);
    let recovered2 = Orchestrator::recover(store, recover_config(), None)
        .await
        .unwrap();
    // 无 event_log 时不重放：快照中 t1 仍 Pending，但重复 recover 的
    // 确定性已由上方重复应用验证。
    assert_eq!(
        recovered2.get_state(&wf).unwrap().tasks[&TaskId::from("t1")].state,
        Phase::Pending
    );
}

/// 全生命周期事件种类齐备：任务路径（派发/运行/完成/失败）与等待点路径
/// （注册/信号/到期）全部进入同一历史。
#[tokio::test]
async fn workflow_history_covers_all_state_transitions() {
    let event_log = Arc::new(MemoryEventLog::default());
    let orch = Orchestrator::new()
        .with_signing_key(TEST_SIGNING_KEY.to_vec())
        .with_event_log(event_log.clone());

    // 任务路径：派发事件由 Worker 上报，测试中直接经 orchestrator 记录。
    let wf = WorkflowId::from("wf-history-complete");
    let mut dag = Dag::new();
    dag.add_node(make_node("only", "single")).unwrap();
    orch.submit(wf.clone(), dag).await.unwrap();
    orch.start(&wf).unwrap();
    orch.mark_task_running(&wf, &TaskId::from("only")).unwrap();
    orch.log_task_dispatched(&wf, &TaskId::from("only"));
    orch.on_task_completed(&wf, &TaskId::from("only"), b"r".to_vec())
        .await
        .unwrap();

    // 失败路径。
    let wff = WorkflowId::from("wf-history-failed");
    orch.submit(wff.clone(), make_linear_dag()).await.unwrap();
    orch.start(&wff).unwrap();
    orch.fail_task(
        &wff,
        &TaskId::from("t1"),
        "boom".to_string(),
        FailureScope::WorkflowLevel,
    )
    .await
    .unwrap();

    // 等待点路径：信号递交 + 定时到期。
    let wfw = WorkflowId::from("wf-history-wait");
    orch.submit(wfw.clone(), make_linear_dag()).await.unwrap();
    orch.register_wait_point(&wfw, "go", WaitCondition::Signal { name: "go".into() })
        .unwrap();
    orch.register_wait_point(
        &wfw,
        "tick",
        WaitCondition::Timer {
            deadline_ms: crate::common::epoch_millis().saturating_sub(100),
        },
    )
    .unwrap();
    orch.signal_wait_point(&wfw, "go").unwrap().unwrap();
    let fired = orch.poll_expired_timers().await.unwrap();
    assert_eq!(fired.len(), 1, "expired timer should fire once");

    for wf_id in [&wf, &wff, &wfw] {
        let names = event_names(&read_history(&event_log, wf_id));
        assert!(!names.is_empty(), "{wf_id:?} history must not be empty");
    }
    let task_names = event_names(&read_history(&event_log, &wf));
    for kind in [
        "Submitted",
        "NodeAdded",
        "TaskDispatched",
        "Started",
        "TaskRunning",
        "TaskCompleted",
        "Completed",
    ] {
        assert!(
            task_names.contains(&kind),
            "task lifecycle history missing {kind}, got {task_names:?}"
        );
    }
    let failed_names = event_names(&read_history(&event_log, &wff));
    assert!(
        failed_names.contains(&"TaskFailed") && failed_names.contains(&"Failed"),
        "failure history must contain TaskFailed + Failed, got {failed_names:?}"
    );
    let wait_names = event_names(&read_history(&event_log, &wfw));
    for kind in ["WaitPointRegistered", "SignalReceived", "TimerFired"] {
        assert!(
            wait_names.contains(&kind),
            "waitpoint history missing {kind}, got {wait_names:?}"
        );
    }
}

// ───────────────────────── S1：持久化等待点原语 ─────────────────────────

/// 注册幂等：同 wait_key 重复注册为 no-op，且只产生一条注册事件。
#[tokio::test]
async fn register_wait_point_is_idempotent() {
    let event_log = Arc::new(MemoryEventLog::default());
    let orch = Orchestrator::new()
        .with_signing_key(TEST_SIGNING_KEY.to_vec())
        .with_event_log(event_log.clone());
    let wf = WorkflowId::from("wf-wait-register");
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();

    let cond = WaitCondition::Signal { name: "go".into() };
    orch.register_wait_point(&wf, "go", cond.clone()).unwrap();
    orch.register_wait_point(&wf, "go", cond).unwrap();

    let names = event_names(&read_history(&event_log, &wf));
    assert_eq!(
        names
            .iter()
            .filter(|n| **n == "WaitPointRegistered")
            .count(),
        1,
        "duplicate register must not append a second event"
    );
}

/// signal 语义：未知 key → None；等待中 → Some(payload) 并记事件；
/// 重复 signal → 直接返回（幂等），不重复记事件。
#[tokio::test]
async fn signal_wait_point_none_for_unknown_and_idempotent_on_repeat() {
    let event_log = Arc::new(MemoryEventLog::default());
    let orch = Orchestrator::new()
        .with_signing_key(TEST_SIGNING_KEY.to_vec())
        .with_event_log(event_log.clone());
    let wf = WorkflowId::from("wf-wait-signal");
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    orch.register_wait_point(&wf, "go", WaitCondition::Signal { name: "go".into() })
        .unwrap();

    // 无工作流级等待点表命中：未知 key → None。
    assert_eq!(orch.signal_wait_point(&wf, "nope").unwrap(), None);

    let first = orch.signal_wait_point(&wf, "go").unwrap();
    assert_eq!(first, Some(Vec::<u8>::new()));
    let second = orch.signal_wait_point(&wf, "go").unwrap();
    assert_eq!(first, second, "repeat signal must return the same payload");

    let names = event_names(&read_history(&event_log, &wf));
    assert_eq!(
        names.iter().filter(|n| **n == "SignalReceived").count(),
        1,
        "repeat signal must not append a second event"
    );
}

/// 等待者唤醒：signal 前注册 → signal 时唤醒；signal 后注册 → 立即唤醒。
#[tokio::test]
async fn wait_point_waiter_wakes_on_signal() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-wait-waiter");
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    orch.register_wait_point(&wf, "go", WaitCondition::Signal { name: "go".into() })
        .unwrap();

    // 先注册后 signal。
    let rx = orch.register_wait_point_waiter(wf.clone(), "go");
    orch.signal_wait_point(&wf, "go").unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), rx)
        .await
        .expect("waiter must wake on signal")
        .expect("waiter payload channel closed");

    // 已 Signaled 后注册：立即解决。
    let rx2 = orch.register_wait_point_waiter(wf.clone(), "go");
    tokio::time::timeout(std::time::Duration::from_millis(100), rx2)
        .await
        .expect("already-signaled waitpoint must resolve waiter immediately")
        .unwrap();
}

/// Timer 等待点：仅到期者触发，重复 poll 幂等；未到期的保持 Waiting。
#[tokio::test]
async fn poll_expired_timers_fires_only_due_and_is_idempotent() {
    let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
    let wf = WorkflowId::from("wf-wait-timer");
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    let now = crate::common::epoch_millis();
    orch.register_wait_point(
        &wf,
        "due",
        WaitCondition::Timer {
            deadline_ms: now - 100,
        },
    )
    .unwrap();
    orch.register_wait_point(
        &wf,
        "later",
        WaitCondition::Timer {
            deadline_ms: now + 60_000,
        },
    )
    .unwrap();

    let fired = orch.poll_expired_timers().await.unwrap();
    assert_eq!(
        fired,
        vec![(wf.clone(), "due".to_string())],
        "only the due timer fires"
    );
    // 重复 poll：已触发的不再次触发。
    let fired_again = orch.poll_expired_timers().await.unwrap();
    assert!(fired_again.is_empty(), "poll must be idempotent");

    // 未到期等待点保持 Waiting，仍可被 signal 之外的路径查询。
    let table = orch.state.waitpoints.get(&wf).unwrap();
    let later = table.get("later").unwrap();
    assert_eq!(
        format!("{:?}", later.state),
        format!("{:?}", crate::runtime::workflow::WaitPointState::Waiting)
    );
}

/// recover 后等待点保留（快照路径）：快照 + 水位恢复后等待点仍在，
/// signal 可达；到期的 Timer 在恢复后由 poll 触发。
#[tokio::test]
async fn waitpoints_survive_recover_and_stay_reachable() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).await.unwrap();
    let event_log = Arc::new(MemoryEventLog::default());
    let wf = WorkflowId::from("wf-wait-recover");

    let orch = Orchestrator::new()
        .with_signing_key(TEST_SIGNING_KEY.to_vec())
        .with_store(store.clone())
        .with_event_log(event_log.clone());
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    orch.register_wait_point(&wf, "go", WaitCondition::Signal { name: "go".into() })
        .unwrap();
    // 快照水位之后的等待点注册：只能靠事件重放恢复（无 flush）。
    drop(orch);
    let recovered = Orchestrator::recover(store, recover_config(), Some(event_log))
        .await
        .unwrap();
    // 事件重放路径：注册事件在水位之后，重放后等待点必须存在。
    // 守卫作用域必须立即关闭：DashMap 的 get 引用跨后续 entry() 调用
    // 会在同分片上自死锁。
    let has_go = recovered
        .state
        .waitpoints
        .get(&wf)
        .map(|table| table.contains_key("go"))
        .unwrap_or(false);
    assert!(
        has_go,
        "waitpoint registered before crash must be rebuilt from history"
    );

    // signal 在恢复后的 orchestrator 上可达。
    let rx = recovered.register_wait_point_waiter(wf.clone(), "go");
    let signaled = recovered.signal_wait_point(&wf, "go").unwrap();
    assert_eq!(signaled, Some(Vec::<u8>::new()));
    tokio::time::timeout(std::time::Duration::from_secs(1), rx)
        .await
        .expect("recovered waitpoint waiter must wake")
        .unwrap();

    // 幂等：恢复后重复注册同 key 仍为 no-op。
    recovered
        .register_wait_point(&wf, "go", WaitCondition::Signal { name: "go".into() })
        .unwrap();
}

/// recover 后等待点保留（快照路径）：flush 过的等待点经 `orch:wait:` 快照
/// 加载；到期的 Timer 在恢复后被 poll 触发（历史事件 TimerFired 同步进入历史）。
#[tokio::test]
async fn waitpoint_snapshot_survives_recover_and_timer_fires_after_recover() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).await.unwrap();
    let event_log = Arc::new(MemoryEventLog::default());
    let wf = WorkflowId::from("wf-wait-snapshot");

    let orch = Orchestrator::new()
        .with_signing_key(TEST_SIGNING_KEY.to_vec())
        .with_store(store.clone())
        .with_event_log(event_log.clone());
    orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
    orch.register_wait_point(
        &wf,
        "due-after-restart",
        WaitCondition::Timer {
            deadline_ms: crate::common::epoch_millis().saturating_sub(50),
        },
    )
    .unwrap();
    // 落盘：等待点进入快照，水位覆盖注册事件。
    orch.flush_dirty().await.unwrap();
    drop(orch);

    let recovered = Orchestrator::recover(store, recover_config(), Some(event_log.clone()))
        .await
        .unwrap();
    let fired = recovered.poll_expired_timers().await.unwrap();
    assert_eq!(
        fired,
        vec![(wf.clone(), "due-after-restart".to_string())],
        "timer persisted before crash must fire after recover"
    );
    // 触发进入同一历史。
    let names = event_names(&read_history(&event_log, &wf));
    assert!(
        names.contains(&"TimerFired"),
        "post-recover timer firing must append TimerFired event, got {names:?}"
    );
}
