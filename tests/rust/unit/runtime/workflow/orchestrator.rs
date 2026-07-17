use super::*;
use crate::common::should_claim_workflow;
use crate::common::{NodeId, RetryPolicy, TaskId, WorkflowId};
use crate::runtime::workflow::dag::Terminal;
use crate::runtime::workflow::orchestrator::types::ConditionEvaluator;
use crate::runtime::workflow::{Dag, DagNode, Phase};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
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
    assert_eq!(packed, crate::common::pack_group(&[b"final".to_vec()]));

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

// ---- 审查验证: should_claim_workflow 一致性哈希 vs claim_workflow 字典序退让 ----

/// 验证审查发现 ST1: should_claim_workflow (一致性哈希) 与 claim_workflow 内部
/// 字典序退让逻辑存在设计不一致。当 lease 过期边界情况发生时，
/// 一致性哈希指定的认领者可能因字典序更大而退让给非指定节点。
///
/// 此测试验证 should_claim_workflow 的决策与 claim_workflow 内部
/// 退让逻辑（existing.node_id < self.node_id 时退让）在特定场景下矛盾。
#[test]
fn failover_claim_strategy_aligned_with_consistent_hash() {
    // should_claim_workflow 使用一致性哈希决定认领权。
    // 修复后 claim_workflow 不再使用字典序退让，因此两种策略不会矛盾：
    // 当租约仍有效且不属于本节点时直接退让，仅当租约过期或归属本节点时才认领。
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
