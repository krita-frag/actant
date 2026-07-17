//! Unit tests extracted from `src/runtime/workflow/dag.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;
use crate::common::RetryPolicy;

fn make_node(id: &str, name: &str) -> DagNode {
    DagNode {
        task_id: TaskId::from(id.to_string()),
        name: name.to_string(),
        payload: Vec::new(),
        retry_policy: None,
        timeout_ms: None,
        priority: 0,
        metadata: HashMap::new(),
    }
}

fn make_linear_dag() -> Dag {
    let mut dag = Dag::new();
    dag.add_node(make_node("a", "task_a")).unwrap();
    dag.add_node(make_node("b", "task_b")).unwrap();
    dag.add_node(make_node("c", "task_c")).unwrap();
    dag.add_edge(TaskId::from("a"), TaskId::from("b")).unwrap();
    dag.add_edge(TaskId::from("b"), TaskId::from("c")).unwrap();
    dag
}

// --- Phase 测试 ---

#[test]
fn phase_as_str_returns_canonical_constants() {
    assert_eq!(Phase::Pending.as_str(), phase_str::PENDING);
    assert_eq!(Phase::Running.as_str(), phase_str::RUNNING);
    assert_eq!(Phase::Completed.as_str(), phase_str::COMPLETED);
    assert_eq!(Phase::Failed.as_str(), phase_str::FAILED);
    assert_eq!(Phase::Cancelled.as_str(), phase_str::CANCELLED);
    assert_eq!(Phase::Skipped.as_str(), phase_str::SKIPPED);
}

#[test]
fn phase_parse_roundtrips_all_variants() {
    for p in [
        Phase::Pending,
        Phase::Running,
        Phase::Completed,
        Phase::Failed,
        Phase::Cancelled,
        Phase::Skipped,
    ] {
        let s = p.as_str();
        assert_eq!(Phase::parse(s), Some(p), "roundtrip failed for {s}");
    }
}

#[test]
fn phase_parse_is_case_insensitive() {
    assert_eq!(Phase::parse("pending"), Some(Phase::Pending));
    assert_eq!(Phase::parse("RUNNING"), Some(Phase::Running));
}

#[test]
fn phase_parse_unknown_returns_none() {
    assert_eq!(Phase::parse("unknown"), None);
    assert_eq!(Phase::parse(""), None);
    assert_eq!(Phase::parse("Completedd"), None);
}

#[test]
fn failure_strategy_as_str_returns_canonical_constants() {
    assert_eq!(FailureStrategy::FailFast.as_str(), "fail_fast");
    assert_eq!(FailureStrategy::Continue.as_str(), "continue");
}

#[test]
fn failure_strategy_parse_roundtrips() {
    assert_eq!(
        FailureStrategy::parse("fail_fast"),
        Some(FailureStrategy::FailFast)
    );
    assert_eq!(
        FailureStrategy::parse("continue"),
        Some(FailureStrategy::Continue)
    );
}

#[test]
fn failure_strategy_parse_aliases() {
    assert_eq!(
        FailureStrategy::parse("failfast"),
        Some(FailureStrategy::FailFast)
    );
    assert_eq!(
        FailureStrategy::parse("continue_on_failure"),
        Some(FailureStrategy::Continue)
    );
    assert_eq!(
        FailureStrategy::parse("best_effort"),
        Some(FailureStrategy::Continue)
    );
}

#[test]
fn failure_strategy_parse_unknown_returns_none() {
    assert_eq!(FailureStrategy::parse("unknown"), None);
    assert_eq!(FailureStrategy::parse(""), None);
}

// --- WorkflowExecution 测试 ---

#[test]
fn workflow_execution_new_creates_pending_state() {
    let wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string())],
    );
    assert_eq!(wf.state, Phase::Pending);
    assert_eq!(wf.total_count(), 1);
}

#[test]
fn mark_task_completed_triggers_workflow_completion() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string())],
    );
    wf.mark_running();
    wf.mark_task_completed(&TaskId::from("a"), b"ok".to_vec());
    assert_eq!(wf.state, Phase::Completed);
}

#[test]
fn mark_task_skipped_triggers_workflow_completion() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string())],
    );
    wf.mark_running();
    wf.mark_task_skipped(&TaskId::from("a"));
    assert_eq!(wf.state, Phase::Completed);
}

#[test]
fn fail_task_with_fail_fast_strategy() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string())],
    )
    .with_failure_strategy(FailureStrategy::FailFast);
    wf.mark_running();
    wf.fail_task(
        &TaskId::from("a"),
        "boom".into(),
        FailureScope::WorkflowLevel,
    );
    assert_eq!(wf.state, Phase::Failed);
    assert!(wf.error.as_deref().unwrap().contains("boom"));
}

#[test]
fn fail_task_with_continue_strategy_keeps_running() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string()), TaskId::from("b".to_string())],
    )
    .with_failure_strategy(FailureStrategy::Continue);
    wf.mark_running();
    wf.fail_task(
        &TaskId::from("a"),
        "boom".into(),
        FailureScope::WorkflowLevel,
    );
    // 工作流未完成，因为 task b 仍在运行
    assert!(!wf.state.is_terminal());

    // task b 完成 → 工作流转 Failed
    wf.mark_task_completed(&TaskId::from("b"), b"ok".to_vec());
    assert_eq!(wf.state, Phase::Failed);
}

#[test]
fn fail_task_with_task_only_scope_does_not_trigger_workflow() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string())],
    )
    .with_failure_strategy(FailureStrategy::FailFast);
    wf.mark_running();
    wf.fail_task(&TaskId::from("a"), "boom".into(), FailureScope::TaskOnly);
    // TaskOnly 不触发 workflow 级别状态转换
    assert_eq!(wf.state, Phase::Running);
}

#[test]
fn fail_task_idempotent_when_already_terminal() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string())],
    )
    .with_failure_strategy(FailureStrategy::FailFast);
    wf.mark_running();
    wf.fail_task(
        &TaskId::from("a"),
        "first".into(),
        FailureScope::WorkflowLevel,
    );
    wf.fail_task(
        &TaskId::from("a"),
        "second".into(),
        FailureScope::WorkflowLevel,
    );
    // 第二次调用不应修改错误消息
    assert!(wf.error.as_deref().unwrap().contains("first"));
}

#[test]
fn reset_task_from_failed_state() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string())],
    );
    wf.mark_running();
    wf.fail_task(
        &TaskId::from("a"),
        "boom".into(),
        FailureScope::WorkflowLevel,
    );
    wf.reset_task(&TaskId::from("a"), true, false);
    let task = &wf.tasks[&TaskId::from("a")];
    assert_eq!(task.state, Phase::Pending);
    assert_eq!(task.retry_count(), 1);
    assert!(task.result.is_none());
}

#[test]
fn reset_task_idempotent_for_completed_task() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string())],
    );
    wf.mark_running();
    wf.mark_task_completed(&TaskId::from("a"), b"ok".to_vec());
    wf.reset_task(&TaskId::from("a"), false, true);
    let task = &wf.tasks[&TaskId::from("a")];
    assert_eq!(task.state, Phase::Completed);
}

#[test]
fn is_expired_returns_true_when_deadline_passed() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string())],
    );
    wf.set_deadline_ms(0);
    wf.started_at_ms = Some(crate::common::epoch_millis() - 100);
    assert!(wf.is_expired());
}

#[test]
fn is_expired_returns_false_when_no_deadline_set() {
    let wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string())],
    );
    assert!(!wf.is_expired());
}

#[test]
fn mark_workflow_failed_sets_all_running_tasks() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string()), TaskId::from("b".to_string())],
    );
    wf.mark_running();
    wf.mark_task_running(&TaskId::from("a"));
    wf.mark_task_running(&TaskId::from("b"));
    wf.mark_workflow_failed("fatal".into());
    assert_eq!(wf.state, Phase::Failed);
    assert_eq!(wf.tasks[&TaskId::from("a")].state, Phase::Failed);
    assert_eq!(wf.tasks[&TaskId::from("b")].state, Phase::Failed);
}

#[test]
fn cancel_task_cancels_running_task_only() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string())],
    );
    wf.mark_running();
    wf.mark_task_running(&TaskId::from("a"));
    assert!(wf.cancel_task(&TaskId::from("a")));
    assert_eq!(wf.tasks[&TaskId::from("a")].state, Phase::Cancelled);
    assert!(!wf.cancel_task(&TaskId::from("a"))); // 幂等：不会再次取消
}

#[test]
fn collected_results_sorts_by_task_id() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![
            TaskId::from("c".to_string()),
            TaskId::from("a".to_string()),
            TaskId::from("b".to_string()),
        ],
    );
    wf.mark_running();
    wf.mark_task_completed(&TaskId::from("c"), b"result-c".to_vec());
    wf.mark_task_completed(&TaskId::from("a"), b"result-a".to_vec());
    wf.mark_task_completed(&TaskId::from("b"), b"result-b".to_vec());
    let results = wf.collected_results();
    assert_eq!(results.len(), 3);
    // 应按 task_id 升序：a,b,c
    assert_eq!(results[0], b"result-a");
    assert_eq!(results[1], b"result-b");
    assert_eq!(results[2], b"result-c");
}

// --- Terminal trait 测试 ---

fn assert_terminal(variants: &[Phase], expected: bool) {
    for v in variants {
        assert_eq!(
            v.is_terminal(),
            expected,
            "{v:?} terminal expected {expected}"
        );
    }
}

#[test]
fn terminal_states_are_terminal() {
    assert_terminal(
        &[
            Phase::Completed,
            Phase::Failed,
            Phase::Cancelled,
            Phase::Skipped,
        ],
        true,
    );
    assert_terminal(&[Phase::Pending, Phase::Running], false);
}

// --- Dag 图测试 ---

#[test]
fn add_node_increments_count_and_allows_lookup() {
    let mut dag = Dag::new();
    dag.add_node(make_node("t1", "task1")).unwrap();
    assert_eq!(dag.node_count(), 1);
    assert!(dag.get_node(&TaskId::from("t1")).is_some());
    assert!(dag.get_node(&TaskId::from("missing")).is_none());
}

#[test]
fn add_edge_links_predecessors_and_successors() {
    let dag = make_linear_dag();
    assert_eq!(dag.edge_count(), 2);
    assert_eq!(dag.predecessor_count(&TaskId::from("a")), 0);
    assert_eq!(dag.predecessor_count(&TaskId::from("b")), 1);
    assert_eq!(dag.predecessor_count(&TaskId::from("c")), 1);

    let succ_ids = dag.successor_ids(&TaskId::from("a"));
    assert_eq!(succ_ids, vec![TaskId::from("b")]);
}

#[test]
fn add_edge_rejects_missing_from_node() {
    let mut dag = Dag::new();
    dag.add_node(make_node("b", "task_b")).unwrap();
    let err = dag
        .add_edge(TaskId::from("missing"), TaskId::from("b"))
        .unwrap_err();
    assert!(matches!(err, crate::common::ActantError::Workflow(_)));
}

#[test]
fn add_edge_rejects_missing_to_node() {
    let mut dag = Dag::new();
    dag.add_node(make_node("a", "task_a")).unwrap();
    let err = dag
        .add_edge(TaskId::from("a"), TaskId::from("missing"))
        .unwrap_err();
    assert!(matches!(err, crate::common::ActantError::Workflow(_)));
}

#[test]
fn add_edge_rejects_self_loop() {
    let mut dag = Dag::new();
    dag.add_node(make_node("a", "task_a")).unwrap();
    let err = dag
        .add_edge(TaskId::from("a"), TaskId::from("a"))
        .unwrap_err();
    assert!(matches!(err, crate::common::ActantError::Workflow(_)));
}

#[test]
fn add_edge_rejects_cycle() {
    let mut dag = Dag::new();
    dag.add_node(make_node("a", "task_a")).unwrap();
    dag.add_node(make_node("b", "task_b")).unwrap();
    dag.add_edge(TaskId::from("a"), TaskId::from("b")).unwrap();
    let err = dag
        .add_edge(TaskId::from("b"), TaskId::from("a"))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::common::ActantError::Workflow(ref m) if m.contains("cycle")
    ));
}

#[test]
fn add_conditional_edge_stores_condition() {
    let mut dag = Dag::new();
    dag.add_node(make_node("a", "task_a")).unwrap();
    dag.add_node(make_node("b", "task_b")).unwrap();
    dag.add_conditional_edge(TaskId::from("a"), TaskId::from("b"), "cond_x".into())
        .unwrap();
    let conds = dag.conditional_edges_from(&TaskId::from("a"));
    assert_eq!(conds.len(), 1);
    assert_eq!(conds[0].0, TaskId::from("b"));
    assert_eq!(conds[0].1, "cond_x");
}

#[test]
fn add_conditional_edge_rejects_missing_node() {
    let mut dag = Dag::new();
    dag.add_node(make_node("a", "task_a")).unwrap();
    let err = dag
        .add_conditional_edge(TaskId::from("a"), TaskId::from("missing"), "cond".into())
        .unwrap_err();
    assert!(matches!(err, crate::common::ActantError::Workflow(_)));
}

#[test]
fn add_conditional_edge_rejects_self_loop() {
    let mut dag = Dag::new();
    dag.add_node(make_node("a", "task_a")).unwrap();
    let err = dag
        .add_conditional_edge(TaskId::from("a"), TaskId::from("a"), "cond".into())
        .unwrap_err();
    assert!(
        matches!(err, crate::common::ActantError::Workflow(ref m) if m.contains("cycle")),
        "self-loop conditional edge should be rejected as a cycle, got {:?}",
        err
    );
}

#[test]
fn add_conditional_edge_detects_cycle() {
    let mut dag = Dag::new();
    dag.add_node(make_node("a", "task_a")).unwrap();
    dag.add_node(make_node("b", "task_b")).unwrap();
    dag.add_conditional_edge(TaskId::from("a"), TaskId::from("b"), "cond".into())
        .unwrap();
    let err = dag
        .add_edge(TaskId::from("b"), TaskId::from("a"))
        .unwrap_err();
    assert!(
        matches!(err, crate::common::ActantError::Workflow(ref m) if m.contains("cycle")),
        "reverse edge forming a cycle should be rejected, got {:?}",
        err
    );
}

#[test]
fn add_conditional_edge_rejects_cycle_against_existing_path() {
    let mut dag = Dag::new();
    dag.add_node(make_node("a", "task_a")).unwrap();
    dag.add_node(make_node("b", "task_b")).unwrap();
    dag.add_edge(TaskId::from("a"), TaskId::from("b")).unwrap();
    let err = dag
        .add_conditional_edge(TaskId::from("b"), TaskId::from("a"), "cond".into())
        .unwrap_err();
    assert!(
        matches!(err, crate::common::ActantError::Workflow(ref m) if m.contains("cycle")),
        "conditional edge forming a cycle should be rejected, got {:?}",
        err
    );
}

#[test]
fn roots_and_sinks_identify_terminal_nodes() {
    let mut dag = Dag::new();
    dag.add_node(make_node("a", "task_a")).unwrap();
    dag.add_node(make_node("b", "task_b")).unwrap();
    dag.add_node(make_node("c", "task_c")).unwrap();
    dag.add_edge(TaskId::from("a"), TaskId::from("b")).unwrap();
    dag.add_edge(TaskId::from("b"), TaskId::from("c")).unwrap();
    dag.add_edge(TaskId::from("a"), TaskId::from("c")).unwrap();

    let root_ids: Vec<&str> = dag.roots().iter().map(|n| n.task_id.as_str()).collect();
    let sink_ids: Vec<&str> = dag.sinks().iter().map(|n| n.task_id.as_str()).collect();
    assert_eq!(root_ids, vec!["a"]);
    assert_eq!(sink_ids, vec!["c"]);
}

#[test]
fn topological_sort_orders_correctly() {
    let dag = make_linear_dag();
    let sorted = dag.topological_sort().unwrap();
    let ids: Vec<&str> = sorted.iter().map(|n| n.task_id.as_str()).collect();
    assert_eq!(ids, vec!["a", "b", "c"]);
}

#[test]
fn topological_sort_detects_cycle() {
    let mut dag = Dag::new();
    dag.add_node(make_node("a", "task_a")).unwrap();
    dag.add_node(make_node("b", "task_b")).unwrap();
    dag.add_edge(TaskId::from("a"), TaskId::from("b")).unwrap();
    let sorted = dag.topological_sort().unwrap();
    assert_eq!(sorted.len(), 2);
}

#[test]
fn topological_sort_empty_dag() {
    let dag = Dag::new();
    let sorted = dag.topological_sort().unwrap();
    assert!(sorted.is_empty());
}

#[test]
fn predecessors_of_returns_node_references() {
    let dag = make_linear_dag();
    let preds = dag.predecessors_of(&TaskId::from("c"));
    assert_eq!(preds.len(), 1);
    assert_eq!(preds[0].task_id.as_str(), "b");
}

#[test]
fn successors_of_returns_node_references() {
    let dag = make_linear_dag();
    let succs = dag.successors_of(&TaskId::from("a"));
    assert_eq!(succs.len(), 1);
    assert_eq!(succs[0].task_id.as_str(), "b");
}

#[test]
fn effective_retry_policy_prefers_node_over_dag_default() {
    let mut dag = Dag::new();
    let mut node = make_node("a", "task_a");
    node.retry_policy = Some(RetryPolicy {
        max_retries: 5,
        delay_ms: 200,
        backoff_multiplier: 3.0,
        max_delay_ms: 10000,
    });
    dag.add_node(node).unwrap();
    dag.default_retry_policy = Some(RetryPolicy::default());

    let policy = dag.effective_retry_policy(&TaskId::from("a")).unwrap();
    assert_eq!(policy.max_retries, 5);
    assert_eq!(policy.delay_ms, 200);
}

#[test]
fn effective_retry_policy_falls_back_to_dag_default() {
    let mut dag = Dag::new();
    dag.add_node(make_node("a", "task_a")).unwrap();
    dag.default_retry_policy = Some(RetryPolicy {
        max_retries: 7,
        delay_ms: 500,
        backoff_multiplier: 2.0,
        max_delay_ms: 30000,
    });

    let policy = dag.effective_retry_policy(&TaskId::from("a")).unwrap();
    assert_eq!(policy.max_retries, 7);
}

#[test]
fn effective_retry_policy_returns_none_when_no_policy() {
    let mut dag = Dag::new();
    dag.add_node(make_node("a", "task_a")).unwrap();
    assert!(dag.effective_retry_policy(&TaskId::from("a")).is_none());
}

#[test]
fn effective_retry_policy_returns_none_for_missing_node() {
    let dag = Dag::new();
    assert!(dag
        .effective_retry_policy(&TaskId::from("missing"))
        .is_none());
}

#[test]
fn nodes_iterator_visits_all_nodes() {
    let dag = make_linear_dag();
    let count = dag.nodes().count();
    assert_eq!(count, 3);
}
