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
    wf.mark_task_completed(&TaskId::from("a"), b"ok".to_vec(), None);
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
        None,
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
        None,
    );
    // 工作流未完成，因为 task b 仍在运行
    assert!(!wf.state.is_terminal());

    // task b 完成 → 工作流转 Failed
    wf.mark_task_completed(&TaskId::from("b"), b"ok".to_vec(), None);
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
    wf.fail_task(
        &TaskId::from("a"),
        "boom".into(),
        FailureScope::TaskOnly,
        None,
    );
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
        None,
    );
    wf.fail_task(
        &TaskId::from("a"),
        "second".into(),
        FailureScope::WorkflowLevel,
        None,
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
        None,
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
    wf.mark_task_completed(&TaskId::from("a"), b"ok".to_vec(), None);
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
    wf.mark_task_completed(&TaskId::from("c"), b"result-c".to_vec(), None);
    wf.mark_task_completed(&TaskId::from("a"), b"result-a".to_vec(), None);
    wf.mark_task_completed(&TaskId::from("b"), b"result-b".to_vec(), None);
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

// --- 终态守卫（can_transition_task）测试 ---

/// 取消后迟到的完成结果不得把 Cancelled 任务改写为 Completed，
/// 也不得把 Cancelled 工作流"复活"为 Completed。
#[test]
fn late_completion_after_cancel_does_not_revive_workflow() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string()), TaskId::from("b".to_string())],
    );
    wf.mark_running();
    wf.mark_task_running(&TaskId::from("a".to_string()));
    wf.mark_cancelled();
    assert_eq!(wf.state, Phase::Cancelled);

    // 迟到完成：被守卫拒绝，返回 false，状态不发生任何变化。
    let applied = wf.mark_task_completed(&TaskId::from("a".to_string()), b"r".to_vec(), None);
    assert!(!applied, "late completion must be rejected");
    assert_eq!(wf.state, Phase::Cancelled, "workflow must stay Cancelled");
    let task = wf.tasks.get(&TaskId::from("a".to_string())).unwrap();
    assert_eq!(task.state, Phase::Cancelled, "task must stay Cancelled");
    assert!(task.result.is_none(), "no result may be attached");
    assert_eq!(wf.succeeded_count(), 0);
}

/// 失败（FailFast 工作流级失败）后迟到的完成结果不得把 Failed 工作流
/// 复活为 Completed，也不得覆盖 Failed 任务状态。
#[test]
fn late_completion_after_failure_does_not_revive_workflow() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string()), TaskId::from("b".to_string())],
    );
    wf.mark_running();
    wf.mark_task_running(&TaskId::from("a".to_string()));
    wf.fail_task(
        &TaskId::from("a".to_string()),
        "boom".to_string(),
        FailureScope::WorkflowLevel,
        None,
    );
    assert_eq!(wf.state, Phase::Failed, "FailFast must fail the workflow");

    let applied = wf.mark_task_completed(&TaskId::from("a".to_string()), b"r".to_vec(), None);
    assert!(!applied, "late completion must be rejected");
    assert_eq!(wf.state, Phase::Failed, "workflow must stay Failed");
    let task = wf.tasks.get(&TaskId::from("a".to_string())).unwrap();
    assert_eq!(task.state, Phase::Failed);
    assert_eq!(task.error.as_deref(), Some("boom"));
}

/// 已 Completed 的任务重复完成仍被拒绝（幂等），且不重复累计 succeeded_count。
#[test]
fn duplicate_completion_is_idempotent_and_returns_false() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string())],
    );
    wf.mark_running();
    assert!(wf.mark_task_completed(&TaskId::from("a".to_string()), b"r1".to_vec(), None));
    assert!(wf.is_terminal());

    let applied = wf.mark_task_completed(&TaskId::from("a".to_string()), b"r2".to_vec(), None);
    assert!(!applied, "duplicate completion must be rejected");
    assert_eq!(
        wf.succeeded_count(),
        1,
        "succeeded_count must not double count"
    );
    let task = wf.tasks.get(&TaskId::from("a".to_string())).unwrap();
    assert_eq!(task.result.as_deref(), Some(b"r1".as_slice()));
}

/// can_transition_task 的判定口径：终态工作流一律拒绝；
/// 非终态工作流下，终态任务拒绝、Pending/Running 任务放行。
#[test]
fn can_transition_task_guards_workflow_and_task_terminal_states() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string()), TaskId::from("b".to_string())],
    );
    wf.mark_running();
    wf.mark_task_running(&TaskId::from("a".to_string()));

    assert!(wf.can_transition_task(&TaskId::from("a".to_string())));
    assert!(wf.can_transition_task(&TaskId::from("b".to_string())));

    // 任务终态 → 拒绝
    wf.mark_task_completed(&TaskId::from("b".to_string()), b"r".to_vec(), None);
    assert!(!wf.can_transition_task(&TaskId::from("b".to_string())));

    // 工作流终态 → 全部拒绝
    wf.cancel_task(&TaskId::from("a".to_string()));
    wf.mark_cancelled();
    assert!(wf.is_terminal());
    assert!(!wf.can_transition_task(&TaskId::from("a".to_string())));
    assert!(!wf.can_transition_task(&TaskId::from("b".to_string())));
}

// --- attempt fencing（结果接受侧派发代数校验）测试 ---

/// 过期派发代数（attempt 0）的迟到完成结果在代数推进（重派发 → attempt 1）后
/// 被丢弃：任务状态、结果与 succeeded_count 均不变；同代（attempt 1）结果正常接受。
#[test]
fn stale_attempt_result_is_dropped_and_current_attempt_accepted() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string())],
    );
    wf.mark_running();
    wf.mark_task_running(&TaskId::from("a".to_string()));
    // 故障转移重派发：attempt 0 → 1
    wf.reset_task(&TaskId::from("a".to_string()), false, true);
    assert_eq!(wf.tasks[&TaskId::from("a".to_string())].attempt(), 1);

    // 旧代在途执行的迟到结果 → 丢弃
    let applied =
        wf.mark_task_completed(&TaskId::from("a".to_string()), b"stale".to_vec(), Some(0));
    assert!(!applied, "stale-generation result must be dropped");
    let task = wf.tasks.get(&TaskId::from("a".to_string())).unwrap();
    assert_eq!(task.state, Phase::Pending, "task must stay Pending");
    assert!(task.result.is_none(), "no stale result may be attached");
    assert_eq!(wf.succeeded_count(), 0);

    // 当代结果 → 接受
    assert!(wf.mark_task_completed(&TaskId::from("a".to_string()), b"fresh".to_vec(), Some(1)));
    let task = wf.tasks.get(&TaskId::from("a".to_string())).unwrap();
    assert_eq!(task.state, Phase::Completed);
    assert_eq!(task.result.as_deref(), Some(b"fresh".as_slice()));
    assert_eq!(wf.succeeded_count(), 1);
}

/// 更新一代（attempt 大于当前记录）的结果不高于任何守卫拦截——fencing 只拒绝
/// 落后代数，超前代数（如调度侧已推进但本节点状态滞后）放行。
#[test]
fn newer_attempt_result_is_accepted() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string())],
    );
    wf.mark_running();
    assert!(wf.mark_task_completed(&TaskId::from("a".to_string()), b"r".to_vec(), Some(7)));
    assert_eq!(wf.state, Phase::Completed);
}

/// 结果通路未携带派发代数（`None`）时 fencing 放行——向后兼容：
/// 协议扩展前所有结果都走该路径，不得被误拒。
#[test]
fn unknown_attempt_result_is_accepted_for_backward_compat() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string())],
    );
    wf.mark_running();
    wf.reset_task(&TaskId::from("a".to_string()), false, true);
    assert!(wf.mark_task_completed(&TaskId::from("a".to_string()), b"r".to_vec(), None));
    assert_eq!(wf.state, Phase::Completed);
}

/// 过期派发代数的失败报告同样被 fencing 丢弃；同代失败报告正常生效。
#[test]
fn stale_attempt_failure_report_is_dropped() {
    let mut wf = WorkflowExecution::new(
        WorkflowId::from("wf-1".to_string()),
        vec![TaskId::from("a".to_string())],
    )
    .with_failure_strategy(FailureStrategy::Continue);
    wf.mark_running();
    wf.reset_task(&TaskId::from("a".to_string()), false, true);

    let applied_before = wf.tasks[&TaskId::from("a".to_string())].clone();
    wf.fail_task(
        &TaskId::from("a".to_string()),
        "stale boom".to_string(),
        FailureScope::TaskOnly,
        Some(0),
    );
    let task = wf.tasks.get(&TaskId::from("a".to_string())).unwrap();
    assert_eq!(
        task.state, applied_before.state,
        "stale failure must be dropped"
    );
    assert!(task.error.is_none(), "stale failure must not record error");

    wf.fail_task(
        &TaskId::from("a".to_string()),
        "fresh boom".to_string(),
        FailureScope::TaskOnly,
        Some(1),
    );
    let task = wf.tasks.get(&TaskId::from("a".to_string())).unwrap();
    assert_eq!(task.state, Phase::Failed);
    assert_eq!(task.error.as_deref(), Some("fresh boom"));
}
