//! Unit tests extracted from `src/runtime/workflow/actor.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use std::collections::HashMap;

use super::*;
use crate::common::{ActorId, RetryPolicy};
use crate::runtime::actor::ActorSystem;
use crate::runtime::workflow::DagNode;
use crate::test_support::MockTransport;

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

#[tokio::test]
async fn workflow_actor_submit_and_start_roundtrip() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("wf-actor-1");
    let actor = WorkflowActor::new(Orchestrator::new());
    system.spawn(actor_id.clone(), actor).await.unwrap();

    let mut dag = Dag::new();
    dag.add_node(make_node("a", "task_a")).unwrap();
    dag.add_node(make_node("b", "task_b")).unwrap();
    dag.add_edge(TaskId::from("a"), TaskId::from("b")).unwrap();

    let wf_id = WorkflowId::from("wf-1");
    system
        .call(
            &actor_id,
            workflow_methods::SUBMIT,
            encode(&(wf_id.clone(), dag)).unwrap(),
        )
        .await
        .unwrap();

    let result = system
        .call(&actor_id, workflow_methods::START, encode(&wf_id).unwrap())
        .await
        .unwrap();
    assert!(result.error.is_none(), "{:?}", result.error);
    let roots: Vec<TaskDefinition> = decode(&result.payload).unwrap();
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].id.as_str(), "a");

    system.stop(&actor_id).await.unwrap();
}

#[tokio::test]
async fn scheduler_actor_enqueue_and_dequeue() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("sched-actor-1");
    system
        .spawn(actor_id.clone(), fifo_scheduler_actor())
        .await
        .unwrap();

    let tasks = vec![TaskDefinition {
        id: TaskId::from("t1"),
        name: "task1".into(),
        payload: vec![1, 2, 3],
        workflow_id: None,
        target_node: None,
        origin_node: None,
        retry_policy: Some(RetryPolicy::default()),
        priority: 0,
        timeout_ms: None,
        attempt: 0,
        enqueued_at_ms: 0,
        target_endpoint_addr: None,
        origin_endpoint_addr: None,
    }];

    system
        .call(
            &actor_id,
            scheduler_methods::ENQUEUE_BATCH,
            encode(&tasks).unwrap(),
        )
        .await
        .unwrap();

    let result = system
        .call(
            &actor_id,
            scheduler_methods::DEQUEUE_BATCH,
            encode(&1usize).unwrap(),
        )
        .await
        .unwrap();
    assert!(result.error.is_none());
    let out: Vec<TaskDefinition> = decode(&result.payload).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].id.as_str(), "t1");

    system.stop(&actor_id).await.unwrap();
}

#[tokio::test]
async fn failover_actor_update_capacity_and_get_peers() {
    let system = Arc::new(ActorSystem::new());
    let node_id = NodeId::from("node-1");
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-1"));
    let wf_actor_id = ActorId::workflow(&node_id);
    let actor = failover_actor(node_id.clone(), network, system.clone(), wf_actor_id, None).await;
    let actor_id = ActorId::failover(&node_id);
    system.spawn(actor_id.clone(), actor).await.unwrap();

    system
        .call(
            &actor_id,
            failover_methods::UPDATE_LOCAL_CAPACITY,
            encode(&(2u32, 8u32)).unwrap(),
        )
        .await
        .unwrap();

    let result = system
        .call(&actor_id, failover_methods::GET_PEER_INFOS, vec![])
        .await
        .unwrap();
    assert!(result.error.is_none());
    // 返回类型是 HashMap，可能为空（本地节点不把自己加入 peers）。
    let peers: std::collections::HashMap<NodeId, crate::runtime::workflow::failover::PeerInfo> =
        decode(&result.payload).unwrap();
    assert!(peers.is_empty() || peers.contains_key(&node_id));

    system.stop(&actor_id).await.unwrap();
}

#[tokio::test]
async fn scheduler_actor_close_and_is_empty() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("sched-actor-2");
    system
        .spawn(actor_id.clone(), priority_scheduler_actor())
        .await
        .unwrap();

    system
        .call(
            &actor_id,
            scheduler_methods::ENQUEUE,
            encode(&TaskDefinition {
                id: TaskId::from("t1"),
                name: "task1".into(),
                payload: Vec::new(),
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
            })
            .unwrap(),
        )
        .await
        .unwrap();

    let result = system
        .call(&actor_id, scheduler_methods::IS_EMPTY, vec![])
        .await
        .unwrap();
    assert!(!decode::<bool>(&result.payload).unwrap());

    system
        .call(&actor_id, scheduler_methods::CLOSE, vec![])
        .await
        .unwrap();

    let result = system
        .call(&actor_id, scheduler_methods::IS_CLOSED, vec![])
        .await
        .unwrap();
    assert!(decode::<bool>(&result.payload).unwrap());

    system.stop(&actor_id).await.unwrap();
}

#[tokio::test]
async fn failover_actor_unknown_method_returns_error() {
    let system = Arc::new(ActorSystem::new());
    let node_id = NodeId::from("node-3");
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-3"));
    let wf_actor_id = ActorId::workflow(&node_id);
    let actor = failover_actor(node_id.clone(), network, system.clone(), wf_actor_id, None).await;
    let actor_id = ActorId::failover(&node_id);
    system.spawn(actor_id.clone(), actor).await.unwrap();

    let result = system
        .call(&actor_id, "unknown_method", vec![])
        .await
        .unwrap();
    assert!(
        result.error.is_some(),
        "unknown method should set error field"
    );

    system.stop(&actor_id).await.unwrap();
}

// ───────────────────────── S8：结果回灌单路化 ─────────────────────────

use crate::runtime::state::event_log::{EventLog, MemoryEventLog};
use crate::runtime::workflow::orchestrator::types::WorkflowEventPayload;
use crate::runtime::workflow::{FailureStrategy, Phase, Terminal};

/// 构造带事件日志的 WorkflowActor 并 spawn。
async fn spawn_workflow_actor(
    system: &ActorSystem,
    actor_id: &ActorId,
    event_log: Arc<MemoryEventLog>,
) {
    let orchestrator = Orchestrator::new().with_event_log(event_log);
    system
        .spawn(actor_id.clone(), WorkflowActor::new(orchestrator))
        .await
        .unwrap();
}

/// 经三条路径注入同一任务结果（Local / Remote / Gossip）。
///
/// `ON_TASK_RESULT` 载荷：`(workflow_id, task_id, outcome, attempt, source)`。
async fn inject_result(
    system: &ActorSystem,
    actor_id: &ActorId,
    wf_id: &WorkflowId,
    task_id: &TaskId,
    outcome: TaskResultOutcome,
    source: ResultSource,
) {
    system
        .call(
            actor_id,
            workflow_methods::ON_TASK_RESULT,
            encode(&(wf_id.clone(), task_id.clone(), outcome, None::<u32>, source)).unwrap(),
        )
        .await
        .unwrap()
        .error
        .map(|e| panic!("inject_result failed: {e}"))
        .unwrap_or(());
}

/// 统计事件日志 topic 中各事件的次数。
fn count_events(event_log: &MemoryEventLog, wf_id: &WorkflowId) -> HashMap<String, usize> {
    let topic = format!("workflow:{}", wf_id.as_str());
    let mut counts: HashMap<String, usize> = HashMap::new();
    for entry in event_log.read_after(&topic, None).unwrap() {
        let payload: WorkflowEventPayload = postcard::from_bytes(&entry.payload).unwrap();
        let name = match payload {
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
        };
        *counts.entry(name.to_string()).or_default() += 1;
    }
    counts
}

/// 同一任务完成结果经三条路径（本地通道 / 远端直连 / gossip）重复注入：
/// 状态只推进一次，历史事件不重复，迟到的失败结果不改写终态（幂等）。
#[tokio::test]
async fn task_result_three_paths_idempotent() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("wf-actor-s8-complete");
    let event_log = Arc::new(MemoryEventLog::default());
    spawn_workflow_actor(&system, &actor_id, event_log.clone()).await;

    // 两个独立节点：a 完成后工作流仍非终态（b 未决），可在非终态阶段
    // 验证三路重复注入的幂等性；b 经本地通道收尾触发工作流终态。
    let wf_id = WorkflowId::from("wf-s8-complete");
    let mut dag = Dag::new();
    dag.add_node(make_node("a", "task_a")).unwrap();
    dag.add_node(make_node("b", "task_b")).unwrap();

    system
        .call(
            &actor_id,
            workflow_methods::SUBMIT,
            encode(&(wf_id.clone(), dag)).unwrap(),
        )
        .await
        .unwrap();
    system
        .call(
            &actor_id,
            workflow_methods::START,
            encode(&wf_id.clone()).unwrap(),
        )
        .await
        .unwrap();

    let task_id = TaskId::from("a");

    // 路径 1：本地完成通道（legacy COMPLETE_TASK）。
    let resp = system
        .call(
            &actor_id,
            workflow_methods::COMPLETE_TASK,
            encode(&(wf_id.clone(), task_id.clone(), vec![1u8, 2, 3])).unwrap(),
        )
        .await
        .unwrap();
    assert!(resp.error.is_none(), "{:?}", resp.error);
    let completion: TaskCompletionResponse = decode(&resp.payload).unwrap();
    assert!(!completion.workflow_terminal, "b is still pending");

    // 路径 2 / 3：远端直连与 gossip 携带同一结果重复注入。
    for source in [ResultSource::Remote, ResultSource::Gossip] {
        inject_result(
            &system,
            &actor_id,
            &wf_id,
            &task_id,
            TaskResultOutcome::Completed(vec![1u8, 2, 3]),
            source,
        )
        .await;
    }

    // 迟到失败结果经三条路径注入：终态不可被改写。
    for source in [
        ResultSource::Local,
        ResultSource::Remote,
        ResultSource::Gossip,
    ] {
        if source == ResultSource::Local {
            system
                .call(
                    &actor_id,
                    workflow_methods::FAIL_TASK,
                    encode(&(
                        wf_id.clone(),
                        task_id.clone(),
                        "late failure".to_string(),
                        crate::runtime::workflow::FailureScope::WorkflowLevel,
                    ))
                    .unwrap(),
                )
                .await
                .unwrap();
        } else {
            inject_result(
                &system,
                &actor_id,
                &wf_id,
                &task_id,
                TaskResultOutcome::Failed("late failure".to_string()),
                source,
            )
            .await;
        }
    }

    // 状态：a Completed（结果未被改写）、工作流非终态（b 未决）。
    let state = system
        .call(
            &actor_id,
            workflow_methods::GET_STATE,
            encode(&wf_id).unwrap(),
        )
        .await
        .unwrap();
    let exec: Option<crate::runtime::workflow::WorkflowExecution> = decode(&state.payload).unwrap();
    let exec = exec.expect("workflow state should exist");
    assert!(!exec.is_terminal());
    assert_eq!(exec.tasks[&task_id].state, Phase::Completed);
    assert_eq!(exec.tasks[&task_id].result, Some(vec![1u8, 2, 3]));

    // 历史：完成事件恰好一次，无失败事件（终态守卫拒绝不追加历史）。
    let counts = count_events(&event_log, &wf_id);
    assert_eq!(counts.get("TaskCompleted"), Some(&1), "{counts:?}");
    assert!(!counts.contains_key("TaskFailed"), "{counts:?}");
    assert!(!counts.contains_key("Failed"), "{counts:?}");

    // b 经本地通道完成：工作流收尾。
    system
        .call(
            &actor_id,
            workflow_methods::COMPLETE_TASK,
            encode(&(wf_id.clone(), TaskId::from("b"), vec![9u8])).unwrap(),
        )
        .await
        .unwrap();

    let state = system
        .call(
            &actor_id,
            workflow_methods::GET_STATE,
            encode(&wf_id).unwrap(),
        )
        .await
        .unwrap();
    let exec: Option<crate::runtime::workflow::WorkflowExecution> = decode(&state.payload).unwrap();
    let exec = exec.expect("workflow state should exist");
    assert_eq!(exec.state, Phase::Completed);

    let counts = count_events(&event_log, &wf_id);
    assert_eq!(counts.get("Completed"), Some(&1), "{counts:?}");

    system.stop(&actor_id).await.unwrap();
}

/// 失败语义（WorkflowLevel 统一）：远端直连的失败结果在 fail-fast
/// 工作流中立即触发工作流 Failed——与本地通道及 gossip 路径行为一致
/// （TaskOnly 会使 fail-fast 工作流悬挂在非终态）。
#[tokio::test]
async fn remote_failure_fail_fast_fails_workflow() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("wf-actor-s8-failfast");
    let event_log = Arc::new(MemoryEventLog::default());
    spawn_workflow_actor(&system, &actor_id, event_log.clone()).await;

    let wf_id = WorkflowId::from("wf-s8-failfast");
    let mut dag = Dag::new();
    dag.add_node(make_node("a", "task_a")).unwrap();
    dag.add_node(make_node("b", "task_b")).unwrap();
    dag.add_edge(TaskId::from("a"), TaskId::from("b")).unwrap();

    system
        .call(
            &actor_id,
            workflow_methods::SUBMIT,
            encode(&(wf_id.clone(), dag)).unwrap(),
        )
        .await
        .unwrap();
    system
        .call(
            &actor_id,
            workflow_methods::START,
            encode(&wf_id.clone()).unwrap(),
        )
        .await
        .unwrap();

    // 远端直连路径报告 a 失败（fail-fast 默认策略）。
    inject_result(
        &system,
        &actor_id,
        &wf_id,
        &TaskId::from("a"),
        TaskResultOutcome::Failed("boom".to_string()),
        ResultSource::Remote,
    )
    .await;

    let state = system
        .call(
            &actor_id,
            workflow_methods::GET_STATE,
            encode(&wf_id).unwrap(),
        )
        .await
        .unwrap();
    let exec: Option<crate::runtime::workflow::WorkflowExecution> = decode(&state.payload).unwrap();
    let exec = exec.expect("workflow state should exist");
    assert_eq!(
        exec.state,
        Phase::Failed,
        "fail-fast must fail the workflow"
    );
    assert_eq!(exec.tasks[&TaskId::from("a")].state, Phase::Failed);

    let counts = count_events(&event_log, &wf_id);
    assert_eq!(counts.get("TaskFailed"), Some(&1), "{counts:?}");
    assert_eq!(counts.get("Failed"), Some(&1), "{counts:?}");

    system.stop(&actor_id).await.unwrap();
}

/// continue 策略下，gossip 失败结果仅标记任务失败、工作流保持非终态；
/// 同一失败经其余路径重复注入不产生重复事件；其余任务完成后工作流
/// 因存在失败任务进入 Failed。
#[tokio::test]
async fn gossip_failure_continue_strategy_consistent_across_paths() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("wf-actor-s8-continue");
    let event_log = Arc::new(MemoryEventLog::default());
    spawn_workflow_actor(&system, &actor_id, event_log.clone()).await;

    let wf_id = WorkflowId::from("wf-s8-continue");
    let mut dag = Dag::new();
    dag.failure_strategy = FailureStrategy::Continue;
    dag.add_node(make_node("a", "task_a")).unwrap();
    dag.add_node(make_node("b", "task_b")).unwrap();

    system
        .call(
            &actor_id,
            workflow_methods::SUBMIT,
            encode(&(wf_id.clone(), dag)).unwrap(),
        )
        .await
        .unwrap();
    system
        .call(
            &actor_id,
            workflow_methods::START,
            encode(&wf_id.clone()).unwrap(),
        )
        .await
        .unwrap();

    // gossip 路径报告 a 失败：任务 Failed，工作流非终态（b 未终态）。
    inject_result(
        &system,
        &actor_id,
        &wf_id,
        &TaskId::from("a"),
        TaskResultOutcome::Failed("boom".to_string()),
        ResultSource::Gossip,
    )
    .await;

    let state = system
        .call(
            &actor_id,
            workflow_methods::GET_STATE,
            encode(&wf_id).unwrap(),
        )
        .await
        .unwrap();
    let exec: Option<crate::runtime::workflow::WorkflowExecution> = decode(&state.payload).unwrap();
    let exec = exec.expect("workflow state should exist");
    assert_eq!(exec.tasks[&TaskId::from("a")].state, Phase::Failed);
    assert!(!exec.is_terminal(), "continue strategy must not fail yet");

    // 同一失败经 Local（legacy FAIL_TASK）与 Remote 重复注入：不重复推进。
    system
        .call(
            &actor_id,
            workflow_methods::FAIL_TASK,
            encode(&(
                wf_id.clone(),
                TaskId::from("a"),
                "boom".to_string(),
                crate::runtime::workflow::FailureScope::TaskOnly,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    inject_result(
        &system,
        &actor_id,
        &wf_id,
        &TaskId::from("a"),
        TaskResultOutcome::Failed("boom".to_string()),
        ResultSource::Remote,
    )
    .await;

    // b 完成：全部任务终态且存在失败 → 工作流 Failed。
    system
        .call(
            &actor_id,
            workflow_methods::COMPLETE_TASK,
            encode(&(wf_id.clone(), TaskId::from("b"), vec![9u8])).unwrap(),
        )
        .await
        .unwrap();

    let state = system
        .call(
            &actor_id,
            workflow_methods::GET_STATE,
            encode(&wf_id).unwrap(),
        )
        .await
        .unwrap();
    let exec: Option<crate::runtime::workflow::WorkflowExecution> = decode(&state.payload).unwrap();
    let exec = exec.expect("workflow state should exist");
    assert_eq!(exec.state, Phase::Failed);
    assert_eq!(exec.tasks[&TaskId::from("a")].state, Phase::Failed);
    assert_eq!(exec.tasks[&TaskId::from("b")].state, Phase::Completed);

    let counts = count_events(&event_log, &wf_id);
    assert_eq!(counts.get("TaskFailed"), Some(&1), "{counts:?}");
    // b 的完成即工作流收尾：存在失败任务 → 终态为 Failed，终态路径只记
    // Failed（不记 TaskCompleted / Completed）。
    assert_eq!(counts.get("Failed"), Some(&1), "{counts:?}");

    system.stop(&actor_id).await.unwrap();
}
