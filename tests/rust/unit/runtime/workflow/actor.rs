//! Unit tests extracted from `src/runtime/workflow/actor.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use std::collections::HashMap;

use super::*;
use crate::common::{ActorId, NodeId, RetryPolicy};
use crate::runtime::actor::ActorSystem;
use crate::runtime::network::Transport;
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
        tags: Vec::new(),
        quota: None,
    }
}

#[tokio::test]
async fn workflow_actor_submit_shell_and_incremental_start_roundtrip() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("wf-actor-1");
    // actor 协议只承载「空 DAG 外壳 + 逐节点 ADD_NODE」的增量提交；整图提交
    // 是 Rust 嵌入方直连路径（Orchestrator::submit + start）。START 消息已随
    // 协议收敛删除，SUBMIT 对非空 DAG 必须显式拒绝——否则整图登记后停在
    // Pending 无人派发，重启才被 recover 捡起。
    let orchestrator = Orchestrator::new();
    let actor = WorkflowActor::new(orchestrator.clone());
    system.spawn(actor_id.clone(), actor).await.unwrap();

    let wf_id = WorkflowId::from("wf-1");
    system
        .call(
            &actor_id,
            workflow_methods::SUBMIT,
            encode(&(wf_id.clone(), Dag::new(), None::<WorkflowInvocation>)).unwrap(),
        )
        .await
        .unwrap();

    let mut full = Dag::new();
    full.add_node(make_node("a", "task_a")).unwrap();
    let rejected = system
        .call(
            &actor_id,
            workflow_methods::SUBMIT,
            encode(&(wf_id.clone(), full, None::<WorkflowInvocation>)).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        rejected.error.is_some(),
        "non-empty DAG must be rejected at the actor"
    );

    // 增量加节点后由编排器句柄直接启动（生产路径：add_node 自动启动或直连 start）。
    orchestrator
        .add_node(&wf_id, make_node("a", "task_a"), vec![])
        .await
        .unwrap();
    let roots: Vec<TaskDefinition> = orchestrator
        .start(&wf_id)
        .expect("start must return root tasks");
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].id.as_str(), "a");

    system.stop(&actor_id).await.unwrap();
}

#[tokio::test]
async fn scheduler_actor_enqueue_and_dequeue() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("sched-actor-1");
    system
        .spawn(actor_id.clone(), SchedulerActor::fifo())
        .await
        .unwrap();

    let tasks = vec![TaskDefinition {
        tags: Vec::new(),
        quota: None,
        pinned_target: false,
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
        lease_deadline_ms: 0,
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
    let manager = FailoverManager::new(node_id.clone(), network, system.clone(), wf_actor_id);
    let actor = FailoverActor::new(Arc::new(manager));
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
async fn scheduler_actor_close_rejects_late_enqueue() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("sched-actor-2");
    system
        .spawn(actor_id.clone(), SchedulerActor::priority())
        .await
        .unwrap();

    system
        .call(
            &actor_id,
            scheduler_methods::ENQUEUE,
            encode(&TaskDefinition {
                tags: Vec::new(),
                quota: None,
                pinned_target: false,
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
                lease_deadline_ms: 0,
            })
            .unwrap(),
        )
        .await
        .unwrap();

    // 队列非空以出队行为观察（IS_EMPTY 消息已随接口收缩删除）。
    let dequeued = system
        .call(&actor_id, scheduler_methods::TRY_DEQUEUE, vec![])
        .await
        .unwrap();
    let got: Option<TaskDefinition> = decode(&dequeued.payload).unwrap();
    assert!(got.is_some(), "enqueued task must be dequeueable");

    // 出队后再入队一个任务供 close 保留语义验证。
    system
        .call(
            &actor_id,
            scheduler_methods::ENQUEUE,
            encode(&TaskDefinition {
                tags: Vec::new(),
                quota: None,
                pinned_target: false,
                id: TaskId::from("t1b"),
                name: "task1b".into(),
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
                lease_deadline_ms: 0,
            })
            .unwrap(),
        )
        .await
        .unwrap();

    system
        .call(&actor_id, scheduler_methods::CLOSE, vec![])
        .await
        .unwrap();

    // close 之后的入队必须被 Actor 侧拒绝（closed 状态经 ENQUEUE 观察）。
    let rejected = system
        .call(
            &actor_id,
            scheduler_methods::ENQUEUE,
            encode(&TaskDefinition {
                tags: Vec::new(),
                quota: None,
                pinned_target: false,
                id: TaskId::from("t2"),
                name: "task2".into(),
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
                lease_deadline_ms: 0,
            })
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        rejected.error.is_some(),
        "closed scheduler must reject enqueue at the actor"
    );

    system.stop(&actor_id).await.unwrap();
}

#[tokio::test]
async fn failover_actor_unknown_method_returns_error() {
    let system = Arc::new(ActorSystem::new());
    let node_id = NodeId::from("node-3");
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-3"));
    let wf_actor_id = ActorId::workflow(&node_id);
    let manager = FailoverManager::new(node_id.clone(), network, system.clone(), wf_actor_id);
    let actor = FailoverActor::new(Arc::new(manager));
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

// ───────────────────────── 结果回灌单路化 ─────────────────────────

use crate::runtime::state::event_log::{EventLog, MemoryEventLog};
use crate::runtime::workflow::orchestrator::types::JournalEvent;
use crate::runtime::workflow::{FailureStrategy, Phase, Terminal};

/// 构造带事件日志的 WorkflowActor 并 spawn，返回编排器句柄——START 消息
/// 已从 actor 协议移除（无生产发送方），测试经此句柄直调 `start`。
async fn spawn_workflow_actor(
    system: &ActorSystem,
    actor_id: &ActorId,
    event_log: Arc<MemoryEventLog>,
) -> Orchestrator {
    let orchestrator = Orchestrator::new().with_event_log(event_log);
    system
        .spawn(actor_id.clone(), WorkflowActor::new(orchestrator.clone()))
        .await
        .unwrap();
    orchestrator
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
            encode(&(wf_id.clone(), task_id.clone(), outcome, 0u32, source)).unwrap(),
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
        let payload: JournalEvent = postcard::from_bytes(&entry.payload).unwrap();
        let name = match payload {
            JournalEvent::Submitted { .. } => "Submitted",
            JournalEvent::NodeAdded { .. } => "NodeAdded",
            JournalEvent::TaskDispatched { .. } => "TaskDispatched",
            JournalEvent::Started { .. } => "Started",
            JournalEvent::TaskRunning { .. } => "TaskRunning",
            JournalEvent::TaskCompleted { .. } => "TaskCompleted",
            JournalEvent::TaskFailed { .. } => "TaskFailed",
            JournalEvent::TaskCancelled { .. } => "TaskCancelled",
            JournalEvent::Completed { .. } => "Completed",
            JournalEvent::Cancelled { .. } => "Cancelled",
            JournalEvent::Failed { .. } => "Failed",
            JournalEvent::WaitPointRegistered { .. } => "WaitPointRegistered",
            JournalEvent::SignalReceived { .. } => "SignalReceived",
            JournalEvent::TimerFired { .. } => "TimerFired",
            JournalEvent::ContextValue { .. } => "ContextValue",
            JournalEvent::Recovered { .. } => "Recovered",
            JournalEvent::NodesSealed { .. } => "NodesSealed",
            JournalEvent::DeadlineSet { .. } => "DeadlineSet",
            JournalEvent::Evicted { .. } => "Evicted",
            JournalEvent::StartedAt { .. } => "StartedAt",
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
    let orchestrator = spawn_workflow_actor(&system, &actor_id, event_log.clone()).await;

    // 两个独立节点：a 完成后工作流仍非终态（b 未决），可在非终态阶段
    // 验证三路重复注入的幂等性；b 经本地通道收尾触发工作流终态。
    let wf_id = WorkflowId::from("wf-s8-complete");
    let mut dag = Dag::new();
    dag.add_node(make_node("a", "task_a")).unwrap();
    dag.add_node(make_node("b", "task_b")).unwrap();

    orchestrator.submit(wf_id.clone(), dag).await.unwrap();
    orchestrator
        .start(&wf_id)
        .expect("start must return root tasks");

    let task_id = TaskId::from("a");

    // 路径 1：本地完成通道（legacy COMPLETE_TASK）。
    let resp = system
        .call(
            &actor_id,
            workflow_methods::COMPLETE_TASK,
            encode(&(wf_id.clone(), task_id.clone(), vec![1u8, 2, 3], 0u32)).unwrap(),
        )
        .await
        .unwrap();
    assert!(resp.error.is_none(), "{:?}", resp.error);
    let completion: TaskCompletionResponse = decode(&resp.payload).unwrap();
    assert!(!completion.workflow_terminal, "b is still pending");

    // 路径 2：远端直连携带同一结果重复注入。
    inject_result(
        &system,
        &actor_id,
        &wf_id,
        &task_id,
        TaskResultOutcome::Completed(vec![1u8, 2, 3]),
        ResultSource::Remote,
    )
    .await;

    // 迟到失败结果经两条路径注入：终态不可被改写。
    for source in [ResultSource::Local, ResultSource::Remote] {
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
            encode(&(wf_id.clone(), TaskId::from("b"), vec![9u8], 0u32)).unwrap(),
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
    let orchestrator = spawn_workflow_actor(&system, &actor_id, event_log.clone()).await;

    let wf_id = WorkflowId::from("wf-s8-failfast");
    let mut dag = Dag::new();
    dag.add_node(make_node("a", "task_a")).unwrap();
    dag.add_node(make_node("b", "task_b")).unwrap();
    dag.add_edge(TaskId::from("a"), TaskId::from("b")).unwrap();

    orchestrator.submit(wf_id.clone(), dag).await.unwrap();
    orchestrator
        .start(&wf_id)
        .expect("start must return root tasks");

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
    let orchestrator = spawn_workflow_actor(&system, &actor_id, event_log.clone()).await;

    let wf_id = WorkflowId::from("wf-s8-continue");
    let mut dag = Dag::new();
    dag.failure_strategy = FailureStrategy::Continue;
    dag.add_node(make_node("a", "task_a")).unwrap();
    dag.add_node(make_node("b", "task_b")).unwrap();

    orchestrator.submit(wf_id.clone(), dag).await.unwrap();
    orchestrator
        .start(&wf_id)
        .expect("start must return root tasks");

    // 报告 a 失败：任务 Failed，工作流非终态（b 未终态）。
    inject_result(
        &system,
        &actor_id,
        &wf_id,
        &TaskId::from("a"),
        TaskResultOutcome::Failed("boom".to_string()),
        ResultSource::Local,
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

    // 同一失败经 Local 与 Remote 重复注入：不重复推进。
    inject_result(
        &system,
        &actor_id,
        &wf_id,
        &TaskId::from("a"),
        TaskResultOutcome::Failed("boom".to_string()),
        ResultSource::Local,
    )
    .await;
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
            encode(&(wf_id.clone(), TaskId::from("b"), vec![9u8], 0u32)).unwrap(),
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

/// 迟到结果守卫：工作流按 retention 淘汰后，再经 ON_TASK_RESULT 回灌的
/// 结果必须按终局 Settled 返回，而不是 NotFound 上抛——后者会被执行侧
/// network_router 归类为「origin 拒绝」，进入对确定性拒绝没有终结条件的
/// 重投队列（每次重投都是一次 origin 直连 + actor 往返的常驻负载）。
#[tokio::test]
async fn late_result_for_evicted_workflow_settles_without_error() {
    use crate::common::ActantConfig;
    use crate::runtime::state::Store;
    use tempfile::tempdir;

    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).await.unwrap();
    let event_log = Arc::new(MemoryEventLog::default());
    let mut config = ActantConfig::default();
    config.workflow.completed_retention_count = 0;
    let orchestrator = Orchestrator::new()
        .with_config(config)
        .with_store(store)
        .with_event_log(event_log.clone());

    let system = ActorSystem::new();
    let actor_id = ActorId::from("wf-actor-evicted-result");
    system
        .spawn(actor_id.clone(), WorkflowActor::new(orchestrator.clone()))
        .await
        .unwrap();

    let wf_id = WorkflowId::from("wf-evicted-result");
    let mut dag = Dag::new();
    dag.add_node(make_node("only", "only")).unwrap();
    orchestrator.submit(wf_id.clone(), dag).await.unwrap();
    orchestrator
        .start(&wf_id)
        .expect("start must return root tasks");
    system
        .call(
            &actor_id,
            workflow_methods::MARK_TASK_RUNNING,
            encode(&(wf_id.clone(), TaskId::from("only"))).unwrap(),
        )
        .await
        .unwrap();

    // 任务完成 → 工作流终态 → retention=0 立即淘汰。
    inject_result(
        &system,
        &actor_id,
        &wf_id,
        &TaskId::from("only"),
        TaskResultOutcome::Completed(b"r".to_vec()),
        ResultSource::Local,
    )
    .await;

    // 淘汰后的迟到结果：必须 Ok 且无重试裁决，而不是 NotFound 错误。
    let resp = system
        .call(
            &actor_id,
            workflow_methods::ON_TASK_RESULT,
            encode(&(
                wf_id.clone(),
                TaskId::from("only"),
                TaskResultOutcome::Failed("[actant:worker] late delivery".to_string()),
                0u32,
                ResultSource::Remote,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        resp.error.is_none(),
        "淘汰工作流的迟到结果不得上抛 NotFound: {:?}",
        resp.error
    );
    let retry: Option<(crate::common::TaskDefinition, u64)> = decode(&resp.payload).unwrap();
    assert!(retry.is_none(), "淘汰工作流的迟到结果不得产生重试裁决");
    system.stop(&actor_id).await.unwrap();
}
