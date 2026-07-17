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
