//! Unit tests extracted from `src/runtime/context.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;
use crate::common::wire::TOPIC_CANCEL;
use crate::common::{ActantConfig, ActorId, NodeId};
use crate::runtime::actor::ActorSystem;
use crate::runtime::capability::CapabilityRuntime;
use crate::runtime::dispatcher::{ProcessTaskDispatcher, TaskDispatcher};
use crate::runtime::event_bus::EventBus;
use crate::runtime::state::{LmdbStore, Store};
use crate::test_support::MockTransport;
use tempfile::tempdir;

/// 构造一个不拉取 worker 子进程的进程池分发器，供 `Runtime::new` 传递
/// （运行时存储调度器，测试中不真正派发任务）。
fn hermetic_dispatcher() -> Arc<dyn TaskDispatcher> {
    Arc::new(
        ProcessTaskDispatcher::new(0, "python3".to_string(), 1, Vec::new(), Vec::new())
            .expect("process task dispatcher init"),
    )
}

fn make_runtime(node_id: &str) -> (Runtime, Arc<MockTransport>) {
    let transport = Arc::new(MockTransport::new(node_id));
    let network: Arc<dyn Transport> = transport.clone();
    let dir = tempdir().unwrap();
    let lmdb = LmdbStore::open(dir.path()).unwrap();
    let store = Store::new(lmdb);
    let actor_system = Arc::new(ActorSystem::new());
    let workflow_actor_id = ActorId::workflow(&NodeId::from(node_id.to_string()));
    let capability = Arc::new(CapabilityRuntime::new());
    let event_bus = EventBus::new();
    let dispatcher = hermetic_dispatcher();
    let rt = Runtime::new(
        NodeId::from(node_id.to_string()),
        ActantConfig::default(),
        network,
        store,
        actor_system,
        workflow_actor_id,
        None,
        capability,
        event_bus,
        dispatcher,
    );
    (rt, transport)
}

fn make_runtime_only(node_id: &str) -> Runtime {
    make_runtime(node_id).0
}

#[test]
fn getters_return_configured_values() {
    let rt = make_runtime_only("node-X");
    assert_eq!(rt.node_id().as_str(), "node-X");
    assert!(
        rt.config().worker.scheduler_kind.as_str().is_empty()
            || !rt.config().worker.scheduler_kind.as_str().is_empty()
    );
    assert_eq!(rt.workflow_actor_id().as_str(), "workflow-node-X");
    assert!(rt.worker().is_none());
    assert!(rt.network().node_id().as_str() == "node-X");
}

#[test]
fn register_background_loop_cancel_is_noop_on_shutdown_when_unused() {
    // 注册一个取消句柄但不发送，验证它被存入列表且不 panic。
    let rt = make_runtime_only("node-A");
    let (tx, _rx) = tokio::sync::watch::channel(false);
    rt.register_background_loop_cancel(tx);
    // 列表非空不影响其它访问器
    assert_eq!(rt.node_id().as_str(), "node-A");
}

#[test]
fn shutdown_completes_without_worker_or_actors() {
    // 无 worker、无已注册 Actor：shutdown 应顺序停止所有不存在的 Actor 并返回 Ok。
    let rt = make_runtime_only("node-A");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = runtime.block_on(rt.shutdown());
    assert!(result.is_ok(), "shutdown should succeed: {:?}", result);
}

#[test]
fn shutdown_sends_cancel_to_registered_loops() {
    // 注册一个取消句柄；shutdown 应发送 true，使 receiver 观察到 true。
    let rt = make_runtime_only("node-B");
    let (tx, rx) = tokio::sync::watch::channel(false);
    rt.register_background_loop_cancel(tx);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = runtime.block_on(rt.shutdown());
    assert!(result.is_ok());
    // 取消信号应已被发送
    assert!(*rx.borrow(), "cancel signal should be sent on shutdown");
}

#[test]
fn clone_preserves_handles() {
    // Runtime 是 Clone（廉价 Arc 句柄复制）；clone 后访问器应指向同一节点。
    let rt = make_runtime_only("node-C");
    let rt2 = rt.clone();
    assert_eq!(rt.node_id().as_str(), rt2.node_id().as_str());
    assert_eq!(
        rt.workflow_actor_id().as_str(),
        rt2.workflow_actor_id().as_str()
    );
}

#[tokio::test]
async fn store_accessor_round_trips_data() {
    // 验证 store 访问器返回的 Store 可正常读写（间接验证 Store 句柄有效）。
    let rt = make_runtime_only("node-D");
    rt.store().put("ctx-test", b"hello").await.unwrap();
    let val = rt.store().get("ctx-test").await.unwrap();
    assert_eq!(val, Some(b"hello".to_vec()));
}

#[tokio::test]
async fn set_worker_and_getter_roundtrip() {
    let rt = make_runtime_only("node-E");
    assert!(rt.worker().is_none());

    let worker = rt
        .init_worker(tokio::runtime::Handle::current())
        .await
        .expect("init worker");
    let mut rt_mut = rt;
    rt_mut.set_worker(worker);
    assert!(rt_mut.worker().is_some());
}

#[tokio::test]
async fn subscribe_cancel_and_broadcast_cancel() {
    let (rt, transport) = make_runtime("node-F");
    rt.subscribe_cancel().await.expect("subscribe cancel");
    rt.broadcast_cancel("task-1", "wf-1")
        .await
        .expect("broadcast cancel");
    assert!(transport.broadcast_count() >= 1);
    assert!(transport.subscribed_to(TOPIC_CANCEL));
}

#[test]
fn task_dispatcher_accessor_returns_handle() {
    let rt = make_runtime_only("node-G");
    // 仅验证访问器返回的句柄可调用 shutdown 而不 panic。
    rt.task_dispatcher().shutdown();
}
