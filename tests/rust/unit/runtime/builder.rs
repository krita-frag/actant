//! Unit tests extracted from `src/runtime/builder.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;

#[test]
fn empty_data_dir_rejected() {
    assert!(matches!(validate_data_dir(""), Err(ActantError::Config(_))));
    assert!(matches!(
        validate_data_dir("   "),
        Err(ActantError::Config(_))
    ));
}

#[test]
fn normal_relative_path_accepted() {
    assert!(validate_data_dir("./actant-data").is_ok());
    assert!(validate_data_dir("actant-data").is_ok());
}

#[test]
fn normal_absolute_path_accepted() {
    assert!(validate_data_dir("/tmp/actant-test-does-not-exist-xyz").is_ok());
}

#[test]
fn parent_component_accepted() {
    assert!(validate_data_dir("../actant-data").is_ok());
    assert!(validate_data_dir("../../actant-data").is_ok());
}

#[test]
fn system_root_rejected() {
    let result = validate_data_dir("/");
    assert!(
        matches!(result, Err(ActantError::Config(_))),
        "data_dir=/ must be rejected, got {:?}",
        result
    );
}

#[test]
fn system_etc_rejected() {
    let result = validate_data_dir("/etc");
    assert!(
        matches!(result, Err(ActantError::Config(_))),
        "data_dir=/etc must be rejected, got {:?}",
        result
    );
}

#[test]
fn system_bin_rejected() {
    let result = validate_data_dir("/bin");
    assert!(
        matches!(result, Err(ActantError::Config(_))),
        "data_dir=/bin must be rejected, got {:?}",
        result
    );
}

#[test]
fn system_dev_rejected() {
    let result = validate_data_dir("/dev");
    assert!(
        matches!(result, Err(ActantError::Config(_))),
        "data_dir=/dev must be rejected, got {:?}",
        result
    );
}

#[test]
fn symlink_to_system_dir_rejected() {
    let result = validate_data_dir("/etc/./");
    assert!(
        matches!(result, Err(ActantError::Config(_))),
        "data_dir=/etc/. should canonicalize and be rejected, got {:?}",
        result
    );
}

#[test]
fn subdir_of_system_dir_accepted() {
    assert!(validate_data_dir("/etc/actant-nonexistent-xyz-123").is_ok());
}

// ───────────────────────── init_actor_system 测试 ─────────────────────────

use crate::common::WorkerConfig;
use crate::runtime::workflow::FailoverManager;
use crate::test_support::MockTransport;
use tempfile::tempdir;

fn make_node(id: &str) -> NodeId {
    NodeId::from(id.to_string())
}

#[test]
fn init_actor_system_without_data_dir_returns_in_memory_system() {
    let node_id = make_node("node-A");
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-A"));
    let event_bus = EventBus::new();
    let system = init_actor_system(None, &node_id, &network, &event_bus).unwrap();
    assert!(Arc::strong_count(&system) >= 1);
}

#[test]
fn init_actor_system_with_data_dir_creates_persistence_files() {
    let dir = tempdir().unwrap();
    let node_id = make_node("node-B");
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-B"));
    let event_bus = EventBus::new();
    let system = init_actor_system(
        Some(dir.path().to_str().unwrap()),
        &node_id,
        &network,
        &event_bus,
    )
    .unwrap();
    // 验证 actor 子目录与 WAL 文件已创建。
    assert!(dir.path().join("actor").exists());
    assert!(dir.path().join("actor.wal").exists());
    drop(system);
}

#[test]
fn init_actor_system_with_invalid_data_dir_returns_storage_io_error() {
    let node_id = make_node("node-C");
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-C"));
    let event_bus = EventBus::new();
    // /dev/null 是文件而非目录，create_dir_all 应失败。
    let result = init_actor_system(Some("/dev/null"), &node_id, &network, &event_bus);
    assert!(matches!(result, Err(ActantError::StorageIo(_))));
}

// ───────────────────────── init_orchestrator 测试 ─────────────────────────

#[tokio::test]
async fn init_orchestrator_without_data_dir_returns_new_orchestrator() {
    let node_id = make_node("node-D");
    let config = ActantConfig::default();
    let orchestrator = init_orchestrator(None, &node_id, &config).await.unwrap();
    assert!(Arc::strong_count(&orchestrator) >= 1);
}

#[tokio::test]
async fn init_orchestrator_with_data_dir_recovers_from_store() {
    let dir = tempdir().unwrap();
    let node_id = make_node("node-E");
    let config = ActantConfig::default();
    let orchestrator = init_orchestrator(Some(dir.path().to_str().unwrap()), &node_id, &config)
        .await
        .unwrap();
    // 验证 orchestrator 子目录已创建。
    assert!(dir.path().join("orchestrator").exists());
    drop(orchestrator);
}

// ───────────────────────── RuntimeBuilder 链式 API 测试 ─────────────────────────

#[test]
fn runtime_builder_new_sets_node_and_config() {
    let node_id = make_node("node-RB-1");
    let config = ActantConfig::default();
    let builder = RuntimeBuilder::new(node_id.clone(), config.clone());
    assert_eq!(builder.node_id, node_id);
    assert_eq!(
        builder.config.payload_signing_key,
        config.payload_signing_key
    );
}

#[test]
fn runtime_builder_with_data_dir_chains() {
    let node_id = make_node("node-RB-2");
    let builder = RuntimeBuilder::new(node_id, ActantConfig::default())
        .with_data_dir("/tmp/actant-rb-test".to_string());
    assert_eq!(builder.data_dir, Some("/tmp/actant-rb-test".to_string()));
}

// ───────────────────────── init_worker 测试 ─────────────────────────

async fn make_failover(node_id: &NodeId, network: &Arc<dyn Transport>) -> Arc<FailoverManager> {
    let actor_system = Arc::new(ActorSystem::new());
    let workflow_actor_id = ActorId::workflow(node_id);
    Arc::new(FailoverManager::new(
        node_id.clone(),
        network.clone(),
        actor_system,
        workflow_actor_id,
    ))
}

#[tokio::test]
async fn init_worker_with_fifo_scheduler_spawns_actor_and_returns_worker() {
    let node_id = make_node("node-F");
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-F"));
    let event_bus = EventBus::new();
    let actor_system = Arc::new(ActorSystem::new());
    let dispatcher = TaskRegistry::new(1, 8, Vec::new())
        .expect("TaskRegistry init")
        .into_dispatcher();
    let failover = make_failover(&node_id, &network).await;
    let handle = tokio::runtime::Handle::current();

    let worker = init_worker(WorkerInitParams {
        node_id: &node_id,
        network: &network,
        event_bus,
        scheduler_kind: scheduler_kind::FIFO,
        worker_config: &WorkerConfig::default(),
        actor_system: actor_system.clone(),
        task_dispatcher: dispatcher,
        failover: &failover,
        tokio_handle: handle,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
    })
    .await
    .expect("init_worker FIFO ok");

    assert_eq!(worker.node_id(), &node_id);
    assert!(worker.scheduler_actor_id().is_some());
}

#[tokio::test]
async fn init_worker_with_priority_scheduler_spawns_actor() {
    let node_id = make_node("node-G");
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-G"));
    let event_bus = EventBus::new();
    let actor_system = Arc::new(ActorSystem::new());
    let dispatcher = TaskRegistry::new(1, 8, Vec::new())
        .expect("TaskRegistry init")
        .into_dispatcher();
    let failover = make_failover(&node_id, &network).await;
    let handle = tokio::runtime::Handle::current();

    let worker = init_worker(WorkerInitParams {
        node_id: &node_id,
        network: &network,
        event_bus,
        scheduler_kind: scheduler_kind::PRIORITY,
        worker_config: &WorkerConfig::default(),
        actor_system: actor_system.clone(),
        task_dispatcher: dispatcher,
        failover: &failover,
        tokio_handle: handle,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
    })
    .await
    .expect("init_worker PRIORITY ok");

    assert_eq!(worker.node_id(), &node_id);
}

#[tokio::test]
async fn init_worker_with_unknown_scheduler_kind_returns_config_error() {
    let node_id = make_node("node-H");
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-H"));
    let event_bus = EventBus::new();
    let actor_system = Arc::new(ActorSystem::new());
    let dispatcher = TaskRegistry::new(1, 8, Vec::new())
        .expect("TaskRegistry init")
        .into_dispatcher();
    let failover = make_failover(&node_id, &network).await;
    let handle = tokio::runtime::Handle::current();

    let result = init_worker(WorkerInitParams {
        node_id: &node_id,
        network: &network,
        event_bus,
        scheduler_kind: "lifo",
        worker_config: &WorkerConfig::default(),
        actor_system: actor_system.clone(),
        task_dispatcher: dispatcher,
        failover: &failover,
        tokio_handle: handle,
        workflow_actor_id: None,
        dag_gossip_actor_id: None,
    })
    .await;

    assert!(
        matches!(result, Err(ActantError::Config(_))),
        "expected Config error for unknown scheduler kind"
    );
}

#[tokio::test]
async fn init_worker_attaches_optional_actor_ids_when_provided() {
    let node_id = make_node("node-I");
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-I"));
    let event_bus = EventBus::new();
    let actor_system = Arc::new(ActorSystem::new());
    let dispatcher = TaskRegistry::new(1, 8, Vec::new())
        .expect("TaskRegistry init")
        .into_dispatcher();
    let failover = make_failover(&node_id, &network).await;
    let handle = tokio::runtime::Handle::current();
    let workflow_actor_id = ActorId::workflow(&node_id);
    let dag_gossip_actor_id = ActorId::dag_gossip(&node_id);

    let worker = init_worker(WorkerInitParams {
        node_id: &node_id,
        network: &network,
        event_bus,
        scheduler_kind: scheduler_kind::FIFO,
        worker_config: &WorkerConfig::default(),
        actor_system: actor_system.clone(),
        task_dispatcher: dispatcher,
        failover: &failover,
        tokio_handle: handle,
        workflow_actor_id: Some(workflow_actor_id.clone()),
        dag_gossip_actor_id: Some(dag_gossip_actor_id.clone()),
    })
    .await
    .expect("init_worker with actor ids ok");

    // 通过 worker 的 scheduler_actor_id 验证 spawn 成功。
    assert!(worker.scheduler_actor_id().is_some());
}

// ───────────────────────── RuntimeBuilder 测试 ─────────────────────────

#[test]
fn runtime_builder_new_initializes_without_data_dir() {
    let node_id = make_node("node-J");
    let builder = RuntimeBuilder::new(node_id, ActantConfig::default());
    // build 需要 data_dir，但 builder 构造本身不应失败。
    assert!(builder.data_dir.is_none());
}

#[test]
fn runtime_builder_with_data_dir_stores_path() {
    let node_id = make_node("node-K");
    let builder = RuntimeBuilder::new(node_id, ActantConfig::default())
        .with_data_dir("/tmp/actant-test".into());
    assert_eq!(builder.data_dir.as_deref(), Some("/tmp/actant-test"));
}

// ───────────────────────── RuntimeBuilder::build 端到端测试 ─────────────────────────

use crate::common::discovery_mode;
use crate::common::DiscoveryMode;

/// 构造一个 discovery_mode=none 的最小配置，避免测试中触发 P2P 发现抖动。
fn make_test_config() -> ActantConfig {
    let mut config = ActantConfig::default();
    config.network.discovery_mode = DiscoveryMode::new_unchecked(discovery_mode::NONE);
    config.network.capability_gossip_interval_ms = 60_000;
    config.worker.drain_timeout_secs = 5;
    config.payload_signing_key = b"test-signing-key".to_vec();
    config
}

#[tokio::test]
async fn build_without_data_dir_returns_config_error() {
    let node_id = make_node("node-build-1");
    let result = RuntimeBuilder::new(node_id, make_test_config())
        .build()
        .await;
    assert!(
        matches!(result, Err(ActantError::Config(_))),
        "expected Config error when data_dir missing, got: {:?}",
        result.map(|_| ())
    );
}

#[tokio::test]
async fn build_with_temp_dir_produces_full_runtime() {
    let dir = tempdir().unwrap();
    let node_id = make_node("node-build-2");
    let config = make_test_config();

    let runtime = RuntimeBuilder::new(node_id.clone(), config)
        .with_data_dir(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .expect("build should succeed with temp dir and discovery=none");

    // 验证所有子系统已初始化。
    assert_eq!(runtime.node_id(), &node_id);
    assert!(runtime.worker().is_some(), "worker should be injected");
    assert_eq!(runtime.workflow_actor_id(), &ActorId::workflow(&node_id));
    // store 应可读写。
    runtime
        .store()
        .put("build-test", b"value")
        .await
        .expect("store put");
    let val = runtime.store().get("build-test").await.unwrap();
    assert_eq!(val, Some(b"value".to_vec()));

    // 清理：shutdown 应不 panic。
    runtime.shutdown().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn build_creates_expected_subdirectories() {
    let dir = tempdir().unwrap();
    let node_id = make_node("node-build-3");
    let config = make_test_config();

    let runtime = RuntimeBuilder::new(node_id, config)
        .with_data_dir(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .expect("build should succeed");

    // build 应创建 actor、orchestrator、store 子目录及 actor.wal 文件。
    assert!(dir.path().join("actor").exists(), "actor dir should exist");
    assert!(
        dir.path().join("actor.wal").exists(),
        "actor.wal should exist"
    );
    assert!(
        dir.path().join("orchestrator").exists(),
        "orchestrator dir should exist"
    );
    assert!(dir.path().join("store").exists(), "store dir should exist");

    runtime.shutdown().await.expect("shutdown ok");
}

#[tokio::test]
async fn build_with_invalid_discovery_mode_returns_error() {
    let dir = tempdir().unwrap();
    let node_id = make_node("node-build-4");
    let mut config = ActantConfig::default();
    // 使用一个明显无效的发现模式名称。
    config.network.discovery_mode = DiscoveryMode::new_unchecked("invalid-mode-xyz");

    let result = RuntimeBuilder::new(node_id, config)
        .with_data_dir(dir.path().to_str().unwrap().to_string())
        .build()
        .await;
    assert!(
        result.is_err(),
        "build should fail with invalid discovery mode"
    );
}

#[tokio::test]
async fn build_worker_has_scheduler_and_failover() {
    let dir = tempdir().unwrap();
    let node_id = make_node("node-build-5");
    let config = make_test_config();

    let runtime = RuntimeBuilder::new(node_id, config)
        .with_data_dir(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .expect("build should succeed");

    let worker = runtime.worker().expect("worker should be set");
    assert!(
        worker.scheduler_actor_id().is_some(),
        "scheduler actor should be spawned"
    );
    assert_eq!(
        worker.node_id(),
        runtime.node_id(),
        "worker node_id should match runtime"
    );

    runtime.shutdown().await.expect("shutdown ok");
}

#[tokio::test]
async fn build_capability_runtime_has_handlers_registered() {
    let dir = tempdir().unwrap();
    let node_id = make_node("node-build-6");
    let config = make_test_config();

    let runtime = RuntimeBuilder::new(node_id, config)
        .with_data_dir(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .expect("build should succeed");

    // build 应注册内置 capability handlers。
    let cap = runtime.capability();
    assert!(
        cap.capability_count() > 0,
        "capability metas should be registered"
    );

    runtime.shutdown().await.expect("shutdown ok");
}

#[tokio::test]
async fn build_actor_system_has_workflow_and_failover_actors() {
    let dir = tempdir().unwrap();
    let node_id = make_node("node-build-7");
    let config = make_test_config();

    let runtime = RuntimeBuilder::new(node_id.clone(), config)
        .with_data_dir(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .expect("build should succeed");

    let actor_system = runtime.actor_system();
    // WorkflowActor、FailoverActor、DagGossipActor、SchedulerActor 应均已 spawn。
    assert!(
        actor_system
            .actor_status(&ActorId::workflow(&node_id))
            .is_some(),
        "WorkflowActor should be spawned"
    );
    assert!(
        actor_system
            .actor_status(&ActorId::failover(&node_id))
            .is_some(),
        "FailoverActor should be spawned"
    );
    assert!(
        actor_system
            .actor_status(&ActorId::dag_gossip(&node_id))
            .is_some(),
        "DagGossipActor should be spawned"
    );
    assert!(
        actor_system
            .actor_status(&ActorId::scheduler(&node_id))
            .is_some(),
        "SchedulerActor should be spawned"
    );

    runtime.shutdown().await.expect("shutdown ok");
}

#[tokio::test]
async fn build_can_be_called_multiple_times_with_different_dirs() {
    // 验证两次独立的 build 不互相干扰（各自使用独立的 temp dir）。
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let config = make_test_config();

    let rt1 = RuntimeBuilder::new(make_node("node-build-8a"), config.clone())
        .with_data_dir(dir1.path().to_str().unwrap().to_string())
        .build()
        .await
        .expect("first build should succeed");

    let rt2 = RuntimeBuilder::new(make_node("node-build-8b"), config)
        .with_data_dir(dir2.path().to_str().unwrap().to_string())
        .build()
        .await
        .expect("second build should succeed");

    assert_ne!(rt1.node_id(), rt2.node_id());

    rt1.shutdown().await.expect("shutdown rt1 ok");
    rt2.shutdown().await.expect("shutdown rt2 ok");
}
