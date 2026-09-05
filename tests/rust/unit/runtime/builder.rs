//! Unit tests extracted from `src/runtime/builder.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;

/// 构造一个不拉取任何 worker 子进程的进程池分发器，供 `init_worker` 传递
/// （`init_worker` 仅存储调度器，不在测试中真正派发任务）。
fn hermetic_dispatcher() -> Arc<dyn TaskDispatcher> {
    Arc::new(
        ProcessTaskDispatcher::new(0, "python3".to_string(), 1, Vec::new(), Vec::new())
            .expect("process task dispatcher init"),
    )
}

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
fn subdir_of_system_dir_rejected() {
    // H6.3：黑名单祖先判断——系统目录的子路径同样拒绝，即使目录尚不存在
    // （canonicalize 失败，走逐级向上找已存在祖先的归一化路径）。
    let result = validate_data_dir("/etc/actant-nonexistent-xyz-123");
    assert!(
        matches!(result, Err(ActantError::Config(_))),
        "data_dir under /etc must be rejected, got {:?}",
        result
    );
}

#[test]
fn deep_subdir_of_system_dir_rejected() {
    let result = validate_data_dir("/usr/local/x");
    assert!(
        matches!(result, Err(ActantError::Config(_))),
        "data_dir=/usr/local/x must be rejected, got {:?}",
        result
    );
}

#[test]
fn tmp_dir_accepted() {
    // /tmp 在 macOS 上是 /private/tmp 的 symlink，验证归一化后不误判。
    assert!(validate_data_dir("/tmp/actant-test").is_ok());
}

#[test]
fn similar_named_path_not_rejected_by_ancestor_check() {
    // 按组件前缀比较：/etcfoo 不是 /etc 的子路径。
    assert!(validate_data_dir("/etcfoo").is_ok());
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
    let event_bus = EventBus::new();
    let config = crate::common::ActantConfig::default();
    let system = init_actor_system(None, &node_id, &event_bus, &config).unwrap();
    assert!(Arc::strong_count(&system) >= 1);
}

#[test]
fn init_actor_system_with_data_dir_creates_persistence_files() {
    let dir = tempdir().unwrap();
    let node_id = make_node("node-B");
    let event_bus = EventBus::new();
    let config = crate::common::ActantConfig::default();
    let system = init_actor_system(
        Some(dir.path().to_str().unwrap()),
        &node_id,
        &event_bus,
        &config,
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
    let event_bus = EventBus::new();
    let config = crate::common::ActantConfig::default();
    // /dev/null 是文件而非目录，LmdbStore::open_with_config 内部的
    // create_dir_all 应失败（错误经 map_err 包装为 Storage）。
    let result = init_actor_system(Some("/dev/null"), &node_id, &event_bus, &config);
    assert!(matches!(result, Err(ActantError::Storage(_))));
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
    let dispatcher = hermetic_dispatcher();
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
        capability_gossip: None,
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
    let dispatcher = hermetic_dispatcher();
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
        capability_gossip: None,
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
    let dispatcher = hermetic_dispatcher();
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
        capability_gossip: None,
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
    let dispatcher = hermetic_dispatcher();
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
        capability_gossip: None,
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
    // 进程池后端：build 会急切拉起 worker 子进程。测试只验证编排子系统，
    // 不真正派发任务；减少进程数并指定解释器路径即可（子进程即使因缺少
    // `actant.task._worker` 模块立即退出也不影响这些断言）。
    config.worker.num_worker_processes = 1;
    config.worker.worker_program = "python3".to_string();
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

// ───────────────────────── P0-5 恢复重派发接线测试 ─────────────────────────

use crate::runtime::workflow::{Dag, DagNode, Orchestrator};

fn make_signed_node(id: &str, name: &str, signing_key: &[u8]) -> DagNode {
    DagNode {
        task_id: crate::common::TaskId::from(id.to_string()),
        name: name.to_string(),
        payload: crate::common::payload::sign(signing_key, b"").unwrap(),
        retry_policy: None,
        timeout_ms: None,
        priority: 0,
        metadata: std::collections::HashMap::new(),
    }
}

/// 端到端：节点重启后（RuntimeBuilder::build），恢复出的 Pending 且依赖已满足
/// 的任务被重新派发入队；已 Completed 的任务不重跑，未就绪的任务不重派发。
///
/// 时间线：预置 data_dir/orchestrator 中一个线性 DAG（t1 → t2 → t3）推进到
/// t1 完成；build 恢复后调度器中应恰好只有 t2。
#[tokio::test]
async fn build_redispatches_recovered_pending_tasks_only() {
    use crate::runtime::state::Store;

    let dir = tempdir().unwrap();
    let signing_key = b"test-signing-key";

    // 预置：在与 build 相同的 orchestrator 子存储中写入崩溃前的进度。
    let orch_dir = dir.path().join("orchestrator");
    let lmdb = LmdbStore::open(&orch_dir).unwrap();
    let pre_store = Store::new(lmdb.clone());
    let pre = Orchestrator::new()
        .with_signing_key(signing_key.to_vec())
        .with_store(pre_store);
    let wf = crate::common::WorkflowId::from("wf-rebuild-1");
    let mut dag = Dag::new();
    dag.add_node(make_signed_node("t1", "first", signing_key))
        .unwrap();
    dag.add_node(make_signed_node("t2", "second", signing_key))
        .unwrap();
    dag.add_node(make_signed_node("t3", "third", signing_key))
        .unwrap();
    dag.add_edge(
        crate::common::TaskId::from("t1"),
        crate::common::TaskId::from("t2"),
    )
    .unwrap();
    dag.add_edge(
        crate::common::TaskId::from("t2"),
        crate::common::TaskId::from("t3"),
    )
    .unwrap();
    pre.submit(wf.clone(), dag).await.unwrap();
    let roots = pre.start(&wf).unwrap();
    assert_eq!(roots.len(), 1);
    pre.mark_task_running(&wf, &roots[0].id).unwrap();
    let (ready, _, _) = pre
        .on_task_completed(&wf, &crate::common::TaskId::from("t1"), b"r1".to_vec())
        .await
        .unwrap();
    assert_eq!(ready.len(), 1, "t2 should become ready after t1 completes");
    pre.flush_dirty().await.unwrap();
    drop(pre);
    drop(lmdb);

    // 模拟重启：build 恢复状态并重派发 ready 任务。
    let node_id = make_node("node-build-recover");
    let mut config = make_test_config();
    config.payload_signing_key = signing_key.to_vec();
    let runtime = RuntimeBuilder::new(node_id, config)
        .with_data_dir(dir.path().to_str().unwrap().to_string())
        .build()
        .await
        .expect("build should recover and redispatch");

    let worker = runtime.worker().expect("worker injected");
    let task = worker
        .scheduler()
        .try_dequeue()
        .await
        .expect("recovered ready task t2 should be re-enqueued");
    assert_eq!(task.id.to_string(), "t2", "only t2 should be redispatched");
    assert!(
        worker.scheduler().try_dequeue().await.is_none(),
        "no other task may be redispatched: t1 is Completed (no re-run), t3 has unfinished dependencies"
    );

    runtime.shutdown().await.expect("shutdown ok");
}

/// build 在无持久化（内存模式编排器）时不应重派发任何任务。
#[tokio::test]
async fn build_without_store_redispatches_nothing() {
    // RuntimeBuilder 要求 data_dir，这里退而验证 init_orchestrator 内存路径
    // 不产出恢复任务：内存 Orchestrator 没有任何 workflow。
    let node_id = make_node("node-D");
    let config = ActantConfig::default();
    let orchestrator = init_orchestrator(None, &node_id, &config).await.unwrap();
    assert!(Arc::strong_count(&orchestrator) >= 1);
    let recovered = orchestrator.recover_ready_tasks();
    assert!(
        recovered.is_empty(),
        "empty orchestrator has no ready tasks"
    );
}
