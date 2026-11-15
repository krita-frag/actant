//! Unit tests extracted from `src/runtime/builder.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;

/// 构造一个不拉取任何 worker 子进程的进程池分发器，供 `init_worker` 传递
/// （`init_worker` 仅存储调度器，不在测试中真正派发任务）。
fn hermetic_dispatcher() -> Arc<dyn TaskDispatcher> {
    Arc::new(
        ProcessTaskDispatcher::new(0, WorkerLaunchSpec::default(), 1, Vec::new(), None)
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
    // 黑名单祖先判断——系统目录的子路径同样拒绝，即使目录尚不存在
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
use crate::runtime::dispatcher::WorkerLaunchSpec;
use crate::runtime::workflow::FailoverManager;
use crate::test_support::MockTransport;
use tempfile::tempdir;

fn make_node(id: &str) -> NodeId {
    NodeId::from(id.to_string())
}

#[test]
fn init_actor_system_binds_node_id_without_persistence() {
    // actor 系统为系统 actor 专用本地运行时：只绑定 node_id 与事件总线，
    // 不接 data_dir——mailbox 与 actor 状态是进程内存活对象，跨重启不保留
    //（工作流恢复由 orchestrator 的统一历史承载）。
    let node_id = make_node("node-A");
    let tap = EventTap::new();
    let (result_feed, _result_rx) = crate::runtime::events::TaskResultFeed::new();
    let _ = &result_feed;
    let system = init_actor_system(&node_id, &tap).unwrap();
    assert_eq!(system.node_id(), Some(&node_id));
}

// ───────────────────────── 节点身份（identity.key）测试 ─────────────────────────

#[test]
fn identity_key_created_with_owner_only_permissions() {
    let dir = tempdir().unwrap();
    let key1 = load_or_create_identity(dir.path()).unwrap();
    let path = dir.path().join("identity.key");
    assert!(path.exists());
    // 0600：仅属主可读写（unix）
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "identity.key must be owner-only");
    }
    // 重启加载同一密钥
    let key2 = load_or_create_identity(dir.path()).unwrap();
    assert_eq!(key1.to_bytes(), key2.to_bytes());
}

#[test]
fn identity_key_corrupt_file_rejected() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("identity.key"), b"short").unwrap();
    assert!(load_or_create_identity(dir.path()).is_err());
}

// ───────────────────────── init_orchestrator 测试 ─────────────────────────

#[tokio::test]
async fn init_orchestrator_without_data_dir_returns_new_orchestrator() {
    let node_id = make_node("node-D");
    let config = ActantConfig::default();
    let orchestrator = init_orchestrator(None, &node_id, &config, None)
        .await
        .unwrap();
    assert!(Arc::strong_count(&orchestrator) >= 1);
}

#[tokio::test]
async fn init_orchestrator_with_data_dir_recovers_from_store() {
    let dir = tempdir().unwrap();
    let node_id = make_node("node-E");
    let config = ActantConfig::default();
    let orchestrator =
        init_orchestrator(Some(dir.path().to_str().unwrap()), &node_id, &config, None)
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
    let tap = EventTap::new();
    let (result_feed, _result_rx) = crate::runtime::events::TaskResultFeed::new();
    let _ = &result_feed;
    let actor_system = Arc::new(ActorSystem::new());
    let dispatcher = hermetic_dispatcher();
    let failover = make_failover(&node_id, &network).await;
    let handle = tokio::runtime::Handle::current();
    let labels = std::collections::BTreeMap::new();

    let worker = init_worker(WorkerInitParams {
        scheduler: None,
        orchestrator_ingest: false,
        node_id: &node_id,
        network: &network,
        tap,
        result_feed,
        node_labels: &labels,
        route_policy: None,
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
    let tap = EventTap::new();
    let (result_feed, _result_rx) = crate::runtime::events::TaskResultFeed::new();
    let _ = &result_feed;
    let actor_system = Arc::new(ActorSystem::new());
    let dispatcher = hermetic_dispatcher();
    let failover = make_failover(&node_id, &network).await;
    let handle = tokio::runtime::Handle::current();
    let labels = std::collections::BTreeMap::new();

    let worker = init_worker(WorkerInitParams {
        scheduler: None,
        orchestrator_ingest: false,
        node_id: &node_id,
        network: &network,
        tap,
        result_feed,
        node_labels: &labels,
        route_policy: None,
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
    let tap = EventTap::new();
    let (result_feed, _result_rx) = crate::runtime::events::TaskResultFeed::new();
    let _ = &result_feed;
    let actor_system = Arc::new(ActorSystem::new());
    let dispatcher = hermetic_dispatcher();
    let failover = make_failover(&node_id, &network).await;
    let handle = tokio::runtime::Handle::current();
    let labels = std::collections::BTreeMap::new();

    let result = init_worker(WorkerInitParams {
        scheduler: None,
        orchestrator_ingest: false,
        node_id: &node_id,
        network: &network,
        tap,
        result_feed,
        node_labels: &labels,
        route_policy: None,
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
    let tap = EventTap::new();
    let (result_feed, _result_rx) = crate::runtime::events::TaskResultFeed::new();
    let _ = &result_feed;
    let actor_system = Arc::new(ActorSystem::new());
    let dispatcher = hermetic_dispatcher();
    let failover = make_failover(&node_id, &network).await;
    let handle = tokio::runtime::Handle::current();
    let labels = std::collections::BTreeMap::new();
    let workflow_actor_id = ActorId::workflow(&node_id);
    let dag_gossip_actor_id = ActorId::dag_gossip(&node_id);

    let worker = init_worker(WorkerInitParams {
        scheduler: None,
        orchestrator_ingest: false,
        node_id: &node_id,
        network: &network,
        tap,
        result_feed,
        node_labels: &labels,
        route_policy: None,
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
    config.worker.drain_timeout_secs = 5;
    // 进程池后端：build 会急切拉起 worker 子进程。测试只验证编排子系统，
    // 不真正派发任务；减少进程数并指定解释器路径即可（子进程即使因缺少
    // `actant._internal.tasks.worker` 模块立即退出也不影响这些断言）。
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

/// 回归测试：注册密钥之后构建失败（此处为 LMDB 存储打开失败），
/// 不得把该节点的 wire 密钥留在进程级注册表中。
///
/// 校验方式：用该节点的 heartbeat 取 MAC，再与“本测试独占密钥”计算的期望值
/// 比较。若条目已注销，wrap 会退化到 primary（可能是其他测试的密钥）或
/// 无 MAC，均不等于期望值；从而不受并行测试的全局 primary 影响。
#[tokio::test]
async fn failed_build_after_key_registration_does_not_leak_wire_key() {
    let dir = tempdir().unwrap();
    // 用普通文件占用 store 路径，使 LMDB 打开在密钥注册之后失败。
    std::fs::write(dir.path().join("store"), b"occupied").unwrap();

    let node_id = make_node("node-build-key-leak");
    let key = b"builder-leak-unique-key".to_vec();
    let mut config = make_test_config();
    config.payload_signing_key = key.clone();

    let result = RuntimeBuilder::new(node_id.clone(), config)
        .with_data_dir(dir.path().to_str().unwrap().to_string())
        .build()
        .await;
    assert!(
        result.is_err(),
        "build must fail when the store path is occupied by a file"
    );

    let envelope = crate::common::WireEnvelope::wrap(crate::common::WireMessage::NodeHeartbeat(
        crate::common::NodeHeartbeat {
            signature: None,
            labels: std::collections::BTreeMap::new(),
            platform: None,
            available_quotas: std::collections::BTreeMap::new(),
            node_id: node_id.clone(),
            active_workflows: vec![],
            timestamp_ms: crate::common::epoch_millis(),
            available_slots: 0,
            max_slots: 0,
            endpoint_addr: None,
            role: crate::common::NodeRole::Both,
            draining: false,
        },
    ))
    .expect("wire envelope wrap");
    let mac = envelope.mac;
    let unsigned = crate::common::WireEnvelope {
        mac: None,
        ..envelope
    };
    if let Some(mac) = mac {
        let bytes = crate::common::encode_postcard(&unsigned).unwrap();
        let expected = crate::common::payload::wire_mac(&key, &bytes).unwrap();
        assert_ne!(
            mac, expected,
            "failed build must not leave the node's wire key registered"
        );
    }
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

    // build 应创建 orchestrator、store 子目录；actor 系统不落盘。
    assert!(
        dir.path().join("orchestrator").exists(),
        "orchestrator dir should exist"
    );
    assert!(dir.path().join("store").exists(), "store dir should exist");
    assert!(
        !dir.path().join("actor").exists(),
        "actor dir must not exist"
    );

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

// ───────────────────────── 租约持久化接线测试 ─────────────────────────

/// 租约键的 store key 布局（与 `persist_lease` / `recover_leases_from_store` 一致）。
fn lease_key(workflow_id: &crate::common::WorkflowId) -> String {
    format!("{}{}", crate::common::STORE_KEY_LEASE, workflow_id.0)
}

/// 端到端验证「租约写盘 → 重启恢复 → 过期清理」在生产装配下真的通电。
///
/// 这三条路径都以 `FailoverManager::with_config(.., Some(store))` 为前提。
/// 若 `RuntimeBuilder::build` 退回 `FailoverManager::new()`（`store: None`），
/// 它们会全部静默变成 no-op：重启后租约表清零（控制面可能重复接管同一
/// 工作流）、过期键永不清理。断言因此只走 builder，不手工构造 manager。
#[tokio::test]
async fn leases_persist_and_expire_across_runtime_restart() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();
    let node_id = make_node("node-lease-restart");
    let live = crate::common::WorkflowId::from("wf-lease-live");
    let stale = crate::common::WorkflowId::from("wf-lease-stale");

    // 第一轮：正常 claim 一条租约，再写入一条「落盘时即已过期」的租约。
    // 默认租期 16s，回拨 60s 保证过期判定与机器速度无关。
    {
        let rt = RuntimeBuilder::new(node_id.clone(), make_test_config())
            .with_data_dir(data_dir.clone())
            .build()
            .await
            .expect("first build should succeed");
        let fm = rt
            .failover_manager()
            .expect("worker 必须注入 failover manager");

        fm.claim_workflow(&live)
            .await
            .expect("claim should succeed");
        fm.handle_claim(&crate::common::OrchestratorClaim {
            node_id: node_id.clone(),
            workflow_id: stale.clone(),
            timestamp_ms: crate::common::epoch_millis().saturating_sub(60_000),
        })
        .await;

        assert_eq!(fm.active_leases().len(), 2, "两条租约都应先生效");
        // 落盘证据：两条 key 都在 store 里，与内存视图相互独立。
        assert!(
            rt.store().get(&lease_key(&live)).await.unwrap().is_some(),
            "claim 必须把租约写进 store"
        );
        assert!(
            rt.store().get(&lease_key(&stale)).await.unwrap().is_some(),
            "过期租约同样先落盘，由恢复路径清理"
        );

        rt.shutdown().await.expect("shutdown should succeed");
    }

    // 第二轮：同一 data_dir 重新 build ≡ 进程重启。
    {
        let rt = RuntimeBuilder::new(node_id, make_test_config())
            .with_data_dir(data_dir)
            .build()
            .await
            .expect("second build should succeed");
        let fm = rt
            .failover_manager()
            .expect("worker 必须注入 failover manager");

        let leases = fm.active_leases();
        assert_eq!(
            leases.len(),
            1,
            "重启后只应恢复未过期租约，实际: {leases:?}"
        );
        assert_eq!(leases[0].0, live.0);
        assert!(
            rt.store().get(&lease_key(&stale)).await.unwrap().is_none(),
            "过期租约键必须在恢复时删除，而不是留到下次启动再判一次"
        );

        rt.shutdown().await.expect("shutdown should succeed");
    }
}

// ───────────────────────── 恢复重派发接线测试 ─────────────────────────

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
        tags: Vec::new(),
        quota: None,
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
    let (ready, _) = pre
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

// ───────────────────────── 注入缝「真实接入」验证 ─────────────────────────
// 6 个 `RuntimeBuilder::with_*` 缝中，`with_event_log`/`with_task_dispatcher`/
// `with_orchestrator_ingest` 已被既有测试与 `examples/rust_embed.rs` 接入验证。
// `with_scheduler` 由下方哨兵调度器用例断言「真实接管」，`with_transport` 由
// `build_uses_injected_transport` 断言「传送层原样采纳」。
//
// `with_discovery` 无法端到端断言：它唯一的作用点是内置 `NetworkManager`
// 的 iroh endpoint 构造，需要真实身份与网络绑定；它与 `with_transport` 的
// 互斥关系由 `build_rejects_transport_and_discovery_together` 守护。

/// 始终报告 `len() == 7`、`is_empty() == false` 的哨兵调度器。
///
/// 默认 FIFO/priority 在空队列下 `len()` 为 0。注入后若能通过
/// `worker.scheduler().len()` 读到哨兵值，即证明注入策略被 `SchedulerActor`
/// 包装并权威接管（而非回退到 config 选的内置策略）。
struct SentinelScheduler;

#[async_trait::async_trait]
impl crate::runtime::workflow::Scheduler for SentinelScheduler {
    async fn enqueue(
        &self,
        _task: crate::common::TaskDefinition,
    ) -> std::result::Result<(), crate::common::ActantError> {
        Ok(())
    }
    async fn enqueue_batch(
        &self,
        _tasks: Vec<crate::common::TaskDefinition>,
    ) -> std::result::Result<(), crate::common::ActantError> {
        Ok(())
    }
    async fn dequeue(&self) -> Option<crate::common::TaskDefinition> {
        None
    }
    async fn try_dequeue(&self) -> Option<crate::common::TaskDefinition> {
        None
    }
    async fn dequeue_batch(&self, _limit: usize) -> Vec<crate::common::TaskDefinition> {
        Vec::new()
    }
    async fn is_empty(&self) -> bool {
        false
    }
    async fn len(&self) -> usize {
        7
    }
}

/// 注入缝 `with_scheduler` 的「真实接管」断言：注入后 `worker.scheduler()`
/// 必须就是该实现——回退到内置 FIFO/priority 时 `len()` 为 0。
#[tokio::test]
async fn init_worker_uses_injected_scheduler() {
    let node_id = make_node("node-Sentinel");
    let network: Arc<dyn Transport> = Arc::new(MockTransport::new("node-Sentinel"));
    let tap = EventTap::new();
    let (result_feed, _result_rx) = crate::runtime::events::TaskResultFeed::new();
    let actor_system = Arc::new(ActorSystem::new());
    let dispatcher = hermetic_dispatcher();
    let failover = make_failover(&node_id, &network).await;
    let handle = tokio::runtime::Handle::current();
    let labels = std::collections::BTreeMap::new();

    let worker = init_worker(WorkerInitParams {
        scheduler: Some(Arc::new(SentinelScheduler)),
        orchestrator_ingest: false,
        node_id: &node_id,
        network: &network,
        tap,
        result_feed,
        node_labels: &labels,
        route_policy: None,
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
    .expect("init_worker with injected scheduler ok");

    assert_eq!(
        worker.scheduler().len().await,
        7,
        "注入的调度器必须真实接管，而不是回退到内置 FIFO"
    );
    assert!(
        !worker.scheduler().is_empty().await,
        "哨兵调度器 is_empty() == false 必须被透传"
    );
}

/// `with_transport` 的「真实接管」：注入的传输层必须被 Runtime 原样采纳。
///
/// 断言用 `Arc::ptr_eq`——同一分配即证明没有换成内置 `NetworkManager`。
/// 这是该缝唯一可离线验证的形态：内置构造需要真实 endpoint 身份与网络绑定。
#[tokio::test]
async fn build_uses_injected_transport() {
    let dir = tempfile::tempdir().expect("tempdir");
    let node_id = make_node("node-SEAM-transport");
    let mut mock = MockTransport::new("node-SEAM-transport");
    // `require_signed_records` 要求 node_id 等于 endpoint id；桩传输层的
    // local_peer_id 默认带 `peer-` 前缀，这里对齐以免被这条守卫拦下。
    mock.local_peer_id = node_id.as_str().to_string();
    let injected: Arc<dyn Transport> = Arc::new(mock);

    let runtime = RuntimeBuilder::new(node_id, ActantConfig::default())
        .with_data_dir(dir.path().to_string_lossy().into_owned())
        .with_transport(injected.clone())
        .with_task_dispatcher(hermetic_dispatcher())
        .build()
        .await
        .expect("build with injected transport");

    assert!(
        Arc::ptr_eq(runtime.network(), &injected),
        "注入的传输层必须原样采纳，而不是换成内置 NetworkManager"
    );
    runtime.shutdown().await.expect("shutdown ok");
}

/// `with_transport` 与 `with_discovery` 互斥。
///
/// `discovery` 的唯一消费者是内置 `NetworkManager`；注入 transport 后它会被
/// 静默丢弃。两者同给属于装配误解，必须在启动期失败而不是留下「配了没生效」。
#[tokio::test]
async fn build_rejects_transport_and_discovery_together() {
    use crate::runtime::network::NoDiscovery;

    let transport: Arc<dyn Transport> = Arc::new(MockTransport::new("node-SEAM-both"));
    // `Runtime` 不是 `Debug`，因此不能直接用 `expect_err`。
    let err = match RuntimeBuilder::new(make_node("node-SEAM-both"), ActantConfig::default())
        .with_transport(transport)
        .with_discovery(Arc::new(NoDiscovery))
        .build()
        .await
    {
        Ok(_) => panic!("transport + discovery must be rejected at build time"),
        Err(e) => e,
    };

    assert!(
        matches!(&err, ActantError::Config(msg) if msg.contains("mutually exclusive")),
        "expected Config error mentioning mutual exclusion, got: {err:?}"
    );
}

/// 记录 `append` 次数的事件日志桩：委托 `MemoryEventLog` 并统计 append 批次，
/// 供 `with_event_log` 注入缝的「真实接管」断言使用。
struct RecordingEventLog {
    inner: crate::runtime::state::event_log::MemoryEventLog,
    appends: std::sync::atomic::AtomicUsize,
}

impl RecordingEventLog {
    fn new() -> Self {
        Self {
            inner: Default::default(),
            appends: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl crate::runtime::state::event_log::EventLog for RecordingEventLog {
    fn append(
        &self,
        topic: &str,
        payload: &[u8],
    ) -> crate::common::Result<crate::runtime::state::event_log::EventId> {
        self.appends
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.append(topic, payload)
    }

    fn read_after(
        &self,
        topic: &str,
        after: Option<&crate::runtime::state::event_log::EventId>,
    ) -> crate::common::Result<Vec<crate::runtime::state::event_log::LogEntry>> {
        self.inner.read_after(topic, after)
    }

    fn latest(
        &self,
        topic: &str,
    ) -> crate::common::Result<Option<crate::runtime::state::event_log::LogEntry>> {
        self.inner.latest(topic)
    }

    fn count(&self, topic: &str) -> crate::common::Result<usize> {
        self.inner.count(topic)
    }

    fn append_batch(
        &self,
        topic: &str,
        payloads: &[Vec<u8>],
    ) -> crate::common::Result<Vec<crate::runtime::state::event_log::EventId>> {
        self.appends
            .fetch_add(payloads.len(), std::sync::atomic::Ordering::Relaxed);
        self.inner.append_batch(topic, payloads)
    }

    fn list_topics(&self) -> crate::common::Result<Vec<String>> {
        self.inner.list_topics()
    }

    fn trim_absorbed(
        &self,
        topic: &str,
        up_to: &crate::runtime::state::event_log::EventId,
        keep: usize,
    ) -> crate::common::Result<usize> {
        self.inner.trim_absorbed(topic, up_to, keep)
    }

    fn hide_topic(&self, topic: &str) -> crate::common::Result<()> {
        self.inner.hide_topic(topic)
    }
}

#[tokio::test]
async fn injected_event_log_receives_workflow_submission() {
    let dir = tempdir().unwrap();
    let log = Arc::new(RecordingEventLog::new());
    let orchestrator = init_orchestrator(
        Some(dir.path().to_str().unwrap()),
        &make_node("node-event-log-seam"),
        &ActantConfig::default(),
        Some(log.clone()),
    )
    .await
    .unwrap();

    let wf = crate::common::WorkflowId::from("wf-event-log-seam");
    let mut dag = Dag::new();
    dag.add_node(make_signed_node("t1", "only", b"test-signing-key"))
        .unwrap();
    orchestrator.submit(wf, dag).await.unwrap();

    assert!(
        log.appends.load(std::sync::atomic::Ordering::Relaxed) >= 2,
        "submit must append Submitted + NodeAdded through the injected EventLog"
    );
}

/// build 在无持久化（内存模式编排器）时不应重派发任何任务。
#[tokio::test]
async fn build_without_store_redispatches_nothing() {
    // RuntimeBuilder 要求 data_dir，这里退而验证 init_orchestrator 内存路径
    // 不产出恢复任务：内存 Orchestrator 没有任何 workflow。
    let node_id = make_node("node-D");
    let config = ActantConfig::default();
    let orchestrator = init_orchestrator(None, &node_id, &config, None)
        .await
        .unwrap();
    assert!(Arc::strong_count(&orchestrator) >= 1);
    let recovered = orchestrator.recover_ready_tasks();
    assert!(
        recovered.is_empty(),
        "empty orchestrator has no ready tasks"
    );
}

#[test]
fn resolve_node_id_derives_stable_endpoint_id_from_identity() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_string_lossy().to_string();

    let first = resolve_node_id(&path).expect("resolve node id");
    let second = resolve_node_id(&path).expect("resolve node id again");
    assert_eq!(first, second, "node id must be stable across calls");
    // 与 iroh endpoint id 同构，才能满足 require_signed_records 的身份绑定。
    assert!(first.as_str().parse::<iroh::EndpointId>().is_ok());
}

#[test]
fn normalize_allowed_peer_ids_accepts_ids_and_rejects_addresses() {
    let key = iroh::SecretKey::generate();
    let id = key.public().to_string();

    let normalized = normalize_allowed_peer_ids(&[format!("  {id}  "), id.clone()])
        .expect("valid endpoint ids must be accepted");
    assert_eq!(normalized, vec![id]);

    let err = normalize_allowed_peer_ids(&["deadbeef".to_string()])
        .expect_err("full address or typo must be rejected at startup");
    assert!(matches!(err, ActantError::Config(_)));
    assert!(err.to_string().contains("allowed_peer_ids"));

    let err = normalize_allowed_peer_ids(&["".to_string()])
        .expect_err("empty entry must be rejected instead of silently dropped");
    assert!(err.to_string().contains("empty entries"));
}

#[test]
fn read_node_id_is_none_until_identity_is_created() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_string_lossy().to_string();

    assert_eq!(read_node_id(&path).expect("read missing identity"), None);

    let created = resolve_node_id(&path).expect("create identity");
    assert_eq!(
        read_node_id(&path).expect("read existing identity"),
        Some(created)
    );
}

#[test]
fn read_node_id_rejects_corrupt_identity_file() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_string_lossy().to_string();
    std::fs::write(dir.path().join("identity.key"), b"short").expect("write corrupt key");

    let err = read_node_id(&path).expect_err("corrupt identity must fail");
    assert!(err.to_string().contains("identity.key corrupt"));
}
