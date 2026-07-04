//! 从 Python 层初始化 Rust 子系统的 bootstrap 助手。
//!
//! 从 `runtime.rs` 抽取，使 `PyRuntimeCore` 定义聚焦于 Python 端方法，
//! 子系统构造则集中在此处。

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::actor::ActorSystem;
use crate::common::{ActantError, NodeId};
use crate::event_bus::EventBus;
use crate::network::Transport;
use crate::orchestrator::{Orchestrator, Scheduler};
use crate::worker::{TaskDispatcher, TaskRegistry, WorkerRuntime};

/// FHS / Windows 关键系统目录的禁止清单。
///
/// `data_dir` 指向这些目录会令 LMDB/Actor 存储在系统关键路径下创建文件，
/// 可能损坏系统或耗尽关键文件系统空间。此处拒绝的是**规范化后的精确匹配**，
/// 不影响在这些目录下的专用子目录（如 `/etc/actant`）——后者是 operator 的合法选择。
///
/// Windows 路径在非 Windows 平台上无法 canonicalize，自然不会误命中。
const FORBIDDEN_SYSTEM_DIRS: &[&str] = &[
    // Unix / FHS
    "/",
    "/etc",
    "/bin",
    "/sbin",
    "/usr",
    "/lib",
    "/lib64",
    "/dev",
    "/proc",
    "/sys",
    "/boot",
    "/var/log",
    // Windows
    "C:\\Windows",
    "C:\\Program Files",
    "C:\\Program Files (x86)",
    "C:\\ProgramData",
];

/// 校验 operator 提供的 `data_dir` 配置。
///
/// # 威胁模型
///
/// `data_dir` 由本地 operator 在节点启动时配置，属**受信输入**。
/// 此函数不是抵御未授权输入的安全边界，而是 **foot-gun 防护**：
/// 拒绝空字符串与系统关键目录，避免 operator 误配导致存储写入 `/etc` 等路径。
/// 不拒绝相对路径或 `..` 组件——这些是 operator 的合法选择。
pub(crate) fn validate_data_dir(data_dir: &str) -> Result<(), ActantError> {
    let trimmed = data_dir.trim();
    if trimmed.is_empty() {
        return Err(ActantError::Config("data_dir must not be empty".into()));
    }
    // 仅对已存在的路径做规范化比较；不存在的路径由 create_dir_all 创建，
    // 此时系统目录必然已存在（如 /etc），故关键路径总能被捕获。
    let candidate = std::path::Path::new(trimmed);
    if let Ok(canon) = candidate.canonicalize() {
        for forbidden in FORBIDDEN_SYSTEM_DIRS {
            if let Ok(canon_forbidden) = std::path::Path::new(forbidden).canonicalize() {
                if canon == canon_forbidden {
                    return Err(ActantError::Config(format!(
                        "data_dir must not point at system directory {}",
                        forbidden
                    )));
                }
            }
        }
    }
    Ok(())
}

/// 初始化 actor 系统，可选持久化。
pub(crate) fn init_actor_system(
    data_dir: Option<&str>,
    node_id: &NodeId,
    network: &Arc<dyn Transport>,
    event_bus: &EventBus,
) -> Result<Arc<ActorSystem>, PyErr> {
    if let Some(dir) = data_dir {
        let db_path = std::path::Path::new(dir).join("actor");
        std::fs::create_dir_all(&db_path).map_err(crate::common::ActantError::StorageIo)?;
        let store = crate::store::Store::open(&db_path).map_err(|e| {
            crate::common::ActantError::Storage(format!("failed to open actor store: {}", e))
        })?;
        let checkpoint = crate::store::CheckpointManager::new(store.clone());
        let wal_path = std::path::Path::new(dir).join("actor.wal");
        let wal_writer = crate::store::WalWriter::open_with_sync(&wal_path, true).map_err(|e| {
            crate::common::ActantError::Storage(format!("failed to open actor WAL: {}", e))
        })?;
        Ok(Arc::new(
            ActorSystem::new()
                .with_node_id(node_id.clone())
                .with_network(network.clone())
                .with_event_bus(event_bus.clone())
                .with_wal(wal_writer, store)
                .with_checkpoint(checkpoint),
        ))
    } else {
        Ok(Arc::new(
            ActorSystem::new()
                .with_node_id(node_id.clone())
                .with_network(network.clone())
                .with_event_bus(event_bus.clone()),
        ))
    }
}

/// 初始化 orchestrator，可选持久化。
pub(crate) async fn init_orchestrator(
    data_dir: Option<&str>,
    node_id: &NodeId,
    config: &crate::common::ActantConfig,
) -> Result<Arc<Orchestrator>, PyErr> {
    if let Some(dir) = data_dir {
        let db_path = std::path::Path::new(dir).join("orchestrator");
        std::fs::create_dir_all(&db_path).map_err(crate::common::ActantError::StorageIo)?;
        let store = crate::store::Store::open(&db_path).map_err(|e| {
            crate::common::ActantError::Storage(format!("failed to open store: {}", e))
        })?;
        Ok(Arc::new(
            Orchestrator::recover(store, config.clone())
                .await
                .map_err(|e| {
                    crate::common::ActantError::Workflow(format!(
                        "orchestrator recover failed: {}",
                        e
                    ))
                })?
                .with_node_id(node_id.clone()),
        ))
    } else {
        Ok(Arc::new(
            Orchestrator::new()
                .with_node_id(node_id.clone())
                .with_config(config.clone()),
        ))
    }
}

/// [init_worker] 的参数，分组以缩短参数列表。
pub(crate) struct WorkerInitParams<'a> {
    pub node_id: &'a NodeId,
    pub network: &'a Arc<dyn Transport>,
    pub event_bus: EventBus,
    pub scheduler: &'a Arc<dyn Scheduler>,
    pub worker_config: &'a crate::common::WorkerConfig,
    pub actor_system: Arc<crate::actor::ActorSystem>,
    pub task_dispatcher: Arc<dyn TaskDispatcher>,
    pub failover: &'a Arc<crate::orchestrator::FailoverManager>,
    pub tokio_handle: tokio::runtime::Handle,
}

/// 初始化 worker runtime，容量回调已连接到 failover manager。
pub(crate) fn init_worker(params: WorkerInitParams<'_>) -> WorkerRuntime {
    let failover_cb = params.failover.clone();
    WorkerRuntime::new(
        params.node_id.clone(),
        params.network.clone(),
        params.event_bus,
        params.scheduler.clone(),
        params.task_dispatcher,
        params.worker_config,
        params.tokio_handle,
    )
    .with_actor_system(params.actor_system)
    .with_capacity_callback(Arc::new(move |available, max| {
        failover_cb.update_local_capacity(available, max);
    }))
}

/// 将 Python 可调用对象注册为 registry 中的任务处理器。
pub(crate) fn register_python_tasks(registry: &TaskRegistry, tasks: HashMap<String, Py<PyAny>>) {
    tracing::debug!("registering {} python tasks", tasks.len());
    for (task_name, python_fn) in tasks {
        let task_name_for_log = task_name.clone();
        registry.register(&task_name, move |payload, cancel_flag| {
            tracing::trace!("python handler invoked for {}", task_name_for_log);
            let t0 = std::time::Instant::now();
            // 解释器已由 Python 宿主初始化（cdylib）。
            let result = Python::attach(|py| {
                let fn_ref = python_fn.bind(py);
                let py_payload = PyBytes::new(py, &payload);
                let py_token = Py::new(py, crate::py::cancel::PyCancelToken::new(cancel_flag))
                    .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))?;
                let result = fn_ref
                    .call1((py_payload, py_token))
                    .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))?;
                result
                    .extract::<Vec<u8>>()
                    .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))
            });
            let elapsed_ms = t0.elapsed().as_millis() as u64;
            tracing::trace!(
                "python handler for {} took {} ms",
                task_name_for_log,
                elapsed_ms
            );
            crate::metrics::observe_python_handler_ms(elapsed_ms);
            result
        });
    }
}

#[cfg(test)]
mod tests {
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
        // 相对路径是 operator 合法选择，不应拒绝
        assert!(validate_data_dir("./actant-data").is_ok());
        assert!(validate_data_dir("actant-data").is_ok());
    }

    #[test]
    fn normal_absolute_path_accepted() {
        // 不存在的绝对路径合法（create_dir_all 会创建）
        assert!(validate_data_dir("/tmp/actant-test-does-not-exist-xyz").is_ok());
    }

    #[test]
    fn parent_component_accepted() {
        // .. 组件是 operator 合法选择，不拒绝
        assert!(validate_data_dir("../actant-data").is_ok());
        assert!(validate_data_dir("../../actant-data").is_ok());
    }

    #[test]
    fn system_root_rejected() {
        // 系统根目录必须拒绝（规范化后精确匹配）
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
        // 规范化解析符号链接：指向系统目录的符号链接应被拒绝
        // /etc 在 macOS 上通常真实存在
        let result = validate_data_dir("/etc/./"); // trailing slash + . should canonicalize to /etc
        assert!(
            matches!(result, Err(ActantError::Config(_))),
            "data_dir=/etc/. should canonicalize and be rejected, got {:?}",
            result
        );
    }

    #[test]
    fn subdir_of_system_dir_accepted() {
        // 系统目录的专用子目录是合法选择（如 /etc/actant 不一定存在，
        // 但即使存在也不应被精确匹配拒绝）
        // 用一个确定不存在的子路径验证
        assert!(validate_data_dir("/etc/actant-nonexistent-xyz-123").is_ok());
    }
}
