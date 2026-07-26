use std::sync::Arc;

use dashmap::DashMap;
use pyo3::prelude::*;

use crate::common::ActorId;
use crate::runtime::actor::ActorSystem;

use super::actor::{PythonActor, SharedDispatcher};
use super::gil_thread::GilThread;
use super::types::{future_into_py_iter, FutureResultToPy};

/// Python 端 actor dispatcher 引用映射的共享句柄。
///
/// 抽出为 pub(crate) 类型别名，便于 `PyRuntimeCore`
/// 与 `PyActorCore`（持有 dispatchers 共享）在构造时传递同一 Arc。
pub(crate) type SharedDispatchers = Arc<DashMap<String, Py<PyAny>>>;

#[pyclass(name = "_ActorCore", skip_from_py_object)]
#[derive(Clone)]
pub struct PyActorCore {
    system: Arc<ActorSystem>,
    tokio_handle: tokio::runtime::Handle,
    gil_thread: GilThread,
    /// 存储 actor 的 Python dispatcher 引用，以便原子重启，
    /// 无需 Python 端单独跟踪和传递 dispatcher。
    ///
    /// 由 `PyRuntimeCore` 在构造时传入，所有 `PyActorCore` 克隆共享同一映射。
    /// shutdown 时由 `PyRuntimeCore::shutdown` 统一清理。
    dispatchers: SharedDispatchers,
}

impl PyActorCore {
    /// 构造一个与 Rust ActorSystem 绑定的 Python actor 操作核心。
    ///
    /// 此构造方法不在 `#[pymethods]` 中暴露，因为 `ActorSystem` 等参数
    /// 无法从 Python 直接传递；它由 `_RuntimeCore` 在初始化时调用。
    ///
    /// `dispatchers` 由 `PyRuntimeCore` 持有并共享，确保所有
    /// `PyActorCore` 克隆使用同一映射，便于统一 shutdown 清理。
    pub(crate) fn new(
        system: Arc<ActorSystem>,
        tokio_handle: tokio::runtime::Handle,
        gil_thread: GilThread,
        dispatchers: SharedDispatchers,
    ) -> Self {
        Self {
            system,
            tokio_handle,
            gil_thread,
            dispatchers,
        }
    }
}

#[pymethods]
impl PyActorCore {
    #[pyo3(signature = (actor_id, method, payload))]
    #[tracing::instrument(name = "py.actor.call_method", level = "debug", skip(self, py, payload), fields(actor_id = %actor_id, method = %method))]
    fn call_method(
        &self,
        py: Python<'_>,
        actor_id: String,
        method: String,
        payload: Vec<u8>,
    ) -> PyResult<Py<PyAny>> {
        let actor_id_obj = ActorId::from(actor_id);
        let system = self.system.clone();

        future_into_py_iter(
            py,
            self.tokio_handle.clone(),
            &self.gil_thread,
            async move {
                match system.call(&actor_id_obj, &method, payload).await {
                    Ok(result) => {
                        if let Some(err) = result.error {
                            Python::attach(|_py| {
                                FutureResultToPy::Err(pyo3::PyErr::from(
                                    crate::common::ActantError::from(err),
                                ))
                            })
                        } else {
                            Python::attach(|py| {
                                FutureResultToPy::Value(
                                    pyo3::types::PyBytes::new(py, &result.payload)
                                        .into_any()
                                        .unbind(),
                                )
                            })
                        }
                    }
                    Err(e) => Python::attach(|_py| FutureResultToPy::Err(pyo3::PyErr::from(e))),
                }
            },
        )
        .map(|b| b.unbind())
    }

    /// 按 actor 类型跨节点路由调用（A2）。
    ///
    /// 与 `call_method` 的区别：调用方只需提供 `actor_type`（如 `"workflow"`、
    /// `"scheduler"`），由 ActorSystem 内置的 `ActorRouter` 根据注册表选择
    /// 承载该类型的节点。若本节点有该类型的本地 actor 实例，优先本地调用；
    /// 否则通过 `DirectRequest::ActorCallByType` 转发到远端节点。
    ///
    /// 返回值与 `call_method` 相同：成功时为 bytes payload，actor 返回错误时
    /// 抛出对应的 `ActantError` 子类。
    #[pyo3(signature = (actor_type, method, payload))]
    #[tracing::instrument(name = "py.actor.call_method_by_type", level = "debug", skip(self, py, payload), fields(actor_type = %actor_type, method = %method))]
    fn call_method_by_type(
        &self,
        py: Python<'_>,
        actor_type: String,
        method: String,
        payload: Vec<u8>,
    ) -> PyResult<Py<PyAny>> {
        let system = self.system.clone();

        future_into_py_iter(
            py,
            self.tokio_handle.clone(),
            &self.gil_thread,
            async move {
                match system.call_by_type(&actor_type, &method, payload).await {
                    Ok(result) => {
                        if let Some(err) = result.error {
                            Python::attach(|_py| {
                                FutureResultToPy::Err(pyo3::PyErr::from(
                                    crate::common::ActantError::from(err),
                                ))
                            })
                        } else {
                            Python::attach(|py| {
                                FutureResultToPy::Value(
                                    pyo3::types::PyBytes::new(py, &result.payload)
                                        .into_any()
                                        .unbind(),
                                )
                            })
                        }
                    }
                    Err(e) => Python::attach(|_py| FutureResultToPy::Err(pyo3::PyErr::from(e))),
                }
            },
        )
        .map(|b| b.unbind())
    }

    /// 创建并生成一个新的 actor，使用指定的 actor_id、actor_type 和 dispatcher。
    ///
    /// 这是 Python 端创建 actor 的**唯一入口**。调用此方法会：
    /// 1. 在 `dispatchers` 映射中注册 dispatcher（用于后续 restart_actor）；
    /// 2. 在 ActorSystem 中生成 `PythonActor` 实例。
    ///
    /// 若 actor_id 已存在，返回 `AlreadyExists` 错误。
    /// 若需重启已存在的 actor，应使用 [`PyActorCore::restart_actor`]。
    #[tracing::instrument(name = "py.actor.spawn", level = "info", skip(self, py, dispatcher), fields(actor_id = %actor_id, actor_type = %actor_type))]
    fn spawn_actor(
        &self,
        py: Python<'_>,
        actor_id: String,
        actor_type: String,
        dispatcher: Py<PyAny>,
    ) -> PyResult<()> {
        let system = self.system.clone();
        let dispatchers = self.dispatchers.clone();
        let handle = self.tokio_handle.clone();

        // 在持 GIL 状态下检查并插入 dispatcher 引用。
        // 提前检查避免无谓的 system.spawn 调用 + dispatcher 泄漏。
        if dispatchers.contains_key(&actor_id) {
            return Err(pyo3::PyErr::from(
                crate::common::ActantError::AlreadyExists(format!(
                    "actor {} already has a dispatcher",
                    actor_id
                )),
            ));
        }
        // dispatcher: Py<PyAny> 是 Python 端传入的引用。
        // 在 dispatchers 映射中存一份 clone_ref（Python refcount +1），
        // 供后续 restart_actor 使用。原 dispatcher 移入 Arc<Py<PyAny>>
        // 交给 PythonActor，不再有额外 refcount 操作。
        dispatchers.insert(actor_id.clone(), dispatcher.clone_ref(py));
        let shared_dispatcher: SharedDispatcher = Arc::new(dispatcher);

        let aid = ActorId::from(actor_id);
        py.detach(|| {
            handle.block_on(async {
                let python_actor = PythonActor::new(actor_type, shared_dispatcher);
                system.spawn(aid, python_actor).await
            })
        })
        .map_err(pyo3::PyErr::from)?;
        Ok(())
    }

    #[tracing::instrument(name = "py.actor.stop", level = "debug", skip(self, py), fields(actor_id = %actor_id))]
    fn stop_actor(&self, py: Python<'_>, actor_id: String) -> PyResult<()> {
        let system = self.system.clone();
        let handle = self.tokio_handle.clone();
        let dispatchers = self.dispatchers.clone();
        let id_for_cleanup = actor_id.clone();
        py.detach(|| handle.block_on(async { system.stop(&ActorId::from(actor_id)).await }))
            .map_err(pyo3::PyErr::from)?;
        // 清理 dispatcher 引用，避免 actor 停止后 Python 可调用对象泄漏。
        // restart_actor 不经过此路径（直接调用 system.kill），故不受影响。
        // 未找到 dispatcher 时仅 debug 日志，因为 actor 可能由其他路径创建
        // （老版本调用方、其他节点），此处不是错误。
        if dispatchers.remove(&id_for_cleanup).is_none() {
            tracing::debug!(
                actor_id = %id_for_cleanup,
                "actor stopped but no dispatcher registered (possibly created via legacy path)"
            );
        }
        Ok(())
    }

    #[tracing::instrument(name = "py.actor.kill", level = "debug", skip(self, py), fields(actor_id = %actor_id))]
    fn kill_actor(&self, py: Python<'_>, actor_id: String) -> PyResult<()> {
        let system = self.system.clone();
        let dispatchers = self.dispatchers.clone();
        let id_for_cleanup = actor_id.clone();
        py.detach(move || system.kill(&ActorId::from(actor_id)))
            .map_err(pyo3::PyErr::from)?;
        // 清理 dispatcher 引用，避免 actor 终止后 Python 可调用对象泄漏。
        if dispatchers.remove(&id_for_cleanup).is_none() {
            tracing::debug!(
                actor_id = %id_for_cleanup,
                "actor killed but no dispatcher registered (possibly created via legacy path)"
            );
        }
        Ok(())
    }

    /// 原子重启 actor：终止现有 actor 并使用存储的 dispatcher 重新生成。
    /// 这是 actor 重启的唯一真实来源 — Python supervision 应调用此方法，
    /// 而非分别调用 kill_actor + create_actor_with_id，
    /// 后者存在消息可能丢失的竞态窗口。
    #[tracing::instrument(name = "py.actor.restart", level = "info", skip(self, py), fields(actor_id = %actor_id, actor_type = %actor_type))]
    fn restart_actor(&self, py: Python<'_>, actor_id: String, actor_type: String) -> PyResult<()> {
        let system = self.system.clone();
        let dispatchers = self.dispatchers.clone();
        let handle = self.tokio_handle.clone();
        py.detach(|| {
            handle.block_on(async {
                let aid = ActorId::from(actor_id.clone());
                if let Err(e) = system.kill(&aid) {
                    return Err(crate::common::ActantError::Actor(format!(
                        "failed to kill actor {} during restart: {}",
                        actor_id, e
                    )));
                }
                // 取出 dispatcher 引用：clone_ref 创建新的 Py<PyAny>，
                // 原引用仍保留在 dispatchers 映射中供下次 restart 使用。
                // 这里直接获取 SharedDispatcher (Arc<Py<PyAny>>)，无需 clone_ref：
                // DashMap 的 read guard 短暂持有，Arc::clone 后立即释放 guard。
                let dispatcher: Option<SharedDispatcher> = Python::attach(|py| {
                    dispatchers
                        .get(&actor_id)
                        .map(|d| Arc::new(d.clone_ref(py)))
                });
                if let Some(disp) = dispatcher {
                    let python_actor = PythonActor::new(actor_type, disp);
                    system.spawn(aid, python_actor).await?;
                } else {
                    return Err(crate::common::ActantError::Actor(format!(
                        "no dispatcher registered for actor {}, cannot restart",
                        actor_id
                    )));
                }
                Ok(())
            })
        })
        .map_err(pyo3::PyErr::from)
    }

    fn actor_status(&self, actor_id: String) -> PyResult<String> {
        let id_for_err = actor_id.clone();
        match self.system.actor_status(&ActorId::from(actor_id)) {
            Some(status) => Ok(status.as_str().to_string()),
            None => Err(pyo3::PyErr::from(crate::common::ActantError::NotFound(
                format!("actor {} not found", id_for_err),
            ))),
        }
    }

    fn list_actors(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let system = self.system.clone();
        let actors = py.detach(move || system.list_actors());
        Ok(actors.into_iter().map(|a| a.to_string()).collect())
    }

    /// 返回本节点承载的 actor 类型集合（A2）。
    ///
    /// 此集合通过 `ActorSystem::spawn` 自动登记到 `ActorRegistry`，
    /// 并通过 `ActorRegistryGossipActor` 周期性广播给对端节点。
    fn local_actor_types(&self) -> Vec<String> {
        self.system.local_actor_types().into_iter().collect()
    }

    /// 返回当前已知的所有 actor 类型（本节点 + 远端节点）（A2）。
    ///
    /// 仅用于观测/调试：调用方不应依赖此快照做路由决策（注册表是动态的）。
    /// 返回值按字母序排列以保证可重复输出。
    fn known_actor_types(&self) -> Vec<String> {
        self.system.known_actor_types()
    }
}

/// 在 Python 模块上注册所有 actor 相关类。
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyActorCore>()?;
    Ok(())
}
