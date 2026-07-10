use std::sync::Arc;

use pyo3::prelude::*;

use crate::common::ActorId;
use crate::runtime::actor::ActorSystem;
use dashmap::DashMap;

use super::actor::PythonActor;
use super::gil_thread::GilThread;
use super::types::{future_into_py_iter, FutureResultToPy};

#[pyclass(name = "_ActorCore", skip_from_py_object)]
#[derive(Clone)]
pub struct PyActorCore {
    system: Arc<ActorSystem>,
    tokio_handle: tokio::runtime::Handle,
    gil_thread: GilThread,
    /// 存储 actor 的 Python dispatcher 引用，以便原子重启，
    /// 无需 Python 端单独跟踪和传递 dispatcher。
    dispatchers: Arc<DashMap<String, Py<PyAny>>>,
}

#[pymethods]
impl PyActorCore {
    #[pyo3(signature = (actor_id, method, payload))]
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
                                    crate::common::ActantError::Actor(err),
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

    fn stop_actor(&self, py: Python<'_>, actor_id: String) -> PyResult<()> {
        let system = self.system.clone();
        let handle = self.tokio_handle.clone();
        let dispatchers = self.dispatchers.clone();
        let id_for_cleanup = actor_id.clone();
        py.detach(|| handle.block_on(async { system.stop(&ActorId::from(actor_id)).await }))
            .map_err(pyo3::PyErr::from)?;
        // 清理 dispatcher 引用，避免 actor 停止后 Python 可调用对象泄漏。
        // restart_actor 不经过此路径（直接调用 system.kill），故不受影响。
        if dispatchers.remove(&id_for_cleanup).is_none() {
            // 可选：记录警告，actor 停止时未找到对应的 dispatcher
            eprintln!("warning: actor {} stopped but no dispatcher found", id_for_cleanup);
        }
        Ok(())
    }

    fn kill_actor(&self, py: Python<'_>, actor_id: String) -> PyResult<()> {
        let system = self.system.clone();
        let dispatchers = self.dispatchers.clone();
        let id_for_cleanup = actor_id.clone();
        py.detach(move || system.kill(&ActorId::from(actor_id)))
            .map_err(pyo3::PyErr::from)?;
        // 清理 dispatcher 引用，避免 actor 终止后 Python 可调用对象泄漏。
        // restart_actor 不经过此路径（直接调用 system.kill），故不受影响。
        if dispatchers.remove(&id_for_cleanup).is_none() {
            // 可选：记录警告，actor 终止时未找到对应的 dispatcher
            eprintln!("warning: actor {} killed but no dispatcher found", id_for_cleanup);
        }
        Ok(())
    }

    /// 原子重启 actor：终止现有 actor 并使用存储的 dispatcher 重新生成。
    /// 这是 actor 重启的唯一真实来源 — Python supervision 应调用此方法，
    /// 而非分别调用 kill_actor + create_actor_with_id，
    /// 后者存在消息可能丢失的竞态窗口。
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
                let dispatcher =
                    Python::attach(|py| dispatchers.get(&actor_id).map(|d| d.clone_ref(py)));
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
}

/// 在 Python 模块上注册所有 actor 相关类。
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyActorCore>()?;
    Ok(())
}
