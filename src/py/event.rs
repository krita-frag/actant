//! 事件桥接：Rust EventBus → Python asyncio 事件循环。

use std::sync::{Arc, RwLock};

use pyo3::prelude::*;

use super::gil_thread::GilThread;

use crate::actor::supervision::SupervisionEvent;
use crate::common::TaskCompletion;
use crate::event_bus::BusEvent;

/// PyEventBridge 中存储的 (event_loop, callback) 对。
type EventLoopCallback = (Py<PyAny>, Py<PyAny>);

// ---------------------------------------------------------------------------
// Python 端事件类型
// ---------------------------------------------------------------------------

/// 通过 `PyEventBridge` 交付给 Python 的统一事件类型。
#[pyclass(name = "_Event", skip_from_py_object)]
#[derive(Clone)]
pub struct PyEvent {
    #[pyo3(get)]
    pub kind: String, // "completion" | "orchestration" | "supervision"
    #[pyo3(get)]
    pub completion: Option<PyTaskCompletion>,
    #[pyo3(get)]
    pub orchestration: Option<PyOrchestrationEvent>,
    #[pyo3(get)]
    pub supervision: Option<PySupervisionEventData>,
}

#[pyclass(name = "_TaskCompletion", skip_from_py_object)]
#[derive(Clone)]
pub struct PyTaskCompletion {
    #[pyo3(get)]
    pub workflow_id: String,
    #[pyo3(get)]
    pub task_id: String,
    #[pyo3(get)]
    pub task_name: String,
    #[pyo3(get)]
    pub state: String,
    #[pyo3(get)]
    pub result: Option<Vec<u8>>,
    #[pyo3(get)]
    pub error: Option<String>,
    #[pyo3(get)]
    pub target_node: Option<String>,
}

#[pyclass(name = "_OrchestrationEvent", skip_from_py_object)]
#[derive(Clone)]
pub struct PyOrchestrationEvent {
    #[pyo3(get)]
    pub event_type: String,
    #[pyo3(get)]
    pub workflow_id: Option<String>,
    #[pyo3(get)]
    pub task_id: Option<String>,
    #[pyo3(get)]
    pub node_id: Option<String>,
    #[pyo3(get)]
    pub active_workflows: Option<Vec<String>>,
    #[pyo3(get)]
    pub data: Option<Vec<u8>>,
    #[pyo3(get)]
    pub state: String,
    #[pyo3(get)]
    pub available_capacity: Option<u32>,
    #[pyo3(get)]
    pub max_capacity: Option<u32>,
}

#[pyclass(name = "_SupervisionEventData", skip_from_py_object)]
#[derive(Clone)]
pub struct PySupervisionEventData {
    #[pyo3(get)]
    pub event_type: String, // "ActorStarted" | "ActorFailed" | "ActorStopped"
    #[pyo3(get)]
    pub actor_id: String,
    #[pyo3(get)]
    pub error: Option<String>,
}

impl From<SupervisionEvent> for PySupervisionEventData {
    fn from(event: SupervisionEvent) -> Self {
        match event {
            SupervisionEvent::ActorStarted { actor_id } => PySupervisionEventData {
                event_type: "ActorStarted".into(),
                actor_id: actor_id.to_string(),
                error: None,
            },
            SupervisionEvent::ActorFailed { actor_id, error } => PySupervisionEventData {
                event_type: "ActorFailed".into(),
                actor_id: actor_id.to_string(),
                error: Some(error),
            },
            SupervisionEvent::ActorStopped { actor_id } => PySupervisionEventData {
                event_type: "ActorStopped".into(),
                actor_id: actor_id.to_string(),
                error: None,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// 转换函数：Rust 类型 → Python 类型
// ---------------------------------------------------------------------------

/// 将 BusEvent 转换为 Python 端的 PyOrchestrationEvent。
fn convert_bus_event_to_py(event: &BusEvent) -> Option<PyOrchestrationEvent> {
    match event {
        BusEvent::DagUpdate(update) => Some(PyOrchestrationEvent {
            event_type: "DagStateUpdate".to_string(),
            workflow_id: Some(update.workflow_id.to_string()),
            task_id: Some(update.task_id.to_string()),
            node_id: None,
            active_workflows: None,
            data: match &update.task_state {
                crate::common::WireTaskState::Running => None,
                crate::common::WireTaskState::Completed { result } => Some(result.clone()),
                crate::common::WireTaskState::Failed { error } => Some(error.as_bytes().to_vec()),
                crate::common::WireTaskState::Cancelled => None,
                crate::common::WireTaskState::Skipped => None,
            },
            state: update.task_state.as_str().to_string(),
            available_capacity: None,
            max_capacity: None,
        }),
        BusEvent::Heartbeat(hb) => Some(PyOrchestrationEvent {
            event_type: "NodeHeartbeat".to_string(),
            workflow_id: None,
            task_id: None,
            node_id: Some(hb.node_id.to_string()),
            active_workflows: Some(hb.active_workflows.iter().map(|w| w.to_string()).collect()),
            data: None,
            state: String::new(),
            available_capacity: Some(hb.available_slots),
            max_capacity: Some(hb.max_slots),
        }),
        BusEvent::Claim(claim) => Some(PyOrchestrationEvent {
            event_type: "OrchestratorClaim".to_string(),
            workflow_id: Some(claim.workflow_id.to_string()),
            task_id: None,
            node_id: Some(claim.node_id.to_string()),
            active_workflows: None,
            data: None,
            state: String::new(),
            available_capacity: None,
            max_capacity: None,
        }),
        BusEvent::HeadsExchange(exchange) => Some(PyOrchestrationEvent {
            event_type: "HeadsExchange".to_string(),
            workflow_id: None,
            task_id: None,
            node_id: Some(exchange.node_id.to_string()),
            active_workflows: None,
            data: match postcard::to_allocvec(&exchange.heads) {
                Ok(bytes) if !bytes.is_empty() => Some(bytes),
                Ok(_) => None,
                Err(e) => {
                    tracing::error!("failed to serialize heads exchange: {:?}", e);
                    None
                }
            },
            state: String::new(),
            available_capacity: None,
            max_capacity: None,
        }),
        BusEvent::WorkerDraining { node_id } => Some(PyOrchestrationEvent {
            event_type: "WorkerDraining".to_string(),
            workflow_id: None,
            task_id: None,
            node_id: Some(node_id.to_string()),
            active_workflows: None,
            data: None,
            state: "Draining".to_string(),
            available_capacity: Some(0),
            max_capacity: Some(0),
        }),
        BusEvent::WorkerDrained { node_id } => Some(PyOrchestrationEvent {
            event_type: "WorkerDrained".to_string(),
            workflow_id: None,
            task_id: None,
            node_id: Some(node_id.to_string()),
            active_workflows: None,
            data: None,
            state: "Drained".to_string(),
            available_capacity: Some(0),
            max_capacity: Some(0),
        }),
        BusEvent::WorkerStopped { node_id } => Some(PyOrchestrationEvent {
            event_type: "WorkerStopped".to_string(),
            workflow_id: None,
            task_id: None,
            node_id: Some(node_id.to_string()),
            active_workflows: None,
            data: None,
            state: "Stopped".to_string(),
            available_capacity: Some(0),
            max_capacity: Some(0),
        }),
        _ => None,
    }
}

/// 将 Rust TaskCompletion 转换为 PyTaskCompletion。
fn task_completion_to_py(completion: &TaskCompletion) -> PyTaskCompletion {
    let (result, error) = match completion {
        TaskCompletion::Completed { result, .. } => (Some(result.clone()), None),
        TaskCompletion::Failed { error, .. } => (None, Some(error.clone())),
        TaskCompletion::Cancelled { .. } | TaskCompletion::Skipped { .. } => (None, None),
    };
    PyTaskCompletion {
        workflow_id: completion.workflow_id().to_string(),
        task_id: completion.task_id().to_string(),
        task_name: completion.task_name().to_string(),
        state: completion.as_str().to_string(),
        result,
        error,
        target_node: completion.target_node().as_ref().map(|n| n.to_string()),
    }
}

// ---------------------------------------------------------------------------
// PyEventCallback — 通过 call_soon_threadsafe 调度
// ---------------------------------------------------------------------------

/// 携带已转换 `PyEvent` 的 Python 可调用包装器。
/// 通过 `loop.call_soon_threadsafe` 调度到 Python 事件循环，
/// 因此回调在正确的线程上、已持有 GIL 的状态下运行。
#[pyclass(skip_from_py_object)]
pub struct PyEventCallback {
    /// 要调用的 Python 可调用对象：`callback(event)`。
    callback: Py<PyAny>,
    /// 作为参数传递的已转换事件。
    event: PyEvent,
}

#[pymethods]
impl PyEventCallback {
    fn __call__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.callback.call1(py, (self.event.clone(),))
    }
}

// ---------------------------------------------------------------------------
// PyEventBridge — on_publish 钩子
// ---------------------------------------------------------------------------

/// Rust EventBus 与 Python asyncio 事件循环之间的桥接。
///
/// 作为 `on_publish` 回调安装到 EventBus。事件发布时，
/// 桥接将其转换为 `PyEvent` 并通过 `call_soon_threadsafe`
/// 在 Python 事件循环上调度一个 `PyEventCallback`。
///
/// 使用 RwLock，因为回调在每个事件（热路径）上读取，
/// 仅在启动/关闭时写入。
///
/// 内部 tuple 为 `(event_loop, callback)` — 始终一起设置或清除，
/// 使无效状态不可表达。
pub struct PyEventBridge {
    state: Arc<RwLock<Option<EventLoopCallback>>>,
}

impl Default for PyEventBridge {
    fn default() -> Self {
        Self::new()
    }
}

impl PyEventBridge {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(None)),
        }
    }

    /// 安装 Python 回调和事件循环引用。
    /// 必须在持有 GIL 时调用。
    pub fn set_callback(&self, event_loop: Py<PyAny>, callback: Py<PyAny>) {
        *self.state.write().unwrap_or_else(|e| e.into_inner()) = Some((event_loop, callback));
    }

    /// 清除回调引用，打破阻止 GC 回收持有 Arc 句柄对象的
    /// Python→Rust→Python 引用循环。
    pub fn clear_callback(&self) {
        *self.state.write().unwrap_or_else(|e| e.into_inner()) = None;
    }

    /// 将 `BusEvent` 转换为 `PyEvent`，对不应转发到 Python 的事件返回 `None`。
    fn convert_event(event: &BusEvent) -> Option<PyEvent> {
        match event {
            BusEvent::TaskCompleted(completion) => Some(PyEvent {
                kind: "completion".to_string(),
                completion: Some(task_completion_to_py(completion)),
                orchestration: None,
                supervision: None,
            }),
            BusEvent::TaskFailed(completion) => Some(PyEvent {
                kind: "completion".to_string(),
                completion: Some(task_completion_to_py(completion)),
                orchestration: None,
                supervision: None,
            }),
            BusEvent::TaskCancelled(completion) => Some(PyEvent {
                kind: "completion".to_string(),
                completion: Some(task_completion_to_py(completion)),
                orchestration: None,
                supervision: None,
            }),
            BusEvent::TaskSkipped(completion) => Some(PyEvent {
                kind: "completion".to_string(),
                completion: Some(task_completion_to_py(completion)),
                orchestration: None,
                supervision: None,
            }),
            BusEvent::SupervisionEvent(se) => Some(PyEvent {
                kind: "supervision".to_string(),
                completion: None,
                orchestration: None,
                supervision: Some(PySupervisionEventData::from(se.clone())),
            }),
            _ => convert_bus_event_to_py(event).map(|py_event| PyEvent {
                kind: "orchestration".to_string(),
                completion: None,
                orchestration: Some(py_event),
                supervision: None,
            }),
        }
    }

    /// 为 EventBus 创建 `on_publish` 闭包。
    /// 使用 GIL worker 线程以避免热路径上的 `Python::attach` 开销。
    pub(crate) fn make_on_publish(
        &self,
        gil_thread: &GilThread,
    ) -> Arc<dyn Fn(&BusEvent) + Send + Sync> {
        let state = self.state.clone();
        let gw = gil_thread.clone();
        Arc::new(move |event: &BusEvent| {
            let t0 = std::time::Instant::now();
            let py_event = match Self::convert_event(event) {
                Some(e) => e,
                None => return,
            };

            let state = state.clone();
            let gw = gw.clone();
            let _ = gw.send(move |py| {
                let (event_loop, callback) = {
                    let guard = state.read().unwrap_or_else(|e| e.into_inner());
                    match &*guard {
                        Some((el, cb)) => (el.clone_ref(py), cb.clone_ref(py)),
                        None => return,
                    }
                };

                let callback_obj = match pyo3::Py::new(
                    py,
                    PyEventCallback {
                        callback,
                        event: py_event,
                    },
                ) {
                    Ok(cb) => cb,
                    Err(e) => {
                        tracing::warn!("failed to create PyEventCallback: {}", e);
                        return;
                    }
                };

                if let Err(e) = event_loop
                    .bind(py)
                    .call_method1("call_soon_threadsafe", (callback_obj.bind(py),))
                {
                    // RuntimeError 通常表示 event loop 已关闭，无需警告
                    let err_str = e.to_string();
                    if err_str.contains("RuntimeError") || err_str.contains("cannot schedule") {
                        tracing::debug!("event loop closed, dropping event: {}", e);
                    } else {
                        tracing::warn!("failed to call_soon_threadsafe for event: {}", e);
                    }
                }
                crate::metrics::observe_event_bridge_ms(t0.elapsed().as_millis() as u64);
            });
        })
    }
}

/// 在 Python 模块上注册所有事件相关类。
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyEvent>()?;
    m.add_class::<PyTaskCompletion>()?;
    m.add_class::<PyOrchestrationEvent>()?;
    m.add_class::<PySupervisionEventData>()?;
    m.add_class::<PyEventCallback>()?;
    Ok(())
}
