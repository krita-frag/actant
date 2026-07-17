//! PyO3 <-> Rust 类型转换与辅助原语统一入口。
//! 为每个内置 capability 提供实现 `PyAskCodec` / `PyPerformCodec` / `PyEmitCodec`
//! 的 marker 类型。`capability_registry!` 通过声明式映射完成分发。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{atomic, Arc, OnceLock};

use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::common::{ActorId, MessageId, NodeId, TaskId, WorkflowId};
use crate::py::gil_thread::GilThread;
use crate::runtime::actor::SupervisionEvent;
use crate::runtime::capability::{
    ActorEvent, ActorFailureCtx, ActorLifecycle, ActorMessageReq, ActorMessaging, ActorSupervision,
    Execute, ExecuteCtx, ExecuteOutcome, NodeEvent, NodeLifecycle, Serialization, SerializationReq,
    Store, StoreReq, SupervisionDecision, TaskEvent, TaskLifecycle, Transport, TransportReq,
    WorkflowEvent, WorkflowLifecycle,
};
use crate::runtime::dispatcher::CancelFlag;

/// `ask` effect 的 PyO3 编解码 trait。
///
/// `Runtime::ask` 返回 `Option<C::Response>`（第一个返回 `Some` 的 handler），
/// 因此 `encode_response` 接收的是 `Option<C::Response>`。
pub trait PyAskCodec<C: crate::runtime::capability::Capability> {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<C::Request>;
    fn encode_response(
        py: Python<'_>,
        resp: Option<C::Response>,
    ) -> PyResult<Option<Bound<'_, PyAny>>>;
}

/// `perform` effect 的 PyO3 编解码 trait。
pub trait PyPerformCodec<C: crate::runtime::capability::Capability> {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<C::Request>;
    fn encode_response(py: Python<'_>, resp: C::Response) -> PyResult<Bound<'_, PyAny>>;
}

/// `emit` effect 的 PyO3 解码 trait（emit 无返回值）。
pub trait PyEmitCodec<C: crate::runtime::capability::Capability> {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<C::Request>;
}

/// 将 Rust 请求编码为 Python 对象，并把 Python handler 的返回值解码回 Rust 响应。
///
/// 用于把 Python callable 注册为 Rust `CapabilityRuntime` handler，实现单一路径分发。
pub trait PyHandlerAskCodec<C: crate::runtime::capability::Capability> {
    fn encode_request<'py>(py: Python<'py>, req: &C::Request) -> PyResult<Bound<'py, PyAny>>;
    fn decode_response<'py>(
        py: Python<'py>,
        obj: &Bound<'py, PyAny>,
    ) -> PyResult<Option<C::Response>>;
}

pub trait PyHandlerPerformCodec<C: crate::runtime::capability::Capability> {
    fn encode_request<'py>(py: Python<'py>, req: &C::Request) -> PyResult<Bound<'py, PyAny>>;
    fn decode_response<'py>(py: Python<'py>, obj: &Bound<'py, PyAny>) -> PyResult<C::Response>;
}

pub trait PyHandlerEmitCodec<C: crate::runtime::capability::Capability> {
    fn encode_request<'py>(py: Python<'py>, req: &C::Request) -> PyResult<Bound<'py, PyAny>>;
}

fn get_string(ob: &Bound<'_, PyAny>, name: &str) -> PyResult<String> {
    ob.getattr(name)?.extract()
}

fn get_opt_string(ob: &Bound<'_, PyAny>, name: &str) -> PyResult<Option<String>> {
    let val = ob.getattr(name)?;
    if val.is_none() {
        Ok(None)
    } else {
        Ok(Some(val.extract()?))
    }
}

fn get_u32(ob: &Bound<'_, PyAny>, name: &str) -> PyResult<u32> {
    ob.getattr(name)?.extract()
}

fn get_u64(ob: &Bound<'_, PyAny>, name: &str) -> PyResult<u64> {
    ob.getattr(name)?.extract()
}

fn get_bytes(ob: &Bound<'_, PyAny>, name: &str) -> PyResult<Vec<u8>> {
    ob.getattr(name)?.extract()
}

fn node_id(ob: &Bound<'_, PyAny>, name: &str) -> PyResult<NodeId> {
    let s: String = get_string(ob, name)?;
    Ok(NodeId::from(s))
}

fn task_id(ob: &Bound<'_, PyAny>, name: &str) -> PyResult<TaskId> {
    let s: String = get_string(ob, name)?;
    Ok(TaskId::from(s))
}

fn workflow_id(ob: &Bound<'_, PyAny>, name: &str) -> PyResult<WorkflowId> {
    let s: String = get_string(ob, name)?;
    Ok(WorkflowId::from(s))
}

fn actor_id(ob: &Bound<'_, PyAny>, name: &str) -> PyResult<ActorId> {
    let s: String = get_string(ob, name)?;
    Ok(ActorId::from(s))
}

// 策略型 capability（Routing / Scheduling / RetryPolicy）由 Python 编排循环实现，
// Rust 核心不提供这些 marker 类型及其 codec。

pub struct ActorSupervisionCodec;

impl PyAskCodec<ActorSupervision> for ActorSupervisionCodec {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<ActorFailureCtx> {
        Ok(ActorFailureCtx {
            actor_id: actor_id(ob, "actor_id")?,
            error: get_string(ob, "error")?,
            restart_count: get_u32(ob, "restart_count")?,
            max_restarts: get_u32(ob, "max_restarts")?,
        })
    }

    fn encode_response(
        py: Python<'_>,
        resp: Option<Option<SupervisionDecision>>,
    ) -> PyResult<Option<Bound<'_, PyAny>>> {
        use pyo3::conversion::IntoPyObject;
        // 闭包内不能使用 `?`，改为显式 match 传播错误（H4 改进）。
        let d = match resp.flatten() {
            Some(d) => d,
            None => return Ok(None),
        };
        let s = match d {
            SupervisionDecision::Restart => "restart",
            SupervisionDecision::Stop => "stop",
            SupervisionDecision::Resume => "resume",
        };
        Ok(Some(s.into_pyobject(py)?.to_owned().into_any()))
    }
}

impl PyHandlerAskCodec<ActorSupervision> for ActorSupervisionCodec {
    fn encode_request<'py>(py: Python<'py>, req: &ActorFailureCtx) -> PyResult<Bound<'py, PyAny>> {
        let dict = dict_response(py);
        dict.set_item("actor_id", req.actor_id.to_string())?;
        dict.set_item("error", &req.error)?;
        dict.set_item("restart_count", req.restart_count)?;
        dict.set_item("max_restarts", req.max_restarts)?;
        Ok(dict.into_any())
    }

    fn decode_response<'py>(
        _py: Python<'py>,
        obj: &Bound<'py, PyAny>,
    ) -> PyResult<Option<Option<SupervisionDecision>>> {
        if obj.is_none() {
            return Ok(None);
        }
        let s: String = obj.extract()?;
        let decision = match s.as_str() {
            "restart" => SupervisionDecision::Restart,
            "stop" => SupervisionDecision::Stop,
            "resume" => SupervisionDecision::Resume,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unknown SupervisionDecision: {}",
                    s
                )))
            }
        };
        Ok(Some(Some(decision)))
    }
}
pub struct SerializationCodec;

impl PyPerformCodec<Serialization> for SerializationCodec {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<SerializationReq> {
        let op: String = get_string(ob, "op")?;
        match op.as_str() {
            "dump" => Ok(SerializationReq::Dump {
                payload: get_bytes(ob, "data")?,
            }),
            "load" => Ok(SerializationReq::Load {
                data: get_bytes(ob, "data")?,
            }),
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unknown SerializationReq op: {}",
                op
            ))),
        }
    }

    fn encode_response(
        py: Python<'_>,
        resp: Result<Vec<u8>, String>,
    ) -> PyResult<Bound<'_, PyAny>> {
        match resp {
            Ok(data) => bytes_response(py, data),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
        }
    }
}

impl PyHandlerPerformCodec<Serialization> for SerializationCodec {
    fn encode_request<'py>(py: Python<'py>, req: &SerializationReq) -> PyResult<Bound<'py, PyAny>> {
        let dict = dict_response(py);
        match req {
            SerializationReq::Dump { payload } => {
                dict.set_item("op", "dump")?;
                dict.set_item("data", bytes_response(py, payload.clone())?)?;
            }
            SerializationReq::Load { data } => {
                dict.set_item("op", "load")?;
                dict.set_item("data", bytes_response(py, data.clone())?)?;
            }
        }
        Ok(dict.into_any())
    }

    fn decode_response<'py>(
        _py: Python<'py>,
        obj: &Bound<'py, PyAny>,
    ) -> PyResult<Result<Vec<u8>, String>> {
        if obj.is_none() {
            return Ok(Ok(Vec::new()));
        }
        match obj.extract::<Vec<u8>>() {
            Ok(data) => Ok(Ok(data)),
            Err(e) => Ok(Err(e.to_string())),
        }
    }
}

pub struct TransportCodec;

impl PyPerformCodec<Transport> for TransportCodec {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<TransportReq> {
        let op: String = get_string(ob, "op")?;
        match op.as_str() {
            "send_task" => Ok(TransportReq::SendTask {
                target: node_id(ob, "target")?,
                payload: get_bytes(ob, "payload")?,
            }),
            "send_actor_message" => Ok(TransportReq::SendActorMessage {
                target: node_id(ob, "target")?,
                payload: get_bytes(ob, "payload")?,
            }),
            "broadcast_heartbeat" => Ok(TransportReq::BroadcastHeartbeat {
                payload: get_bytes(ob, "payload")?,
            }),
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unknown TransportReq op: {}",
                op
            ))),
        }
    }

    fn encode_response(py: Python<'_>, resp: Result<(), String>) -> PyResult<Bound<'_, PyAny>> {
        use pyo3::conversion::IntoPyObject;
        match resp {
            Ok(()) => {
                let b = true.into_pyobject(py)?;
                Ok(b.to_owned().into_any())
            }
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
        }
    }
}

impl PyHandlerPerformCodec<Transport> for TransportCodec {
    fn encode_request<'py>(py: Python<'py>, req: &TransportReq) -> PyResult<Bound<'py, PyAny>> {
        let dict = dict_response(py);
        match req {
            TransportReq::SendTask { target, payload } => {
                dict.set_item("op", "send_task")?;
                dict.set_item("target", target.to_string())?;
                dict.set_item("payload", bytes_response(py, payload.clone())?)?;
            }
            TransportReq::SendActorMessage { target, payload } => {
                dict.set_item("op", "send_actor_message")?;
                dict.set_item("target", target.to_string())?;
                dict.set_item("payload", bytes_response(py, payload.clone())?)?;
            }
            TransportReq::BroadcastHeartbeat { payload } => {
                dict.set_item("op", "broadcast_heartbeat")?;
                dict.set_item("payload", bytes_response(py, payload.clone())?)?;
            }
        }
        Ok(dict.into_any())
    }

    fn decode_response<'py>(
        _py: Python<'py>,
        _obj: &Bound<'py, PyAny>,
    ) -> PyResult<Result<(), String>> {
        Ok(Ok(()))
    }
}

pub struct StoreCodec;

impl PyPerformCodec<Store> for StoreCodec {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<StoreReq> {
        let op: String = get_string(ob, "op")?;
        match op.as_str() {
            "put" => Ok(StoreReq::Put {
                key: get_bytes(ob, "key")?,
                value: get_bytes(ob, "value")?,
            }),
            "get" => Ok(StoreReq::Get {
                key: get_bytes(ob, "key")?,
            }),
            "delete" => Ok(StoreReq::Delete {
                key: get_bytes(ob, "key")?,
            }),
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unknown StoreReq op: {}",
                op
            ))),
        }
    }

    fn encode_response(
        py: Python<'_>,
        resp: Result<Option<Vec<u8>>, String>,
    ) -> PyResult<Bound<'_, PyAny>> {
        use pyo3::conversion::IntoPyObject;
        match resp {
            Ok(None) => {
                let none = py.None().into_pyobject(py)?;
                Ok(none.to_owned().into_any())
            }
            Ok(Some(data)) => bytes_response(py, data),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
        }
    }
}

impl PyHandlerPerformCodec<Store> for StoreCodec {
    fn encode_request<'py>(py: Python<'py>, req: &StoreReq) -> PyResult<Bound<'py, PyAny>> {
        let dict = dict_response(py);
        match req {
            StoreReq::Put { key, value } => {
                dict.set_item("op", "put")?;
                dict.set_item("key", bytes_response(py, key.clone())?)?;
                dict.set_item("value", bytes_response(py, value.clone())?)?;
            }
            StoreReq::Get { key } => {
                dict.set_item("op", "get")?;
                dict.set_item("key", bytes_response(py, key.clone())?)?;
            }
            StoreReq::Delete { key } => {
                dict.set_item("op", "delete")?;
                dict.set_item("key", bytes_response(py, key.clone())?)?;
            }
        }
        Ok(dict.into_any())
    }

    fn decode_response<'py>(
        _py: Python<'py>,
        obj: &Bound<'py, PyAny>,
    ) -> PyResult<Result<Option<Vec<u8>>, String>> {
        if obj.is_none() {
            return Ok(Ok(None));
        }
        match obj.extract::<Vec<u8>>() {
            Ok(data) => Ok(Ok(Some(data))),
            Err(e) => Ok(Err(e.to_string())),
        }
    }
}

pub struct ExecuteCodec;

impl PyPerformCodec<Execute> for ExecuteCodec {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<ExecuteCtx> {
        Ok(ExecuteCtx {
            task_id: task_id(ob, "task_id")?,
            workflow_id: workflow_id(ob, "workflow_id")?,
            payload: get_bytes(ob, "payload")?,
            timeout_ms: get_u64(ob, "timeout_ms")?,
        })
    }

    fn encode_response(
        py: Python<'_>,
        resp: Result<ExecuteOutcome, String>,
    ) -> PyResult<Bound<'_, PyAny>> {
        use pyo3::types::PyBytes;
        match resp {
            Ok(outcome) => {
                let dict = dict_response(py);
                dict.set_item("task_id", outcome.task_id.to_string())?;
                dict.set_item("result_payload", PyBytes::new(py, &outcome.result_payload))?;
                Ok(dict.into_any())
            }
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
        }
    }
}

impl PyHandlerPerformCodec<Execute> for ExecuteCodec {
    fn encode_request<'py>(py: Python<'py>, req: &ExecuteCtx) -> PyResult<Bound<'py, PyAny>> {
        let dict = dict_response(py);
        dict.set_item("task_id", req.task_id.to_string())?;
        dict.set_item("workflow_id", req.workflow_id.to_string())?;
        dict.set_item("payload", bytes_response(py, req.payload.clone())?)?;
        dict.set_item("timeout_ms", req.timeout_ms)?;
        Ok(dict.into_any())
    }

    fn decode_response<'py>(
        _py: Python<'py>,
        obj: &Bound<'py, PyAny>,
    ) -> PyResult<Result<ExecuteOutcome, String>> {
        let outcome = ExecuteOutcome {
            task_id: TaskId::from(get_string(obj, "task_id")?),
            result_payload: get_bytes(obj, "result_payload").unwrap_or_default(),
        };
        Ok(Ok(outcome))
    }
}

pub struct ActorMessagingCodec;

impl PyPerformCodec<ActorMessaging> for ActorMessagingCodec {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<ActorMessageReq> {
        let sender: Option<String> = get_opt_string(ob, "sender")?;
        Ok(ActorMessageReq {
            target: actor_id(ob, "target")?,
            payload: get_bytes(ob, "payload")?,
            sender: sender.map(ActorId::from),
        })
    }

    fn encode_response(
        py: Python<'_>,
        resp: Result<MessageId, String>,
    ) -> PyResult<Bound<'_, PyAny>> {
        use pyo3::conversion::IntoPyObject;
        match resp {
            Ok(id) => {
                let s = id.to_string().into_pyobject(py)?;
                Ok(s.to_owned().into_any())
            }
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
        }
    }
}

impl PyHandlerPerformCodec<ActorMessaging> for ActorMessagingCodec {
    fn encode_request<'py>(py: Python<'py>, req: &ActorMessageReq) -> PyResult<Bound<'py, PyAny>> {
        let dict = dict_response(py);
        dict.set_item("target", req.target.to_string())?;
        dict.set_item("payload", bytes_response(py, req.payload.clone())?)?;
        dict.set_item(
            "sender",
            req.sender
                .as_ref()
                .map(|a| a.to_string())
                .unwrap_or_default(),
        )?;
        Ok(dict.into_any())
    }

    fn decode_response<'py>(
        _py: Python<'py>,
        obj: &Bound<'py, PyAny>,
    ) -> PyResult<Result<MessageId, String>> {
        match obj.extract::<String>() {
            Ok(s) => Ok(Ok(MessageId::from(s))),
            Err(e) => Ok(Err(e.to_string())),
        }
    }
}

pub struct TaskLifecycleCodec;

impl PyEmitCodec<TaskLifecycle> for TaskLifecycleCodec {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<TaskEvent> {
        let kind: String = get_string(ob, "kind")?;
        match kind.as_str() {
            "started" => Ok(TaskEvent::Started {
                task_id: task_id(ob, "task_id")?,
                workflow_id: workflow_id(ob, "workflow_id")?,
            }),
            "completed" => Ok(TaskEvent::Completed {
                task_id: task_id(ob, "task_id")?,
                result_payload: get_bytes(ob, "result_payload")?,
            }),
            "failed" => Ok(TaskEvent::Failed {
                task_id: task_id(ob, "task_id")?,
                error: get_string(ob, "error")?,
                attempt: get_u32(ob, "attempt")?,
            }),
            "retried" => Ok(TaskEvent::Retried {
                task_id: task_id(ob, "task_id")?,
                next_attempt: get_u32(ob, "next_attempt")?,
            }),
            "cancelled" => Ok(TaskEvent::Cancelled {
                task_id: task_id(ob, "task_id")?,
            }),
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unknown TaskEvent kind: {}",
                kind
            ))),
        }
    }
}

impl PyHandlerEmitCodec<TaskLifecycle> for TaskLifecycleCodec {
    fn encode_request<'py>(py: Python<'py>, req: &TaskEvent) -> PyResult<Bound<'py, PyAny>> {
        let dict = dict_response(py);
        match req {
            TaskEvent::Started {
                task_id,
                workflow_id,
            } => {
                dict.set_item("kind", "started")?;
                dict.set_item("task_id", task_id.to_string())?;
                dict.set_item("workflow_id", workflow_id.to_string())?;
            }
            TaskEvent::Completed {
                task_id,
                result_payload,
            } => {
                dict.set_item("kind", "completed")?;
                dict.set_item("task_id", task_id.to_string())?;
                dict.set_item("payload", bytes_response(py, result_payload.clone())?)?;
            }
            TaskEvent::Failed {
                task_id,
                error,
                attempt,
            } => {
                dict.set_item("kind", "failed")?;
                dict.set_item("task_id", task_id.to_string())?;
                dict.set_item("error", error)?;
                dict.set_item("attempt", attempt)?;
            }
            TaskEvent::Retried {
                task_id,
                next_attempt,
            } => {
                dict.set_item("kind", "retried")?;
                dict.set_item("task_id", task_id.to_string())?;
                dict.set_item("next_attempt", next_attempt)?;
            }
            TaskEvent::Cancelled { task_id } => {
                dict.set_item("kind", "cancelled")?;
                dict.set_item("task_id", task_id.to_string())?;
            }
        }
        Ok(dict.into_any())
    }
}

pub struct WorkflowLifecycleCodec;

impl PyEmitCodec<WorkflowLifecycle> for WorkflowLifecycleCodec {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<WorkflowEvent> {
        let kind: String = get_string(ob, "kind")?;
        match kind.as_str() {
            "submitted" => Ok(WorkflowEvent::Submitted {
                workflow_id: workflow_id(ob, "workflow_id")?,
            }),
            "started" => Ok(WorkflowEvent::Started {
                workflow_id: workflow_id(ob, "workflow_id")?,
            }),
            "completed" => Ok(WorkflowEvent::Completed {
                workflow_id: workflow_id(ob, "workflow_id")?,
            }),
            "failed" => Ok(WorkflowEvent::Failed {
                workflow_id: workflow_id(ob, "workflow_id")?,
                error: get_string(ob, "error")?,
            }),
            "cancelled" => Ok(WorkflowEvent::Cancelled {
                workflow_id: workflow_id(ob, "workflow_id")?,
            }),
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unknown WorkflowEvent kind: {}",
                kind
            ))),
        }
    }
}

impl PyHandlerEmitCodec<WorkflowLifecycle> for WorkflowLifecycleCodec {
    fn encode_request<'py>(py: Python<'py>, req: &WorkflowEvent) -> PyResult<Bound<'py, PyAny>> {
        let dict = dict_response(py);
        match req {
            WorkflowEvent::Submitted { workflow_id } => {
                dict.set_item("kind", "submitted")?;
                dict.set_item("workflow_id", workflow_id.to_string())?;
            }
            WorkflowEvent::Started { workflow_id } => {
                dict.set_item("kind", "started")?;
                dict.set_item("workflow_id", workflow_id.to_string())?;
            }
            WorkflowEvent::Completed { workflow_id } => {
                dict.set_item("kind", "completed")?;
                dict.set_item("workflow_id", workflow_id.to_string())?;
            }
            WorkflowEvent::Failed { workflow_id, error } => {
                dict.set_item("kind", "failed")?;
                dict.set_item("workflow_id", workflow_id.to_string())?;
                dict.set_item("error", error)?;
            }
            WorkflowEvent::Cancelled { workflow_id } => {
                dict.set_item("kind", "cancelled")?;
                dict.set_item("workflow_id", workflow_id.to_string())?;
            }
        }
        Ok(dict.into_any())
    }
}

pub struct NodeLifecycleCodec;

impl PyEmitCodec<NodeLifecycle> for NodeLifecycleCodec {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<NodeEvent> {
        let kind: String = get_string(ob, "kind")?;
        match kind.as_str() {
            "started" => Ok(NodeEvent::Started {
                node_id: node_id(ob, "node_id")?,
            }),
            "stopped" => Ok(NodeEvent::Stopped {
                node_id: node_id(ob, "node_id")?,
            }),
            "peer_joined" => Ok(NodeEvent::PeerJoined {
                peer_id: node_id(ob, "peer_id")?,
            }),
            "peer_left" => Ok(NodeEvent::PeerLeft {
                peer_id: node_id(ob, "peer_id")?,
            }),
            "heartbeat" => Ok(NodeEvent::Heartbeat {
                node_id: node_id(ob, "node_id")?,
                timestamp_ms: get_u64(ob, "timestamp_ms")?,
            }),
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unknown NodeEvent kind: {}",
                kind
            ))),
        }
    }
}

impl PyHandlerEmitCodec<NodeLifecycle> for NodeLifecycleCodec {
    fn encode_request<'py>(py: Python<'py>, req: &NodeEvent) -> PyResult<Bound<'py, PyAny>> {
        let dict = dict_response(py);
        match req {
            NodeEvent::Started { node_id } => {
                dict.set_item("kind", "started")?;
                dict.set_item("node_id", node_id.to_string())?;
            }
            NodeEvent::Stopped { node_id } => {
                dict.set_item("kind", "stopped")?;
                dict.set_item("node_id", node_id.to_string())?;
            }
            NodeEvent::PeerJoined { peer_id } => {
                dict.set_item("kind", "peer_joined")?;
                dict.set_item("peer_id", peer_id.to_string())?;
            }
            NodeEvent::PeerLeft { peer_id } => {
                dict.set_item("kind", "peer_left")?;
                dict.set_item("peer_id", peer_id.to_string())?;
            }
            NodeEvent::Heartbeat {
                node_id,
                timestamp_ms,
            } => {
                dict.set_item("kind", "heartbeat")?;
                dict.set_item("node_id", node_id.to_string())?;
                dict.set_item("timestamp_ms", timestamp_ms)?;
            }
        }
        Ok(dict.into_any())
    }
}

pub struct ActorLifecycleCodec;

impl PyEmitCodec<ActorLifecycle> for ActorLifecycleCodec {
    fn decode_request(ob: &Bound<'_, PyAny>) -> PyResult<ActorEvent> {
        let kind: String = get_string(ob, "kind")?;
        match kind.as_str() {
            "spawned" => Ok(ActorEvent::Spawned {
                actor_id: actor_id(ob, "actor_id")?,
                name: get_string(ob, "name")?,
            }),
            "stopped" => Ok(ActorEvent::Stopped {
                actor_id: actor_id(ob, "actor_id")?,
            }),
            "failed" => Ok(ActorEvent::Failed {
                actor_id: actor_id(ob, "actor_id")?,
                error: get_string(ob, "error")?,
            }),
            "restarted" => Ok(ActorEvent::Restarted {
                actor_id: actor_id(ob, "actor_id")?,
                attempt: get_u32(ob, "attempt")?,
            }),
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unknown ActorEvent kind: {}",
                kind
            ))),
        }
    }
}

impl PyHandlerEmitCodec<ActorLifecycle> for ActorLifecycleCodec {
    fn encode_request<'py>(py: Python<'py>, req: &ActorEvent) -> PyResult<Bound<'py, PyAny>> {
        let dict = dict_response(py);
        match req {
            ActorEvent::Spawned { actor_id, name } => {
                dict.set_item("kind", "spawned")?;
                dict.set_item("actor_id", actor_id.to_string())?;
                dict.set_item("name", name)?;
            }
            ActorEvent::Stopped { actor_id } => {
                dict.set_item("kind", "stopped")?;
                dict.set_item("actor_id", actor_id.to_string())?;
            }
            ActorEvent::Failed { actor_id, error } => {
                dict.set_item("kind", "failed")?;
                dict.set_item("actor_id", actor_id.to_string())?;
                dict.set_item("error", error)?;
            }
            ActorEvent::Restarted { actor_id, attempt } => {
                dict.set_item("kind", "restarted")?;
                dict.set_item("actor_id", actor_id.to_string())?;
                dict.set_item("attempt", attempt)?;
            }
        }
        Ok(dict.into_any())
    }
}

pub fn bytes_response<'py>(py: Python<'py>, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
    use pyo3::types::PyBytes;
    Ok(PyBytes::new(py, &data).into_any())
}

pub fn dict_response<'py>(py: Python<'py>) -> Bound<'py, PyDict> {
    PyDict::new(py)
}

pub fn ok_response<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    use pyo3::conversion::IntoPyObject;
    let b = true.into_pyobject(py)?;
    Ok(b.to_owned().into_any())
}

pub fn opt_string_response<'py>(
    py: Python<'py>,
    value: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    use pyo3::conversion::IntoPyObject;
    match value {
        Some(s) => {
            let b = s.into_pyobject(py)?;
            Ok(b.to_owned().into_any())
        }
        None => {
            let b = py.None().into_pyobject(py)?;
            Ok(b.to_owned().into_any())
        }
    }
}
pub(crate) enum FutureResultToPy {
    Value(Py<PyAny>),
    Err(PyErr),
}

/// 状态机：0 = Pending，1 = Completed
const STATE_PENDING: u8 = 0;
const STATE_COMPLETED: u8 = 1;

#[pyclass(frozen, freelist = 128, module = "actant.actant")]
pub(crate) struct PyAsyncAwaitable {
    state: atomic::AtomicU8,
    result: OnceLock<FutureResultToPy>,
}

impl PyAsyncAwaitable {
    pub(crate) fn new() -> Self {
        Self {
            state: atomic::AtomicU8::new(STATE_PENDING),
            result: OnceLock::new(),
        }
    }

    pub(crate) fn set_result(pyself: Py<Self>, py: Python, result: FutureResultToPy) {
        let rself = pyself.get();
        match rself.result.set(result) {
            Ok(_) => {}
            Err(_) => {
                eprintln!("set_result: result already set");
            }
        }
        rself
            .state
            .store(STATE_COMPLETED, atomic::Ordering::Release);
        pyself.drop_ref(py);
    }
}

#[pymethods]
impl PyAsyncAwaitable {
    fn __await__(pyself: PyRef<'_, Self>) -> PyRef<'_, Self> {
        pyself
    }

    fn __iter__(pyself: PyRef<'_, Self>) -> PyRef<'_, Self> {
        pyself
    }

    fn __next__(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        if self.state.load(atomic::Ordering::Acquire) == STATE_COMPLETED {
            let result = self.result.get().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "awaitable completed but result is missing (internal invariant violated)",
                )
            })?;
            return match result {
                FutureResultToPy::Value(v) => Err(PyStopIteration::new_err(v.clone_ref(py))),
                FutureResultToPy::Err(e) => Err(e.clone_ref(py)),
            };
        }

        Ok(Some(py.None()))
    }
}

pub(crate) fn future_into_py_iter<'py, F>(
    py: Python<'py>,
    handle: tokio::runtime::Handle,
    gil_thread: &GilThread,
    fut: F,
) -> PyResult<Bound<'py, PyAny>>
where
    F: std::future::Future<Output = FutureResultToPy> + Send + 'static,
{
    let aw = Py::new(py, PyAsyncAwaitable::new())?;
    let py_fut = aw.clone_ref(py);

    let gw = gil_thread.clone();
    handle.spawn(async move {
        let result = fut.await;
        let _ = gw.send(move |py| {
            PyAsyncAwaitable::set_result(aw, py, result);
        });
    });

    Ok(py_fut.into_any().into_bound(py))
}

/// 在 Rust 和 Python 之间共享的协作式取消 flag。
#[pyclass(frozen, name = "CancelToken", module = "actant")]
pub struct PyCancelToken {
    flag: CancelFlag,
}

impl PyCancelToken {
    pub fn new(flag: CancelFlag) -> Self {
        Self { flag }
    }

    pub fn flag(&self) -> &Arc<AtomicBool> {
        &self.flag
    }
}

#[pymethods]
impl PyCancelToken {
    fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    #[getter]
    fn cancelled(&self) -> bool {
        self.is_cancelled()
    }

    fn __bool__(&self) -> bool {
        self.is_cancelled()
    }

    fn __repr__(&self) -> String {
        format!("CancelToken(cancelled={})", self.is_cancelled())
    }
}

/// 通过事件总线交付给 Python 的统一事件类型。
#[pyclass(name = "_Event", skip_from_py_object)]
#[derive(Clone)]
pub struct PyEvent {
    #[pyo3(get)]
    pub kind: String,
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
    pub event_type: String,
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

/// 生成 Rust 内置 capability 的 PyO3 分发函数。
#[macro_export]
macro_rules! capability_registry {
    (
        ask: { $( $ask_name:literal => $ask_cap:ty => $ask_codec:path ),* $(,)? }
        perform: { $( $perf_name:literal => $perf_cap:ty => $perf_codec:path ),* $(,)? }
        emit: { $( $emit_name:literal => $emit_cap:ty => $emit_codec:path ),* $(,)? }
    ) => {
        fn dispatch_ask<'py>(
            py: pyo3::Python<'py>,
            name: &str,
            request: &pyo3::Bound<'_, pyo3::PyAny>,
            inner: std::sync::Arc<$crate::runtime::capability::CapabilityRuntime>,
            tokio: &tokio::runtime::Runtime,
        ) -> pyo3::PyResult<Option<pyo3::Bound<'py, pyo3::PyAny>>> {
            match name {
                $(
                    $ask_name => {
                        use $crate::py::types::PyAskCodec;
                        let _t = std::time::Instant::now();
                        let req = <$ask_codec as PyAskCodec<$ask_cap>>::decode_request(request)?;
                        // 释放 GIL 期间 block_on：避免 tokio worker pyo3_log 回调死锁。
                        let resp = py.detach(move || {
                            tokio.block_on(inner.ask::<$ask_cap>(req))
                        })
                            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                        tracing::trace!(cap = $ask_name, ms = _t.elapsed().as_millis() as u64, "dispatch_ask");
                        <$ask_codec as PyAskCodec<$ask_cap>>::encode_response(py, resp)
                    }
                ),*
                _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "ask capability {} not supported by Rust runtime",
                    name
                ))),
            }
        }

        fn dispatch_perform<'py>(
            py: pyo3::Python<'py>,
            name: &str,
            request: &pyo3::Bound<'_, pyo3::PyAny>,
            inner: std::sync::Arc<$crate::runtime::capability::CapabilityRuntime>,
            tokio: &tokio::runtime::Runtime,
        ) -> pyo3::PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
            match name {
                $(
                    $perf_name => {
                        use $crate::py::types::PyPerformCodec;
                        let _t = std::time::Instant::now();
                        let req = <$perf_codec as PyPerformCodec<$perf_cap>>::decode_request(request)?;
                        // 释放 GIL 期间 block_on：避免 tokio worker pyo3_log 回调死锁。
                        let resp = py.detach(move || {
                            tokio.block_on(inner.perform::<$perf_cap>(req))
                        })
                            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                        tracing::trace!(cap = $perf_name, ms = _t.elapsed().as_millis() as u64, "dispatch_perform");
                        <$perf_codec as PyPerformCodec<$perf_cap>>::encode_response(py, resp)
                    }
                ),*
                _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "perform capability {} not supported by Rust runtime",
                    name
                ))),
            }
        }

        fn dispatch_emit(
            py: pyo3::Python<'_>,
            name: &str,
            request: &pyo3::Bound<'_, pyo3::PyAny>,
            inner: std::sync::Arc<$crate::runtime::capability::CapabilityRuntime>,
            tokio: &tokio::runtime::Runtime,
        ) -> pyo3::PyResult<()> {
            match name {
                $(
                    $emit_name => {
                        use $crate::py::types::PyEmitCodec;
                        let _t = std::time::Instant::now();
                        let req = <$emit_codec as PyEmitCodec<$emit_cap>>::decode_request(request)?;
                        // 释放 GIL 期间 block_on：避免 tokio worker pyo3_log 回调死锁。
                        py.detach(move || {
                            tokio.block_on(inner.emit::<$emit_cap>(req))
                        })
                            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                        tracing::trace!(cap = $emit_name, ms = _t.elapsed().as_millis() as u64, "dispatch_emit");
                        Ok(())
                    }
                ),*
                _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "emit capability {} not supported by Rust runtime",
                    name
                ))),
            }
        }
    };
}

/// 在 Python 模块上注册 `types.rs` 中所有 PyO3 类型。
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyAsyncAwaitable>()?;
    m.add_class::<PyCancelToken>()?;
    m.add_class::<PyEvent>()?;
    m.add_class::<PyTaskCompletion>()?;
    m.add_class::<PyOrchestrationEvent>()?;
    m.add_class::<PySupervisionEventData>()?;
    Ok(())
}
