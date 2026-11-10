//! Python `RoutePolicy` 桥：把用户 callable 接入 Rust 路由决策点。
//!
//! Python 侧以 `Runtime(route_policy=fn)` 传入
//! `fn(ctx: dict, candidates: list[dict]) -> str | tuple[str, str] | None`。
//!
//! # 为什么用 `try_attach` 而非 `attach`
//!
//! [`RoutePolicy::select_target`] 是同步方法，调用发生在 tokio worker 线程上。
//! 若在此处阻塞等待 GIL，而主线程正持 GIL 等待任务结果，双方互等即死锁。
//! `try_attach` 拿不到 GIL 时立即返回 `None`，本模块据此降级到内置策略。
//!
//! # 失败语义
//!
//! 回调抛异常、返回非法值、或目标节点没有 endpoint 地址，一律降级而非 panic：
//! `select_target` 运行在 Actor 的 workflow 执行循环内，一次 panic 会毒化同一
//! Actor 上的全部工作流。

use std::collections::BTreeMap;
use std::sync::Arc;

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use actant_core::runtime::workflow::{RouteCandidate, RouteContext, RouteDecision, RoutePolicy};

/// Python 回调不可用或失败时接管的内置策略。
type Fallback = Arc<dyn RoutePolicy>;

/// 用 Python callable 实现的 [`RoutePolicy`]。
pub(crate) struct PyRoutePolicy {
    /// 用户回调，签名 `(ctx: dict, candidates: list[dict]) -> 决策`。
    func: Py<PyAny>,
    /// 回调不可用时的内置回退策略。
    fallback: Fallback,
}

impl PyRoutePolicy {
    pub(crate) fn new(func: Py<PyAny>, fallback: Fallback) -> Self {
        Self { func, fallback }
    }
}

impl RoutePolicy for PyRoutePolicy {
    fn select_target(&self, ctx: &RouteContext, candidates: &[RouteCandidate]) -> RouteDecision {
        // 闭包返回 `PyResult<Option<RouteDecision>>`：`Ok(None)` 表示回调显式弃权，
        // 交由内置策略决定；`Err` 表示回调失败。
        let outcome = Python::try_attach(|py| {
            let py_ctx = context_to_py(py, ctx)?;
            let py_candidates = candidates_to_py(py, candidates)?;
            let reply = self.func.bind(py).call1((py_ctx, py_candidates))?;
            parse_decision(&reply, candidates)
        });

        match outcome {
            Some(Ok(Some(decision))) => decision,
            Some(Ok(None)) => {
                tracing::debug!("python route_policy returned None; using builtin policy");
                self.fallback.select_target(ctx, candidates)
            }
            Some(Err(error)) => {
                tracing::warn!(
                    error = %error,
                    "python route_policy failed; falling back to builtin policy"
                );
                self.fallback.select_target(ctx, candidates)
            }
            None => {
                tracing::warn!(
                    "python route_policy skipped: GIL unavailable on routing thread; \
                     falling back to builtin policy"
                );
                self.fallback.select_target(ctx, candidates)
            }
        }
    }
}

/// `RouteContext` → Python dict（键名与 Python 侧文档一致）。
fn context_to_py<'py>(py: Python<'py>, ctx: &RouteContext) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("local_role", ctx.local_role.as_str())?;
    dict.set_item("local_draining", ctx.local_draining)?;
    dict.set_item("local_available_slots", ctx.local_available_slots)?;
    dict.set_item("heartbeat_ttl_ms", ctx.heartbeat_ttl_ms)?;
    dict.set_item("local_labels", labels_to_py(py, &ctx.local_labels)?)?;
    dict.set_item(
        "local_available_quotas",
        quotas_to_py(py, &ctx.local_available_quotas)?,
    )?;
    dict.set_item("required_tags", tags_to_py(py, &ctx.required_tags)?)?;
    match ctx.required_quota.as_deref() {
        Some(quota) => dict.set_item("required_quota", quota)?,
        None => dict.set_item("required_quota", py.None())?,
    }
    Ok(dict)
}

/// `[RouteCandidate]` → Python list[dict]。
fn candidates_to_py<'py>(
    py: Python<'py>,
    candidates: &[RouteCandidate],
) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty(py);
    for candidate in candidates {
        let dict = PyDict::new(py);
        dict.set_item("node_id", candidate.node_id.as_str())?;
        match candidate.endpoint_addr.as_deref() {
            Some(addr) => dict.set_item("endpoint_addr", addr)?,
            None => dict.set_item("endpoint_addr", py.None())?,
        }
        dict.set_item("available_slots", candidate.available_slots)?;
        dict.set_item("max_slots", candidate.max_slots)?;
        dict.set_item("role", candidate.role.as_str())?;
        dict.set_item("draining", candidate.draining)?;
        dict.set_item("last_heartbeat_ms", candidate.last_heartbeat_ms)?;
        dict.set_item("labels", labels_to_py(py, &candidate.labels)?)?;
        dict.set_item(
            "available_quotas",
            quotas_to_py(py, &candidate.available_quotas)?,
        )?;
        list.append(dict)?;
    }
    Ok(list)
}

fn labels_to_py<'py>(
    py: Python<'py>,
    labels: &BTreeMap<String, String>,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (key, value) in labels {
        dict.set_item(key, value)?;
    }
    Ok(dict)
}

fn quotas_to_py<'py>(
    py: Python<'py>,
    quotas: &BTreeMap<String, u32>,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (key, value) in quotas {
        dict.set_item(key, *value)?;
    }
    Ok(dict)
}

fn tags_to_py<'py>(py: Python<'py>, tags: &[String]) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty(py);
    for tag in tags {
        list.append(tag)?;
    }
    Ok(list)
}

/// 解析回调返回值。
///
/// 接受三种形状：
/// - `"local"` / `"defer"`（大小写不敏感）
/// - `("remote", node_id)` / `("defer", reason)`
/// - `None`：弃权，交回内置策略
fn parse_decision(
    reply: &Bound<'_, PyAny>,
    candidates: &[RouteCandidate],
) -> PyResult<Option<RouteDecision>> {
    if reply.is_none() {
        return Ok(None);
    }
    if let Ok(name) = reply.extract::<String>() {
        return match name.to_ascii_lowercase().as_str() {
            "local" => Ok(Some(RouteDecision::Local)),
            "defer" => Ok(Some(RouteDecision::Defer {
                reason: "deferred by python route policy".to_string(),
            })),
            other => Err(PyValueError::new_err(format!(
                "unknown route decision '{other}': expected 'local', 'defer', \
                 ('remote', node_id) or ('defer', reason)"
            ))),
        };
    }
    if let Ok((kind, argument)) = reply.extract::<(String, String)>() {
        return match kind.to_ascii_lowercase().as_str() {
            "remote" => Ok(Some(remote_decision(candidates, &argument))),
            "defer" => Ok(Some(RouteDecision::Defer { reason: argument })),
            other => Err(PyValueError::new_err(format!(
                "unknown route decision '{other}': expected 'remote' or 'defer'"
            ))),
        };
    }
    Err(PyTypeError::new_err(
        "route_policy callable must return 'local', 'defer', ('remote', node_id), \
         ('defer', reason) or None",
    ))
}

/// 把 Python 选择的 `node_id` 转为 `Remote` 决策。
///
/// `RouteDecision::Remote` 要求非空 `endpoint_addr`，而候选快照里该字段是
/// `Option`：节点尚未完成地址交换时为空。两种情况都降级为 `Defer` 并说明原因。
fn remote_decision(candidates: &[RouteCandidate], node_id: &str) -> RouteDecision {
    let Some(candidate) = candidates
        .iter()
        .find(|candidate| candidate.node_id.as_str() == node_id)
    else {
        return RouteDecision::Defer {
            reason: format!("python route policy chose '{node_id}', which is not a candidate"),
        };
    };
    match candidate.endpoint_addr.as_ref() {
        Some(addr) => RouteDecision::Remote {
            node_id: candidate.node_id.clone(),
            endpoint_addr: addr.clone(),
        },
        None => RouteDecision::Defer {
            reason: format!("candidate '{node_id}' has no endpoint address yet"),
        },
    }
}
