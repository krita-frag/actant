use std::sync::Arc;

use async_trait::async_trait;
use pyo3::prelude::*;

use crate::common::{ActorMessage, ActorMessageResult, Result};
use crate::runtime::actor::Actor;

/// Python 端 dispatcher 的引用计数句柄。
///
/// 使用 `Arc<Py<PyAny>>` 而非裸 `Py<PyAny>`：actor 消息循环每次 handle_message
/// 都要把 dispatcher 引用移入 `spawn_blocking` 闭包，旧实现每次 `clone_ref(py)`
/// 都会触发 Python 对象的 refcount ++/--（atomic 操作 + 可能的 GIL）。
///
/// 改用 `Arc` 后：消息路径仅 Arc::clone（非原子 ++），完全无 GIL 交互；
/// Python refcount 在 actor 创建/销毁时各一次。在高频消息场景（>1k msg/s）
/// 显著降低 GIL 压力。
pub(crate) type SharedDispatcher = Arc<Py<PyAny>>;

pub struct PythonActor {
    actor_type: String,
    dispatcher: SharedDispatcher,
}

impl PythonActor {
    pub(crate) fn new(actor_type: String, dispatcher: SharedDispatcher) -> Self {
        Self {
            actor_type,
            dispatcher,
        }
    }
}

#[async_trait]
impl Actor for PythonActor {
    fn actor_type(&self) -> &str {
        &self.actor_type
    }

    async fn handle_message(&mut self, msg: ActorMessage) -> Result<ActorMessageResult> {
        // Arc::clone 是非原子引用计数（单线程内），无 GIL 交互。
        // 对比旧实现的 `Python::attach(|py| self.dispatcher.clone_ref(py))`：
        // 省去一次 GIL acquire + Python refcount atomic ++。
        let dispatcher = self.dispatcher.clone();
        // 直接解构 msg，避免 payload/method/id 的额外 clone。
        let ActorMessage {
            id: message_id,
            method,
            payload,
            ..
        } = msg;

        // spawn_blocking 直接在 tokio 的 blocking 池上执行 Python 调用，
        // 结果通过 JoinHandle 直接返回，无额外 channel 跳转。
        // 闭包内部 Python::attach 获取 GIL 调用 dispatcher。
        tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                let disp = dispatcher.bind(py);
                // PyBytes::new 内部 memcpy 一次：Vec<u8> → Python 堆缓冲。
                // 这是从 Rust 拥有的 Vec 跨 FFI 传给 Python 的必要拷贝，
                // 安全且 pyo3 已优化。若未来要零拷贝，需 unsafe + memoryview 协议。
                let py_payload = pyo3::types::PyBytes::new(py, &payload);
                let result_bytes: Vec<u8> = disp
                    .call_method1("_handle_message", (method.as_str(), py_payload))
                    .map_err(|e| {
                        crate::common::ActantError::Internal(format!("actor dispatch error: {}", e))
                    })?
                    .extract()
                    .map_err(|e| {
                        crate::common::ActantError::Internal(format!(
                            "actor result extraction error: {}",
                            e
                        ))
                    })?;

                Ok(ActorMessageResult {
                    message_id,
                    payload: result_bytes,
                    error: None,
                })
            })
        })
        .await
        .map_err(|e| crate::common::ActantError::Internal(e.to_string()))?
    }

    fn supports_state_persistence(&self) -> bool {
        true
    }

    fn save_state(&self) -> Result<Vec<u8>> {
        // save_state 在 ActorSystem 的同步回收路径中被 catch_unwind 调用，
        // 不在 async 上下文中，因此直接使用 Python::attach，
        // 避免 block_in_place 阻塞 tokio worker。
        // Arc::clone 无 GIL 交互。
        let dispatcher = self.dispatcher.clone();
        Python::attach(|py| {
            let disp = dispatcher.bind(py);
            let result: Vec<u8> = disp
                .call_method0("_save_state")
                .map_err(|e| {
                    crate::common::ActantError::Internal(format!("actor save_state error: {}", e))
                })?
                .extract()
                .map_err(|e| {
                    crate::common::ActantError::Internal(format!(
                        "actor save_state extraction error: {}",
                        e
                    ))
                })?;
            Ok(result)
        })
    }

    fn load_state(&mut self, state: &[u8]) -> Result<()> {
        // 同样使用 Arc::clone 避免 GIL 交互。
        let dispatcher = self.dispatcher.clone();
        Python::attach(|py| {
            let disp = dispatcher.bind(py);
            let py_state = pyo3::types::PyBytes::new(py, state);
            disp.call_method1("_load_state", (py_state,)).map_err(|e| {
                crate::common::ActantError::Internal(format!("actor load_state error: {}", e))
            })?;
            Ok(())
        })
    }
}
