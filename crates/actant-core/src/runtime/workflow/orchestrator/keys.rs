//! Store key 生成与 task payload 构造。
//!
//! 这些纯函数将 workflow_id 映射为 Store key，
//! 以及在有前驱依赖时构造带 MAC 签名的 task payload。

use crate::common::{
    Result, TaskId, WorkflowId, STORE_KEY_DAG, STORE_KEY_EVENT_SEQ, STORE_KEY_EXEC,
    STORE_KEY_PENDING, STORE_KEY_RESULT, STORE_KEY_SIGNAL_BUF, STORE_KEY_WAIT,
};
use crate::runtime::workflow::{Dag, WorkflowExecution};

pub(super) fn dag_key(wf_id: &WorkflowId) -> String {
    format!("{}{}", STORE_KEY_DAG, wf_id.as_str())
}

pub(super) fn exec_key(wf_id: &WorkflowId) -> String {
    format!("{}{}", STORE_KEY_EXEC, wf_id.as_str())
}

pub(super) fn pending_key(wf_id: &WorkflowId) -> String {
    format!("{}{}", STORE_KEY_PENDING, wf_id.as_str())
}

pub(super) fn result_key(wf_id: &WorkflowId) -> String {
    format!("{}{}", STORE_KEY_RESULT, wf_id.as_str())
}

pub(super) fn wait_key(wf_id: &WorkflowId) -> String {
    format!("{}{}", STORE_KEY_WAIT, wf_id.as_str())
}

/// 信号缓冲快照键：与 [`wait_key`] 同批落盘 / 同批删除。
pub(super) fn signal_buf_key(wf_id: &WorkflowId) -> String {
    format!("{}{}", STORE_KEY_SIGNAL_BUF, wf_id.as_str())
}

pub(super) fn event_seq_key(wf_id: &WorkflowId) -> String {
    format!("{}{}", STORE_KEY_EVENT_SEQ, wf_id.as_str())
}

/// 构造带 MAC 签名的 task payload。
///
/// 节点 payload 即 Python 提交侧构建的 v2 envelope（函数 + 已解析参数 +
/// 控制头部，自足完整——flow 语义下上游结果在提交方父进程解析后内联进
/// 参数）。当前仅对 payload 做 MAC 签名包装。
pub(super) fn build_task_payload(
    _dag: &Dag,
    _execution: &WorkflowExecution,
    _task_id: &TaskId,
    default_payload: &[u8],
    signing_key: &[u8],
) -> Result<Vec<u8>> {
    crate::common::payload::sign(signing_key, default_payload)
        .map_err(|e| crate::common::ActantError::Internal(format!("payload sign: {}", e)))
}
