//! Store key 生成与 task payload 构造。
//!
//! 这些纯函数将 workflow_id 映射为 Store key，
//! 以及在有前驱依赖时构造带 MAC 签名的 task payload。

use crate::common::{
    Result, TaskId, WorkflowId, STORE_KEY_DAG, STORE_KEY_EXEC, STORE_KEY_PENDING, STORE_KEY_RESULT,
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

/// 构造带 MAC 签名的 task payload：有前驱依赖时，把所有已产出结果的前驱
/// 结果统一前置到原始 payload 前并重新签名。
///
/// ## 约定：无结果的前驱被跳过
///
/// 收集 `upstream_results` 时，`filter_map` 会跳过没有 `result` 的前驱
/// （典型为条件分支中未被激活、已被标记 `Skipped` 的条件前驱——它们按
/// 设计不产生结果）。因此下游任务收到的上游结果数量可能少于其前驱数量，
/// 该行为是有意的：参数合并逻辑（Python dispatcher 侧）按顺序消费存在的
/// 结果，不依赖与前驱数量一一对应。
pub(super) fn build_task_payload(
    dag: &Dag,
    execution: &WorkflowExecution,
    task_id: &TaskId,
    default_payload: &[u8],
    signing_key: &[u8],
) -> Result<Vec<u8>> {
    let predecessors = dag.predecessors_of(task_id);
    if predecessors.is_empty() {
        return Ok(default_payload.to_vec());
    }
    // 先验证并解包原始任务 payload，再重新包装上游结果并签名。
    // 这保证了带依赖的任务 payload 仍具有端到端 MAC 保护。
    let raw_payload =
        crate::common::payload::verify(signing_key, default_payload).map_err(|e| {
            crate::common::ActantError::Internal(format!("payload verification: {}", e))
        })?;
    // 收集前驱任务结果（按 DAG 边顺序），统一前置到 default_payload。
    // Rust 核心不感知 default_payload 的 tag 类型 — 参数合并逻辑由 Python dispatcher 处理。
    let upstream_results: Vec<Vec<u8>> = predecessors
        .iter()
        .filter_map(|pred| {
            execution
                .tasks
                .get(&pred.task_id)
                .and_then(|t| t.result.clone())
        })
        .collect();
    let inner = crate::common::payload::pack_upstream_prefix(&upstream_results, &raw_payload)?;
    crate::common::payload::sign(signing_key, &inner)
        .map_err(|e| crate::common::ActantError::Internal(format!("payload sign: {}", e)))
}
