//! Orchestrator 的 `queries` 职责子模块。
//!
//! 负责只读查询：状态快照、DAG 读取、结果聚合、重试信息、过期工作流等。

use crate::common::payload::unpack_payload;
use crate::common::serialization::serialize_rkyv;
use crate::common::{RetryPolicy, TaskId, WorkflowId};
use crate::runtime::workflow::{Dag, Phase, WorkflowExecution};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{keys::*, Orchestrator};

/// DAG 结构快照（Python 暴露面，`get_dag` 的序列化形态）。
///
/// **刻意不含 `payload`**：它是签名的 cloudpickle 字节，对 Rust 完全不透明
///（架构不变量「载荷不透明」），且可能很大——经 actor 通道搬运整份 DAG 不划算。
/// 需要任务结果用 `get_workflow_state()` 的 `tasks[*].result`
///（任务结果经 `get_workflow_state()` 的 `tasks[*].result` 暴露，不随本快照搬运）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagSnapshot {
    pub workflow_id: String,
    pub failure_strategy: String,
    /// DAG 级默认重试策略。**必须暴露**：节点 `retry_policy=None` 时生效的是它，
    /// 缺了这一项就无法在 Python 侧复现 `Dag::effective_retry_policy` 的语义。
    pub default_retry_policy: Option<RetryPolicy>,
    pub nodes: Vec<DagNodeSnapshot>,
    pub edges: Vec<DagEdgeSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagNodeSnapshot {
    pub task_id: String,
    pub name: String,
    /// 前驱 task_id 列表（依赖边）。
    pub deps: Vec<String>,
    pub timeout_ms: Option<u64>,
    pub priority: i32,
    pub metadata: HashMap<String, String>,
    pub retry_policy: Option<RetryPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagEdgeSnapshot {
    pub from: String,
    pub to: String,
    pub condition: Option<String>,
}

impl Orchestrator {
    pub fn get_state(&self, workflow_id: &WorkflowId) -> Option<WorkflowExecution> {
        self.state
            .slots
            .get(workflow_id)
            .map(|s| s.execution.clone())
    }

    pub fn get_dag(&self, workflow_id: &WorkflowId) -> Option<Dag> {
        self.state.slots.get(workflow_id).map(|s| s.dag.clone())
    }

    /// DAG 结构快照：不含 payload 的可序列化形态，供 Python 暴露面使用。
    ///
    /// 与 [`Self::get_dag`] 的区别是**用途**：后者返回完整 `Dag`（含 payload），
    /// 供 Rust 内部与 gossip 状态同步；本方法只做"结构可见"，刻意不搬 payload。
    pub fn dag_snapshot(&self, workflow_id: &WorkflowId) -> Option<DagSnapshot> {
        let slot = self.state.slots.get(workflow_id)?;
        let dag = &slot.dag;
        Some(DagSnapshot {
            workflow_id: workflow_id.as_str().to_string(),
            failure_strategy: dag.failure_strategy.as_str().to_string(),
            default_retry_policy: dag.default_retry_policy.clone(),
            nodes: dag
                .nodes()
                .map(|n| DagNodeSnapshot {
                    task_id: n.task_id.as_str().to_string(),
                    name: n.name.clone(),
                    deps: dag
                        .predecessors_of(&n.task_id)
                        .iter()
                        .map(|p| p.task_id.as_str().to_string())
                        .collect(),
                    timeout_ms: n.timeout_ms,
                    priority: n.priority,
                    metadata: n.metadata.clone(),
                    retry_policy: n.retry_policy.clone(),
                })
                .collect(),
            edges: dag
                .edges()
                .map(|e| DagEdgeSnapshot {
                    from: e.from.as_str().to_string(),
                    to: e.to.as_str().to_string(),
                    condition: e.condition.clone(),
                })
                .collect(),
        })
    }

    /// 读取工作流的事件历史（审计出口）。
    ///
    /// `after = None` 表示从头读取。事件载荷是 postcard 编码的
    /// `WorkflowEventPayload`；调用方（PyO3 边界）负责解码出"过滤用字段"
    /// （kind / task_id / error），`payload` 本身对 Python 保持不透明
    ///（`payload` 对 Python 保持不透明，Rust 枚举字段增减不致破坏跨语言契约）。
    ///
    /// 无 event_log 或无该工作流时返回空（历史是**可选**的观测面，缺它不应
    /// 让查询失败）。
    pub fn workflow_history(
        &self,
        workflow_id: &WorkflowId,
        after: Option<crate::runtime::state::event_log::EventId>,
    ) -> Vec<crate::runtime::state::event_log::LogEntry> {
        let Some(log) = self.event_log.clone() else {
            return Vec::new();
        };
        if !self.state.contains_workflow(workflow_id) {
            return Vec::new();
        }
        let topic = format!("workflow:{}", workflow_id.as_str());
        log.read_after(&topic, after.as_ref()).unwrap_or_else(|e| {
            tracing::warn!(
                workflow = %workflow_id.as_str(),
                error = %e,
                "workflow_history: failed to read event log"
            );
            Vec::new()
        })
    }

    pub fn active_workflow_ids(&self) -> Vec<WorkflowId> {
        self.state.active_workflow_ids()
    }

    pub fn has_workflow(&self, workflow_id: &WorkflowId) -> bool {
        self.state.contains_workflow(workflow_id)
    }

    /// Serialize the current workflow state (dag, execution, pending) as rkyv bytes.
    /// Returns (dag_bytes, exec_bytes, pending_bytes) or None if workflow not found.
    pub async fn get_workflow_state_bytes(
        &self,
        workflow_id: &WorkflowId,
    ) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        if let Some(slot) = self.state.slots.get(workflow_id) {
            // 序列化失败时记录 warning 并返回 None，而非静默吞错——
            // 调用方（如状态恢复、跨节点同步）需区分 "工作流不存在" 与
            // "状态损坏"。序列化失败必须显式报错而非映射为 None——静默映射会把
            // 隐藏了 rkyv 序列化路径上的 bug。
            let dag_bytes = match serialize_rkyv(&slot.dag) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        workflow_id = %workflow_id.0,
                        "serialize dag failed: {}",
                        e
                    );
                    return None;
                }
            };
            let exec_bytes = match serialize_rkyv(&slot.execution) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        workflow_id = %workflow_id.0,
                        "serialize execution failed: {}",
                        e
                    );
                    return None;
                }
            };
            let pending_bytes = match serialize_rkyv(&slot.pending) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        workflow_id = %workflow_id.0,
                        "serialize pending failed: {}",
                        e
                    );
                    return None;
                }
            };
            return Some((dag_bytes, exec_bytes, pending_bytes));
        }
        if let Some(ref store) = self.store {
            // store.get 失败时记录 warning，避免持久化错误被当作 "未找到" 处理。
            let dag = match store.get(&dag_key(workflow_id)).await {
                Ok(v) => v?,
                Err(e) => {
                    tracing::warn!(
                        workflow_id = %workflow_id.0,
                        "store get dag failed: {}",
                        e
                    );
                    return None;
                }
            };
            let exec = match store.get(&exec_key(workflow_id)).await {
                Ok(v) => v?,
                Err(e) => {
                    tracing::warn!(
                        workflow_id = %workflow_id.0,
                        "store get exec failed: {}",
                        e
                    );
                    return None;
                }
            };
            let pending = match store.get(&pending_key(workflow_id)).await {
                Ok(v) => v?,
                Err(e) => {
                    tracing::warn!(
                        workflow_id = %workflow_id.0,
                        "store get pending failed: {}",
                        e
                    );
                    return None;
                }
            };
            return Some((dag, exec, pending));
        }
        None
    }

    pub fn remove_active_workflow(&self, workflow_id: &WorkflowId) {
        self.state.remove_workflow(workflow_id);
    }

    /// 返回指定工作流中处于 `Running` 状态的任务 ID 列表。
    ///
    /// 工作流不存在时返回空列表。用于崩溃故障转移时枚举需要重新调度
    /// 的在途任务（见 `reschedule_running_tasks`）。
    pub fn get_running_task_ids(&self, workflow_id: &WorkflowId) -> Vec<TaskId> {
        if let Some(slot) = self.state.slots.get(workflow_id) {
            slot.execution
                .tasks
                .iter()
                .filter(|(_, ts)| ts.state == Phase::Running)
                .map(|(id, _)| id.clone())
                .collect()
        } else {
            vec![]
        }
    }

    pub async fn get_result(&self, workflow_id: &WorkflowId) -> Option<Vec<u8>> {
        if let Some(ref store) = self.store {
            let key = result_key(workflow_id);
            // 持久化错误不应被静默吞为 "未找到"，记录 warning 以暴露底层故障。
            match store.get(&key).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        workflow_id = %workflow_id.0,
                        "store get result failed: {}",
                        e
                    );
                    None
                }
            }
        } else {
            // 内存路径：与 store 路径一致，将所有已完成任务的结果打包为 group。
            // 使用 collected_results() + pack_group 保证与 store 路径一致。
            let slot = self.state.slots.get(workflow_id)?;
            let results = slot.execution.collected_results();
            if results.is_empty() {
                None
            } else {
                // pack_group 仅在 results.len() 或任一 item.len() 超 u32::MAX 时失败，
                // 内存路径下结果集由本节点收集，不可能接近 u32::MAX，但仍需传播错误。
                match crate::common::pack_group(&results) {
                    Ok(bytes) => Some(bytes),
                    Err(e) => {
                        tracing::warn!(
                            workflow_id = %workflow_id.0,
                            "pack_group in memory path failed: {}",
                            e
                        );
                        None
                    }
                }
            }
        }
    }

    /// Returns unpacked task results for a completed workflow.
    /// Handles both single-result and group-encoded payloads.
    pub async fn get_results(&self, workflow_id: &WorkflowId) -> Option<Vec<Vec<u8>>> {
        let raw = self.get_result(workflow_id).await?;
        match unpack_payload(&raw) {
            Ok(items) => Some(items),
            Err(_) => Some(vec![raw]),
        }
    }

    pub fn get_retry_info(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Option<(u32, crate::common::RetryPolicy, u64)> {
        let slot = self.state.slots.get(workflow_id)?;
        let policy = slot.dag.effective_retry_policy(task_id)?;
        let task_state = slot.execution.tasks.get(task_id)?;
        let retry_count = task_state.retry_count();
        let delay_ms = Self::compute_retry_delay(retry_count, &policy);
        Some((retry_count, policy.clone(), delay_ms))
    }

    fn compute_retry_delay(retry_count: u32, policy: &crate::common::RetryPolicy) -> u64 {
        let base = policy.delay_ms as f64;
        let multiplier = policy.backoff_multiplier.powi(retry_count as i32);
        (base * multiplier).min(policy.max_delay_ms as f64) as u64
    }
}
