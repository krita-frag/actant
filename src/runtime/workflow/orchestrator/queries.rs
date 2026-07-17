//! Orchestrator 的 `queries` 职责子模块。
//!
//! 负责只读查询：状态快照、DAG 读取、结果聚合、重试信息、过期工作流等。

use crate::common::payload::unpack_payload;
use crate::common::serialization::serialize_rkyv;
use crate::common::{TaskId, WorkflowId};
use crate::runtime::workflow::{Dag, Phase, WorkflowExecution};

use super::{keys::*, Orchestrator};

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
            let dag_bytes = serialize_rkyv(&slot.dag).ok()?;
            let exec_bytes = serialize_rkyv(&slot.execution).ok()?;
            let pending_bytes = serialize_rkyv(&slot.pending).ok()?;
            return Some((dag_bytes, exec_bytes, pending_bytes));
        }
        if let Some(ref store) = self.store {
            let dag = store.get(&dag_key(workflow_id)).await.ok().flatten()?;
            let exec = store.get(&exec_key(workflow_id)).await.ok().flatten()?;
            let pending = store.get(&pending_key(workflow_id)).await.ok().flatten()?;
            return Some((dag, exec, pending));
        }
        None
    }

    pub fn remove_active_workflow(&self, workflow_id: &WorkflowId) {
        self.state.remove_workflow(workflow_id);
    }

    /// 强制删除一个工作流及其持久化数据。
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
            store.get(&key).await.ok().flatten()
        } else {
            // 内存路径：与 store 路径一致，将所有已完成任务的结果打包为 group。
            // 使用 collected_results() + pack_group 保证与 store 路径一致。
            let slot = self.state.slots.get(workflow_id)?;
            let results = slot.execution.collected_results();
            if results.is_empty() {
                None
            } else {
                Some(crate::common::pack_group(&results))
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

    pub fn get_expired_workflow_ids(&self) -> Vec<WorkflowId> {
        self.state.expired_workflow_ids()
    }
}
