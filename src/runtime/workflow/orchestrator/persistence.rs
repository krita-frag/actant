//! Orchestrator 的 `persistence` 职责子模块。
//!
//! 负责工作流状态恢复、事件日志记录、后台落盘、工作流迁移与淘汰。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::common::{
    serialization::serialize_rkyv, ActantConfig, Result, TaskId, WorkflowId, STORE_KEY_DAG,
    STORE_KEY_EXEC, STORE_KEY_PENDING,
};
use crate::runtime::state::event_log::EventLog;
use crate::runtime::state::{HybridLogicalClock, Store};
use crate::runtime::workflow::{Dag, Terminal, WorkflowExecution};

use super::{keys::*, types::*, Orchestrator};

impl Orchestrator {
    pub(crate) fn log_event(&self, payload: WorkflowEventPayload) {
        if let Some(log) = self.event_log.as_ref() {
            let topic = payload.topic();
            match postcard::to_allocvec(&payload) {
                Ok(bytes) => {
                    if let Err(e) = log.append(&topic, &bytes) {
                        tracing::warn!(error = %e, topic = %topic, "failed to append workflow event");
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, topic = %topic, "failed to serialize workflow event");
                }
            }
        }
        // 规则评估在 Python 编排循环中实现：远端订阅者通过
        // WireEnvelope::TaskDispatch / TaskResult 接收事件，业务规则在
        // Python 侧订阅 EventBus 自行处理。
    }

    /// 从持久化 [Store] 恢复 orchestrator 状态。
    ///
    /// 扫描所有已知前缀（dag、exec、pending）重建内存状态。Running 状态的
    /// 任务被重置为 Pending，以便重启后重新调度。
    ///
    /// **事件记录**：每个成功恢复的 workflow 会向 `event_log`（若提供）
    /// 追加一条 [`WorkflowEventPayload::Recovered`] 事件（`corrupt=false`），
    /// 损坏被移除的 workflow 追加 `corrupt=true` 事件。外部订阅者可通过
    /// `workflow:{id}` topic 观测恢复进度。
    ///
    /// **损坏处理**：dag / exec / pending 三类条目对同一 workflow 是绑定的
    /// （三者共同构成 `WorkflowSlot`）。若任一条目反序列化失败，则整个
    /// workflow 视为损坏——已创建的 slot 将被移除，对应的 store 条目也会
    /// 被清理，避免"exec 数据悬空而 dag 缺失"导致后续 `on_task_completed`
    /// 等操作因 `workflow not found` 失败。损坏事件通过 tracing::warn 记录。
    pub async fn recover(
        store: Store,
        config: ActantConfig,
        event_log: Option<Arc<dyn EventLog>>,
    ) -> Result<Self> {
        config.validate()?;
        let state = Arc::new(OrchestratorState::new());

        // 跟踪所有反序列化失败的 workflow ID。任意一类条目损坏即视为整个 workflow 损坏。
        let mut corrupt: HashSet<WorkflowId> = HashSet::new();

        let dag_entries = store.scan_prefix(STORE_KEY_DAG).await?;
        for (key, data) in dag_entries {
            let wf_id_str = key.strip_prefix(STORE_KEY_DAG).unwrap_or(&key);
            let wf_id = WorkflowId::from(wf_id_str.to_string());
            match rkyv::from_bytes::<Dag, rkyv::rancor::Error>(&data) {
                Ok(dag) => {
                    let task_ids: Vec<TaskId> = dag.nodes().map(|n| n.task_id.clone()).collect();
                    let mut pending: HashMap<TaskId, usize> = HashMap::new();
                    for node in dag.nodes() {
                        let pred_count = dag.predecessor_count(&node.task_id);
                        pending.insert(node.task_id.clone(), pred_count);
                    }
                    let execution = WorkflowExecution::new(wf_id.clone(), task_ids)
                        .with_failure_strategy(dag.failure_strategy);
                    state.insert_workflow(wf_id, dag, execution, pending);
                }
                Err(e) => {
                    tracing::warn!(
                        workflow = %wf_id_str,
                        error = ?e,
                        "recover: corrupt dag entry, marking workflow as corrupt",
                    );
                    corrupt.insert(wf_id);
                }
            }
        }

        let exec_entries = store.scan_prefix(STORE_KEY_EXEC).await?;
        for (key, data) in exec_entries {
            let wf_id_str = key.strip_prefix(STORE_KEY_EXEC).unwrap_or(&key);
            let wf_id = WorkflowId::from(wf_id_str.to_string());
            match rkyv::from_bytes::<WorkflowExecution, rkyv::rancor::Error>(&data) {
                Ok(mut execution) => {
                    if !execution.is_terminal() {
                        let task_ids: Vec<TaskId> = execution.tasks.keys().cloned().collect();
                        for tid in &task_ids {
                            execution.reset_task(tid, false, true);
                        }
                    }
                    if let Some(mut slot) = state.slots.get_mut(&wf_id) {
                        slot.execution = execution;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        workflow = %wf_id_str,
                        error = ?e,
                        "recover: corrupt exec entry, marking workflow as corrupt",
                    );
                    corrupt.insert(wf_id);
                }
            }
        }

        let pending_entries = store.scan_prefix(STORE_KEY_PENDING).await?;
        for (key, data) in pending_entries {
            let wf_id_str = key.strip_prefix(STORE_KEY_PENDING).unwrap_or(&key);
            let wf_id = WorkflowId::from(wf_id_str.to_string());
            match rkyv::from_bytes::<HashMap<TaskId, usize>, rkyv::rancor::Error>(&data) {
                Ok(pending) => {
                    if let Some(mut slot) = state.slots.get_mut(&wf_id) {
                        slot.pending = pending;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        workflow = %wf_id_str,
                        error = ?e,
                        "recover: corrupt pending entry, marking workflow as corrupt",
                    );
                    corrupt.insert(wf_id);
                }
            }
        }

        // 任何条目损坏的 workflow：移除内存 slot，并删除 store 中的所有三类条目，
        // 避免"exec 数据悬空而 dag 缺失"等不一致状态在后续操作中触发 workflow not found。
        for wf_id in &corrupt {
            state.remove_workflow(wf_id);
            // 删除 store 中的所有三类条目（若存在），清理孤儿数据。
            if let Err(e) = store.delete(&dag_key(wf_id)).await {
                tracing::warn!(workflow = %wf_id.as_str(), error = %e, "recover: failed to delete corrupt dag entry");
            }
            if let Err(e) = store.delete(&exec_key(wf_id)).await {
                tracing::warn!(workflow = %wf_id.as_str(), error = %e, "recover: failed to delete corrupt exec entry");
            }
            if let Err(e) = store.delete(&pending_key(wf_id)).await {
                tracing::warn!(workflow = %wf_id.as_str(), error = %e, "recover: failed to delete corrupt pending entry");
            }
            crate::metrics::inc_workflows_recovered_corrupt();
            tracing::warn!(
                workflow = %wf_id.as_str(),
                "workflow removed due to corrupt data; total corrupt={}",
                corrupt.len(),
            );
            // 记录损坏恢复事件（corrupt=true）供外部观测。
            Self::log_recovered_event(event_log.as_deref(), wf_id, 0, true);
        }

        // 为成功恢复的非损坏 workflow 记录恢复事件（corrupt=false）。
        // 遍历 state.slots，跳过已标记为 corrupt 的 workflow。
        for entry in state.slots.iter() {
            let wf_id = entry.key();
            if corrupt.contains(wf_id) {
                continue;
            }
            let task_count = entry.execution.tasks.len();
            Self::log_recovered_event(event_log.as_deref(), wf_id, task_count, false);
        }

        let orchestrator = Self {
            state,
            config: config.clone(),
            store: Some(store),
            event_log,
            condition_evaluator: None,
            node_id: None,
            hlc: Arc::new(HybridLogicalClock::with_max_drift_ms(
                config.network.hlc_max_drift_ms,
            )),
        };

        Ok(orchestrator)
    }

    /// 记录 [`WorkflowEventPayload::Recovered`] 事件到 event_log（若存在）。
    ///
    /// 此为 `recover` 内部的辅助函数——`recover` 是关联函数，构造 `Self` 前
    /// 无法调用实例方法 `log_event`，因此直接操作 `event_log` 引用。
    fn log_recovered_event(
        event_log: Option<&dyn EventLog>,
        workflow_id: &WorkflowId,
        task_count: usize,
        corrupt: bool,
    ) {
        let Some(log) = event_log else {
            return;
        };
        let payload = WorkflowEventPayload::Recovered {
            workflow_id: workflow_id.clone(),
            task_count,
            corrupt,
        };
        let topic = payload.topic();
        match postcard::to_allocvec(&payload) {
            Ok(bytes) => {
                if let Err(e) = log.append(&topic, &bytes) {
                    tracing::warn!(
                        error = %e,
                        topic = %topic,
                        "recover: failed to append Recovered event",
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    topic = %topic,
                    "recover: failed to serialize Recovered event",
                );
            }
        }
    }

    pub fn start_persist_flush(&self) -> tokio::sync::watch::Sender<bool> {
        let (cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);
        let state = self.state.clone();
        let store = self.store.clone();
        let flush_interval =
            std::time::Duration::from_millis(self.config.workflow.persist_flush_interval_ms);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_interval);
            loop {
                tokio::select! {
                    _ = cancel_rx.changed() => break,
                    _ = interval.tick() => {
                        let dirty_ids = state.drain_dirty();
                        if dirty_ids.is_empty() {
                            continue;
                        }
                        let Some(ref store) = store else {
                            continue;
                        };

                        let mut batch: Vec<(String, Vec<u8>)> = Vec::new();
                        for wf_id in &dirty_ids {
                            let Some(slot) = state.slots.get(wf_id) else {
                                continue;
                            };
                            if let Ok(exec_bytes) = serialize_rkyv(&slot.execution) {
                                batch.push((exec_key(wf_id), exec_bytes));
                            }
                            if let Ok(pending_bytes) = serialize_rkyv(&slot.pending) {
                                batch.push((pending_key(wf_id), pending_bytes));
                            }
                        }
                        if !batch.is_empty() {
                            if let Err(e) = store.put_batch(&batch).await {
                                tracing::error!("persist flush failed: {}", e);
                            }
                        }
                    }
                }
            }
        });

        cancel_tx
    }

    /// Immediately persist all dirty workflow states to the store.
    /// Useful for testing and graceful shutdown.
    pub async fn flush_dirty(&self) -> Result<()> {
        let Some(ref store) = self.store else {
            return Ok(());
        };
        let dirty_ids = self.state.drain_dirty();
        if dirty_ids.is_empty() {
            return Ok(());
        }
        let mut batch: Vec<(String, Vec<u8>)> = Vec::new();
        for wf_id in &dirty_ids {
            let Some(slot) = self.state.slots.get(wf_id) else {
                continue;
            };
            if let Ok(exec_bytes) = serialize_rkyv(&slot.execution) {
                batch.push((exec_key(wf_id), exec_bytes));
            }
            if let Ok(pending_bytes) = serialize_rkyv(&slot.pending) {
                batch.push((pending_key(wf_id), pending_bytes));
            }
        }
        if !batch.is_empty() {
            store.put_batch(&batch).await?;
        }
        Ok(())
    }

    pub async fn delete_workflow(&self, workflow_id: &WorkflowId) {
        self.evict_workflow(workflow_id).await;
    }

    pub async fn adopt_workflow(&self, workflow_id: &WorkflowId) -> Result<()> {
        if self.state.contains_workflow(workflow_id) {
            return Ok(());
        }

        if let Some(ref store) = self.store {
            let dag_key = dag_key(workflow_id);
            if let Ok(Some(data)) = store.get(&dag_key).await {
                if let Ok(dag) = rkyv::from_bytes::<Dag, rkyv::rancor::Error>(&data) {
                    let task_ids: Vec<TaskId> = dag.nodes().map(|n| n.task_id.clone()).collect();
                    let mut pending: HashMap<TaskId, usize> = HashMap::new();
                    for node in dag.nodes() {
                        let pred_count = dag.predecessor_count(&node.task_id);
                        pending.insert(node.task_id.clone(), pred_count);
                    }

                    let exec_key = exec_key(workflow_id);
                    let execution = if let Ok(Some(exec_data)) = store.get(&exec_key).await {
                        match rkyv::from_bytes::<WorkflowExecution, rkyv::rancor::Error>(&exec_data)
                        {
                            Ok(mut execution) => {
                                let task_ids: Vec<TaskId> =
                                    execution.tasks.keys().cloned().collect();
                                for tid in &task_ids {
                                    execution.reset_task(tid, false, true);
                                }
                                execution
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "corrupt exec entry for adopted workflow {}: {:?}",
                                    workflow_id.as_str(),
                                    e
                                );
                                WorkflowExecution::new(workflow_id.clone(), task_ids)
                            }
                        }
                    } else {
                        WorkflowExecution::new(workflow_id.clone(), task_ids)
                    };

                    self.state
                        .insert_workflow(workflow_id.clone(), dag, execution, pending);
                    return Ok(());
                }
            }
        }

        // 无本地数据可用 — 插入占位符以注册 workflow ID。
        // gossip 层会向远程节点请求完整状态，并在数据到达后
        // 调用 restore_workflow 覆盖此占位符（状态从 Loading → Ready）。
        self.state.insert_placeholder(workflow_id.clone());
        Ok(())
    }

    /// 从远程同步得到的字节恢复工作流状态。
    pub async fn restore_workflow_from_bytes(
        &self,
        workflow_id: &WorkflowId,
        dag_bytes: Option<Vec<u8>>,
        exec_bytes: Option<Vec<u8>>,
        pending_bytes: Option<Vec<u8>>,
    ) {
        use crate::common::serialization::deserialize_rkyv_value;

        let dag: Option<Dag> = dag_bytes.and_then(|b| deserialize_rkyv_value(&b).ok());
        let execution: Option<WorkflowExecution> =
            exec_bytes.and_then(|b| deserialize_rkyv_value(&b).ok());
        let pending: Option<HashMap<TaskId, usize>> =
            pending_bytes.and_then(|b| deserialize_rkyv_value(&b).ok());

        match (dag, execution) {
            (Some(dag), Some(mut exec)) => {
                let task_ids: Vec<TaskId> = exec.tasks.keys().cloned().collect();
                for tid in &task_ids {
                    exec.reset_task(tid, false, true);
                }
                let pending = pending.unwrap_or_else(|| {
                    let mut p = HashMap::new();
                    for node in dag.nodes() {
                        p.insert(node.task_id.clone(), dag.predecessor_count(&node.task_id));
                    }
                    p
                });
                self.restore_workflow(workflow_id, dag, exec, pending).await;
            }
            (Some(dag), None) => {
                let exec = WorkflowExecution::new(
                    workflow_id.clone(),
                    dag.nodes().map(|n| n.task_id.clone()).collect(),
                )
                .with_failure_strategy(dag.failure_strategy);
                let mut pending = HashMap::new();
                for node in dag.nodes() {
                    pending.insert(node.task_id.clone(), dag.predecessor_count(&node.task_id));
                }
                self.restore_workflow(workflow_id, dag, exec, pending).await;
            }
            _ => {
                tracing::warn!(
                    "incomplete state bytes for workflow {}, cannot restore",
                    workflow_id.as_str()
                );
            }
        }
    }

    /// Restore a workflow from remote state (DAG + execution + pending).
    /// Called by the gossip layer when a WorkflowStateResponse is received.
    /// Overwrites any existing placeholder entry for this workflow.
    pub async fn restore_workflow(
        &self,
        workflow_id: &WorkflowId,
        dag: Dag,
        execution: WorkflowExecution,
        pending: HashMap<TaskId, usize>,
    ) {
        // 若可用，批量持久化到本地 store（单次事务，减少 fsync）
        if let Some(ref store) = self.store {
            let mut batch = Vec::new();
            if let Ok(dag_bytes) = serialize_rkyv(&dag) {
                batch.push((dag_key(workflow_id), dag_bytes));
            }
            if let Ok(exec_bytes) = serialize_rkyv(&execution) {
                batch.push((exec_key(workflow_id), exec_bytes));
            }
            if let Ok(pending_bytes) = serialize_rkyv(&pending) {
                batch.push((pending_key(workflow_id), pending_bytes));
            }
            if !batch.is_empty() {
                if let Err(e) = store.put_batch(&batch).await {
                    tracing::error!(
                        "failed to persist workflow {} state: {}",
                        workflow_id.as_str(),
                        e
                    );
                }
            }
        }

        self.state
            .insert_workflow(workflow_id.clone(), dag, execution, pending);
    }

    pub async fn evict_workflow(&self, old_id: &WorkflowId) {
        self.state.remove_workflow(old_id);
        if let Some(ref s) = self.store {
            for key in [
                dag_key(old_id),
                exec_key(old_id),
                pending_key(old_id),
                result_key(old_id),
            ] {
                if let Err(e) = s.delete(&key).await {
                    tracing::warn!(
                        "failed to delete key during eviction of workflow {}: {}",
                        old_id.as_str(),
                        e
                    );
                }
            }
        }
    }
}
