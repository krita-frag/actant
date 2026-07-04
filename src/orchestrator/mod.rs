//! Workflow orchestration: DAG submission, execution, and completion tracking.
//!
//! The [Orchestrator] is the central coordinator for workflow lifecycle:
//! - **Submission**: Accepts [Dag] structures and persists them via [Store].
//! - **Execution**: Computes root tasks, tracks pending dependencies, and
//!   emits ready [TaskDefinition]s as predecessors complete.
//! - **Completion**: Notifies waiters when a workflow reaches a terminal state.
//! - **Recovery**: [Orchestrator::recover] restores workflow state from storage
//!   after a restart, resetting in-flight tasks to Pending.
//! - **Eviction**: Completed workflows beyond a configurable retention count
//!   are automatically removed from memory and store.
//!
//! ## Sub-modules
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [dag] | DAG structure, edge validation, topological queries |
//! | [dag_state] | [WorkflowExecution] and [TaskState] types |
//! | [failover] | Lease-based orchestrator failover for distributed deployments |
//! | [gossip] | DAG state replication via gossipsub |
//! | [scheduler] | Task scheduling strategies (FIFO, priority-based) |

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::{DashMap, DashSet};

use crate::common::payload::unpack_payload;
use crate::common::{
    serialization::serialize_rkyv, ActantConfig, NodeId, Result, TaskDefinition, TaskId,
    WorkflowId, STORE_KEY_DAG, STORE_KEY_EXEC, STORE_KEY_PENDING, STORE_KEY_RESULT,
};
use crate::store::hlc::HybridLogicalClock;
use crate::store::Store;

pub(crate) mod dag;
pub(crate) mod dag_state;
pub(crate) mod failover;
pub(crate) mod gossip;
pub(crate) mod scheduler;

pub use dag::{Dag, DagNode};
pub(crate) use dag_state::WorkflowExecution;
pub use dag_state::{FailureScope, FailureStrategy, Phase, Terminal};
pub use failover::FailoverManager;
pub use gossip::DagGossip;
pub use scheduler::{FifoScheduler, PriorityScheduler, Scheduler};

#[derive(Debug, Clone)]
pub(crate) struct CompletionInfo {
    pub workflow_terminal: bool,
    pub ready_successors: Vec<TaskId>,
    /// Conditional edges from the completed task: (successor_task_id, condition_tag).
    /// The Python orchestration loop evaluates these conditions and activates
    /// the selected branches via `activate_conditional_successor`.
    pub conditional_edges: Vec<(TaskId, String)>,
}

struct ReadyResult {
    ready: Vec<TaskId>,
    conditional: Vec<(TaskId, String)>,
}

/// Per-workflow state container stored in a DashMap for fine-grained locking.
/// Each workflow's dag, execution, and pending counts are bundled together
/// so that operations on a single workflow never block other workflows.
pub(crate) struct WorkflowSlot {
    pub dag: Dag,
    pub execution: WorkflowExecution,
    pub pending: HashMap<TaskId, usize>,
}

/// Concurrent orchestrator state using per-workflow DashMap shards.
/// Eliminates the global RwLock bottleneck: different workflows can be
/// read and modified concurrently without contention.
pub struct OrchestratorState {
    slots: DashMap<WorkflowId, WorkflowSlot>,
    /// Per-workflow oneshot channels for terminal state notification.
    /// When a workflow reaches a terminal state, its oneshot sender is
    /// fired (if present), waking the specific waiter instantly — no
    /// polling required. This follows the same pattern as Actix/Ractor
    /// RPC: oneshot channel per request, resolved exactly once.
    terminal_oneshots: DashMap<WorkflowId, tokio::sync::oneshot::Sender<()>>,
    /// Workflow IDs whose execution state has been modified but not yet
    /// persisted. The background flush task drains this set periodically.
    dirty: DashSet<WorkflowId>,
}

impl Default for OrchestratorState {
    fn default() -> Self {
        Self::new()
    }
}

impl OrchestratorState {
    pub fn new() -> Self {
        Self {
            slots: DashMap::new(),
            terminal_oneshots: DashMap::new(),
            dirty: DashSet::new(),
        }
    }

    /// Returns a reference to the workflow slot if it exists.
    pub(crate) fn get_slot(
        &self,
        workflow_id: &WorkflowId,
    ) -> Option<dashmap::mapref::one::Ref<'_, WorkflowId, WorkflowSlot>> {
        self.slots.get(workflow_id)
    }

    /// Register a oneshot receiver for a specific workflow's terminal state.
    /// Returns the receiver that will be resolved when the workflow completes.
    /// This is the event-driven equivalent of polling `ready()` in a loop.
    ///
    /// Race-free: inserts the waiter FIRST, then checks terminal state. If the
    /// workflow completes between the check and the insert, `fire_terminal_oneshot`
    /// will find our registered sender and fire it. If the workflow was already
    /// terminal at insert time, we resolve immediately and clean up the entry.
    pub fn register_terminal_waiter(
        &self,
        workflow_id: WorkflowId,
    ) -> tokio::sync::oneshot::Receiver<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        // Insert BEFORE checking — this closes the race window. If the workflow
        // becomes terminal between our check and insert, fire_terminal_oneshot
        // will fire our sender instead of being a no-op.
        self.terminal_oneshots.insert(workflow_id.clone(), tx);

        // 现在检查是否已处于终态 — 若是，触发自身的 waiter。
        if let Some(slot) = self.slots.get(&workflow_id) {
            if slot.execution.is_terminal() {
                drop(slot);
                if let Some((_, tx)) = self.terminal_oneshots.remove(&workflow_id) {
                    let _ = tx.send(());
                }
                return rx;
            }
        }
        rx
    }

    /// Fire the oneshot for a workflow that has reached terminal state.
    /// Called from `notify_terminal()` and timeout watcher.
    fn fire_terminal_oneshot(&self, workflow_id: &WorkflowId) {
        if let Some((_, tx)) = self.terminal_oneshots.remove(workflow_id) {
            let _ = tx.send(());
        }
    }

    pub(crate) fn insert_workflow(
        &self,
        workflow_id: WorkflowId,
        dag: Dag,
        execution: WorkflowExecution,
        pending: HashMap<TaskId, usize>,
    ) {
        self.slots.insert(
            workflow_id,
            WorkflowSlot {
                dag,
                execution,
                pending,
            },
        );
    }

    pub(crate) fn remove_workflow(&self, workflow_id: &WorkflowId) {
        self.slots.remove(workflow_id);
        self.dirty.remove(workflow_id);
    }

    pub(crate) fn contains_workflow(&self, workflow_id: &WorkflowId) -> bool {
        self.slots.contains_key(workflow_id)
    }

    /// Mark a workflow as needing persistence. The background flush task
    /// will serialize and write it to the store.
    pub(crate) fn mark_dirty(&self, workflow_id: &WorkflowId) {
        self.dirty.insert(workflow_id.clone());
    }

    /// Drain all dirty workflow IDs, returning them for batch persistence.
    pub(crate) fn drain_dirty(&self) -> Vec<WorkflowId> {
        let ids: Vec<WorkflowId> = self.dirty.iter().map(|r| r.key().clone()).collect();
        for id in &ids {
            self.dirty.remove(id);
        }
        ids
    }

    pub(crate) fn active_workflow_ids(&self) -> Vec<WorkflowId> {
        self.slots
            .iter()
            .filter(|entry| !entry.execution.is_terminal())
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub(crate) fn expired_workflow_ids(&self) -> Vec<WorkflowId> {
        self.slots
            .iter()
            .filter(|entry| !entry.execution.is_terminal() && entry.execution.is_expired())
            .map(|entry| entry.key().clone())
            .collect()
    }
}

pub struct Orchestrator {
    state: Arc<OrchestratorState>,
    config: ActantConfig,
    store: Option<Store>,
    node_id: Option<NodeId>,
    hlc: Arc<HybridLogicalClock>,
}

impl Drop for Orchestrator {
    fn drop(&mut self) {
        tracing::debug!(
            "Orchestrator::drop — store is_some = {}",
            self.store.is_some()
        );
    }
}

impl Clone for Orchestrator {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            config: self.config.clone(),
            store: self.store.clone(),
            node_id: self.node_id.clone(),
            hlc: self.hlc.clone(),
        }
    }
}

impl Orchestrator {
    pub fn new() -> Self {
        Self {
            state: Arc::new(OrchestratorState::new()),
            config: ActantConfig::default(),
            store: None,
            node_id: None,
            hlc: Arc::new(HybridLogicalClock::new()),
        }
    }

    pub fn with_node_id(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub fn with_signing_key(mut self, key: Vec<u8>) -> Self {
        self.config.payload_signing_key = key;
        self
    }

    pub fn with_config(mut self, config: ActantConfig) -> Self {
        self.hlc = Arc::new(HybridLogicalClock::with_max_drift_ms(
            config.network.hlc_max_drift_ms,
        ));
        self.config = config;
        self
    }

    pub fn with_store(mut self, store: Store) -> Self {
        self.store = Some(store);
        self
    }

    /// Recovers orchestrator state from a persistent [Store].
    ///
    /// Scans all known prefixes (dag, exec, pending) and reconstructs
    /// the in-memory state. Running tasks are reset to Pending so they
    /// can be rescheduled after a restart.
    pub async fn recover(store: Store, config: ActantConfig) -> Result<Self> {
        config.validate()?;
        let state = Arc::new(OrchestratorState::new());

        let dag_entries = store.scan_prefix(STORE_KEY_DAG)?;
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
                    tracing::warn!("skipping corrupt dag entry {}: {:?}", wf_id_str, e);
                }
            }
        }

        let exec_entries = store.scan_prefix(STORE_KEY_EXEC)?;
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
                    tracing::warn!("skipping corrupt exec entry {}: {:?}", wf_id_str, e);
                }
            }
        }

        let pending_entries = store.scan_prefix(STORE_KEY_PENDING)?;
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
                    tracing::warn!("skipping corrupt pending entry {}: {:?}", wf_id_str, e);
                }
            }
        }

        let orchestrator = Self {
            state,
            config: config.clone(),
            store: Some(store),
            node_id: None,
            hlc: Arc::new(HybridLogicalClock::with_max_drift_ms(
                config.network.hlc_max_drift_ms,
            )),
        };

        Ok(orchestrator)
    }

    /// Submits a workflow DAG for execution.
    #[tracing::instrument(level = "debug", skip(self, dag), fields(workflow = %workflow_id, nodes = dag.nodes().count()))]
    pub async fn submit(&self, workflow_id: WorkflowId, dag: Dag) -> Result<()> {
        let task_ids: Vec<TaskId> = dag.nodes().map(|n| n.task_id.clone()).collect();

        let mut pending: HashMap<TaskId, usize> = HashMap::new();
        for node in dag.nodes() {
            let pred_count = dag.predecessor_count(&node.task_id);
            pending.insert(node.task_id.clone(), pred_count);
        }

        let execution = WorkflowExecution::new(workflow_id.clone(), task_ids)
            .with_failure_strategy(dag.failure_strategy);

        if let Some(ref store) = self.store {
            let dag_bytes = serialize_rkyv(&dag)?;
            let exec_bytes = serialize_rkyv(&execution)?;
            let pending_bytes = serialize_rkyv(&pending)?;
            store.put_batch(&[
                (dag_key(&workflow_id), dag_bytes),
                (exec_key(&workflow_id), exec_bytes),
                (pending_key(&workflow_id), pending_bytes),
            ])?;
        }

        self.state
            .insert_workflow(workflow_id, dag, execution, pending);

        crate::metrics::inc_workflows_submitted();
        crate::metrics::inc_active_workflows();
        Ok(())
    }

    pub async fn submit_with_timeout(
        &self,
        workflow_id: WorkflowId,
        dag: Dag,
        timeout_ms: u64,
    ) -> Result<()> {
        self.submit(workflow_id.clone(), dag).await?;
        if let Some(mut slot) = self.state.slots.get_mut(&workflow_id) {
            slot.execution.set_deadline_ms(timeout_ms);
        }
        Ok(())
    }

    /// Starts a workflow by marking it Running and returning root tasks.
    pub async fn start(&self, workflow_id: &WorkflowId) -> Result<Vec<TaskDefinition>> {
        let roots = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            slot.execution.mark_running();

            let origin_node = self.node_id.clone();
            slot.dag
                .roots()
                .into_iter()
                .map(|node| {
                    let attempt = slot
                        .execution
                        .tasks
                        .get(&node.task_id)
                        .map(|t| t.attempt())
                        .unwrap_or(0);
                    TaskDefinition {
                        id: node.task_id.clone(),
                        name: node.name.clone(),
                        payload: node.payload.clone(),
                        workflow_id: Some(workflow_id.clone()),
                        target_node: None,
                        origin_node: origin_node.clone(),
                        retry_policy: slot.dag.effective_retry_policy(&node.task_id),
                        priority: node.priority,
                        timeout_ms: node.timeout_ms,
                        attempt,
                        enqueued_at_ms: 0,
                        target_endpoint_addr: None,
                        origin_endpoint_addr: None,
                    }
                })
                .collect()
        };

        // Non-terminal: defer persistence to background flush
        self.state.mark_dirty(workflow_id);

        Ok(roots)
    }

    /// Handles a task completion, decrements dependent task counters, and
    /// returns any successor tasks that have become ready.
    /// Also returns conditional edges that need Python-side evaluation.
    pub async fn on_task_completed(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        result: Vec<u8>,
    ) -> Result<(Vec<TaskDefinition>, Vec<(TaskId, String)>)> {
        let info = self.complete_task(workflow_id, task_id, result).await?;
        if info.workflow_terminal {
            crate::metrics::inc_workflows_completed();
            crate::metrics::dec_active_workflows();
            return Ok((vec![], vec![]));
        }
        let ready = self
            .build_ready_tasks_for(workflow_id, &info.ready_successors)
            .await?;
        Ok((ready, info.conditional_edges))
    }

    /// Activate a conditional successor after Python evaluates the condition.
    /// Decrements the pending count and returns the task definition if it
    /// becomes ready (pending count reaches zero).
    pub async fn activate_conditional_successor(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<Option<TaskDefinition>> {
        let ready = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            let count = slot.pending.get_mut(task_id).ok_or_else(|| {
                crate::common::ActantError::Internal(format!(
                    "pending count not found for task {}",
                    task_id.as_str()
                ))
            })?;
            if *count == 0 {
                return Ok(None);
            }
            *count -= 1;
            if *count == 0 {
                Some(task_id.clone())
            } else {
                None
            }
        };
        match ready {
            Some(tid) => {
                let tasks = self.build_ready_tasks_for(workflow_id, &[tid]).await?;
                Ok(tasks.into_iter().next())
            }
            None => Ok(None),
        }
    }

    /// Skip a conditional branch that was not taken.
    ///
    /// Marks the task as Skipped and decrements pending counts of its
    /// unconditional successors. If a successor reaches zero pending:
    ///   - with at least one completed predecessor → ready (returned)
    ///   - with no completed predecessor (all skipped) → cascade-skipped
    ///
    /// This prevents deadlocks when a BranchRef consumer depends on both
    /// branches but only one executes.
    pub async fn skip_conditional_branch(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<Vec<TaskDefinition>> {
        let terminal_execution = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            slot.execution.mark_task_skipped(task_id);
            if slot.execution.is_terminal() {
                Some(slot.execution.clone())
            } else {
                None
            }
        };
        if let Some(execution) = terminal_execution {
            self.complete_terminal(workflow_id, &execution).await?;
            return Ok(Vec::new());
        }

        // Worklist of newly-skipped tasks whose successors need pending decrement.
        let mut worklist: Vec<TaskId> = vec![task_id.clone()];
        let mut ready_ids: Vec<TaskId> = Vec::new();

        while let Some(skipped_id) = worklist.pop() {
            let (newly_ready, mut newly_skipped) = {
                let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                    crate::common::ActantError::NotFound(format!(
                        "workflow {} not found",
                        workflow_id.as_str()
                    ))
                })?;
                let successors: Vec<TaskId> = slot.dag.successor_ids(&skipped_id);
                let conditional: Vec<TaskId> = slot
                    .dag
                    .conditional_edges_from(&skipped_id)
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect();

                let mut ready = Vec::new();
                let mut cascade = Vec::new();
                for succ_id in &successors {
                    // 条件后继由单独逻辑处理，跳过。
                    if conditional.iter().any(|id| id == succ_id) {
                        continue;
                    }
                    let count = match slot.pending.get_mut(succ_id) {
                        Some(c) if *c > 0 => {
                            *c -= 1;
                            *c
                        }
                        _ => continue,
                    };
                    if count != 0 {
                        continue;
                    }
                    // pending 归零：检查是否有任意前驱产生了结果。
                    let has_result = slot.dag.predecessors_of(succ_id).iter().any(|pred| {
                        slot.execution
                            .tasks
                            .get(&pred.task_id)
                            .and_then(|t| t.result.as_ref())
                            .is_some()
                    });
                    if has_result {
                        ready.push(succ_id.clone());
                    } else {
                        // 所有前驱均被跳过 → 级联跳过此任务。
                        slot.execution.mark_task_skipped(succ_id);
                        cascade.push(succ_id.clone());
                    }
                }
                (ready, cascade)
            };
            ready_ids.extend(newly_ready);
            worklist.append(&mut newly_skipped);
        }

        // 若级联跳过使工作流到达终态，完成收尾。
        let terminal_after_cascade = {
            let slot = self.state.slots.get(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            slot.execution.is_terminal()
        };
        if terminal_after_cascade {
            let exec_snapshot = {
                let slot = self.state.slots.get(workflow_id).ok_or_else(|| {
                    crate::common::ActantError::NotFound(format!(
                        "workflow {} not found",
                        workflow_id.as_str()
                    ))
                })?;
                slot.execution.clone()
            };
            self.complete_terminal(workflow_id, &exec_snapshot).await?;
            return Ok(Vec::new());
        }

        self.state.mark_dirty(workflow_id);
        let ready = self.build_ready_tasks_for(workflow_id, &ready_ids).await?;
        Ok(ready)
    }

    /// Cancels a running workflow and moves it to a terminal Cancelled state.
    pub async fn cancel(&self, workflow_id: &WorkflowId) -> Result<()> {
        let store_writes = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            slot.execution.mark_cancelled();
            if self.store.is_some() {
                Some(slot.execution.clone())
            } else {
                None
            }
        };

        if let Some(exec_snapshot) = store_writes {
            if let Some(ref store) = self.store {
                let exec_bytes = serialize_rkyv(&exec_snapshot)?;
                store.put_batch(&[(exec_key(workflow_id), exec_bytes)])?;
            }
        }

        self.notify_terminal();
        Ok(())
    }

    /// Cancel a single running task within a workflow.
    /// Returns Ok(true) if the task was running and is now cancelled,
    /// Ok(false) if the task was not in a running state.
    pub async fn cancel_task(&self, workflow_id: &WorkflowId, task_id: &TaskId) -> Result<bool> {
        let cancelled = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            slot.execution.cancel_task(task_id)
        };
        Ok(cancelled)
    }

    /// Spawns a background task that periodically checks for expired workflows
    /// and marks them failed. Returns a watch sender for shutdown signaling.
    pub fn start_timeout_watcher(&self) -> tokio::sync::watch::Sender<bool> {
        let (cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);
        let state = self.state.clone();
        let store = Arc::new(parking_lot::Mutex::new(self.store.clone()));
        let poll_interval =
            std::time::Duration::from_millis(self.config.workflow.state_poll_interval_ms);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(poll_interval);
            loop {
                tokio::select! {
                    _ = cancel_rx.changed() => break,
                    _ = interval.tick() => {
                        let expired: Vec<WorkflowId> = state.expired_workflow_ids();
                        for wf_id in expired {
                            if let Some(mut slot) = state.slots.get_mut(&wf_id) {
                                if !slot.execution.is_terminal() {
                                    slot.execution.mark_workflow_failed("workflow timeout exceeded".into());
                                    crate::metrics::inc_workflow_timeouts();
                                    crate::metrics::inc_workflows_failed();
                                    crate::metrics::dec_active_workflows();
                                    tracing::warn!("workflow {} timed out, marked as failed", wf_id.as_str());

                                    if let Some(ref store) = *store.lock() {
                                        if let Ok(exec_bytes) = serialize_rkyv(&slot.execution) {
                                            if let Err(e) = store.put_batch(&[(exec_key(&wf_id), exec_bytes)]) {
                                                tracing::error!("failed to persist timed-out workflow {}: {}", wf_id.as_str(), e);
                                            }
                                        }
                                    }

                                    state.fire_terminal_oneshot(&wf_id);
                                }
                            }
                        }
                    }
                }
            }
        });

        cancel_tx
    }

    /// Spawns a background task that periodically flushes dirty workflow
    /// execution states to the store. This replaces per-operation persistence
    /// with batched writes, significantly reducing write amplification.
    ///
    /// Terminal states are always persisted immediately by the caller;
    /// this task only handles non-terminal dirty states.
    pub fn start_persist_flush(&self) -> tokio::sync::watch::Sender<bool> {
        let (cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);
        let state = self.state.clone();
        let store = Arc::new(parking_lot::Mutex::new(self.store.clone()));
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
                        let Some(ref store) = *store.lock() else {
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
                            if let Err(e) = store.put_batch(&batch) {
                                tracing::warn!("persist flush failed: {}", e);
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
    pub fn flush_dirty(&self) -> Result<()> {
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
            store.put_batch(&batch)?;
        }
        Ok(())
    }

    /// Returns a snapshot of the workflow execution state, or `None` if not found.
    pub async fn get_state(&self, workflow_id: &WorkflowId) -> Option<WorkflowExecution> {
        self.state
            .slots
            .get(workflow_id)
            .map(|s| s.execution.clone())
    }

    pub async fn get_dag(&self, workflow_id: &WorkflowId) -> Option<Dag> {
        self.state.slots.get(workflow_id).map(|s| s.dag.clone())
    }

    fn build_ready_tasks_from_slot(
        &self,
        slot: &WorkflowSlot,
        workflow_id: &WorkflowId,
        ready_ids: &[TaskId],
    ) -> Result<Vec<TaskDefinition>> {
        let mut ready: Vec<TaskDefinition> = Vec::with_capacity(ready_ids.len());
        for succ_id in ready_ids {
            let node = slot.dag.get_node(succ_id).ok_or_else(|| {
                crate::common::ActantError::Internal(format!(
                    "node {} not found in dag",
                    succ_id.as_str()
                ))
            })?;

            let payload = build_task_payload(
                &slot.dag,
                &slot.execution,
                succ_id,
                &node.payload,
                &self.config.payload_signing_key,
            )?;

            let attempt = slot
                .execution
                .tasks
                .get(succ_id)
                .map(|t| t.attempt())
                .unwrap_or(0);

            ready.push(TaskDefinition {
                id: succ_id.clone(),
                name: node.name.clone(),
                payload,
                workflow_id: Some(workflow_id.clone()),
                target_node: None,
                origin_node: self.node_id.clone(),
                retry_policy: slot.dag.effective_retry_policy(&node.task_id),
                priority: node.priority,
                timeout_ms: node.timeout_ms,
                attempt,
                enqueued_at_ms: 0,
                target_endpoint_addr: None,
                origin_endpoint_addr: None,
            });
        }

        Ok(ready)
    }

    pub fn node_id(&self) -> Option<&NodeId> {
        self.node_id.as_ref()
    }

    pub fn store(&self) -> &Option<Store> {
        &self.store
    }

    pub fn state_handle(&self) -> Arc<OrchestratorState> {
        self.state.clone()
    }

    pub async fn active_workflow_ids(&self) -> Vec<WorkflowId> {
        self.state.active_workflow_ids()
    }

    pub async fn has_workflow(&self, workflow_id: &WorkflowId) -> bool {
        self.state.contains_workflow(workflow_id)
    }

    /// Serialize the current workflow state (dag, execution, pending) as rkyv bytes.
    /// Returns (dag_bytes, exec_bytes, pending_bytes) or None if workflow not found.
    pub async fn get_workflow_state_bytes(
        &self,
        workflow_id: &WorkflowId,
    ) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let slot = self.state.slots.get(workflow_id)?;
        let dag_bytes = serialize_rkyv(&slot.dag).ok()?;
        let exec_bytes = serialize_rkyv(&slot.execution).ok()?;
        let pending_bytes = serialize_rkyv(&slot.pending).ok()?;
        Some((dag_bytes, exec_bytes, pending_bytes))
    }

    pub async fn remove_active_workflow(&self, workflow_id: &WorkflowId) {
        self.state.remove_workflow(workflow_id);
    }

    pub async fn adopt_workflow(&self, workflow_id: &WorkflowId) -> Result<()> {
        if self.state.contains_workflow(workflow_id) {
            return Ok(());
        }

        if let Some(ref store) = self.store {
            let dag_key = dag_key(workflow_id);
            if let Ok(Some(data)) = store.get(&dag_key) {
                if let Ok(dag) = rkyv::from_bytes::<Dag, rkyv::rancor::Error>(&data) {
                    let task_ids: Vec<TaskId> = dag.nodes().map(|n| n.task_id.clone()).collect();
                    let mut pending: HashMap<TaskId, usize> = HashMap::new();
                    for node in dag.nodes() {
                        let pred_count = dag.predecessor_count(&node.task_id);
                        pending.insert(node.task_id.clone(), pred_count);
                    }

                    let exec_key = exec_key(workflow_id);
                    let execution = if let Ok(Some(exec_data)) = store.get(&exec_key) {
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
        // 调用 restore_workflow。
        let execution = WorkflowExecution::new(workflow_id.clone(), vec![]);
        self.state
            .insert_workflow(workflow_id.clone(), Dag::new(), execution, HashMap::new());
        Ok(())
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
                if let Err(e) = store.put_batch(&batch) {
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

    pub async fn get_running_task_ids(&self, workflow_id: &WorkflowId) -> Vec<TaskId> {
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

    pub async fn mark_task_pending(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<()> {
        let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
            crate::common::ActantError::NotFound(format!(
                "workflow {} not found",
                workflow_id.as_str()
            ))
        })?;
        slot.execution.reset_task(task_id, false, true);
        Ok(())
    }

    pub async fn build_task_for_id(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<Option<TaskDefinition>> {
        let slot = match self.state.slots.get(workflow_id) {
            Some(s) => s,
            None => return Ok(None),
        };
        let node = match slot.dag.get_node(task_id) {
            Some(n) => n,
            None => return Ok(None),
        };

        let attempt = slot
            .execution
            .tasks
            .get(task_id)
            .map(|t| t.attempt())
            .unwrap_or(0);

        let payload = build_task_payload(
            &slot.dag,
            &slot.execution,
            task_id,
            &node.payload,
            &self.config.payload_signing_key,
        )?;
        Ok(Some(TaskDefinition {
            id: node.task_id.clone(),
            name: node.name.clone(),
            payload,
            workflow_id: Some(workflow_id.clone()),
            target_node: None,
            origin_node: self.node_id.clone(),
            retry_policy: slot.dag.effective_retry_policy(&node.task_id),
            priority: node.priority,
            timeout_ms: node.timeout_ms,
            attempt,
            enqueued_at_ms: 0,
            target_endpoint_addr: None,
            origin_endpoint_addr: None,
        }))
    }

    pub async fn mark_task_running(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<()> {
        let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
            crate::common::ActantError::NotFound(format!(
                "workflow {} not found",
                workflow_id.as_str()
            ))
        })?;
        slot.execution.mark_task_running(task_id);
        Ok(())
    }

    /// Mark a task as failed.
    ///
    /// The `mode` parameter controls the scope:
    /// - `FailureScope::TaskOnly`: Only mark the task as Failed. The workflow
    ///   remains non-terminal, allowing `prepare_retry` to reset the task.
    /// - `FailureScope::WorkflowLevel`: Mark the task as Failed AND apply workflow-level
    ///   failure semantics. If the workflow becomes terminal, metrics are
    ///   updated and the terminal notification is sent.
    pub async fn fail_task(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        error: String,
        mode: FailureScope,
    ) -> Result<()> {
        let is_terminal = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            slot.execution.fail_task(task_id, error, mode);
            slot.execution.is_terminal()
        };

        if is_terminal {
            // Terminal: persist immediately for crash safety
            let exec_snapshot = {
                let slot = self.state.slots.get(workflow_id).ok_or_else(|| {
                    crate::common::ActantError::NotFound(format!(
                        "workflow {} not found",
                        workflow_id.as_str()
                    ))
                })?;
                slot.execution.clone()
            };
            if let Some(ref store) = self.store {
                let exec_bytes = serialize_rkyv(&exec_snapshot)?;
                store.put_batch(&[(exec_key(workflow_id), exec_bytes)])?;
            }
            crate::metrics::inc_workflows_failed();
            crate::metrics::dec_active_workflows();
            self.notify_terminal();
        } else {
            // Non-terminal: defer to background flush
            self.state.mark_dirty(workflow_id);
        }
        Ok(())
    }

    pub(crate) async fn complete_task(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        result: Vec<u8>,
    ) -> Result<CompletionInfo> {
        let is_terminal = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            slot.execution.mark_task_completed(task_id, result);
            slot.execution.is_terminal()
        };

        if is_terminal {
            let exec_snapshot = {
                let slot = self.state.slots.get(workflow_id).ok_or_else(|| {
                    crate::common::ActantError::NotFound(format!(
                        "workflow {} not found",
                        workflow_id.as_str()
                    ))
                })?;
                slot.execution.clone()
            };
            return self.complete_terminal(workflow_id, &exec_snapshot).await;
        }

        let ready_ids = self.compute_ready_successors(workflow_id, task_id).await?;

        // Non-terminal: defer persistence to background flush
        self.state.mark_dirty(workflow_id);

        Ok(CompletionInfo {
            workflow_terminal: false,
            ready_successors: ready_ids.ready,
            conditional_edges: ready_ids.conditional,
        })
    }

    async fn complete_terminal(
        &self,
        workflow_id: &WorkflowId,
        exec_snapshot: &crate::orchestrator::dag_state::WorkflowExecution,
    ) -> Result<CompletionInfo> {
        if let Some(started_at_ms) = exec_snapshot.started_at_ms() {
            let now_ms = crate::common::epoch_millis();
            crate::metrics::observe_workflow_duration_ms(now_ms.saturating_sub(started_at_ms));
        }

        if self.config.workflow.completed_retention_count == 0 {
            self.evict_workflow(workflow_id).await;
        } else if let Some(ref store) = self.store {
            let exec_bytes = serialize_rkyv(exec_snapshot)?;
            let mut batch = vec![(exec_key(workflow_id), exec_bytes)];

            if matches!(exec_snapshot.state, Phase::Completed) {
                let results: Vec<Vec<u8>> = exec_snapshot.collected_results();
                if !results.is_empty() {
                    let result_bytes = crate::common::pack_group(&results);
                    batch.push((result_key(workflow_id), result_bytes));
                }
            }
            store.put_batch(&batch)?;
        }
        self.notify_terminal();
        Ok(CompletionInfo {
            workflow_terminal: true,
            ready_successors: vec![],
            conditional_edges: vec![],
        })
    }

    /// Notify waiters that a workflow has reached a terminal state.
    /// Fires the per-workflow oneshot channel for instant wake-up.
    fn notify_terminal(&self) {
        // 查找所有终态 workflow 并触发其 oneshot
        let terminal_ids: Vec<WorkflowId> = self
            .state
            .slots
            .iter()
            .filter(|entry| entry.value().execution.is_terminal())
            .map(|entry| entry.key().clone())
            .collect();
        for id in &terminal_ids {
            self.state.fire_terminal_oneshot(id);
        }
    }

    async fn compute_ready_successors(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<ReadyResult> {
        let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
            crate::common::ActantError::NotFound(format!(
                "workflow {} not found",
                workflow_id.as_str()
            ))
        })?;

        let successors: Vec<TaskId> = slot.dag.successor_ids(task_id);
        let conditional_edges: Vec<(TaskId, String)> = slot.dag.conditional_edges_from(task_id);

        let mut ready_ids: Vec<TaskId> = Vec::new();
        for succ_id in &successors {
            // 跳过条件后继 — 它们由单独逻辑处理
            if conditional_edges.iter().any(|(id, _)| id == succ_id) {
                continue;
            }
            let count = slot.pending.get_mut(succ_id).ok_or_else(|| {
                crate::common::ActantError::Internal(format!(
                    "pending count not found for task {}",
                    succ_id.as_str()
                ))
            })?;
            if *count == 0 {
                continue;
            }
            *count -= 1;
            if *count == 0 {
                ready_ids.push(succ_id.clone());
            }
        }
        Ok(ReadyResult {
            ready: ready_ids,
            conditional: conditional_edges,
        })
    }

    pub async fn mark_workflow_failed(
        &self,
        workflow_id: &WorkflowId,
        error: String,
    ) -> Result<()> {
        let store_writes = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            slot.execution.mark_workflow_failed(error);
            if self.store.is_some() {
                Some(slot.execution.clone())
            } else {
                None
            }
        };

        if let Some(exec_snapshot) = store_writes {
            if let Some(ref store) = self.store {
                let exec_bytes = serialize_rkyv(&exec_snapshot)?;
                store.put_batch(&[(exec_key(workflow_id), exec_bytes)])?;
            }
        }

        self.notify_terminal();
        Ok(())
    }

    pub async fn build_ready_tasks_for(
        &self,
        workflow_id: &WorkflowId,
        task_ids: &[TaskId],
    ) -> Result<Vec<TaskDefinition>> {
        let slot = self.state.slots.get(workflow_id).ok_or_else(|| {
            crate::common::ActantError::NotFound(format!(
                "workflow {} not found",
                workflow_id.as_str()
            ))
        })?;
        self.build_ready_tasks_from_slot(&slot, workflow_id, task_ids)
    }

    /// After recovery, returns all tasks with pending_count == 0 (ready to run)
    /// for every non-terminal workflow. The caller should enqueue these into
    /// the scheduler so they can be dispatched.
    pub fn recover_ready_tasks(&self) -> Vec<TaskDefinition> {
        let mut all_ready = Vec::new();
        for entry in self.state.slots.iter() {
            let workflow_id = entry.key();
            let slot = entry.value();

            if slot.execution.is_terminal() {
                continue;
            }

            let ready_ids: Vec<TaskId> = slot
                .pending
                .iter()
                .filter(|(_, &count)| count == 0)
                .map(|(tid, _)| tid.clone())
                .collect();

            if ready_ids.is_empty() {
                continue;
            }

            if let Ok(tasks) = self.build_ready_tasks_from_slot(slot, workflow_id, &ready_ids) {
                tracing::info!(
                    "recovered workflow {} with {} ready tasks",
                    workflow_id.as_str(),
                    tasks.len()
                );
                all_ready.extend(tasks);
            }
        }
        all_ready
    }

    pub async fn get_result(&self, workflow_id: &WorkflowId) -> Option<Vec<u8>> {
        if let Some(ref store) = self.store {
            let key = result_key(workflow_id);
            store.get(&key).ok().flatten()
        } else {
            // 内存路径：与 store 路径一致，将所有已完成任务的结果打包为 group。
            // 之前用 HashMap::values().last() 取单个结果，顺序未定义且与 store
            // 路径不一致；现在统一使用 collected_results() + pack_group。
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

    pub async fn get_retry_info(
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

    pub async fn prepare_task_retry(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<Option<TaskDefinition>> {
        {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;

            let pred_count = slot.dag.predecessor_count(task_id);

            // 普通 task 重试使用 reset_task 并 increment_retry=true。
            // 这同时处理 Failed → Pending 的状态转换。
            slot.execution.reset_task(task_id, true, false);

            // 验证 task 是否确实被重置（可能由于状态不匹配，
            // 例如处于 Completed 等不可重置状态）。
            let task_state = slot.execution.tasks.get(task_id).ok_or_else(|| {
                crate::common::ActantError::Internal(format!(
                    "task {} state not found",
                    task_id.as_str()
                ))
            })?;
            if task_state.state != Phase::Pending {
                return Ok(None);
            }

            slot.pending.insert(task_id.clone(), pred_count);
        }

        // Non-terminal: defer persistence to background flush
        self.state.mark_dirty(workflow_id);

        self.build_task_for_id(workflow_id, task_id).await
    }

    pub async fn get_expired_workflow_ids(&self) -> Vec<WorkflowId> {
        self.state.expired_workflow_ids()
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
                if let Err(e) = s.delete(&key) {
                    tracing::warn!(
                        "failed to delete key during eviction of workflow {}: {}",
                        old_id.as_str(),
                        e
                    );
                }
            }
        }
    }

    /// Reschedule all running tasks of a workflow back to Pending state.
    ///
    /// Used by the failover subsystem when an orchestrator node fails and
    /// another node takes over its workflows. Returns the task definitions
    /// that need to be re-enqueued into the scheduler.
    ///
    /// This method owns the workflow state transitions (mark pending, build
    /// definitions). The caller is responsible for broadcasting state updates
    /// and enqueuing the returned tasks into a scheduler.
    pub async fn reschedule_running_tasks(
        &self,
        workflow_id: &WorkflowId,
    ) -> Result<Vec<TaskDefinition>> {
        let running_task_ids = self.get_running_task_ids(workflow_id).await;
        let mut tasks_to_reschedule = Vec::with_capacity(running_task_ids.len());

        for task_id in &running_task_ids {
            self.mark_task_pending(workflow_id, task_id).await?;

            if let Some(task_def) = self.build_task_for_id(workflow_id, task_id).await? {
                tasks_to_reschedule.push(task_def);
            }
        }

        Ok(tasks_to_reschedule)
    }
}

impl Default for Orchestrator {
    fn default() -> Self {
        Self::new()
    }
}

fn dag_key(wf_id: &WorkflowId) -> String {
    format!("{}{}", STORE_KEY_DAG, wf_id.as_str())
}

fn exec_key(wf_id: &WorkflowId) -> String {
    format!("{}{}", STORE_KEY_EXEC, wf_id.as_str())
}

fn pending_key(wf_id: &WorkflowId) -> String {
    format!("{}{}", STORE_KEY_PENDING, wf_id.as_str())
}

fn result_key(wf_id: &WorkflowId) -> String {
    format!("{}{}", STORE_KEY_RESULT, wf_id.as_str())
}

fn build_task_payload(
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
    let inner = crate::common::payload::pack_upstream_prefix(&upstream_results, &raw_payload);
    crate::common::payload::sign(signing_key, &inner)
        .map_err(|e| crate::common::ActantError::Internal(format!("payload sign: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::should_claim_workflow;
    use crate::common::RetryPolicy;
    use std::collections::HashMap;

    const TEST_SIGNING_KEY: &[u8] = b"test-key";

    fn make_node(id: &str, name: &str) -> DagNode {
        DagNode {
            task_id: TaskId::from(id.to_string()),
            name: name.to_string(),
            payload: crate::common::payload::sign(TEST_SIGNING_KEY, b"").unwrap(),
            retry_policy: None,
            timeout_ms: None,
            priority: 0,
            metadata: HashMap::new(),
        }
    }

    fn make_linear_dag() -> Dag {
        // t1 → t2 → t3
        let mut dag = Dag::new();
        dag.add_node(make_node("t1", "first")).unwrap();
        dag.add_node(make_node("t2", "second")).unwrap();
        dag.add_node(make_node("t3", "third")).unwrap();
        dag.add_edge(TaskId::from("t1"), TaskId::from("t2"))
            .unwrap();
        dag.add_edge(TaskId::from("t2"), TaskId::from("t3"))
            .unwrap();
        dag
    }

    fn make_diamond_dag() -> Dag {
        //     t1
        //    /  \
        //   t2   t3
        //    \  /
        //     t4
        let mut dag = Dag::new();
        dag.add_node(make_node("t1", "root")).unwrap();
        dag.add_node(make_node("t2", "left")).unwrap();
        dag.add_node(make_node("t3", "right")).unwrap();
        dag.add_node(make_node("t4", "join")).unwrap();
        dag.add_edge(TaskId::from("t1"), TaskId::from("t2"))
            .unwrap();
        dag.add_edge(TaskId::from("t1"), TaskId::from("t3"))
            .unwrap();
        dag.add_edge(TaskId::from("t2"), TaskId::from("t4"))
            .unwrap();
        dag.add_edge(TaskId::from("t3"), TaskId::from("t4"))
            .unwrap();
        dag
    }

    // ---- submit + start ----

    #[tokio::test]
    async fn submit_registers_workflow_in_state() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        let dag = make_linear_dag();

        orch.submit(wf.clone(), dag).await.unwrap();

        assert!(orch.has_workflow(&wf).await);
        let ids = orch.active_workflow_ids().await;
        assert!(ids.contains(&wf));
    }

    #[tokio::test]
    async fn start_returns_root_tasks_and_marks_running() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        orch.submit(wf.clone(), make_linear_dag()).await.unwrap();

        let roots = orch.start(&wf).await.unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].id, TaskId::from("t1"));

        let state = orch.get_state(&wf).await.unwrap();
        assert_eq!(state.state, Phase::Running);
    }

    #[tokio::test]
    async fn start_returns_error_for_unknown_workflow() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let err = orch
            .start(&WorkflowId::from("nonexistent"))
            .await
            .unwrap_err();
        assert!(matches!(err, crate::common::ActantError::NotFound(_)));
    }

    // ---- on_task_completed ----

    #[tokio::test]
    async fn completing_task_returns_ready_successors() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
        orch.start(&wf).await.unwrap();

        let (ready, _) = orch
            .on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
            .await
            .unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].id, TaskId::from("t2"));
    }

    #[tokio::test]
    async fn completing_last_task_signals_workflow_terminal() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
        orch.start(&wf).await.unwrap();

        orch.on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
            .await
            .unwrap();
        orch.on_task_completed(&wf, &TaskId::from("t2"), b"r2".to_vec())
            .await
            .unwrap();
        let (ready, _) = orch
            .on_task_completed(&wf, &TaskId::from("t3"), b"r3".to_vec())
            .await
            .unwrap();

        // Terminal: no more ready tasks
        assert!(ready.is_empty());

        let state = orch.get_state(&wf).await.unwrap();
        assert!(state.is_terminal());
        assert_eq!(state.state, Phase::Completed);
    }

    #[tokio::test]
    async fn completing_diamond_join_waits_for_both_predecessors() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        orch.submit(wf.clone(), make_diamond_dag()).await.unwrap();
        orch.start(&wf).await.unwrap();

        // Complete root t1 → t2 and t3 become ready
        let (ready, _) = orch
            .on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
            .await
            .unwrap();
        assert_eq!(ready.len(), 2);

        // Complete t2 → t4 NOT ready (still waiting on t3)
        let (ready, _) = orch
            .on_task_completed(&wf, &TaskId::from("t2"), b"r2".to_vec())
            .await
            .unwrap();
        assert!(ready.is_empty());

        // Complete t3 → t4 NOW ready
        let (ready, _) = orch
            .on_task_completed(&wf, &TaskId::from("t3"), b"r3".to_vec())
            .await
            .unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].id, TaskId::from("t4"));
    }

    // ---- skip_conditional_branch ----

    #[tokio::test]
    async fn skip_conditional_branch_skips_task_without_failing_workflow() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");

        // t1 → t2 → t3 where t2 is skipped
        let dag = make_linear_dag();
        orch.submit(wf.clone(), dag).await.unwrap();
        orch.start(&wf).await.unwrap();

        orch.skip_conditional_branch(&wf, &TaskId::from("t2"))
            .await
            .unwrap();

        let state = orch.get_state(&wf).await.unwrap();
        let t2_state = state.tasks.get(&TaskId::from("t2")).unwrap();
        assert_eq!(t2_state.state, Phase::Skipped);
    }

    // ---- cancel ----

    #[tokio::test]
    async fn cancel_marks_workflow_cancelled() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
        orch.start(&wf).await.unwrap();

        orch.cancel(&wf).await.unwrap();

        let state = orch.get_state(&wf).await.unwrap();
        assert!(state.is_terminal());
        assert_eq!(state.state, Phase::Cancelled);
    }

    #[tokio::test]
    async fn cancel_unknown_workflow_returns_not_found() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let err = orch.cancel(&WorkflowId::from("nope")).await.unwrap_err();
        assert!(matches!(err, crate::common::ActantError::NotFound(_)));
    }

    // ---- register_terminal_waiter ----

    #[tokio::test]
    async fn terminal_waiter_resolves_after_completion() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
        orch.start(&wf).await.unwrap();

        let rx = orch.state.register_terminal_waiter(wf.clone());

        // Complete all tasks
        orch.on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
            .await
            .unwrap();
        orch.on_task_completed(&wf, &TaskId::from("t2"), b"r2".to_vec())
            .await
            .unwrap();
        orch.on_task_completed(&wf, &TaskId::from("t3"), b"r3".to_vec())
            .await
            .unwrap();

        // Waiter should resolve
        tokio::time::timeout(std::time::Duration::from_secs(1), rx)
            .await
            .expect("waiter did not resolve within 1s")
            .expect("waiter was dropped without signaling");
    }

    #[tokio::test]
    async fn terminal_waiter_resolves_immediately_if_already_terminal() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        orch.submit(wf.clone(), make_linear_dag()).await.unwrap();
        orch.start(&wf).await.unwrap();
        orch.cancel(&wf).await.unwrap();

        let rx = orch.state.register_terminal_waiter(wf.clone());
        // Should resolve immediately
        tokio::time::timeout(std::time::Duration::from_millis(100), rx)
            .await
            .expect("waiter did not resolve immediately")
            .expect("waiter was dropped without signaling");
    }

    // ---- builder methods ----

    #[test]
    fn builder_with_node_id_sets_node_id() {
        let orch = Orchestrator::new()
            .with_signing_key(TEST_SIGNING_KEY.to_vec())
            .with_node_id(NodeId::from("node-1"));
        assert_eq!(orch.node_id(), Some(&NodeId::from("node-1")));
    }

    #[test]
    fn new_orchestrator_has_no_store() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        assert!(orch.store().is_none());
    }

    // ---- submit_with_timeout ----

    #[tokio::test]
    async fn submit_with_timeout_sets_deadline() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        orch.submit_with_timeout(wf.clone(), make_linear_dag(), 5000)
            .await
            .unwrap();

        let state = orch.get_state(&wf).await.unwrap();
        assert!(state.deadline_ms().is_some());
    }

    // ---- get_dag / get_state ----

    #[tokio::test]
    async fn get_dag_returns_submitted_dag() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");
        orch.submit(wf.clone(), make_diamond_dag()).await.unwrap();

        let dag = orch.get_dag(&wf).await.unwrap();
        assert_eq!(dag.node_count(), 4);
    }

    #[tokio::test]
    async fn get_state_returns_none_for_unknown_workflow() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        assert!(orch
            .get_state(&WorkflowId::from("nonexistent"))
            .await
            .is_none());
    }

    // ---- get_result / get_results ----
    // get_result 返回 pack_group 打包的所有已完成任务结果（与 store 路径一致）。
    // get_results 解包 get_result 的返回值，得到 Vec<Vec<u8>>。
    // 结果按 task_id 升序排序，保证确定性（HashMap 迭代顺序未定义）。

    #[tokio::test]
    async fn get_result_returns_packed_group_after_completion() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");

        let mut dag = Dag::new();
        dag.add_node(make_node("t1", "only")).unwrap();
        orch.submit(wf.clone(), dag).await.unwrap();
        orch.start(&wf).await.unwrap();

        orch.on_task_completed(&wf, &TaskId::from("t1"), b"final".to_vec())
            .await
            .unwrap();

        // get_result 返回 pack_group 编码的字节（与 store 路径一致）。
        let packed = orch.get_result(&wf).await.expect("should have result");
        assert_eq!(packed, crate::common::pack_group(&[b"final".to_vec()]));

        // get_results 解包得到原始任务结果列表。
        let results = orch.get_results(&wf).await.expect("should have results");
        assert_eq!(results, vec![b"final".to_vec()]);
    }

    #[tokio::test]
    async fn get_results_orders_by_task_id_deterministically() {
        // 多任务工作流：验证结果按 task_id 升序排序，而非 HashMap 随机顺序。
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-multi");

        let mut dag = Dag::new();
        // 故意用非字母序的提交顺序，且 task_id 字典序与提交序不同。
        dag.add_node(make_node("t3", "third")).unwrap();
        dag.add_node(make_node("t1", "first")).unwrap();
        dag.add_node(make_node("t2", "second")).unwrap();
        orch.submit(wf.clone(), dag).await.unwrap();
        orch.start(&wf).await.unwrap();

        // 按非字典序完成，排除"提交即排序"的巧合。
        orch.on_task_completed(&wf, &TaskId::from("t2"), b"r2".to_vec())
            .await
            .unwrap();
        orch.on_task_completed(&wf, &TaskId::from("t3"), b"r3".to_vec())
            .await
            .unwrap();
        orch.on_task_completed(&wf, &TaskId::from("t1"), b"r1".to_vec())
            .await
            .unwrap();

        let results = orch.get_results(&wf).await.expect("should have results");
        // 期望按 task_id 升序：t1, t2, t3 → r1, r2, r3
        assert_eq!(
            results,
            vec![b"r1".to_vec(), b"r2".to_vec(), b"r3".to_vec()]
        );
    }

    #[tokio::test]
    async fn get_result_returns_none_when_no_completed_tasks() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-empty");

        let mut dag = Dag::new();
        dag.add_node(make_node("t1", "pending")).unwrap();
        orch.submit(wf.clone(), dag).await.unwrap();
        orch.start(&wf).await.unwrap();

        // 任务未完成，无结果。
        assert_eq!(orch.get_result(&wf).await, None);
        assert_eq!(orch.get_results(&wf).await, None);
    }

    // ---- retry policy propagation ----

    #[tokio::test]
    async fn start_propagates_retry_policy_from_dag_node() {
        let orch = Orchestrator::new().with_signing_key(TEST_SIGNING_KEY.to_vec());
        let wf = WorkflowId::from("wf-1");

        let mut dag = Dag::new();
        let mut node = make_node("t1", "retryable");
        node.retry_policy = Some(RetryPolicy {
            max_retries: 5,
            delay_ms: 1000,
            backoff_multiplier: 2.0,
            max_delay_ms: 60000,
        });
        dag.add_node(node).unwrap();

        orch.submit(wf.clone(), dag).await.unwrap();
        let roots = orch.start(&wf).await.unwrap();

        assert_eq!(roots.len(), 1);
        let policy = roots[0].retry_policy.as_ref().unwrap();
        assert_eq!(policy.max_retries, 5);
        assert_eq!(policy.delay_ms, 1000);
    }

    // ---- 审查验证: should_claim_workflow 一致性哈希 vs claim_workflow 字典序退让 ----

    /// 验证审查发现 ST1: should_claim_workflow (一致性哈希) 与 claim_workflow 内部
    /// 字典序退让逻辑存在设计不一致。当 lease 过期边界情况发生时，
    /// 一致性哈希指定的认领者可能因字典序更大而退让给非指定节点。
    ///
    /// 此测试验证 should_claim_workflow 的决策与 claim_workflow 内部
    /// 退让逻辑（existing.node_id < self.node_id 时退让）在特定场景下矛盾。
    #[test]
    fn audit_st1_consistent_hash_vs_lexicographic_yield_inconsistency() {
        // should_claim_workflow 使用一致性哈希决定认领权
        // claim_workflow 内部: if existing.node_id < self.node_id { return Ok(()) }
        // 这意味着字典序更小的节点总是"赢"

        // 构造场景: 两个节点 node_a, node_b (字典序 node_a < node_b)
        // 某个 workflow_key 经一致性哈希映射到 node_b
        // 但如果 node_a 先认领（如 lease 过期后先抢到），node_b 退让

        // 验证: 对于 node_a 和 node_b，should_claim 对某些 key 指向 node_b
        let candidates = vec!["node_a".to_string(), "node_b".to_string()];

        // 暴力搜索一个 key 使一致性哈希指向 node_b 而非 node_a
        let mut conflict_key = String::new();
        for i in 0..10000 {
            let key = format!("wf-{}", i);
            if should_claim_workflow(&key, "node_b", candidates.clone())
                && !should_claim_workflow(&key, "node_a", candidates.clone())
            {
                conflict_key = key;
                break;
            }
        }
        assert!(
            !conflict_key.is_empty(),
            "should find a key mapping to node_b via consistent hash"
        );

        // 一致性哈希说 node_b 应认领
        assert!(should_claim_workflow(
            &conflict_key,
            "node_b",
            candidates.clone()
        ));
        assert!(!should_claim_workflow(
            &conflict_key,
            "node_a",
            candidates.clone()
        ));

        // 但字典序退让逻辑: node_a < node_b => node_b 退让
        assert!(
            "node_a" < "node_b",
            "lexicographic: node_a < node_b, so node_b would yield"
        );

        // 结论: 两种策略在此场景下矛盾
        // - 一致性哈希: node_b 认领 ✓
        // - 字典序退让: node_b 退让给 node_a ✗
        eprintln!(
            "AUDIT ST1 CONFIRMED: key='{}' consistent_hash=>node_b, \
             but lexicographic yield=>node_a wins. \
             These two conflict resolution strategies are inconsistent.",
            conflict_key
        );
    }
}
