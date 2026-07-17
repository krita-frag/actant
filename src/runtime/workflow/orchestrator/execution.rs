//! Orchestrator 的 `execution` 职责子模块。
//!
//! 负责工作流提交、启动、任务完成处理、条件边求值、取消、重试与调度。

use std::collections::HashMap;

use crate::common::serialization::serialize_rkyv;
use crate::common::{Result, TaskDefinition, TaskId, WorkflowId};
use crate::runtime::workflow::{Dag, FailureScope, Phase, Terminal, WorkflowExecution};

use super::{keys::*, types::*, Orchestrator};

impl Orchestrator {
    /// Submits a workflow DAG for execution.
    ///
    /// 若 `workflow_id` 已存在且为占位符（[`SlotState::Loading`]），此调用会覆盖
    /// 占位符——这是 `adopt_workflow` 后本地再次提交同一 workflow 的合法路径。
    /// 若已存在且为 [`SlotState::Ready`]，返回 [`ActantError::AlreadyExists`]。
    #[tracing::instrument(level = "debug", skip(self, dag), fields(workflow = %workflow_id, nodes = dag.nodes().count()))]
    pub async fn submit(&self, workflow_id: WorkflowId, dag: Dag) -> Result<()> {
        // 已就绪的工作流不允许重复提交；占位符允许覆盖（adopt 后本地重新提交）。
        if self.state.is_ready(&workflow_id) {
            return Err(crate::common::ActantError::AlreadyExists(format!(
                "workflow {} already submitted",
                workflow_id.as_str()
            )));
        }
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
            store
                .put_batch(&[
                    (dag_key(&workflow_id), dag_bytes),
                    (exec_key(&workflow_id), exec_bytes),
                    (pending_key(&workflow_id), pending_bytes),
                ])
                .await?;
        }

        self.state
            .insert_workflow(workflow_id.clone(), dag, execution, pending);

        self.log_event(WorkflowEventPayload::Submitted { workflow_id });

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
    pub fn start(&self, workflow_id: &WorkflowId) -> Result<Vec<TaskDefinition>> {
        let roots = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            if slot.state == SlotState::Loading {
                return Err(crate::common::ActantError::InvalidState(format!(
                    "workflow {} is still loading (placeholder), cannot start",
                    workflow_id.as_str()
                )));
            }
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

        self.log_event(WorkflowEventPayload::Started {
            workflow_id: workflow_id.clone(),
        });

        Ok(roots)
    }

    /// Handles a task completion, decrements dependent task counters, and
    /// returns any successor tasks that have become ready.
    ///
    /// 若 `condition_evaluator` 已设置，条件边在 Rust 核心内直接求值并处理，
    /// 返回空的 `conditional_edges`；否则将条件边返回给调用方（如 Python 编排
    /// 循环）外部评估。
    ///
    /// 返回值第三个元素 `workflow_terminal` 是显式的终态标志，**不应**通过
    /// `ready.is_empty() && conditional_edges.is_empty()` 推断——条件求值器
    /// 全部跳过条件后继也会产生空列表，但工作流未必进入终态（其他分支可能
    /// 仍在运行）。调用方（如 `WorkflowActor`）必须使用此标志判断是否触发
    /// 终态通知。
    pub async fn on_task_completed(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        result: Vec<u8>,
    ) -> Result<(Vec<TaskDefinition>, Vec<(TaskId, String)>, bool)> {
        // 阶段 1：更新执行状态、计算 ready 与 conditional_edges、判断终态。
        let info = self.complete_task(workflow_id, task_id, result).await?;
        if info.workflow_terminal {
            crate::metrics::inc_workflows_completed();
            crate::metrics::dec_active_workflows();
            return Ok((vec![], vec![], true));
        }

        // 阶段 2：将 ready_successors 转换为 TaskDefinition。
        let mut ready = self.build_ready_tasks_for(workflow_id, &info.ready_successors)?;

        // 阶段 3：处理条件边——内部求值（若有 evaluator）或返回给调用方外部评估。
        let conditional_edges = self
            .process_conditional_edges(workflow_id, task_id, info.conditional_edges, &mut ready)
            .await?;

        Ok((ready, conditional_edges, false))
    }

    /// 处理条件边：内部求值或返回给调用方。
    ///
    /// 若 `condition_evaluator` 已设置，对每条条件边求值：
    /// - 激活 → 减少后继 pending 计数，可能加入 ready
    /// - 不激活 → 级联跳过该后继分支，可能产生新的 ready
    ///
    /// 求值后返回空列表（所有条件边已在内部处理）。
    ///
    /// 若未设置 evaluator，原样返回条件边列表，由调用方（如 Python 编排循环）外部评估。
    ///
    /// 此方法是 `on_task_completed` 的"阶段 3"，提取自原函数以隔离条件求值逻辑。
    async fn process_conditional_edges(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        conditional_edges: Vec<(TaskId, String)>,
        ready: &mut Vec<TaskDefinition>,
    ) -> Result<Vec<(TaskId, String)>> {
        let mut conditional_edges = conditional_edges;
        if let Some(evaluator) = self.condition_evaluator.as_ref() {
            for (succ_id, condition) in &conditional_edges {
                let activate = evaluator.evaluate(workflow_id, task_id, condition).await?;
                if activate {
                    if let Some(task) = self.activate_conditional_successor(workflow_id, succ_id)? {
                        ready.push(task);
                    }
                } else {
                    let cascade_ready = self.skip_conditional_branch(workflow_id, succ_id).await?;
                    ready.extend(cascade_ready);
                }
            }
            conditional_edges = Vec::new();
        }
        Ok(conditional_edges)
    }

    /// Activate a conditional successor after Python evaluates the condition.
    /// Decrements the pending count and returns the task definition if it
    /// becomes ready (pending count reaches zero).
    pub fn activate_conditional_successor(
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
                let tasks = self.build_ready_tasks_for(workflow_id, &[tid])?;
                Ok(tasks.into_iter().next())
            }
            None => Ok(None),
        }
    }

    /// Skip a conditional branch that was not taken.
    ///
    /// 条件前驱完成且条件不激活时，减少 `task_id` 的 pending 计数（对应条件前驱）。
    /// 根据剩余 pending 决定后续行为：
    /// - pending > 0：仍有其他未完成前驱，**不跳过** task_id，仅减少 pending
    /// - pending == 0 且有已完成前驱：task_id 变为 ready（返回）
    /// - pending == 0 且无已完成前驱：标记 task_id 为 Skipped，级联跳过其非条件后继
    ///
    /// 级联跳过后继的逻辑：
    ///   - 后继 pending 归零且有已完成前驱 → ready（返回）
    ///   - 后继 pending 归零且所有前驱均被跳过 → 级联跳过
    ///
    /// 此方法防止 BranchRef consumer 依赖两个分支但只有一个执行时死锁。
    pub async fn skip_conditional_branch(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<Vec<TaskDefinition>> {
        // 阶段 0：减少条件前驱对应的 pending 计数，决定是否应跳过 task_id。
        // compute_ready_successors 跳过条件后继的 pending 减少，延迟到此处处理。
        match self.decrement_conditional_pending(workflow_id, task_id)? {
            ConditionalSkipDecision::PendingRemaining => {
                // 仍有其他未完成前驱，不跳过 task_id。
                self.state.mark_dirty(workflow_id);
                return Ok(Vec::new());
            }
            ConditionalSkipDecision::Ready => {
                // pending 归零且有已完成前驱 → task_id 变为 ready。
                self.state.mark_dirty(workflow_id);
                // build_ready_tasks_for 接受 &[TaskId]（owned），无法用 from_ref 消除 clone。
                #[allow(clippy::cloned_ref_to_slice_refs)]
                return self.build_ready_tasks_for(workflow_id, &[task_id.clone()]);
            }
            ConditionalSkipDecision::Skip => {
                // pending 归零且无已完成前驱 → 跳过 task_id，继续级联逻辑。
            }
        }

        // 阶段 1：标记被跳过的任务。若工作流直接进入终态，立即收尾。
        if let Some(execution) = self.mark_skipped_and_check_terminal(workflow_id, task_id)? {
            self.complete_terminal(workflow_id, task_id, &execution)
                .await?;
            return Ok(Vec::new());
        }

        // 阶段 2：级联跳过——沿非条件后继边递归减少 pending，收集 ready 与新增跳过任务。
        let ready_ids = self.cascade_skip(workflow_id, task_id).await?;

        // 阶段 3：级联后若工作流进入终态，完成收尾。
        if let Some(execution) = self.execution_if_terminal(workflow_id)? {
            self.complete_terminal(workflow_id, task_id, &execution)
                .await?;
            return Ok(Vec::new());
        }

        self.state.mark_dirty(workflow_id);
        let ready = self.build_ready_tasks_for(workflow_id, &ready_ids)?;
        Ok(ready)
    }

    /// 减少条件前驱对应的 pending 计数，并决定后续行为。
    ///
    /// `compute_ready_successors` 完成前驱时不减少条件后继的 pending（条件边需求值后才处理）。
    /// 此方法在条件求值返回 false 时调用，减少 pending 一次，对应已完成的条件前驱。
    ///
    /// 决策逻辑：
    /// - 减少后 pending > 0 → `PendingRemaining`（仍有其他前驱未完成）
    /// - 减少后 pending == 0 且有已完成前驱 → `Ready`
    /// - 减少后 pending == 0 且无已完成前驱 → `Skip`
    fn decrement_conditional_pending(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<ConditionalSkipDecision> {
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
        if *count > 0 {
            *count -= 1;
        }
        if *count > 0 {
            return Ok(ConditionalSkipDecision::PendingRemaining);
        }

        // pending 归零：检查是否有任意前驱产生了结果。
        let has_result = slot.dag.predecessors_of(task_id).iter().any(|pred| {
            slot.execution
                .tasks
                .get(&pred.task_id)
                .and_then(|t| t.result.as_ref())
                .is_some()
        });
        if has_result {
            Ok(ConditionalSkipDecision::Ready)
        } else {
            Ok(ConditionalSkipDecision::Skip)
        }
    }

    /// 标记 `task_id` 为 `Skipped`。
    ///
    /// 返回 `Some(execution)` 表示工作流因此次标记直接进入终态（如该任务是唯一未完成任务），
    /// 调用方应执行终态收尾。返回 `None` 表示工作流仍在运行，需继续级联跳过逻辑。
    ///
    /// 此方法是 `skip_conditional_branch` 的"阶段 1"，提取自原函数以隔离状态修改与终态判定。
    fn mark_skipped_and_check_terminal(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<Option<WorkflowExecution>> {
        let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
            crate::common::ActantError::NotFound(format!(
                "workflow {} not found",
                workflow_id.as_str()
            ))
        })?;
        slot.execution.mark_task_skipped(task_id);
        if slot.execution.is_terminal() {
            Ok(Some(slot.execution.clone()))
        } else {
            Ok(None)
        }
    }

    /// 沿非条件后继边递归减少 pending 计数，处理级联跳过。
    ///
    /// 起点是 `task_id`（已由 `mark_skipped_and_check_terminal` 标记为 Skipped）。
    /// 对每个起点的非条件后继：
    /// - pending 归零且有已完成前驱 → 加入 ready 列表（返回给调用方调度）
    /// - pending 归零且所有前驱均被跳过 → 级联标记为 Skipped，加入 worklist 继续传播
    ///
    /// 返回所有因级联而变为 ready 的任务 ID。
    ///
    /// 此方法是 `skip_conditional_branch` 的"阶段 2"。
    async fn cascade_skip(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<Vec<TaskId>> {
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

        Ok(ready_ids)
    }

    /// 返回 `Some(execution)` 若工作流当前处于终态，否则 `None`。
    ///
    /// 此方法是 `skip_conditional_branch` 的"阶段 3"的一部分，
    /// 隔离终态判定与终态收尾逻辑，避免在每个调用点重复 NotFound 检查。
    fn execution_if_terminal(&self, workflow_id: &WorkflowId) -> Result<Option<WorkflowExecution>> {
        let slot = self.state.slots.get(workflow_id).ok_or_else(|| {
            crate::common::ActantError::NotFound(format!(
                "workflow {} not found",
                workflow_id.as_str()
            ))
        })?;
        if slot.execution.is_terminal() {
            Ok(Some(slot.execution.clone()))
        } else {
            Ok(None)
        }
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
                store
                    .put_batch(&[(exec_key(workflow_id), exec_bytes)])
                    .await?;
            }
        }

        self.notify_terminal();
        Ok(())
    }

    /// Cancel a single running task within a workflow.
    /// Returns Ok(true) if the task was running and is now cancelled,
    /// Ok(false) if the task was not in a running state.
    pub fn cancel_task(&self, workflow_id: &WorkflowId, task_id: &TaskId) -> Result<bool> {
        let cancelled = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            slot.execution.cancel_task(task_id)
        };
        if cancelled {
            self.log_event(WorkflowEventPayload::TaskCancelled {
                workflow_id: workflow_id.clone(),
                task_id: task_id.clone(),
            });
        }
        Ok(cancelled)
    }

    /// Spawns a background task that periodically checks for expired workflows
    /// and marks them failed. Returns a watch sender for shutdown signaling.
    pub fn start_timeout_watcher(&self) -> tokio::sync::watch::Sender<bool> {
        let (cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);
        let state = self.state.clone();
        let store = self.store.clone();
        let poll_interval =
            std::time::Duration::from_millis(self.config.workflow.state_poll_interval_ms);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(poll_interval);
            loop {
                tokio::select! {
                    _ = cancel_rx.changed() => break,
                    _ = interval.tick() => {
                        let expired: Vec<WorkflowId> = state.expired_workflow_ids();
                        // 收集本轮所有超时 workflow 的 exec 快照，统一一次 put_batch 提交，
                        // 避免在 expired 列表较长时产生 N 次独立 LMDB 事务。
                        let mut persist_batch: Vec<(String, Vec<u8>)> = Vec::new();
                        let mut to_fire: Vec<WorkflowId> = Vec::new();
                        for wf_id in expired {
                            if let Some(mut slot) = state.slots.get_mut(&wf_id) {
                                if !slot.execution.is_terminal() {
                                    slot.execution.mark_workflow_failed("workflow timeout exceeded".into());
                                    crate::metrics::inc_workflow_timeouts();
                                    crate::metrics::inc_workflows_failed();
                                    crate::metrics::dec_active_workflows();
                                    tracing::warn!("workflow {} timed out, marked as failed", wf_id.as_str());

                                    if let Ok(exec_bytes) = serialize_rkyv(&slot.execution) {
                                        persist_batch.push((exec_key(&wf_id), exec_bytes));
                                    }

                                    to_fire.push(wf_id);
                                }
                            }
                        }
                        if let Some(ref store) = store {
                            if !persist_batch.is_empty() {
                                if let Err(e) = store.put_batch(&persist_batch).await {
                                    tracing::error!("failed to persist timed-out workflows: {}", e);
                                }
                            }
                        }
                        for wf_id in to_fire {
                            state.fire_terminal_oneshot(&wf_id);
                        }
                    }
                }
            }
        });

        cancel_tx
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

    pub fn mark_task_pending(&self, workflow_id: &WorkflowId, task_id: &TaskId) -> Result<()> {
        let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
            crate::common::ActantError::NotFound(format!(
                "workflow {} not found",
                workflow_id.as_str()
            ))
        })?;
        slot.execution.reset_task(task_id, false, true);
        Ok(())
    }

    pub fn build_task_for_id(
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

    pub fn mark_task_running(&self, workflow_id: &WorkflowId, task_id: &TaskId) -> Result<()> {
        let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
            crate::common::ActantError::NotFound(format!(
                "workflow {} not found",
                workflow_id.as_str()
            ))
        })?;
        slot.execution.mark_task_running(task_id);
        self.log_event(WorkflowEventPayload::TaskRunning {
            workflow_id: workflow_id.clone(),
            task_id: task_id.clone(),
        });
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
        let error_for_event = error.clone();
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

        self.log_event(WorkflowEventPayload::TaskFailed {
            workflow_id: workflow_id.clone(),
            task_id: task_id.clone(),
            error: error_for_event,
        });

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
                store
                    .put_batch(&[(exec_key(workflow_id), exec_bytes)])
                    .await?;
            }
            self.log_event(WorkflowEventPayload::Failed {
                workflow_id: workflow_id.clone(),
                error: format!("workflow failed at task {}", task_id.as_str()),
            });
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
            if slot.state == SlotState::Loading {
                return Err(crate::common::ActantError::InvalidState(format!(
                    "workflow {} is still loading (placeholder), cannot complete task",
                    workflow_id.as_str()
                )));
            }
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
            return self
                .complete_terminal(workflow_id, task_id, &exec_snapshot)
                .await;
        }

        let ready_ids = self.compute_ready_successors(workflow_id, task_id)?;

        // Non-terminal: defer persistence to background flush
        self.state.mark_dirty(workflow_id);

        self.log_event(WorkflowEventPayload::TaskCompleted {
            workflow_id: workflow_id.clone(),
            task_id: task_id.clone(),
        });

        Ok(CompletionInfo {
            workflow_terminal: false,
            ready_successors: ready_ids.ready,
            conditional_edges: ready_ids.conditional,
        })
    }

    async fn complete_terminal(
        &self,
        workflow_id: &WorkflowId,
        completed_task_id: &TaskId,
        exec_snapshot: &crate::runtime::workflow::dag::WorkflowExecution,
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
            store.put_batch(&batch).await?;
        }

        if matches!(exec_snapshot.state, Phase::Completed) {
            self.log_event(WorkflowEventPayload::Completed {
                workflow_id: workflow_id.clone(),
            });
        } else {
            self.log_event(WorkflowEventPayload::Failed {
                workflow_id: workflow_id.clone(),
                error: format!("workflow failed at task {}", completed_task_id.as_str()),
            });
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

    fn compute_ready_successors(
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
                store
                    .put_batch(&[(exec_key(workflow_id), exec_bytes)])
                    .await?;
            }
        }

        self.notify_terminal();
        Ok(())
    }

    pub fn build_ready_tasks_for(
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

    pub fn prepare_task_retry(
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

        let task = self.build_task_for_id(workflow_id, task_id)?;
        if task.is_some() {
            crate::metrics::inc_tasks_retried();
        }
        Ok(task)
    }

    pub fn reschedule_running_tasks(
        &self,
        workflow_id: &WorkflowId,
    ) -> Result<Vec<TaskDefinition>> {
        let running_task_ids = self.get_running_task_ids(workflow_id);
        let mut tasks_to_reschedule = Vec::with_capacity(running_task_ids.len());

        for task_id in &running_task_ids {
            self.mark_task_pending(workflow_id, task_id)?;

            if let Some(task_def) = self.build_task_for_id(workflow_id, task_id)? {
                crate::metrics::inc_retry_scheduled();
                tasks_to_reschedule.push(task_def);
            }
        }

        Ok(tasks_to_reschedule)
    }
}
