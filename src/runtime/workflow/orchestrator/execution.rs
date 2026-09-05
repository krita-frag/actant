//! Orchestrator 的 `execution` 职责子模块。
//!
//! 负责工作流提交、启动、任务完成处理、条件边求值、取消、重试与调度。

use std::collections::HashMap;

use crate::common::serialization::serialize_rkyv;
use crate::common::wire::TOPIC_CANCEL;
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
        // deadline 设置晚于 submit 的同步落盘，必须标记脏，否则在后台 flush
        // 触发前崩溃会丢失 deadline，重启后工作流失去超时保护。
        self.state.mark_dirty(&workflow_id);
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

    /// 结果接受决策（attempt fencing）的唯一查询入口，供
    /// `WorkflowActor::on_task_result` 在推进状态前判定结果所属派发代数是否
    /// 仍可被接受。workflow 不存在时放行（NotFound 由后续状态推进路径返回）。
    pub fn result_attempt_accepted(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        result_attempt: Option<u32>,
    ) -> bool {
        match self.state.slots.get(workflow_id) {
            Some(slot) => slot
                .execution
                .attempt_fencing_passes(task_id, result_attempt),
            None => true,
        }
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
        // 结果通路（WireTaskResult / WireDagStateUpdate / COMPLETE_TASK actor
        // 载荷）尚未携带派发代数（wire.rs 本批次冻结），传 `None` 放行 attempt
        // fencing；待协议扩展后由调用方传入结果的 attempt 即接入 fencing。
        let info = self
            .complete_task(workflow_id, task_id, result, None)
            .await?;
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
    /// - 求值出错 → 记录 warn 日志，该边**原样保留**并返回给调用方外部评估。
    ///   已就绪任务照常返回，保证单条边的求值失败不会让整个完成事件失败、
    ///   也不会丢弃已 ready 的后继（否则重试同一完成消息会因任务已终态
    ///   被守卫拒绝而永久卡死）。
    ///
    /// 全部边求值成功后返回空列表（所有条件边已在内部处理）。
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
            let mut deferred: Vec<(TaskId, String)> = Vec::new();
            for (succ_id, condition) in &conditional_edges {
                match evaluator.evaluate(workflow_id, task_id, condition).await {
                    Ok(true) => {
                        if let Some(task) =
                            self.activate_conditional_successor(workflow_id, succ_id)?
                        {
                            ready.push(task);
                        }
                    }
                    Ok(false) => {
                        let cascade_ready =
                            self.skip_conditional_branch(workflow_id, succ_id).await?;
                        ready.extend(cascade_ready);
                    }
                    Err(e) => {
                        tracing::warn!(
                            workflow = %workflow_id.as_str(),
                            task = %task_id.as_str(),
                            successor = %succ_id.as_str(),
                            error = %e,
                            "condition evaluation failed, deferring edge to caller"
                        );
                        deferred.push((succ_id.clone(), condition.clone()));
                    }
                }
            }
            conditional_edges = deferred;
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

        self.notify_terminal(workflow_id);
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
    ///
    /// B2：当 `network` 已注入时，超时处理路径会主动广播 `CancelBroadcast`
    /// 给所有正在运行的任务，触发本地与远端 Worker 协作式取消。这确保
    /// 即使任务自身没有超时（per-task timeout），工作流级硬超时也能及时
    /// 释放资源。未注入 `network` 时（如单元测试）仅标记状态，不广播取消。
    pub fn start_timeout_watcher(&self) -> tokio::sync::watch::Sender<bool> {
        let (cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);
        let state = self.state.clone();
        let store = self.store.clone();
        let network = self.network.clone();
        let event_log = self.event_log.clone();
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
                        // B2：收集 (workflow_id, task_id) 对，统一在持久化后广播取消。
                        // 先收集再 mark_workflow_failed，因为 mark 会将 Running 状态
                        // 改为 Failed，导致 running_task_ids 在后续读取时为空。
                        let mut cancels_to_broadcast: Vec<(WorkflowId, TaskId)> = Vec::new();
                        for wf_id in &expired {
                            if let Some(slot) = state.slots.get(wf_id) {
                                if !slot.execution.is_terminal() {
                                    for task_id in slot.execution.tasks.keys() {
                                        if slot.execution.tasks.get(task_id)
                                            .map(|ts| ts.state == Phase::Running)
                                            .unwrap_or(false)
                                        {
                                            cancels_to_broadcast.push((wf_id.clone(), task_id.clone()));
                                        }
                                    }
                                }
                            }
                        }
                        for wf_id in expired {
                            if let Some(mut slot) = state.slots.get_mut(&wf_id) {
                                if !slot.execution.is_terminal() {
                                    slot.execution.mark_workflow_failed("workflow timeout exceeded".into());
                                    crate::metrics::inc_workflow_timeouts();
                                    crate::metrics::inc_workflows_failed();
                                    crate::metrics::dec_active_workflows();
                                    tracing::warn!("workflow {} timed out, marked as failed", wf_id.as_str());

                                    match serialize_rkyv(&slot.execution) {
                                        Ok(exec_bytes) => {
                                            persist_batch.push((exec_key(&wf_id), exec_bytes));
                                        }
                                        Err(e) => {
                                            // 序列化失败不得静默丢弃：记录 error 并重新标记脏，
                                            // 让后台 flush 在下一轮重试落盘。
                                            tracing::error!(
                                                workflow = %wf_id.as_str(),
                                                error = %e,
                                                "failed to serialize timed-out workflow execution"
                                            );
                                            state.mark_dirty(&wf_id);
                                        }
                                    }

                                    // 对齐 fail_task 路径：超时失败的工作流写入 Failed 事件，
                                    // 供订阅者观测（事件写入失败不阻断状态推进）。
                                    super::persistence::append_event(
                                        event_log.as_ref(),
                                        WorkflowEventPayload::Failed {
                                            workflow_id: wf_id.clone(),
                                            error: "workflow timeout exceeded".into(),
                                        },
                                    );

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
                        // B2：广播取消消息。即使持久化失败也要尝试取消，否则运行中的
                        // 任务会继续占用 Worker 槽位直到自身完成或超时。
                        if let Some(ref network) = network {
                            for (wf_id, task_id) in &cancels_to_broadcast {
                                let msg = crate::common::wire::CancelBroadcast {
                                    task_id: task_id.clone(),
                                    workflow_id: wf_id.clone(),
                                };
                                match postcard::to_allocvec(&msg) {
                                    Ok(bytes) => {
                                        if let Err(e) = network
                                            .broadcast(TOPIC_CANCEL, bytes)
                                            .await
                                        {
                                            tracing::warn!(
                                                workflow_id = %wf_id,
                                                task_id = %task_id,
                                                error = %e,
                                                "failed to broadcast cancel for timed-out workflow task"
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            workflow_id = %wf_id,
                                            task_id = %task_id,
                                            error = %e,
                                            "failed to encode CancelBroadcast for timed-out workflow task"
                                        );
                                    }
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
        // increment_attempt=true：故障转移重派发推进派发代数（TaskState.attempt），
        // 新一代 TaskDefinition 携带递增后的 attempt，用于区分旧代在途执行的迟到结果。
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
        // 终态守卫拒绝（workflow/task 已终态）时状态未变：不追加 TaskFailed
        // 事件、不重复终态收尾——同一失败的重复/迟到投递（本地通道 / 远端
        // 直连 / gossip 三路回灌）必须产生一致的状态与历史。attempt fencing
        // 由 `WorkflowActor::on_task_result` 在进入本方法前统一裁决（当前
        // 协议传 `None` 放行，DAG 层 `fail_task` 内部校验保留为最终防线）。
        let (accepted, is_terminal) = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            let accepted = slot.execution.can_transition_task(task_id);
            slot.execution.fail_task(task_id, error, mode, None);
            (accepted, accepted && slot.execution.is_terminal())
        };

        if accepted {
            self.log_event(WorkflowEventPayload::TaskFailed {
                workflow_id: workflow_id.clone(),
                task_id: task_id.clone(),
                error: error_for_event,
            });
        }

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
            self.notify_terminal(workflow_id);
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
        result_attempt: Option<u32>,
    ) -> Result<CompletionInfo> {
        let (skipped, is_terminal) = {
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
            if !slot.execution.can_transition_task(task_id) {
                // 迟到/重复的完成结果：工作流或任务已终态（含 Cancelled/Failed），
                // 拒绝改写，仅记录 debug 级日志，不产生事件、不推进状态。
                tracing::debug!(
                    workflow = %workflow_id.as_str(),
                    task = %task_id.as_str(),
                    "ignoring completion for already-terminal workflow/task"
                );
                (true, false)
            } else if !slot
                .execution
                .attempt_fencing_passes(task_id, result_attempt)
            {
                // 过期派发代数的迟到结果：丢弃，不推进状态、不发事件。
                (true, false)
            } else {
                slot.execution
                    .mark_task_completed(task_id, result, result_attempt);
                (false, slot.execution.is_terminal())
            }
        };

        if skipped {
            return Ok(CompletionInfo {
                workflow_terminal: false,
                ready_successors: vec![],
                conditional_edges: vec![],
            });
        }

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
                    let result_bytes = crate::common::pack_group(&results)?;
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

        self.notify_terminal(workflow_id);
        Ok(CompletionInfo {
            workflow_terminal: true,
            ready_successors: vec![],
            conditional_edges: vec![],
        })
    }

    /// Notify waiters that a workflow has reached a terminal state.
    /// Fires the per-workflow oneshot channel for instant wake-up.
    ///
    /// 只触发当前 workflow 的 oneshot：其他已终态工作流的等待者在注册时
    /// 已由 `register_terminal_waiter` 的"注册后检查"立即解决，无需在此
    /// 全量扫描兜底。
    fn notify_terminal(&self, workflow_id: &WorkflowId) {
        self.state.fire_terminal_oneshot(workflow_id);
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

        self.notify_terminal(workflow_id);
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

    /// After recovery, returns all tasks that are ready to run for every
    /// non-terminal workflow: only tasks whose persisted state is
    /// [`Phase::Pending`] **and** whose pending counter has reached 0.
    ///
    /// 状态过滤不可省略——`Orchestrator::recover`（`orchestrator/persistence.rs`）
    /// 会把非终态工作流的 Running 任务重置为 Pending，而 Completed/Failed/
    /// Cancelled/Skipped 任务保持原状态；仅按 `pending == 0` 过滤会把已完成
    /// 任务也重建派发，导致副作用重复执行（根任务的 pending 恒为 0）。
    ///
    /// ## 接线约定
    ///
    /// 本方法只做重建，**不负责派发**。生产接线位于 `builder.rs`：节点启动时
    /// 先于 Worker 事件循环调用本方法（此时 Orchestrator 仍独占引用），返回的
    /// 任务经 `SchedulerActor::enqueue_batch` 快路径交给调度器。
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
                .filter(|(tid, &count)| {
                    count == 0
                        && slot
                            .execution
                            .tasks
                            .get(tid)
                            .is_some_and(|t| t.state == Phase::Pending)
                })
                .map(|(tid, _)| tid.clone())
                .collect();

            if ready_ids.is_empty() {
                continue;
            }

            match self.build_ready_tasks_from_slot(slot, workflow_id, &ready_ids) {
                Ok(tasks) => {
                    tracing::info!(
                        "recovered workflow {} with {} ready tasks",
                        workflow_id.as_str(),
                        tasks.len()
                    );
                    all_ready.extend(tasks);
                }
                Err(e) => {
                    tracing::error!(
                        workflow = %workflow_id.as_str(),
                        error = %e,
                        "failed to rebuild ready tasks for recovered workflow"
                    );
                }
            }
        }
        if !all_ready.is_empty() {
            tracing::debug!(
                count = all_ready.len(),
                "recovered ready tasks rebuilt; caller (builder) enqueues them into the scheduler"
            );
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
            // 这同时处理 Failed → Pending 的状态转换。attempt 同步递增：
            // 每次重派发都推进派发代数，使重试结果可与其他代区分（fencing 前提）。
            slot.execution.reset_task(task_id, true, true);

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
