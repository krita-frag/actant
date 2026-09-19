//! Orchestrator 的 `execution` 职责子模块。
//!
//! 负责工作流提交（整图与增量）、启动、任务完成处理、条件边求值、
//! 取消、重试（orchestrator 单层化）与调度。

use std::collections::HashMap;

use crate::common::serialization::serialize_rkyv;
use crate::common::wire::TOPIC_CANCEL;
use crate::common::{Result, TaskDefinition, TaskId, WorkflowId};
use crate::runtime::network::{NetworkEvent, NetworkMessage};
use crate::runtime::workflow::{Dag, DagNode, FailureScope, Phase, Terminal, WorkflowExecution};

use super::{keys::*, types::*, Orchestrator};

/// flow 增量提交的节点指纹比对（重放 fail-fast 的实现核心）。
///
/// 比对范围覆盖决定任务行为的全部字段（name / payload / timeout / priority /
/// retry_policy）与依赖边集合：重放体的第 n 次提交必须与历史中同序位节点
/// 完全一致，任何漂移（如上游结果差异、提交序列错位）都判为指纹不一致。
fn flow_node_matches(existing: &DagNode, incoming: &DagNode, dag: &Dag, deps: &[TaskId]) -> bool {
    if existing.name != incoming.name
        || existing.payload != incoming.payload
        || existing.timeout_ms != incoming.timeout_ms
        || existing.priority != incoming.priority
        || existing.retry_policy != incoming.retry_policy
    {
        return false;
    }
    let recorded: std::collections::HashSet<&TaskId> = dag
        .predecessors_of(&incoming.task_id)
        .iter()
        .map(|n| &n.task_id)
        .collect();
    let incoming_deps: std::collections::HashSet<&TaskId> = deps
        .iter()
        .filter(|d| **d != incoming.task_id && dag.get_node(d).is_some())
        .collect();
    recorded == incoming_deps
}

impl Orchestrator {
    /// Submits a workflow DAG for execution.
    ///
    /// 若 `workflow_id` 已存在且为占位符（[`SlotState::Loading`]），此调用会覆盖
    /// 占位符——这是 `adopt_workflow` 后本地再次提交同一 workflow 的合法路径。
    /// 若已存在且为 [`SlotState::Ready`]，返回 [`ActantError::AlreadyExists`]；
    /// 唯一例外是空 DAG（flow 的持久化外壳）：flow 重放会对同一
    /// workflow 再次创建外壳，节点与历史归既存工作流所有，按幂等 no-op 处理。
    #[tracing::instrument(level = "debug", skip(self, dag), fields(workflow = %workflow_id, nodes = dag.nodes().count()))]
    pub async fn submit(&self, workflow_id: WorkflowId, dag: Dag) -> Result<()> {
        // 已就绪的工作流不允许重复提交；占位符允许覆盖（adopt 后本地重新提交）。
        if self.state.is_ready(&workflow_id) {
            if dag.node_count() == 0 {
                // flow 外壳的幂等重建（重放 / 重入）：不覆盖既存状态。
                return Ok(());
            }
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

        let mut execution = WorkflowExecution::new(workflow_id.clone(), task_ids)
            .with_failure_strategy(dag.failure_strategy);
        // 整图提交：全部节点已知，立即封口。空 DAG 是 flow 增量提交
        // 的外壳（节点随后经 add_node 加入），保持未封口。
        if dag.node_count() > 0 {
            execution.seal_nodes();
        }

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

        self.log_event(WorkflowEventPayload::Submitted {
            workflow_id: workflow_id.clone(),
        });
        // 节点新增事件。当前节点唯一变更点是整图提交；增量提交后
        // 每个节点在其写入点单独追加。携带完整节点定义使历史可重建 DAG。
        {
            let slot = self.state.slots.get(&workflow_id);
            if let Some(slot) = slot {
                for node in slot.dag.nodes() {
                    self.log_event(WorkflowEventPayload::NodeAdded {
                        workflow_id: workflow_id.clone(),
                        node: node.clone(),
                    });
                }
            }
        }

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

    /// 增量加入单个节点（flow 提交路径）。
    ///
    /// flow 函数体每次 `task.submit()` 经此把节点写入工作流：登记进 DAG 与
    /// execution、建立依赖边、同步落盘（先持久化再派发），并返回派发裁决。
    /// 依赖边指向的节点若已在 DAG 中且未终态，`pending` 计数逐边登记，由
    /// 前驱完成事件触发派发；依赖值本身已由提交方父进程解析内联进 payload
    /// （flow 语义：函数体阻塞解析上游结果后才序列化载荷），边仅承载结构
    /// 与派发门槛。
    ///
    /// ## 重放命中
    ///
    /// 节点已存在时做指纹比对（name / payload / timeout / priority /
    /// retry_policy / 依赖边集合）：一致 → 返回 [`AddNodeOutcome::Existing`]
    /// 携带当前状态与结果字节（不重新提交、不重跑）；不一致 →
    /// [`ActantError::Replay`]——提交序列确定性契约被破坏，显式失败。
    ///
    /// 工作流仍处 `Pending`（尚无任何节点）时随首个节点自动转入 `Running`
    /// 并追加 `Started` 事件，使工作流级 deadline 生效。
    pub async fn add_node(
        &self,
        workflow_id: &WorkflowId,
        node: DagNode,
        deps: Vec<TaskId>,
    ) -> Result<AddNodeOutcome> {
        let task_id = node.task_id.clone();

        // 阶段 1：重放检测 + 结构登记（slot 写锁内完成）。
        let (auto_started, pending_count) = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            if slot.state == SlotState::Loading {
                return Err(crate::common::ActantError::InvalidState(format!(
                    "workflow {} is still loading (placeholder), cannot add node",
                    workflow_id.as_str()
                )));
            }
            // 重放命中判定先于终态守卫：工作流已终态（如全部节点在重放前
            // 完成）时，命中历史节点的提交仍是合法重放，须返回记录结果。
            if let Some(existing) = slot.dag.get_node(&task_id) {
                if !flow_node_matches(existing, &node, &slot.dag, &deps) {
                    return Err(crate::common::ActantError::Replay(format!(
                        "node {} in workflow {} exists with a different definition; \
                         the flow body submit sequence diverged from the recorded history",
                        task_id.as_str(),
                        workflow_id.as_str()
                    )));
                }
                let ts = slot.execution.tasks.get(&task_id);
                return Ok(AddNodeOutcome::Existing {
                    state: ts.map(|t| t.state).unwrap_or(Phase::Pending),
                    result: ts.and_then(|t| t.result.clone()),
                    error: ts.and_then(|t| t.error.clone()),
                });
            }

            if slot.execution.is_terminal() {
                return Err(crate::common::ActantError::InvalidState(format!(
                    "workflow {} is already terminal, cannot add node",
                    workflow_id.as_str()
                )));
            }

            let auto_started = slot.execution.state == Phase::Pending;
            slot.dag.add_node(node)?;
            let mut pending_count = 0usize;
            for dep in deps {
                if dep == task_id || slot.dag.get_node(&dep).is_none() {
                    // 外部依赖（flow 之外提交的任务）：结果已内联进 payload，
                    // 不构成 DAG 边（与 FlowDAG 时代的 to_edges 过滤语义一致）。
                    continue;
                }
                slot.dag.add_edge(dep.clone(), task_id.clone())?;
                let dep_unfinished = !slot
                    .execution
                    .tasks
                    .get(&dep)
                    .is_some_and(|t| t.state.is_terminal());
                if dep_unfinished {
                    pending_count += 1;
                }
            }
            slot.execution.register_task(task_id.clone());
            slot.pending.insert(task_id.clone(), pending_count);
            if auto_started {
                slot.execution.mark_running();
            }
            (auto_started, pending_count)
        };

        // 阶段 2：同步落盘（先持久化再派发）。节点定义必须在派发前可恢复，
        // 否则节点死亡后工作流历史缺失该节点，重放无法重建提交序列。
        if let Some(ref store) = self.store {
            let (dag_bytes, exec_bytes, pending_bytes) = {
                let slot = self.state.slots.get(workflow_id).ok_or_else(|| {
                    crate::common::ActantError::NotFound(format!(
                        "workflow {} not found",
                        workflow_id.as_str()
                    ))
                })?;
                (
                    serialize_rkyv(&slot.dag)?,
                    serialize_rkyv(&slot.execution)?,
                    serialize_rkyv(&slot.pending)?,
                )
            };
            store
                .put_batch(&[
                    (dag_key(workflow_id), dag_bytes),
                    (exec_key(workflow_id), exec_bytes),
                    (pending_key(workflow_id), pending_bytes),
                ])
                .await?;
        }

        // 阶段 3：历史事件。NodeAdded 携带完整节点定义使历史可重建 DAG。
        {
            let node = self
                .state
                .slots
                .get(workflow_id)
                .and_then(|slot| slot.dag.get_node(&task_id).cloned());
            if let Some(node) = node {
                self.log_event(WorkflowEventPayload::NodeAdded {
                    workflow_id: workflow_id.clone(),
                    node,
                });
            }
        }
        if auto_started {
            self.log_event(WorkflowEventPayload::Started {
                workflow_id: workflow_id.clone(),
            });
        }

        // 阶段 4：派发裁决。依赖已满足 → 构造 TaskDefinition 交调用方入队。
        let ready = if pending_count == 0 {
            self.build_task_for_id(workflow_id, &task_id)?
        } else {
            None
        };
        Ok(AddNodeOutcome::Created {
            ready: ready.map(Box::new),
        })
    }

    /// 封口工作流节点集（flow 函数体返回信号）。
    ///
    /// flow 增量提交期间节点集不封口，终态判定延迟到封口后（见
    /// [`WorkflowExecution::check_workflow_completion`]）。封口时若全部任务
    /// 已终态（fire-and-forget flow），立即执行终态收尾（持久化、事件、
    /// 指标、唤醒等待者）；封口前已终态（重放命中已完结工作流）为幂等 no-op。
    pub async fn seal_workflow(&self, workflow_id: &WorkflowId) -> Result<()> {
        let terminal_snapshot = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            if slot.state == SlotState::Loading {
                return Err(crate::common::ActantError::InvalidState(format!(
                    "workflow {} is still loading (placeholder), cannot seal",
                    workflow_id.as_str()
                )));
            }
            let was_terminal = slot.execution.is_terminal();
            slot.execution.seal_nodes();
            if !was_terminal && slot.execution.is_terminal() {
                Some(slot.execution.clone())
            } else {
                None
            }
        };

        match terminal_snapshot {
            Some(exec_snapshot) => {
                self.complete_terminal(workflow_id, None, &exec_snapshot)
                    .await?;
            }
            None => {
                self.state.mark_dirty(workflow_id);
            }
        }
        Ok(())
    }

    /// 节点失败裁决：orchestrator 是 flow 任务的唯一重试
    /// 执行者。
    ///
    /// 节点 RetryPolicy（来自 `@task(retries=...)` 的映射）仍有余量且任务/
    /// 工作流未终态时：`begin_orchestrated_retry` 把任务直接从 Running 重置回
    /// Pending（attempt 同步递增，fencing 前提），返回重派发的
    /// [`TaskDefinition`] 与重试间隔，调用方延迟入队调度器；余量耗尽或不可
    /// 重试 → 回退到既有 `WorkflowLevel` 失败语义。派发侧 payload 头部
    /// retries 已剥离（Python flow 提交路径置 0），worker 层不再重试，两层
    /// 不会叠加。
    ///
    /// 返回 `None` 表示失败为最终结果，调用方应将结果事件发布给提交方。
    ///
    /// ## 迟到失败守卫
    ///
    /// 重试中的任务处于 `Pending`（`retry_count > 0` 可区分"从未运行的
    /// Pending"）：此时到达的重复失败（gossip / 直连重投）按过期结果忽略，
    /// 不得把已排定重试的任务改写为 Failed。wire 协议尚未携带派发代数，
    /// attempt fencing 放行，此守卫是引入重试后的必要补偿。
    pub async fn handle_task_failure(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        error: String,
    ) -> Result<Option<(TaskDefinition, u64)>> {
        let (should_retry, delay_ms) = {
            let slot = self.state.slots.get(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            if !slot.execution.can_transition_task(task_id) {
                return Ok(None);
            }
            match slot
                .execution
                .tasks
                .get(task_id)
                .map(|t| (t.state, t.retry_count()))
            {
                // 重试已在途：迟到的重复失败（gossip / 直连重投）按过期结果忽略，
                // 不得把已排定重试的任务改写为 Failed。
                Some((Phase::Pending, count)) if count > 0 => return Ok(None),
                // 从未派发的本地 Pending（retry_count == 0）且工作流尚未封口：
                // 只可能来自编排内部的合成失败，忽略。封口后的 Pending 失败
                // 报告按真实失败处理——远端执行中任务的真实失败可与本地状态
                // 推进乱序到达（gossip / 直连），交由 can_transition_task 与
                // fencing 决定取舍。
                Some((Phase::Pending, 0)) if !slot.execution.nodes_sealed() => {
                    return Ok(None);
                }
                Some((Phase::Cancelled, _)) => return Ok(None),
                _ => {}
            }
            match slot.dag.effective_retry_policy(task_id) {
                Some(policy) => {
                    let count = slot
                        .execution
                        .tasks
                        .get(task_id)
                        .map(|t| t.retry_count())
                        .unwrap_or(0);
                    if count < policy.max_retries {
                        (true, policy.delay_ms)
                    } else {
                        (false, 0)
                    }
                }
                None => (false, 0),
            }
        };

        if should_retry {
            let reset = {
                let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                    crate::common::ActantError::NotFound(format!(
                        "workflow {} not found",
                        workflow_id.as_str()
                    ))
                })?;
                let reset = slot.execution.begin_orchestrated_retry(task_id);
                if reset {
                    let pred_count = slot.dag.predecessor_count(task_id);
                    slot.pending.insert(task_id.clone(), pred_count);
                }
                reset
            };
            if reset {
                self.state.mark_dirty(workflow_id);
                let task = self.build_task_for_id(workflow_id, task_id)?;
                if let Some(task) = task {
                    crate::metrics::inc_tasks_retried();
                    tracing::info!(
                        workflow = %workflow_id.as_str(),
                        task = %task_id.as_str(),
                        "orchestrator-driven retry scheduled"
                    );
                    return Ok(Some((task, delay_ms)));
                }
                // build 失败（并发淘汰）：按终局失败处理。
            }
        }
        self.fail_task(workflow_id, task_id, error, FailureScope::WorkflowLevel)
            .await?;
        Ok(None)
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

        // 阶段 2：将 ready_successors 转换为 TaskDefinition。
        let mut ready = self.build_ready_tasks_for(workflow_id, &info.ready_successors)?;

        // 阶段 3：处理条件边——内部求值（若有 evaluator）或返回给调用方外部评估。
        let conditional_edges = self
            .process_conditional_edges(workflow_id, task_id, info.conditional_edges, &mut ready)
            .await?;

        Ok((ready, conditional_edges, info.workflow_terminal))
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
            self.complete_terminal(workflow_id, Some(task_id), &execution)
                .await?;
            return Ok(Vec::new());
        }

        // 阶段 2：级联跳过——沿非条件后继边递归减少 pending，收集 ready 与新增跳过任务。
        let ready_ids = self.cascade_skip(workflow_id, task_id).await?;

        // 阶段 3：级联后若工作流进入终态，完成收尾。
        if let Some(execution) = self.execution_if_terminal(workflow_id)? {
            self.complete_terminal(workflow_id, Some(task_id), &execution)
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

    /// 取消整个工作流并置为 `Cancelled` 终态。
    ///
    /// 收尾统一走 [`Self::complete_terminal`]：持久化终态快照、追加
    /// `Cancelled` 终态事件、更新指标并唤醒等待者。已终态（Failed /
    /// Completed / Cancelled）的工作流为幂等 no-op——`mark_cancelled` 自带终态
    /// 守卫，兜底取消不得改写更准确的事实源（如 fail-fast 的 Failed）。
    pub async fn cancel(&self, workflow_id: &WorkflowId) -> Result<()> {
        let terminal_snapshot = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            let was_terminal = slot.execution.is_terminal();
            slot.execution.mark_cancelled();
            (!was_terminal && slot.execution.is_terminal()).then(|| slot.execution.clone())
        };

        if let Some(exec_snapshot) = terminal_snapshot {
            self.complete_terminal(workflow_id, None, &exec_snapshot)
                .await?;
        }
        Ok(())
    }

    /// 取消工作流内的单个任务。
    ///
    /// 返回 `Ok(true)` 表示任务处于 Running/Pending 并已置为 Cancelled；
    /// `Ok(false)` 表示任务不存在或已在终态。
    ///
    /// 取消是终态事件：若本次取消使工作流全部节点终态（典型为「最后一个在途
    /// 节点被取消」），立即走 [`Self::complete_terminal`] 收尾——持久化终态
    /// 快照、追加终态事件、更新指标并唤醒等待者。此前该路径只改节点状态、
    /// 不触发收尾，工作流会永久停留在 `Running`（flow 的终态轮询因此挂起）。
    pub async fn cancel_task(&self, workflow_id: &WorkflowId, task_id: &TaskId) -> Result<bool> {
        let (cancelled, terminal_snapshot) = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            // 收尾只在「本次取消导致进入终态」时执行一次：已终态工作流上的
            // 迟到取消不得重复触发终态事件与指标。
            let was_terminal = slot.execution.is_terminal();
            let cancelled = slot.execution.cancel_task(task_id);
            let became_terminal = !was_terminal && slot.execution.is_terminal();
            let snapshot = became_terminal.then(|| slot.execution.clone());
            (cancelled, snapshot)
        };

        if cancelled {
            self.log_event(WorkflowEventPayload::TaskCancelled {
                workflow_id: workflow_id.clone(),
                task_id: task_id.clone(),
            });
        }

        match terminal_snapshot {
            Some(exec_snapshot) => {
                self.complete_terminal(workflow_id, Some(task_id), &exec_snapshot)
                    .await?;
            }
            None => {
                if cancelled {
                    self.state.mark_dirty(workflow_id);
                }
            }
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
        // 等待点到期扫描（`poll_expired_timers`）需要一条不依赖 `&self`
        // 的调用入口——spawn 闭包有 `'static` 约束，无法借用 `self`。这里整体克隆
        // 一个 `Orchestrator` 句柄带进闭包：其字段全是 `Arc`/轻量值（`Store`、
        // `Transport`、`EventLog`、`OrchestratorState` 均为共享句柄），克隆成本可忽略；
        // 好处是**复用已测试的同一方法实现**，避免在闭包内重写一份判定逻辑造成行为漂移。
        let orchestrator = self.clone();
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
                                    if let Some(id) = super::persistence::append_event(
                                        event_log.as_ref(),
                                        WorkflowEventPayload::Failed {
                                            workflow_id: wf_id.clone(),
                                            error: "workflow timeout exceeded".into(),
                                        },
                                    ) {
                                        state.record_event_seq(&wf_id, id);
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
                        // B2：广播取消消息。即使持久化失败也要尝试取消，否则运行中的
                        // 任务会继续占用 Worker 槽位直到自身完成或超时。
                        //
                        // 注意这里有**两条腿**，缺一不可：
                        // - `broadcast` 走 gossip，只到邻居——管远端执行的任务；
                        // - `inject_local_event` 把同一份字节投回本节点事件通道，
                        //   管本节点自己执行的任务。gossip **不会**把消息发回发送者，
                        //   所以只广播不做自投递时，本节点在途任务不会被取消：工作流
                        //   已标 Failed，而阻塞在任务等待上的 flow 体永久挂起（实测
                        //   本地句柄 10s 观察窗内始终 running）。
                        // 自投递复用 `NetworkEventRouter` 的 `TopicRoute::Cancel`
                        // 分支，与远端收到广播后的处理**完全同路**，避免本地/远端
                        // 行为漂移。
                        if let Some(ref network) = network {
                            for (wf_id, task_id) in &cancels_to_broadcast {
                                let msg = crate::common::wire::CancelBroadcast {
                                    task_id: task_id.clone(),
                                    workflow_id: wf_id.clone(),
                                };
                                let bytes = match postcard::to_allocvec(&msg) {
                                    Ok(bytes) => bytes,
                                    Err(e) => {
                                        tracing::warn!(
                                            workflow_id = %wf_id,
                                            task_id = %task_id,
                                            error = %e,
                                            "failed to encode CancelBroadcast for timed-out workflow task"
                                        );
                                        continue;
                                    }
                                };
                                if let Err(e) =
                                    network.broadcast(TOPIC_CANCEL, bytes.clone()).await
                                {
                                    tracing::warn!(
                                        workflow_id = %wf_id,
                                        task_id = %task_id,
                                        error = %e,
                                        "failed to broadcast cancel for timed-out workflow task"
                                    );
                                }
                                if !network.inject_local_event(NetworkEvent::Message(
                                    NetworkMessage {
                                        topic: TOPIC_CANCEL.to_string(),
                                        data: bytes,
                                    },
                                )) {
                                    tracing::warn!(
                                        workflow_id = %wf_id,
                                        task_id = %task_id,
                                        "failed to self-deliver cancel for timed-out workflow task; \
                                         a locally running task may not be released"
                                    );
                                }
                            }
                        }
                        for wf_id in to_fire {
                            state.fire_terminal_oneshot(&wf_id);
                        }

                        // 扫描到期的 `Timer` 等待点。
                        //
                        // 此前 `poll_expired_timers` 的唯一调用者是测试——生产路径
                        // 没有任何定时任务调用它，导致 Timer 类等待点**永不自动到期**，
                        // 在等待点 park 的 flow 线程永久挂起。这里复用超时 watcher 已有
                        // 的轮询周期（`state_poll_interval_ms`），故：
                        //   **等待点唤醒延迟上界 = state_poll_interval_ms（默认 500ms）**。
                        // 到期即追加 `TimerFired` 事件（进统一历史）、标记脏（随快照落盘）
                        // 并唤醒 oneshot 等待者；重复调用幂等（已 Signaled 不再触发）。
                        match orchestrator.poll_expired_timers().await {
                            Ok(fired) if !fired.is_empty() => {
                                tracing::debug!(
                                    count = fired.len(),
                                    "fired expired timer wait points"
                                );
                            }
                            Ok(_) => {}
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    "failed to scan expired wait points"
                                );
                            }
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

    /// 将任务推进到 `Running`（本地派发 / 远端 gossip 同路径）。
    ///
    /// 终态吸收：工作流已终态或任务已终态时不推进——迟到的 `Running` 不得把
    /// `Cancelled` / `Failed` 任务"复活"，与 `mark_task_completed` /
    /// `mark_task_skipped` 的守卫语义一致。
    pub fn mark_task_running(&self, workflow_id: &WorkflowId, task_id: &TaskId) -> Result<()> {
        let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
            crate::common::ActantError::NotFound(format!(
                "workflow {} not found",
                workflow_id.as_str()
            ))
        })?;
        if !slot.execution.can_transition_task(task_id) {
            return Ok(());
        }
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
    ///   remains non-terminal. 当前生产路径不再使用（重试裁决在
    ///   `handle_task_failure` 内完成，失败终局一律 WorkflowLevel），
    ///   保留供 DAG 层 API 完整性与绕过入口的最终防线。
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
                    .mark_task_completed(task_id, result.clone(), result_attempt);
                // 任务完成事件在终态判定之前追加：末任务完成使工作流进入
                // 终态时，complete_terminal 早退路径不得吞掉 per-task 事件，
                // 否则历史缺失 TaskCompleted（重放/审计无法重建任务终态）。
                self.log_event(WorkflowEventPayload::TaskCompleted {
                    workflow_id: workflow_id.clone(),
                    task_id: task_id.clone(),
                    result: result.clone(),
                });
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
                .complete_terminal(workflow_id, Some(task_id), &exec_snapshot)
                .await;
        }

        let ready_ids = self.compute_ready_successors(workflow_id, task_id)?;

        // Non-terminal: defer persistence to background flush
        self.state.mark_dirty(workflow_id);

        Ok(CompletionInfo {
            workflow_terminal: false,
            ready_successors: ready_ids.ready,
            conditional_edges: ready_ids.conditional,
        })
    }

    /// 工作流终态收尾的唯一实现点：持久化终态快照（Completed 附带聚合结果）、
    /// 追加终态事件（Completed / Cancelled / Failed 三选一）、更新指标并唤醒
    /// 等待者。四条触发路径共用：末任务完成（`complete_task`）、条件分支级联
    /// 跳过（`skip_conditional_branch`）、封口时已全部终态（`seal_workflow`）、
    /// 取消使最后在途节点终态（`cancel_task`）。
    async fn complete_terminal(
        &self,
        workflow_id: &WorkflowId,
        completed_task_id: Option<&TaskId>,
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

        match exec_snapshot.state {
            Phase::Completed => {
                self.log_event(WorkflowEventPayload::Completed {
                    workflow_id: workflow_id.clone(),
                });
                crate::metrics::inc_workflows_completed();
            }
            Phase::Cancelled => {
                // 取消收尾（末节点被取消 / 工作流级 cancel）：单独成支，不得
                // 复用 Failed 事件——消费方据事件类型区分「取消」与「失败」。
                self.log_event(WorkflowEventPayload::Cancelled {
                    workflow_id: workflow_id.clone(),
                });
                crate::metrics::inc_workflows_cancelled();
            }
            _ => {
                // 封口触发的失败收尾（continue 策略下失败在封口时才使工作流
                // 终态）没有单一触发任务，错误摘要取自执行快照。
                let error = match completed_task_id {
                    Some(tid) => format!("workflow failed at task {}", tid.as_str()),
                    None => exec_snapshot
                        .error
                        .clone()
                        .unwrap_or_else(|| "workflow failed".into()),
                };
                self.log_event(WorkflowEventPayload::Failed {
                    workflow_id: workflow_id.clone(),
                    error,
                });
                crate::metrics::inc_workflows_failed();
            }
        }
        crate::metrics::dec_active_workflows();

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
    ///
    /// 同时释放该工作流**等待点 park 的等待者**。`AsyncResult` 的等待由
    /// `BusEvent::TaskCancelled` 结算，而等待点 park 是另一条阻塞原语，此前唯一
    /// 的释放点是运行时的 `release_all_wait_point_waiters`（关停）。缺了这一步，
    /// cancel / fail / deadline 都到不了 park 中的 flow 体——工作流已终态而函数体
    /// 永久挂起（实测：8s 观察窗内纹丝不动）。
    /// 三个终态入口（`fail_task` / `complete_terminal` / `mark_workflow_failed`）
    /// 共用本方法，故在此单点释放即可。
    fn notify_terminal(&self, workflow_id: &WorkflowId) {
        self.state.fire_terminal_oneshot(workflow_id);
        self.state.release_wait_waiters(workflow_id);
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
