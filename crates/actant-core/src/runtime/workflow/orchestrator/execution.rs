//! Orchestrator 的 `execution` 职责子模块。
//!
//! 负责工作流提交（整图与增量）、启动、任务完成与失败处理、
//! 取消、重试（orchestrator 单层化）与调度。

use std::collections::HashMap;

use crate::common::serialization::serialize_rkyv;
use crate::common::{Result, RetryPolicy, TaskDefinition, TaskId, WorkflowId, WorkflowInvocation};
use crate::runtime::workflow::{Dag, DagNode, FailureScope, Phase, Terminal, WorkflowExecution};

use super::{keys::*, types::*, Orchestrator};

mod timeout;

/// 第 `retry_index` 次重试（0 基）的等待时长。
///
/// 公式：`delay_ms * backoff_multiplier^retry_index`，被 `max_delay_ms` 钳制。
/// `backoff_multiplier` 小于 1（含 NaN）时按 1 处理，保证延迟不递减；
/// 浮点转整数使用饱和转换，不会因极端配置溢出。
fn retry_delay_ms(policy: &RetryPolicy, retry_index: u32) -> u64 {
    let base = policy.delay_ms as f64;
    let factor = policy.backoff_multiplier.max(1.0).powi(retry_index as i32);
    let capped = (base * factor).min(policy.max_delay_ms as f64);
    capped.max(0.0) as u64
}

/// flow 增量提交的节点指纹比对（重放 fail-fast 的实现核心）。
///
/// 比对范围覆盖决定任务行为的全部字段（name / payload / timeout / priority /
/// retry_policy / tags / quota）与依赖边集合：重放体的第 n 次提交必须与历史中同序位节点
/// 完全一致，任何漂移（如上游结果差异、提交序列错位）都判为指纹不一致。
fn flow_node_matches(existing: &DagNode, incoming: &DagNode, dag: &Dag, deps: &[TaskId]) -> bool {
    if existing.name != incoming.name
        || existing.payload != incoming.payload
        || existing.timeout_ms != incoming.timeout_ms
        || existing.priority != incoming.priority
        || existing.retry_policy != incoming.retry_policy
        || existing.tags != incoming.tags
        || existing.quota != incoming.quota
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
        self.submit_with_invocation(workflow_id, dag, None).await
    }

    /// 与 [`Self::submit`] 相同，但把调用描述写入 `Submitted` 事件。
    ///
    /// `invocation` 是 journal 的一部分：恢复后的任意节点都能据此重建
    /// “这是什么工作流、哪版定义、带什么入参”，不依赖提交进程内存。
    #[tracing::instrument(level = "debug", skip(self, dag, invocation), fields(workflow = %workflow_id, nodes = dag.nodes().count()))]
    pub async fn submit_with_invocation(
        &self,
        workflow_id: WorkflowId,
        dag: Dag,
        invocation: Option<WorkflowInvocation>,
    ) -> Result<()> {
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

        self.log_event(JournalEvent::Submitted {
            workflow_id: workflow_id.clone(),
            invocation,
        });
        // 节点新增事件。当前节点唯一变更点是整图提交；增量提交后
        // 每个节点在其写入点单独追加。携带完整节点定义使历史可重建 DAG。
        {
            let slot = self.state.slots.get(&workflow_id);
            if let Some(slot) = slot {
                for node in slot.dag.nodes() {
                    self.log_event(JournalEvent::NodeAdded {
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
        self.submit_with_timeout_and_invocation(workflow_id, dag, timeout_ms, None)
            .await
    }

    /// 带工作流级 deadline 与 invocation 描述的提交。
    pub async fn submit_with_timeout_and_invocation(
        &self,
        workflow_id: WorkflowId,
        dag: Dag,
        timeout_ms: u64,
        invocation: Option<WorkflowInvocation>,
    ) -> Result<()> {
        self.submit_with_invocation(workflow_id.clone(), dag, invocation)
            .await?;
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
                self.log_event(JournalEvent::NodeAdded {
                    workflow_id: workflow_id.clone(),
                    node,
                });
            }
        }
        if auto_started {
            self.log_event(JournalEvent::Started {
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
    /// retries 为 0（Python flow 提交路径置 0），worker 层不重试，两层
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
                        (true, retry_delay_ms(&policy, count))
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

    /// 由 DAG 节点构造待派发的任务定义。
    ///
    /// 从节点派生的字段（名称 / 载荷 / 重试策略 / 优先级 / 超时 / 放置诉求）
    /// 集中在此转换：派发点若各自抄写字面量，新增字段（如 tags/quota）会
    /// 在某个出口被静默丢弃。
    fn task_definition(
        &self,
        dag: &Dag,
        node: &DagNode,
        workflow_id: Option<WorkflowId>,
        payload: Vec<u8>,
        attempt: u32,
    ) -> TaskDefinition {
        // 远端执行完的节点结果要直连回到编排节点。node_id 是逻辑名（Python
        // 嵌入下即 Runtime.name），不能当作网络地址使用；这里带上本节点可
        // 连接的 endpoint addr，远端 completion 才能完成直连投递。
        let origin_endpoint_addr =
            self.network
                .as_ref()
                .and_then(|network| match network.listen_addresses() {
                    Ok(addresses) => Some(addresses.endpoint_addr),
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            "workflow task origin endpoint address unavailable"
                        );
                        None
                    }
                });
        TaskDefinition {
            id: node.task_id.clone(),
            name: node.name.clone(),
            payload,
            workflow_id,
            target_node: None,
            origin_node: self.node_id.clone(),
            retry_policy: dag.effective_retry_policy(&node.task_id),
            priority: node.priority,
            timeout_ms: node.timeout_ms,
            attempt,
            lease_deadline_ms: 0,
            enqueued_at_ms: 0,
            target_endpoint_addr: None,
            origin_endpoint_addr,
            tags: node.tags.clone(),
            quota: node.quota.clone(),
            pinned_target: false,
        }
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

            // 根任务与后继任务走同一构造入口：payload 必须在派发前签名，
            // 否则 `require_payload_signing` 节点会以“unsigned payload”拒绝
            // 整个工作流的首批任务。
            let root_ids: Vec<TaskId> = slot
                .dag
                .roots()
                .into_iter()
                .map(|node| node.task_id.clone())
                .collect();
            self.build_ready_tasks_from_slot(&slot, workflow_id, &root_ids)?
        };

        // Non-terminal: defer persistence to background flush
        self.state.mark_dirty(workflow_id);

        self.log_event(JournalEvent::Started {
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
    /// 返回值为 `(ready_successors, workflow_terminal)`。第二个元素是显式的
    /// 终态标志，**不应**通过 `ready_successors.is_empty()` 推断——末任务完成
    /// 或封口收尾同样返回空列表，是否终态只有执行快照知道。调用方
    /// （如 `WorkflowActor`）必须使用此标志判断是否触发终态通知。
    pub async fn on_task_completed(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        result: Vec<u8>,
    ) -> Result<(Vec<TaskDefinition>, bool)> {
        // 结果通路（WireTaskResult / WireDagStateUpdate / COMPLETE_TASK actor
        // 载荷）尚未携带派发代数（wire.rs 本批次冻结），传 `None` 放行 attempt
        // fencing；待协议扩展后由调用方传入结果的 attempt 即接入 fencing。
        let info = self
            .complete_task(workflow_id, task_id, result, None)
            .await?;

        // 将 ready_successors 转换为 TaskDefinition。
        let ready = self.build_ready_tasks_for(workflow_id, &info.ready_successors)?;

        Ok((ready, info.workflow_terminal))
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
    /// 快照、追加终态事件、更新指标并唤醒等待者——取消必须触发收尾，否则
    /// 工作流会永久停留在 `Running`（flow 的终态轮询因此挂起）。
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
            self.log_event(JournalEvent::TaskCancelled {
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

            ready.push(self.task_definition(
                &slot.dag,
                node,
                Some(workflow_id.clone()),
                payload,
                attempt,
            ));
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
        Ok(Some(self.task_definition(
            &slot.dag,
            node,
            Some(workflow_id.clone()),
            payload,
            attempt,
        )))
    }

    /// 将任务推进到 `Running`（本地派发 / 远端 gossip 同路径）。
    ///
    /// 终态吸收：工作流已终态或任务已终态时不推进——迟到的 `Running` 不得把
    /// `Cancelled` / `Failed` 任务"复活"，与 `mark_task_completed` 的守卫语义一致。
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
        self.log_event(JournalEvent::TaskRunning {
            workflow_id: workflow_id.clone(),
            task_id: task_id.clone(),
        });
        Ok(())
    }

    /// Mark a task as failed.
    ///
    /// The `mode` parameter controls the scope:
    /// - `FailureScope::TaskOnly`: Only mark the task as Failed. The workflow
    ///   remains non-terminal. 当前生产路径不使用（重试裁决在
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
            self.log_event(JournalEvent::TaskFailed {
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
            self.log_event(JournalEvent::Failed {
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
                self.log_event(JournalEvent::TaskCompleted {
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
            ready_successors: ready_ids,
        })
    }

    /// 工作流终态收尾的唯一实现点：持久化终态快照（Completed 附带聚合结果）、
    /// 追加终态事件（Completed / Cancelled / Failed 三选一）、更新指标并唤醒
    /// 等待者。三条触发路径共用：末任务完成（`complete_task`）、
    /// 封口时已全部终态（`seal_workflow`）、
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
                self.log_event(JournalEvent::Completed {
                    workflow_id: workflow_id.clone(),
                });
                crate::metrics::inc_workflows_completed();
            }
            Phase::Cancelled => {
                // 取消收尾（末节点被取消 / 工作流级 cancel）：单独成支，不得
                // 复用 Failed 事件——消费方据事件类型区分「取消」与「失败」。
                self.log_event(JournalEvent::Cancelled {
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
                self.log_event(JournalEvent::Failed {
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
    /// `BusEvent::TaskCancelled` 结算，而等待点 park 是另一条阻塞原语，
    /// 仅靠运行时关停的 `release_all_wait_point_waiters` 释放不够：缺了这一步，
    /// cancel / fail / deadline 都到不了 park 中的 flow 体——工作流已终态而函数体
    /// 永久挂起（实测：8s 观察窗内纹丝不动）。
    /// 三个终态入口（`fail_task` / `complete_terminal` / `mark_workflow_failed`）
    /// 共用本方法，故在此单点释放即可。
    fn notify_terminal(&self, workflow_id: &WorkflowId) {
        self.state.release_wait_waiters(workflow_id);
    }

    fn compute_ready_successors(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<Vec<TaskId>> {
        let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
            crate::common::ActantError::NotFound(format!(
                "workflow {} not found",
                workflow_id.as_str()
            ))
        })?;

        let successors: Vec<TaskId> = slot.dag.successor_ids(task_id);

        let mut ready_ids: Vec<TaskId> = Vec::new();
        for succ_id in &successors {
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
        Ok(ready_ids)
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
    /// Cancelled 任务保持原状态；仅按 `pending == 0` 过滤会把已完成
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
