//! Orchestrator 的 `execution` 职责子模块。
//!
//! 负责工作流提交（整图与增量）、启动、任务完成与失败处理、
//! 取消、重试（orchestrator 单层化）与调度。

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::common::serialization::serialize_rkyv;
use crate::common::{Result, RetryPolicy, TaskDefinition, TaskId, WorkflowId, WorkflowInvocation};
use crate::runtime::workflow::{Dag, DagNode, FailureScope, Phase, Terminal, WorkflowExecution};

use super::{keys::*, types::*, Orchestrator};

mod timeout;

/// [`Orchestrator::handle_task_failure`] 的裁决结果。
///
/// 结果回灌是 at-least-once（重试队列重投 + 直连/gossip 三路回灌），迟到
/// 重复失败是常态而非异常——`Ignored` 与 `Final` 必须可区分：把迟到重复
/// 失败当作终局发布会让提交方句柄在重试在途时被错误置为失败（甚至已
/// Completed 之后被翻转为失败）。跨 actor 消息边界（`ON_TASK_RESULT`
/// 响应）传递，须可序列化。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FailureOutcome {
    /// 重试已排定：携带重派发定义与延迟毫秒，调用方延迟入队调度器。
    /// `TaskDefinition` 装箱以压平枚举尺寸（裁决值在 actor 消息路径高频传递）。
    Retry(Box<TaskDefinition>, u64),
    /// 终局失败：任务已按失败语义（`failure_strategy`）落定，调用方应把
    /// 失败终态发布给提交方。
    Final,
    /// 忽略：迟到重复失败（重试在途 / 任务已终态 / 未封口合成失败）。
    /// 调用方不得发布终态、不得结算句柄。
    Ignored,
}

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
///
/// `metadata` **刻意不参与指纹**：它是纯标注字段，无放置/调度语义（消费方
/// 仅 `queries` 透传给观测面），变化不构成提交序列漂移；纳入指纹会让纯
/// 标注改动误判为重放冲突。
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

    /// workflow_id 进入 journal topic（`workflow:{id}`）与 event_log 键拼接，
    /// `:` 会让字符串前缀扫描跨工作流污染（id `"a"` 与 `"a:b"` 的 topic
    /// 前缀互为包含，重放会读进别人的事件、trim 甚至误删）；空 id 产生空
    /// topic。提交与增量加节点是仅有的两个 id 进入点，在此统一校验。
    fn validate_workflow_id(workflow_id: &WorkflowId) -> Result<()> {
        let id = workflow_id.as_str();
        if id.is_empty() {
            return Err(crate::common::ActantError::Config(
                "workflow id must not be empty".into(),
            ));
        }
        if id.contains(':') {
            return Err(crate::common::ActantError::Config(format!(
                "workflow id must not contain ':': {id}"
            )));
        }
        Ok(())
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
        Self::validate_workflow_id(&workflow_id)?;
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
        // 整图提交立即封口；flow 的空 DAG 外壳保持未封口，由 seal_workflow 收口。
        let sealed_on_submit = dag.node_count() > 0;

        let mut pending: HashMap<TaskId, usize> = HashMap::new();
        for node in dag.nodes() {
            let pred_count = dag.predecessor_count(&node.task_id);
            pending.insert(node.task_id.clone(), pred_count);
        }

        let mut execution = WorkflowExecution::new(workflow_id.clone(), task_ids)
            .with_failure_strategy(dag.failure_strategy);
        // 整图提交：全部节点已知，立即封口。空 DAG 是 flow 增量提交
        // 的外壳（节点随后经 add_node 加入），保持未封口。
        if sealed_on_submit {
            execution.seal_nodes();
        }

        // 节点事件（含前驱）按拓扑序预生成：journal 单独即可重建 DAG 边、
        // execution 任务与 pending 计数（schema v2）。必须在把 dag 移入
        // 内存状态之前算好。
        let node_events: Vec<JournalEvent> = {
            let ordered = dag.topological_sort()?;
            ordered
                .into_iter()
                .map(|node| {
                    let deps = dag
                        .predecessors_of(&node.task_id)
                        .into_iter()
                        .map(|pred| pred.task_id.clone())
                        .collect();
                    JournalEvent::NodeAdded {
                        workflow_id: workflow_id.clone(),
                        node: node.clone(),
                        deps,
                    }
                })
                .collect()
        };

        self.state
            .insert_workflow(workflow_id.clone(), dag, execution.clone(), pending.clone());

        // 阶段 2：journal-first。Submitted 与全部 NodeAdded 作为一个原子批次
        // 追加，随后才写快照；若快照写入前崩溃，恢复期可从 journal topic
        // 发现该工作流并重放出完整 DAG（见 `Orchestrator::recover`）。
        {
            let mut batch = Vec::with_capacity(node_events.len() + 1);
            batch.push(JournalEvent::Submitted {
                workflow_id: workflow_id.clone(),
                invocation,
            });
            batch.extend(node_events);
            // 封口与结构登记同属一个原子批次：崩溃窗口若只重放出 DAG 而没有
            // 封口，恢复后的工作流永远不进入终态判定。
            if sealed_on_submit {
                batch.push(JournalEvent::NodesSealed {
                    workflow_id: workflow_id.clone(),
                });
            }
            self.log_events(&batch);
        }

        // 阶段 3：同步落盘。写失败返回错误，但内存状态与 journal 已就绪：
        // 后台 flush 会重试，崩溃后恢复也能从 journal 重建。
        if let Some(ref store) = self.store {
            let dag_bytes = {
                let slot = self.state.slots.get(&workflow_id).ok_or_else(|| {
                    crate::common::ActantError::NotFound(format!(
                        "workflow {} not found after submit",
                        workflow_id.as_str()
                    ))
                })?;
                serialize_rkyv(&slot.dag)?
            };
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
        // 0 表示无工作流级 deadline（与 Python 层 `timeout_ms=0` 语义一致）。
        // 写入 Some(0) 会让 is_expired 在下一毫秒即判定超时。
        if timeout_ms > 0 {
            {
                let mut slot = self.state.slots.get_mut(&workflow_id).ok_or_else(|| {
                    crate::common::ActantError::NotFound(format!(
                        "workflow {} not found after submit",
                        workflow_id.as_str()
                    ))
                })?;
                slot.execution.set_deadline_ms(timeout_ms);
            }
            // deadline 晚于 submit 的同步落盘：除标脏让后台 flush 写快照外，
            // 还要写入 journal，否则 flush 前崩溃会让恢复后的工作流失去超时保护。
            self.log_event(JournalEvent::DeadlineSet {
                workflow_id: workflow_id.clone(),
                timeout_ms,
            });
            self.state.mark_dirty(&workflow_id);
        }
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
        Self::validate_workflow_id(workflow_id)?;
        let task_id = node.task_id.clone();

        // 阶段 1：重放检测 + 结构登记 + 事件载荷构造（同一次 slot 写锁内完成）。
        // 结构与 journal 事件必须来自同一视图：拆成「先登记、再重取表写事件」
        // 会在两次取表之间留下并发淘汰窗口，报错时阶段 1 的状态变更无法回滚。
        let (auto_started, pending_count, node_event, started_at_ms) = {
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
            let started_at_ms = if auto_started {
                slot.execution.mark_running();
                slot.execution.started_at_ms()
            } else {
                None
            };
            // 节点定义与实际前驱边取自刚登记完的同一个 slot；快照写入前
            // 崩溃时，重放可据此独立重建节点、边与 execution 任务。
            let event_node = slot.dag.get_node(&task_id).cloned().ok_or_else(|| {
                crate::common::ActantError::Internal(format!(
                    "node {} missing immediately after registration",
                    task_id.as_str()
                ))
            })?;
            let deps: Vec<TaskId> = slot
                .dag
                .predecessors_of(&task_id)
                .into_iter()
                .map(|pred| pred.task_id.clone())
                .collect();
            (
                auto_started,
                pending_count,
                JournalEvent::NodeAdded {
                    workflow_id: workflow_id.clone(),
                    node: event_node,
                    deps,
                },
                started_at_ms,
            )
        };

        // 阶段 2：历史事件（journal-first），在写锁释放后追加。
        self.log_event(node_event);
        if auto_started {
            self.log_event(JournalEvent::Started {
                workflow_id: workflow_id.clone(),
            });
            if let Some(started_at_ms) = started_at_ms {
                self.log_event(JournalEvent::StartedAt {
                    workflow_id: workflow_id.clone(),
                    started_at_ms,
                });
            }
        }

        // 阶段 3：同步落盘（journal 之后）。节点定义在派发前可恢复；
        // 事件已先写，崩溃后的重放能补齐快照尚未包含的节点。
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
        let (terminal_snapshot, seal_event_needed) = {
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
            let was_sealed = slot.execution.nodes_sealed();
            let was_terminal = slot.execution.is_terminal();
            slot.execution.seal_nodes();
            let snapshot =
                (!was_terminal && slot.execution.is_terminal()).then(|| slot.execution.clone());
            (snapshot, !was_sealed)
        };

        // 封口是终态判定的前置条件，必须进入历史：journal-only 恢复时靠它
        // 重建封口状态，否则工作流永远收敛不到终态。
        if seal_event_needed {
            self.log_event(JournalEvent::NodesSealed {
                workflow_id: workflow_id.clone(),
            });
        }

        match terminal_snapshot {
            Some(exec_snapshot) => {
                // 封口触发终态 ⇒ 全部任务已终态，无在途对象需要取消传播。
                self.complete_terminal(workflow_id, None, &exec_snapshot, Vec::new())
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
    /// Pending（attempt 同步递增，fencing 前提），返回 [`FailureOutcome::Retry`]
    /// 与重试间隔，调用方延迟入队调度器；余量耗尽或不可重试 → 回退到既有
    /// `WorkflowLevel` 失败语义并返回 [`FailureOutcome::Final`]。派发侧
    /// payload 头部 retries 为 0（Python flow 提交路径置 0），worker 层不重试，
    /// 两层不会叠加。
    ///
    /// ## 迟到失败守卫
    ///
    /// 重试中的任务处于 `Pending`（`retry_count > 0` 可区分"从未运行的
    /// Pending"）：此时到达的重复失败（gossip / 直连重投）按过期结果返回
    /// [`FailureOutcome::Ignored`]，不得把已排定重试的任务改写为 Failed。
    /// wire 协议尚未携带派发代数，attempt fencing 放行，此守卫是引入重试后
    /// 的必要补偿。
    ///
    /// 结果回灌整体是 at-least-once（重试队列无限重投 + 直连/gossip 三路），
    /// `Ignored` 与 `Final` 必须可区分：把迟到重复失败当终局发布会让提交方
    /// 句柄在重试在途时被错误置为失败（甚至已 Completed 之后被翻转）。
    pub async fn handle_task_failure(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        error: String,
        result_attempt: Option<u32>,
    ) -> Result<FailureOutcome> {
        let (should_retry, delay_ms) = {
            let slot = self.state.slots.get(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            if !slot.execution.can_transition_task(task_id) {
                return Ok(FailureOutcome::Ignored);
            }
            match slot
                .execution
                .tasks
                .get(task_id)
                .map(|t| (t.state, t.retry_count()))
            {
                // 重试已在途：迟到的重复失败（gossip / 直连重投）按过期结果忽略，
                // 不得把已排定重试的任务改写为 Failed。
                Some((Phase::Pending, count)) if count > 0 => {
                    return Ok(FailureOutcome::Ignored);
                }
                // 从未派发的本地 Pending（retry_count == 0）且工作流尚未封口：
                // 只可能来自编排内部的合成失败，忽略。封口后的 Pending 失败
                // 报告按真实失败处理——远端执行中任务的真实失败可与本地状态
                // 推进乱序到达（gossip / 直连），交由 can_transition_task 与
                // fencing 决定取舍。
                Some((Phase::Pending, 0)) if !slot.execution.nodes_sealed() => {
                    return Ok(FailureOutcome::Ignored);
                }
                // Cancelled 分支不可达：can_transition_task 已在入口拒绝一切
                // 终态任务，无需单独的取消语义分支。
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
                    // 任务能进入 Running 说明其前驱已全部完成；重试立即可派发。
                    // 若沿用 predecessor_count，故障转移后 pending != 0，重试任务
                    // 永远不会被 recover_ready_tasks / reschedule_incomplete_tasks 选中。
                    slot.pending.insert(task_id.clone(), 0);
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
                    return Ok(FailureOutcome::Retry(Box::new(task), delay_ms));
                }
                // reset 失败 = 任务已被并发推进（终态/工作流终态）：按
                // can_transition_task 复查——不可迁移返回 Ignored（迟到重复
                // 失败，调用方不得发布终态），可迁移才按终局失败处理。
                // 直接持有 Orchestrator 句柄的调用方（绕过 actor 串行化）可
                // 触发该窗口。
                let still_migratable = {
                    match self.state.slots.get(workflow_id) {
                        Some(slot) => slot.execution.can_transition_task(task_id),
                        None => false,
                    }
                };
                if !still_migratable {
                    return Ok(FailureOutcome::Ignored);
                }
            }
        }
        self.fail_task(
            workflow_id,
            task_id,
            error,
            FailureScope::WorkflowLevel,
            result_attempt,
        )
        .await?;
        Ok(FailureOutcome::Final)
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
        let (roots, started_at_ms) = {
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
            let started_at_ms = slot.execution.started_at_ms();

            // 根任务与后继任务走同一构造入口：payload 必须在派发前签名，
            // 否则 `require_payload_signing` 节点会以“unsigned payload”拒绝
            // 整个工作流的首批任务。
            let root_ids: Vec<TaskId> = slot
                .dag
                .roots()
                .into_iter()
                .map(|node| node.task_id.clone())
                .collect();
            (
                self.build_ready_tasks_from_slot(&slot, workflow_id, &root_ids)?,
                started_at_ms,
            )
        };

        // Non-terminal: defer persistence to background flush
        self.state.mark_dirty(workflow_id);

        self.log_event(JournalEvent::Started {
            workflow_id: workflow_id.clone(),
        });
        if let Some(started_at_ms) = started_at_ms {
            self.log_event(JournalEvent::StartedAt {
                workflow_id: workflow_id.clone(),
                started_at_ms,
            });
        }

        Ok(roots)
    }

    /// 结果接受决策（attempt fencing）的唯一查询入口，供
    /// `WorkflowActor::on_task_result` 在推进状态前判定结果所属派发代数是否
    /// 仍可被接受。workflow 不存在时放行（NotFound 由后续状态推进路径返回）。
    pub fn result_attempt_accepted(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
        result_attempt: u32,
    ) -> bool {
        match self.state.slots.get(workflow_id) {
            Some(slot) => slot
                .execution
                .attempt_fencing_passes(task_id, Some(result_attempt)),
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
        result_attempt: Option<u32>,
    ) -> Result<(Vec<TaskDefinition>, bool)> {
        let info = self
            .complete_task(workflow_id, task_id, result, result_attempt)
            .await?;

        // 出现终态时后继清单必然为空；且 `completed_retention_count == 0`
        // 的收尾会把工作流直接淘汰，此时查 slot 只会得到 NotFound。
        let ready = if info.workflow_terminal {
            Vec::new()
        } else {
            self.build_ready_tasks_for(workflow_id, &info.ready_successors)?
        };

        Ok((ready, info.workflow_terminal))
    }

    /// 取消整个工作流并置为 `Cancelled` 终态，并**向全部非终态任务传播取消**。
    ///
    /// 传播是必须的：只翻转状态不广播时，远端在途任务要占满自身超时才释放，
    /// 排队任务会继续被派发执行、结果再被终态守卫丢弃（纯浪费的副作用执行）。
    /// 传播复用 deadline 路径的双腿广播（[`Self::broadcast_task_cancel`]）：
    /// gossip 腿管远端在途任务，本地自投递腿经 `cancelled_tasks` 注册表让
    /// 本节点在途/排队任务被取消标志或派发前短路命中。
    ///
    /// 收尾统一走 [`Self::complete_terminal`]：持久化终态快照、追加
    /// `Cancelled` 终态事件、更新指标并唤醒等待者。已终态（Failed /
    /// Completed / Cancelled）的工作流为幂等 no-op——`mark_cancelled` 自带终态
    /// 守卫，兜底取消不得改写更准确的事实源（如 fail-fast 的 Failed）。
    ///
    /// Loading 占位符守卫与 `complete_task` 同口径：占位符尚未加载真实 DAG，
    /// 取消落空后 gossip 快照到达会全量覆盖占位符、把已取消的工作流复活为
    /// Running——显式拒绝，调用方待数据就绪后重试。
    pub async fn cancel(&self, workflow_id: &WorkflowId) -> Result<()> {
        let (cancels_to_broadcast, terminal_snapshot) = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            if slot.state == SlotState::Loading {
                return Err(crate::common::ActantError::InvalidState(format!(
                    "workflow {} is still loading (placeholder), cannot cancel yet",
                    workflow_id.as_str()
                )));
            }
            let was_terminal = slot.execution.is_terminal();
            // 状态翻转前收集非终态任务：mark_cancelled 会把它们置 Cancelled，
            // 之后读不到。Running + Pending 都要传播——Pending 可能已在
            // 调度队列中（排队任务经注册表短路）或已转发远端。
            let cancels: Vec<TaskId> = slot
                .execution
                .tasks
                .iter()
                .filter(|(_, ts)| !ts.state.is_terminal())
                .map(|(id, _)| id.clone())
                .collect();
            slot.execution.mark_cancelled();
            let snapshot =
                (!was_terminal && slot.execution.is_terminal()).then(|| slot.execution.clone());
            (cancels, snapshot)
        };

        if let Some(exec_snapshot) = terminal_snapshot {
            // cancels_to_broadcast 即翻转临界区内采集的全部非终态任务
            // （mark_cancelled 会擦除它们的状态）。即使持久化失败也要传播取消
            // ——complete_terminal 内部对落盘失败只记日志不返回 Err，广播不跳过。
            self.complete_terminal(workflow_id, None, &exec_snapshot, cancels_to_broadcast)
                .await?;
        }
        Ok(())
    }

    /// 把单个任务的取消指令经双腿发出：gossip 广播 + 本地自投递。
    ///
    /// 两条腿缺一不可：gossip 只到邻居（管远端在途任务），且**不会**把消息
    /// 发回发送者；本地自投递复用 `NetworkEventRouter` 的 `TopicRoute::Cancel`
    /// 分支，与远端收到广播后的处理完全同路（置位在途任务的 cancel 标志 +
    /// 登记 `cancelled_tasks` 注册表让排队任务被派发前短路）。deadline 路径
    /// （timeout watcher）与用户级 `cancel` 共用本方法，避免两类入口的取消
    /// 语义漂移。未注入 `network` 时（单元测试）为 no-op。
    pub(crate) async fn broadcast_task_cancel(&self, workflow_id: &WorkflowId, task_id: &TaskId) {
        let Some(ref network) = self.network else {
            return;
        };
        // 载荷经 `WireEnvelope` 封装（签名集群下随 MAC 验证），与
        // `Runtime::broadcast_cancel` 同管线——两条取消路径不得一条加密一条裸奔。
        let bytes =
            crate::common::WireEnvelope::wrap(crate::common::wire::WireMessage::CancelBroadcast(
                crate::common::wire::CancelBroadcast {
                    task_id: task_id.clone(),
                    workflow_id: workflow_id.clone(),
                },
            ))
            .and_then(|envelope| {
                postcard::to_allocvec(&envelope)
                    .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))
            });
        let bytes = match bytes {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(
                    workflow_id = %workflow_id.as_str(),
                    task_id = %task_id.as_str(),
                    error = %e,
                    "failed to encode CancelBroadcast"
                );
                return;
            }
        };
        if let Err(e) = network
            .broadcast(crate::common::wire::TOPIC_CANCEL, bytes.clone())
            .await
        {
            tracing::warn!(
                workflow_id = %workflow_id.as_str(),
                task_id = %task_id.as_str(),
                error = %e,
                "failed to broadcast cancel"
            );
        }
        if !network.inject_local_event(crate::runtime::network::NetworkEvent::Message(
            crate::runtime::network::NetworkMessage {
                topic: crate::common::wire::TOPIC_CANCEL.to_string(),
                data: bytes,
            },
        )) {
            tracing::warn!(
                workflow_id = %workflow_id.as_str(),
                task_id = %task_id.as_str(),
                "failed to self-deliver cancel; a locally running task may not be released"
            );
        }
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
    ///
    /// `pub(crate)` 限定：本方法**不广播取消**，只做状态入账。唯一调用方
    /// `WorkflowActor`（worker 上报 `Cancelled` 结果）到达时执行体已经停止，
    /// 无需传播；用户级单任务取消的 P2P 双腿广播发生在更上层
    /// （Python `_request_cancel` / `Worker::cancel_task`），广播完成后才
    /// 落到本入账路径。若放开为 pub，Rust 嵌入方直调会得到「状态已取消、
    /// 执行体仍在跑」的错位——这正是工作流级 `cancel()` 明令避免的行为。
    pub(crate) async fn cancel_task(
        &self,
        workflow_id: &WorkflowId,
        task_id: &TaskId,
    ) -> Result<bool> {
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
                // 本次取消导致终态 ⇒ 其余任务已全部终态，无在途对象需传播。
                self.complete_terminal(workflow_id, Some(task_id), &exec_snapshot, Vec::new())
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

            let payload = build_task_payload(&node.payload, &self.config.payload_signing_key)?;

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

        let payload = build_task_payload(&node.payload, &self.config.payload_signing_key)?;
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
        let transitioned = {
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
            true
        };
        // 事件追加在写锁释放后执行：journal 同步写不入 DashMap 分片守卫，
        // 避免慢盘期间同分片的其它工作流无法推进（与 `add_node` 同口径）。
        if transitioned {
            self.log_event(JournalEvent::TaskRunning {
                workflow_id: workflow_id.clone(),
                task_id: task_id.clone(),
            });
        }
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
        result_attempt: Option<u32>,
    ) -> Result<()> {
        let error_for_event = error.clone();
        // 终态守卫拒绝（workflow/task 已终态）时状态未变：不追加 TaskFailed
        // 事件、不重复终态收尾——同一失败的重复/迟到投递（本地通道 / 远端
        // 直连 / gossip 三路回灌）必须产生一致的状态与历史。attempt fencing
        // 由 `WorkflowActor::on_task_result` 在进入本方法前统一裁决；
        // `result_attempt` 在此再校验一次，兜住不经入口的直调（recover 重放
        // 传 `None`，重放不参与 fencing）。
        let (accepted, is_terminal) = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            // 字段级解构拆分借用：fail_task 同时需要 execution（可变）与
            // dag 的邻接表（Continue 级联取消），整体借用无法通过。
            let crate::runtime::workflow::orchestrator::types::WorkflowSlot {
                dag, execution, ..
            } = &mut *slot;
            let accepted = execution.can_transition_task(task_id);
            execution.fail_task(dag, task_id, error, mode, result_attempt);
            (accepted, accepted && execution.is_terminal())
        };

        if accepted {
            self.log_event(JournalEvent::TaskFailed {
                workflow_id: workflow_id.clone(),
                task_id: task_id.clone(),
                error: error_for_event,
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
            // 与其它终态入口共用收尾：终态事件、指标、等待者释放、淘汰和
            // 落盘失败容错只有一处实现，落盘失败不得跳过内存侧结算。
            // FailFast 兄弟任务未被翻转（set_failed 只置工作流相位），仍是
            // 收尾点快照扫描可见的非终态对象，无需翻转前采集。
            return self
                .complete_terminal(workflow_id, Some(task_id), &exec_snapshot, Vec::new())
                .await
                .map(|_| ());
        }
        // Non-terminal: defer to background flush
        self.state.mark_dirty(workflow_id);
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
            } else if !slot
                .execution
                .mark_task_completed(task_id, result.clone(), result_attempt)
            {
                // 入口预检与写入侧守卫同源，此分支当前不可达；一旦守卫口径
                // 分叉（预检放行、写入拒绝），状态未变就不写事件——事件与
                // 状态必须同进退，否则出现「事件写了、状态没变」的历史。
                tracing::error!(
                    workflow = %workflow_id.as_str(),
                    task = %task_id.as_str(),
                    "completion precheck passed but write-side guard rejected; no event written"
                );
                (true, false)
            } else {
                (false, slot.execution.is_terminal())
            }
        };

        if skipped {
            return Ok(CompletionInfo {
                workflow_terminal: false,
                ready_successors: vec![],
            });
        }

        // 任务完成事件在终态判定之前落历史：末任务完成使工作流进入终态时，
        // complete_terminal 的早退路径不得吞掉 per-task 事件，否则历史缺失
        // TaskCompleted（重放/审计无法重建任务终态）。追加在写锁释放后执行：
        // journal 同步写不入 DashMap 分片守卫（与 `add_node` 同口径）。
        self.log_event(JournalEvent::TaskCompleted {
            workflow_id: workflow_id.clone(),
            task_id: task_id.clone(),
            result: result.clone(),
        });

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
            // 完成驱动的终态 ⇒ 全部任务已终态，无在途对象需取消传播。
            return self
                .complete_terminal(workflow_id, Some(task_id), &exec_snapshot, Vec::new())
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
    /// 追加终态事件（Completed / Cancelled / Failed 三选一）、更新指标、
    /// 传播取消并唤醒等待者。全部终态入口共用。
    ///
    /// 取消传播的两个来源在收尾点汇合：
    /// - `running_cancels`：**必填**。翻转函数会擦除 Running 状态的入口
    ///   （`mark_workflow_failed` / `mark_cancelled`），必须在翻转临界区内
    ///   采集后传入；终态系「全部任务已终态」的入口（完成驱动、封口、
    ///   单任务取消入账）传空。漏传无法编译通过，消除「新增入口忘记补采集」
    ///   的契约漂移。
    /// - 快照中仍可见的非终态任务：FailFast（`set_failed` 只置工作流相位、
    ///   不动兄弟任务）与 Pending 兄弟（`mark_workflow_failed` 只翻 Running）。
    ///   两集合互斥，快照扫描是其唯一防线，不可删除。
    async fn complete_terminal(
        &self,
        workflow_id: &WorkflowId,
        completed_task_id: Option<&TaskId>,
        exec_snapshot: &crate::runtime::workflow::dag::WorkflowExecution,
        running_cancels: Vec<TaskId>,
    ) -> Result<CompletionInfo> {
        if !exec_snapshot.state.is_terminal() {
            // 所有调用方都应在翻转到终态后再进入本方法。若这里收到非终态
            // 快照，下面的 `_` 分支会把它记成 Failed 并污染指标，故显式
            // 暴露而非静默吞掉；仍继续结算以保持内存侧一致性。
            tracing::error!(
                workflow = %workflow_id.as_str(),
                state = %exec_snapshot.state.as_str(),
                "complete_terminal called with a non-terminal execution snapshot"
            );
        }
        if let Some(started_at_ms) = exec_snapshot.started_at_ms() {
            let now_ms = crate::common::epoch_millis();
            crate::metrics::observe_workflow_duration_ms(now_ms.saturating_sub(started_at_ms));
        }

        // 终态事件与指标先结算：事件日志是恢复的事实源，且终态事件必须在
        // 淘汰（`completed_retention_count == 0`）**之前**写入——淘汰会清理
        // 事件水位，之后再写会让 `record_event_seq` 把水位重新插回并永久残留。
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

        let retention = self.config.workflow.completed_retention_count;
        if retention == 0 {
            // 0 = 终态后立即淘汰。
            self.evict_workflow(workflow_id).await;
        } else {
            if let Some(ref store) = self.store {
                if let Err(err) = Self::persist_terminal(store, workflow_id, exec_snapshot).await {
                    // 终态在内存中已是事实，journal（同为恢复事实源）也已记录终态
                    // 事件；快照落盘只是恢复加速。此处上抛会跳过下面的指标扣减与
                    // 等待者释放——等待者 park 到超时、`active_workflows` 永久泄漏。
                    // 因此只记录并重标脏，交给后台 flush 在容量恢复后重试。
                    tracing::error!(
                        workflow = %workflow_id.as_str(),
                        error = %err,
                        "failed to persist terminal workflow snapshot; settling in memory"
                    );
                    self.state.mark_dirty(workflow_id);
                }
            }
            // 非 0 时按「最多保留 N 个终态工作流」淘汰最旧的那些，
            // 避免内存与存储随已完成工作流无限增长。
            for old_id in self.state.record_completed(workflow_id.clone(), retention) {
                self.evict_workflow(&old_id).await;
            }
        }

        crate::metrics::dec_active_workflows();

        // 终态收尾统一广播取消（双腿：gossip + 本地自投递）。两个来源见
        // 本方法文档：翻转擦除态由调用方经 `running_cancels` 传入，快照可见
        // 的非终态兄弟在此扫描补齐——远端在途任务占满自身超时、排队任务
        // 继续派发而结果再被终态守卫丢弃，都是纯浪费的副作用执行。
        for task_id in &running_cancels {
            self.broadcast_task_cancel(workflow_id, task_id).await;
        }
        for (task_id, ts) in &exec_snapshot.tasks {
            if !ts.state.is_terminal() {
                self.broadcast_task_cancel(workflow_id, task_id).await;
            }
        }

        self.notify_terminal(workflow_id);
        Ok(CompletionInfo {
            workflow_terminal: true,
            ready_successors: vec![],
        })
    }

    /// 持久化终态快照：执行快照必写，`Completed` 时附带聚合结果。
    ///
    /// 与收尾的其余步骤（终态事件、指标、等待者释放）分开，使调用方能在
    /// 落盘失败时仍完成内存侧结算。
    async fn persist_terminal(
        store: &crate::runtime::state::Store,
        workflow_id: &WorkflowId,
        exec_snapshot: &crate::runtime::workflow::dag::WorkflowExecution,
    ) -> Result<()> {
        let exec_bytes = serialize_rkyv(exec_snapshot)?;
        let mut batch = vec![(exec_key(workflow_id), exec_bytes)];

        if matches!(exec_snapshot.state, Phase::Completed) {
            let results: Vec<Vec<u8>> = exec_snapshot.collected_results();
            if !results.is_empty() {
                let result_bytes = crate::common::pack_group(&results)?;
                batch.push((result_key(workflow_id), result_bytes));
            }
        }
        store.put_batch(&batch).await
    }

    /// 释放工作流进入终态后仍 park 在等待点上的等待者。
    ///
    /// `AsyncResult` 的等待由 correctness 通道的 `TaskCancelled` 事件结算，而等待点 park
    /// 是另一条阻塞原语，仅靠运行时关停的 `release_all_wait_point_waiters`
    /// 释放不够：缺了这一步，cancel / fail / deadline 都到不了 park 中的
    /// flow 体——工作流已终态而函数体永久挂起。
    /// 丢弃 sender 使 receiver 立即收到 `Err`，park 方据此查工作流终态判定
    /// 真实原因；此处不投递 oneshot payload。
    /// 所有终态入口共用 `complete_terminal`，故在此单点释放即可。
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

    /// 将工作流标记为 Failed 并保留真实错误（flow 函数体异常收尾）。
    ///
    /// 与 [`Self::cancel`] 的区别：取消表示外部主动终止（Cancelled），
    /// 本方法表示执行失败（Failed），错误串进入终态快照与 `Failed` 事件，
    /// 使 `FlowHandle.result()` 能区分失败与取消。
    pub async fn fail_workflow(&self, workflow_id: &WorkflowId, error: String) -> Result<()> {
        // 采集与翻转在同一临界区：mark_workflow_failed 会把 Running 翻成
        // Failed，complete_terminal 的快照收口看不到它们（Pending 兄弟不被
        // 翻转、仍可见，由收尾点的快照扫描覆盖）——Running 集合必须由本入口
        // 经 `running_cancels` 参数显式传入，漏传编译不过。
        let (running_cancels, terminal_snapshot) = {
            let mut slot = self.state.slots.get_mut(workflow_id).ok_or_else(|| {
                crate::common::ActantError::NotFound(format!(
                    "workflow {} not found",
                    workflow_id.as_str()
                ))
            })?;
            // Loading 占位符守卫与 `cancel` 同口径：占位符上置 Failed 会被
            // gossip 快照的全量覆盖复活。
            if slot.state == SlotState::Loading {
                return Err(crate::common::ActantError::InvalidState(format!(
                    "workflow {} is still loading (placeholder), cannot fail yet",
                    workflow_id.as_str()
                )));
            }
            let running_cancels: Vec<TaskId> = slot
                .execution
                .tasks
                .iter()
                .filter(|(_, ts)| ts.state == Phase::Running)
                .map(|(id, _)| id.clone())
                .collect();
            let was_terminal = slot.execution.is_terminal();
            slot.execution.mark_workflow_failed(error);
            let snapshot =
                (!was_terminal && slot.execution.is_terminal()).then(|| slot.execution.clone());
            (running_cancels, snapshot)
        };

        if let Some(exec_snapshot) = terminal_snapshot {
            // 双腿广播（gossip + 本地自投递）在收尾点统一执行，与 `cancel`
            // 同管线；此处不再自行补广播。
            self.complete_terminal(workflow_id, None, &exec_snapshot, running_cancels)
                .await?;
        }
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

    /// 接管后重新派发单个工作流中「未终结且已就绪」的任务。
    ///
    /// 判定与 [`Self::recover_ready_tasks`] 同构（`pending` 计数为 0 且状态
    /// 为 [`Phase::Pending`]），因为接管方要做的正是"从别人留下的快照继续跑"：
    /// `restore_workflow_from_bytes` 已把非终态任务重置为 Pending 并推进
    /// attempt，若只认 Running，接管方就一个可派发任务都没有——工作流停在
    /// Running 却永不推进（表现为任务永久 Pending）。
    ///
    /// 仍为 Running 的任务先重置为 Pending（attempt 递增，用于按代数栅栏掉
    /// 旧代在途结果）：这类任务来自本地已有的副本而非远端快照。
    pub fn reschedule_incomplete_tasks(
        &self,
        workflow_id: &WorkflowId,
    ) -> Result<Vec<TaskDefinition>> {
        // 终态守卫前置：终态工作流（如 fail-fast 已 Failed）上的 Running 任务
        // 不得被重置回 Pending——状态翻转先于过滤会让快照失真（任务状态与
        // 工作流终态矛盾）。守卫用独立作用域：guard 不得横跨 mark_task_pending
        // （内部对同一 key 取写守卫，DashMap 同分片重入死锁）。
        {
            let slot = match self.state.slots.get(workflow_id) {
                Some(slot) => slot,
                None => return Ok(Vec::new()),
            };
            if slot.execution.is_terminal() {
                return Ok(Vec::new());
            }
        }
        let running_task_ids = self.get_running_task_ids(workflow_id);
        for task_id in &running_task_ids {
            self.mark_task_pending(workflow_id, task_id)?;
        }

        let slot = match self.state.slots.get(workflow_id) {
            Some(slot) => slot,
            None => return Ok(Vec::new()),
        };
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
            return Ok(Vec::new());
        }

        let tasks = self.build_ready_tasks_from_slot(&slot, workflow_id, &ready_ids)?;
        for _ in &tasks {
            crate::metrics::inc_retry_scheduled();
        }
        Ok(tasks)
    }
}
