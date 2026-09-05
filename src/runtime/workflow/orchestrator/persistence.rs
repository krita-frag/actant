//! Orchestrator 的 `persistence` 职责子模块。
//!
//! 负责工作流状态恢复、事件日志记录、后台落盘、工作流迁移与淘汰。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::common::{
    serialization::serialize_rkyv, ActantConfig, Result, TaskId, WorkflowId, STORE_KEY_DAG,
    STORE_KEY_EVENT_SEQ, STORE_KEY_EXEC, STORE_KEY_PENDING, STORE_KEY_WAIT,
};
use crate::runtime::state::event_log::{EventId, EventLog};
use crate::runtime::state::{HybridLogicalClock, Store};
use crate::runtime::workflow::{
    Dag, FailureScope, Phase, Terminal, WaitPoint, WaitPointState, WorkflowExecution,
};

use super::{keys::*, types::*, Orchestrator};

/// 将工作流事件写入 event_log（若存在），返回事件 ID 供调用方记录事件水位。
///
/// 序列化或追加失败仅记录 warn 日志——事件日志供外部观测，写入失败不阻断
/// 核心状态推进。独立于 `Orchestrator` 实例，供超时 watcher 等仅持有
/// `event_log` 克隆的后台任务复用。返回 `None` 表示事件未成功写入
/// （无 event_log 或追加失败），调用方不得推进水位。
pub(crate) fn append_event(
    event_log: Option<&Arc<dyn EventLog>>,
    payload: WorkflowEventPayload,
) -> Option<EventId> {
    let log = event_log?;
    let topic = payload.topic();
    match postcard::to_allocvec(&payload) {
        Ok(bytes) => match log.append(&topic, &bytes) {
            Ok(id) => Some(id),
            Err(e) => {
                tracing::warn!(error = %e, topic = %topic, "failed to append workflow event");
                None
            }
        },
        Err(e) => {
            tracing::warn!(error = %e, topic = %topic, "failed to serialize workflow event");
            None
        }
    }
    // 规则评估在 Python 编排循环中实现：远端订阅者通过
    // WireEnvelope::TaskDispatch / TaskResult 接收事件，业务规则在
    // Python 侧订阅 EventBus 自行处理。
}

impl Orchestrator {
    pub(crate) fn log_event(&self, payload: WorkflowEventPayload) {
        let workflow_id = payload.workflow_id().clone();
        if let Some(id) = append_event(self.event_log.as_ref(), payload) {
            self.state.record_event_seq(&workflow_id, id);
        }
    }

    /// 记录任务派发事件（S0）。由 Worker 在任务被本地接受执行时经
    /// WorkflowActor 调用，进入工作流统一历史。
    pub(crate) fn log_task_dispatched(&self, workflow_id: &WorkflowId, task_id: &TaskId) {
        self.log_event(WorkflowEventPayload::TaskDispatched {
            workflow_id: workflow_id.clone(),
            task_id: task_id.clone(),
        });
    }

    /// 从持久化 [Store] 恢复 orchestrator 状态。
    ///
    /// **恢复语义唯一化（S0）：recover = 快照 + 其后事件重放，别无第三路。**
    ///
    /// 1. 快照加载：扫描 dag / exec / pending 前缀重建内存状态，Running 任务
    ///    重置为 Pending；同时加载等待点快照（`orch:wait:`）与事件水位
    ///    （`orch:eventseq:`）。快照是 dirty-flush 写入的**重放加速缓存**，
    ///    事实源是 event_log。
    /// 2. 事件重放：对每个带水位的工作流，从 `workflow:{id}` topic 读取水位
    ///    之后的事件，按事件语义幂等推进（复用 `mark_task_completed` /
    ///    `fail_task` 既有守卫：已终态的工作流/任务拒绝改写）。快照与事件由
    ///    同一 `EventId` 水位对齐——水位之后的事件是崩溃前尚未落盘的增量。
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

        // 等待点快照与事件水位（S0/S1）：与 exec/pending 同批落盘的加速缓存。
        let wait_entries = store.scan_prefix(STORE_KEY_WAIT).await?;
        for (key, data) in wait_entries {
            let wf_id_str = key.strip_prefix(STORE_KEY_WAIT).unwrap_or(&key);
            let wf_id = WorkflowId::from(wf_id_str.to_string());
            match postcard::from_bytes::<HashMap<String, WaitPoint>>(&data) {
                Ok(table) => {
                    state.waitpoints.insert(wf_id, table.into_iter().collect());
                }
                Err(e) => {
                    // 等待点缓存损坏不影响任务状态恢复：等待点可由事件重放
                    // 补齐（历史是事实源），此处仅记录告警。
                    tracing::warn!(
                        workflow = %wf_id_str,
                        error = ?e,
                        "recover: corrupt waitpoint snapshot, waitpoints will be rebuilt from events"
                    );
                }
            }
        }

        let mut watermarks: HashMap<WorkflowId, EventId> = HashMap::new();
        let seq_entries = store.scan_prefix(STORE_KEY_EVENT_SEQ).await?;
        for (key, data) in seq_entries {
            let wf_id_str = key.strip_prefix(STORE_KEY_EVENT_SEQ).unwrap_or(&key);
            let wf_id = WorkflowId::from(wf_id_str.to_string());
            match postcard::from_bytes::<EventId>(&data) {
                Ok(id) => {
                    watermarks.insert(wf_id, id);
                }
                Err(e) => {
                    tracing::warn!(
                        workflow = %wf_id_str,
                        error = ?e,
                        "recover: corrupt event watermark, events after last flush will not be replayed"
                    );
                }
            }
        }

        // 任何条目损坏的 workflow：移除内存 slot，并删除 store 中的所有三类条目，
        // 避免"exec 数据悬空而 dag 缺失"等不一致状态在后续操作中触发 workflow not found。
        for wf_id in &corrupt {
            state.remove_workflow(wf_id);
            for key in [
                dag_key(wf_id),
                exec_key(wf_id),
                pending_key(wf_id),
                wait_key(wf_id),
                event_seq_key(wf_id),
            ] {
                if let Err(e) = store.delete(&key).await {
                    tracing::warn!(workflow = %wf_id.as_str(), key = %key, error = %e, "recover: failed to delete corrupt entry");
                }
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
            event_log: event_log.clone(),
            condition_evaluator: None,
            node_id: None,
            hlc: Arc::new(HybridLogicalClock::with_max_drift_ms(
                config.network.hlc_max_drift_ms,
            )),
            network: None,
        };

        // S0：快照加载完成后重放其后的事件，恢复语义唯一化（快照 + 事件重放）。
        orchestrator
            .replay_events_after_watermarks(event_log.as_deref(), &watermarks)
            .await;

        Ok(orchestrator)
    }

    /// 重放各工作流水位之后的事件（S0：recover = 快照 + 事件重放）。
    ///
    /// 事件按追加顺序重放，逐条按语义幂等推进：
    /// - 任务事件复用 `WorkflowExecution::mark_task_completed` / `fail_task` /
    ///   `cancel_task` 的既有守卫（终态守卫 + attempt fencing），重复/迟到
    ///   事件被拒绝，不产生二次状态变更；
    /// - 等待点事件幂等：已注册的 wait_key 跳过、已 Signaled 的等待点跳过；
    /// - 节点事件：DAG 中已存在的节点跳过（快照加速），缺失节点从事件重建
    ///   （S7 增量提交的前置能力）。
    ///
    /// 重放不追加新事件、不发网络消息、不触发等待者唤醒（恢复期尚无等待者）。
    /// 重放产生变更的工作流被标记脏，由后台 flush 把水位后的增量并入快照；
    /// 重放中进入终态的工作流立即同步落盘（对齐 `fail_task` 的终态崩溃安全）。
    async fn replay_events_after_watermarks(
        &self,
        event_log: Option<&dyn EventLog>,
        watermarks: &HashMap<WorkflowId, EventId>,
    ) {
        let Some(log) = event_log else {
            return;
        };
        // 先收集再重放：iter() 持有 DashMap 分片读锁，而 apply_replayed_event
        // 会对同一 workflow 执行 get_mut（同分片写锁）——循环体内直接重放是
        // DashMap 重入死锁。
        // 无水位的 workflow 从头重放（read_after(None) = 全量）——幂等守卫
        // 保证与快照内容去重；有水位则只重放其后增量。
        let targets: Vec<(WorkflowId, Option<EventId>)> = self
            .state
            .slots
            .iter()
            .map(|entry| {
                let wf = entry.key().clone();
                let wm = watermarks.get(&wf).copied();
                (wf, wm)
            })
            .collect();
        for (wf_id, watermark) in targets {
            let topic = format!("workflow:{}", wf_id.as_str());
            let entries = match log.read_after(&topic, watermark.as_ref()) {
                Ok(entries) => entries,
                Err(e) => {
                    tracing::warn!(
                        workflow = %wf_id.as_str(),
                        error = %e,
                        "recover: failed to read events after watermark, replay skipped"
                    );
                    continue;
                }
            };
            if entries.is_empty() {
                continue;
            }
            let mut replayed = 0usize;
            for entry in &entries {
                match postcard::from_bytes::<WorkflowEventPayload>(&entry.payload) {
                    Ok(payload) => {
                        self.apply_replayed_event(payload).await;
                        replayed += 1;
                    }
                    Err(e) => {
                        // 解码失败的事件跳过：0.2/0.3.2 历史与新枚举布局不兼容
                        // 时不阻断其余事件重放。
                        tracing::warn!(
                            workflow = %wf_id.as_str(),
                            error = ?e,
                            "recover: failed to decode replayed event, skipping"
                        );
                    }
                }
            }
            tracing::info!(
                workflow = %wf_id.as_str(),
                replayed,
                "replayed workflow events after snapshot watermark"
            );
        }
    }

    /// 按事件语义幂等应用单条重放事件。
    ///
    /// 只做内存状态推进（任务/等待点/节点），不追加事件、不落盘（终态除外，
    /// 见 `replay_events_after_watermarks` 文档）。
    pub(crate) async fn apply_replayed_event(&self, payload: WorkflowEventPayload) {
        match payload {
            WorkflowEventPayload::TaskCompleted {
                workflow_id,
                task_id,
                result,
            } => {
                let mut slot = match self.state.slots.get_mut(&workflow_id) {
                    Some(slot) if slot.state == SlotState::Ready => slot,
                    _ => return,
                };
                // mark_task_completed 内置终态守卫 + attempt fencing：快照已
                // 包含的完成被拒绝，仅水位后的增量真正推进。
                if slot.execution.mark_task_completed(&task_id, result, None) {
                    // 与 compute_ready_successors 一致地递减后继 pending
                    //（跳过条件后继），保证 recover_ready_tasks 的
                    // "pending == 0 且 Pending" 过滤与崩溃前状态一致。
                    let conditional: Vec<TaskId> = slot
                        .dag
                        .conditional_edges_from(&task_id)
                        .into_iter()
                        .map(|(id, _)| id)
                        .collect();
                    for succ in slot.dag.successor_ids(&task_id) {
                        if conditional.iter().any(|id| id == &succ) {
                            continue;
                        }
                        if let Some(count) = slot.pending.get_mut(&succ) {
                            *count = count.saturating_sub(1);
                        }
                    }
                    self.state.mark_dirty(&workflow_id);
                    if slot.execution.is_terminal() {
                        drop(slot);
                        self.persist_terminal_after_replay(&workflow_id).await;
                    }
                }
            }
            WorkflowEventPayload::TaskFailed {
                workflow_id,
                task_id,
                error,
            } => {
                let mut slot = match self.state.slots.get_mut(&workflow_id) {
                    Some(slot) if slot.state == SlotState::Ready => slot,
                    _ => return,
                };
                // 与 on_task_result 单入口一致：失败一律按工作流级语义处理。
                if slot.execution.can_transition_task(&task_id) {
                    slot.execution
                        .fail_task(&task_id, error, FailureScope::WorkflowLevel, None);
                    self.state.mark_dirty(&workflow_id);
                    if slot.execution.is_terminal() {
                        drop(slot);
                        self.persist_terminal_after_replay(&workflow_id).await;
                    }
                }
            }
            WorkflowEventPayload::TaskCancelled {
                workflow_id,
                task_id,
            } => {
                let mut slot = match self.state.slots.get_mut(&workflow_id) {
                    Some(slot) if slot.state == SlotState::Ready => slot,
                    _ => return,
                };
                if slot.execution.cancel_task(&task_id) {
                    self.state.mark_dirty(&workflow_id);
                }
            }
            WorkflowEventPayload::NodeAdded { workflow_id, node } => {
                let mut slot = match self.state.slots.get_mut(&workflow_id) {
                    Some(slot) if slot.state == SlotState::Ready => slot,
                    _ => return,
                };
                if slot.dag.get_node(&node.task_id).is_none() {
                    let task_id = node.task_id.clone();
                    if slot.dag.add_node(node).is_ok() {
                        slot.pending.entry(task_id).or_insert(0);
                        self.state.mark_dirty(&workflow_id);
                    }
                }
            }
            WorkflowEventPayload::Started { workflow_id } => {
                let mut slot = match self.state.slots.get_mut(&workflow_id) {
                    Some(slot) if slot.state == SlotState::Ready => slot,
                    _ => return,
                };
                if !slot.execution.is_terminal() {
                    slot.execution.mark_running();
                    self.state.mark_dirty(&workflow_id);
                }
            }
            WorkflowEventPayload::WaitPointRegistered {
                workflow_id,
                wait_key,
                condition,
            } => {
                // 幂等：同 wait_key 已存在（快照已含）则跳过。
                self.state
                    .waitpoints
                    .entry(workflow_id)
                    .or_default()
                    .entry(wait_key)
                    .or_insert_with(|| WaitPoint {
                        condition,
                        state: WaitPointState::Waiting,
                    });
            }
            WorkflowEventPayload::SignalReceived {
                workflow_id,
                wait_key,
                payload,
            } => {
                if let Some(mut wp) = self
                    .state
                    .waitpoints
                    .entry(workflow_id)
                    .or_default()
                    .get_mut(&wait_key)
                {
                    if wp.state == WaitPointState::Waiting {
                        wp.state = WaitPointState::Signaled { payload };
                    }
                }
            }
            WorkflowEventPayload::TimerFired {
                workflow_id,
                wait_key,
            } => {
                if let Some(mut wp) = self
                    .state
                    .waitpoints
                    .entry(workflow_id)
                    .or_default()
                    .get_mut(&wait_key)
                {
                    if wp.state == WaitPointState::Waiting {
                        wp.state = WaitPointState::Signaled {
                            payload: Vec::new(),
                        };
                    }
                }
            }
            // 派发/运行事件对状态机无增量语义（Running 会在快照加载时被重置
            // 为 Pending 以便重派发）；Submitted/Completed/Failed/Recovered 的
            // 状态已由快照吸收。
            WorkflowEventPayload::TaskDispatched { .. }
            | WorkflowEventPayload::TaskRunning { .. }
            | WorkflowEventPayload::Submitted { .. }
            | WorkflowEventPayload::Completed { .. }
            | WorkflowEventPayload::Failed { .. }
            | WorkflowEventPayload::Recovered { .. } => {}
        }
    }

    /// 重放导致工作流进入终态时立即同步落盘执行状态（含结果聚合），
    /// 对齐 `complete_terminal` 的崩溃安全语义；不追加事件、不更新指标。
    async fn persist_terminal_after_replay(&self, workflow_id: &WorkflowId) {
        let Some(slot) = self.state.slots.get(workflow_id) else {
            return;
        };
        let Some(ref store) = self.store else {
            return;
        };
        let exec_snapshot = slot.execution.clone();
        drop(slot);
        let mut batch = match serialize_rkyv(&exec_snapshot) {
            Ok(bytes) => vec![(exec_key(workflow_id), bytes)],
            Err(e) => {
                tracing::error!(
                    workflow = %workflow_id.as_str(),
                    error = %e,
                    "replay: failed to serialize terminal execution"
                );
                self.state.mark_dirty(workflow_id);
                return;
            }
        };
        if matches!(exec_snapshot.state, Phase::Completed) {
            let results = exec_snapshot.collected_results();
            if !results.is_empty() {
                if let Ok(result_bytes) = crate::common::pack_group(&results) {
                    batch.push((result_key(workflow_id), result_bytes));
                }
            }
        }
        if let Err(e) = store.put_batch(&batch).await {
            tracing::error!(
                workflow = %workflow_id.as_str(),
                error = %e,
                "replay: failed to persist terminal execution"
            );
            self.state.mark_dirty(workflow_id);
        }
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
                            // 事件水位必须先于快照序列化捕获（S0）：保证任何已计入
                            // 水位的状态变更必然已包含在快照中；反向窗口由重放幂等
                            // 守卫兜底。同时收集等待点快照，与 exec/pending 同批落盘。
                            let watermark = state.event_seq(wf_id);
                            let waitpoints = state.clone_waitpoints(wf_id);
                            // 序列化失败不得静默丢弃：记录 error 并重新标记脏，
                            // 让下一轮 flush 重试（drain 已把该 workflow 移出脏集合）。
                            match serialize_rkyv(&slot.execution) {
                                Ok(exec_bytes) => batch.push((exec_key(wf_id), exec_bytes)),
                                Err(e) => {
                                    tracing::error!(
                                        workflow = %wf_id.as_str(),
                                        error = %e,
                                        "persist flush: failed to serialize execution"
                                    );
                                    state.mark_dirty(wf_id);
                                }
                            }
                            match serialize_rkyv(&slot.pending) {
                                Ok(pending_bytes) => {
                                    batch.push((pending_key(wf_id), pending_bytes))
                                }
                                Err(e) => {
                                    tracing::error!(
                                        workflow = %wf_id.as_str(),
                                        error = %e,
                                        "persist flush: failed to serialize pending map"
                                    );
                                    state.mark_dirty(wf_id);
                                }
                            }
                            if let Some(table) = waitpoints {
                                match postcard::to_allocvec(&table) {
                                    Ok(wait_bytes) => batch.push((wait_key(wf_id), wait_bytes)),
                                    Err(e) => {
                                        tracing::error!(
                                            workflow = %wf_id.as_str(),
                                            error = %e,
                                            "persist flush: failed to serialize waitpoints"
                                        );
                                        state.mark_dirty(wf_id);
                                    }
                                }
                            }
                            if let Some(id) = watermark {
                                match postcard::to_allocvec(&id) {
                                    Ok(seq_bytes) => {
                                        batch.push((event_seq_key(wf_id), seq_bytes))
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            workflow = %wf_id.as_str(),
                                            error = %e,
                                            "persist flush: failed to serialize event watermark"
                                        );
                                        state.mark_dirty(wf_id);
                                    }
                                }
                            }
                        }
                        if !batch.is_empty() {
                            if let Err(e) = store.put_batch(&batch).await {
                                // 写入失败时把本轮 drained 的 workflow 重新加入脏集合，
                                // 避免状态丢失直到下一轮 flush 重试。
                                tracing::error!("persist flush failed: {}", e);
                                for wf_id in &dirty_ids {
                                    if state.slots.contains_key(wf_id) {
                                        state.mark_dirty(wf_id);
                                    }
                                }
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
            // 事件水位先于快照序列化捕获（S0），语义同 start_persist_flush。
            let watermark = self.state.event_seq(wf_id);
            let waitpoints = self.state.clone_waitpoints(wf_id);
            // 序列化失败不得静默丢弃：记录 error 并重新标记脏，保持与后台
            // flush 相同的重试语义（drain 已把该 workflow 移出脏集合）。
            match serialize_rkyv(&slot.execution) {
                Ok(exec_bytes) => batch.push((exec_key(wf_id), exec_bytes)),
                Err(e) => {
                    tracing::error!(
                        workflow = %wf_id.as_str(),
                        error = %e,
                        "flush_dirty: failed to serialize execution"
                    );
                    self.state.mark_dirty(wf_id);
                }
            }
            match serialize_rkyv(&slot.pending) {
                Ok(pending_bytes) => batch.push((pending_key(wf_id), pending_bytes)),
                Err(e) => {
                    tracing::error!(
                        workflow = %wf_id.as_str(),
                        error = %e,
                        "flush_dirty: failed to serialize pending map"
                    );
                    self.state.mark_dirty(wf_id);
                }
            }
            if let Some(table) = waitpoints {
                match postcard::to_allocvec(&table) {
                    Ok(wait_bytes) => batch.push((wait_key(wf_id), wait_bytes)),
                    Err(e) => {
                        tracing::error!(
                            workflow = %wf_id.as_str(),
                            error = %e,
                            "flush_dirty: failed to serialize waitpoints"
                        );
                        self.state.mark_dirty(wf_id);
                    }
                }
            }
            if let Some(id) = watermark {
                match postcard::to_allocvec(&id) {
                    Ok(seq_bytes) => batch.push((event_seq_key(wf_id), seq_bytes)),
                    Err(e) => {
                        tracing::error!(
                            workflow = %wf_id.as_str(),
                            error = %e,
                            "flush_dirty: failed to serialize event watermark"
                        );
                        self.state.mark_dirty(wf_id);
                    }
                }
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
                wait_key(old_id),
                event_seq_key(old_id),
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
