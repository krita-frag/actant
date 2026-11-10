//! Orchestrator 的 `persistence` 职责子模块。
//!
//! 负责工作流状态恢复、事件日志记录、后台落盘、工作流迁移与淘汰。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::common::{
    serialization::serialize_rkyv, ActantConfig, Result, TaskId, WorkflowId, STORE_KEY_CONTEXT,
    STORE_KEY_DAG, STORE_KEY_EVENT_SEQ, STORE_KEY_EXEC, STORE_KEY_PENDING, STORE_KEY_SIGNAL_BUF,
    STORE_KEY_WAIT,
};
use crate::runtime::state::event_log::{EventId, EventLog};
use crate::runtime::state::{HybridLogicalClock, Store};
use crate::runtime::workflow::{
    Dag, FailureScope, Phase, Terminal, WaitCondition, WaitPoint, WaitPointState, WorkflowExecution,
};

use super::{keys::*, types::*, Orchestrator};

/// 裁剪已被快照吸收的事件历史（留存策略）。
///
/// 必须在**水位成功落盘之后**调用：此时 `up_to` 之前的事件已进快照，删掉它们
/// 只影响审计，不影响恢复。水位**之后**的事件永不裁剪——重放需要它们。
/// `event_log_max_events_per_workflow == 0` 时不做任何事（保守默认）。
///
/// 两条 flush 路径（`start_persist_flush` / `flush_dirty`）共用本函数，
/// 避免留存语义在两条路径上漂移。
pub(crate) fn trim_absorbed_history(
    event_log: Option<&Arc<dyn EventLog>>,
    config: &ActantConfig,
    workflow_id: &WorkflowId,
    watermark: Option<&EventId>,
) {
    let keep = config.workflow.event_log_max_events_per_workflow;
    if keep == 0 {
        return;
    }
    let (Some(log), Some(up_to)) = (event_log, watermark) else {
        return;
    };
    let topic = format!("workflow:{}", workflow_id.as_str());
    match log.trim_absorbed(&topic, up_to, keep) {
        Ok(0) => {}
        Ok(n) => tracing::debug!(
            workflow = %workflow_id.as_str(),
            trimmed = n,
            keep,
            "trimmed event history absorbed by snapshot watermark"
        ),
        Err(e) => tracing::warn!(
            workflow = %workflow_id.as_str(),
            error = %e,
            "failed to trim event history"
        ),
    }
}

/// 将工作流事件写入 event_log（若存在），返回事件 ID 供调用方记录事件水位。
///
/// 序列化或追加失败仅记录 warn 日志——事件日志供外部观测，写入失败不阻断
/// 核心状态推进。独立于 `Orchestrator` 实例，供超时 watcher 等仅持有
/// `event_log` 克隆的后台任务复用。返回 `None` 表示事件未成功写入
/// （无 event_log 或追加失败），调用方不得推进水位。
pub(crate) fn append_event(
    event_log: Option<&Arc<dyn EventLog>>,
    payload: JournalEvent,
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

/// 序列化单个持久化槽位；失败时记录日志并返回 `None`。
fn encode_slot<E: std::fmt::Display>(
    slot_name: &str,
    workflow_id: &WorkflowId,
    encoded: std::result::Result<Vec<u8>, E>,
) -> Option<Vec<u8>> {
    match encoded {
        Ok(bytes) => Some(bytes),
        Err(e) => {
            tracing::error!(
                workflow = %workflow_id.as_str(),
                slot = slot_name,
                error = %e,
                "persist: failed to serialize workflow slot"
            );
            None
        }
    }
}

/// 单个工作流一轮落盘的全部槽位，以及该轮**捕获时**的事件水位。
struct SlotSnapshot {
    workflow_id: WorkflowId,
    entries: Vec<(String, Vec<u8>)>,
    /// 捕获时的水位。裁剪必须用它（而不是裁剪时刻的实时水位），否则会把
    /// 「捕获之后、落盘之前」新增的事件当成已吸收而删掉。
    watermark: Option<EventId>,
}

/// 单工作流一轮落盘的结果。
enum SlotOutcome {
    /// 全部槽位序列化成功。
    Ready(SlotSnapshot),
    /// 工作流已不在内存（并发淘汰），本无需落盘。
    Gone,
    /// 任一槽位序列化失败——本轮该工作流的槽位一个都不写。
    Failed,
}

/// 收集单个工作流的全部持久化槽位。
///
/// **全成全败**：exec/pending 快照与事件水位若分裂成「一个成功一个失败」，
/// 重启后那段历史已被按水位裁剪，而快照里又没有它，历史不可恢复。因此任一
/// 槽位失败即整体放弃，由调用方重新标脏、下一轮重试。
fn collect_slots(state: &OrchestratorState, workflow_id: &WorkflowId) -> SlotOutcome {
    let (watermark, exec_bytes, pending_bytes) = {
        let Some(slot) = state.slots.get(workflow_id) else {
            return SlotOutcome::Gone;
        };
        // 水位先于快照捕获：保证任何已计入水位的状态变更必然已包含在快照中；
        // 反向窗口（捕获后、序列化前的变更）由重放幂等守卫兜底。
        let watermark = state.event_seq(workflow_id);
        let exec_bytes =
            match encode_slot("execution", workflow_id, serialize_rkyv(&slot.execution)) {
                Some(b) => b,
                None => return SlotOutcome::Failed,
            };
        let pending_bytes = match encode_slot("pending", workflow_id, serialize_rkyv(&slot.pending))
        {
            Some(b) => b,
            None => return SlotOutcome::Failed,
        };
        (watermark, exec_bytes, pending_bytes)
    };

    let mut entries = vec![
        (exec_key(workflow_id), exec_bytes),
        (pending_key(workflow_id), pending_bytes),
    ];
    if let Some(table) = state.clone_waitpoints(workflow_id) {
        match encode_slot("waitpoints", workflow_id, postcard::to_allocvec(&table)) {
            Some(b) => entries.push((wait_key(workflow_id), b)),
            None => return SlotOutcome::Failed,
        }
    }
    if let Some(buf) = state.clone_pending_signals(workflow_id) {
        match encode_slot("signal buffer", workflow_id, postcard::to_allocvec(&buf)) {
            Some(b) => entries.push((signal_buf_key(workflow_id), b)),
            None => return SlotOutcome::Failed,
        }
    }
    if let Some(id) = watermark {
        match encode_slot("event watermark", workflow_id, postcard::to_allocvec(&id)) {
            Some(b) => entries.push((event_seq_key(workflow_id), b)),
            None => return SlotOutcome::Failed,
        }
    }
    if let Some(table) = state.clone_context_values(workflow_id) {
        match encode_slot("context values", workflow_id, postcard::to_allocvec(&table)) {
            Some(b) => entries.push((context_key(workflow_id), b)),
            None => return SlotOutcome::Failed,
        }
    }

    SlotOutcome::Ready(SlotSnapshot {
        workflow_id: workflow_id.clone(),
        entries,
        watermark,
    })
}

/// 一轮 flush 的产物：可提交的槽位、需要重试的、被淘汰的。
struct FlushPlan {
    snapshots: Vec<SlotSnapshot>,
    retry: Vec<WorkflowId>,
}

impl FlushPlan {
    fn collect(state: &OrchestratorState, dirty_ids: &[WorkflowId]) -> Self {
        let mut snapshots = Vec::with_capacity(dirty_ids.len());
        let mut retry = Vec::new();
        for workflow_id in dirty_ids {
            match collect_slots(state, workflow_id) {
                SlotOutcome::Ready(s) => snapshots.push(s),
                SlotOutcome::Gone => {}
                SlotOutcome::Failed => retry.push(workflow_id.clone()),
            }
        }
        Self { snapshots, retry }
    }

    fn entries(&self) -> Vec<(String, Vec<u8>)> {
        self.snapshots
            .iter()
            .flat_map(|s| s.entries.iter().cloned())
            .collect()
    }

    /// 把本轮未写成的工作流重新标脏（序列化失败与写入失败都走这里）。
    fn mark_retry(&self, state: &OrchestratorState, write_failed: bool) {
        for workflow_id in &self.retry {
            state.mark_dirty(workflow_id);
        }
        if write_failed {
            for snapshot in &self.snapshots {
                state.mark_dirty(&snapshot.workflow_id);
            }
        }
    }

    /// 水位已落盘后裁剪各工作流已被快照吸收的历史。
    fn trim_persisted(&self, event_log: Option<&Arc<dyn EventLog>>, config: &ActantConfig) {
        for snapshot in &self.snapshots {
            trim_absorbed_history(
                event_log,
                config,
                &snapshot.workflow_id,
                snapshot.watermark.as_ref(),
            );
        }
    }
}

impl Orchestrator {
    pub(crate) fn log_event(&self, payload: JournalEvent) {
        let workflow_id = payload.workflow_id().clone();
        if let Some(id) = append_event(self.event_log.as_ref(), payload) {
            self.state.record_event_seq(&workflow_id, id);
        }
    }

    /// 记录任务派发事件。由 Worker 在任务被本地接受执行时经
    /// WorkflowActor 调用，进入工作流统一历史。
    pub(crate) fn log_task_dispatched(&self, workflow_id: &WorkflowId, task_id: &TaskId) {
        self.log_event(JournalEvent::TaskDispatched {
            workflow_id: workflow_id.clone(),
            task_id: task_id.clone(),
        });
    }

    /// 从持久化 [Store] 恢复 orchestrator 状态。
    ///
    /// **recover = 快照 + 其后事件重放，别无第三路。** 快照是 dirty-flush 写入的
    /// 重放加速缓存，事实源是 event_log；两者按 `EventId` 水位对齐。
    ///
    /// 完整语义（快照加载、幂等重放、`Recovered` 事件、损坏 workflow 处理）
    /// 见 `docs/architecture/recovery.md`。
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

        // 等待点快照与事件水位：与 exec/pending 同批落盘的加速缓存。
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

        // 信号缓冲快照：与等待点同批恢复，否则"等待点跨重启
        // 存活、递给它的信号不存活"——缓冲在重启路径上等于失效。
        let sigbuf_entries = store.scan_prefix(STORE_KEY_SIGNAL_BUF).await?;
        for (key, data) in sigbuf_entries {
            let wf_id_str = key.strip_prefix(STORE_KEY_SIGNAL_BUF).unwrap_or(&key);
            let wf_id = WorkflowId::from(wf_id_str.to_string());
            match postcard::from_bytes::<HashMap<String, Vec<u8>>>(&data) {
                Ok(table) => {
                    state
                        .pending_signals
                        .insert(wf_id, table.into_iter().collect());
                }
                Err(e) => {
                    // 缓冲损坏只影响"先到的信号"，不阻塞任务状态恢复；与等待点
                    // 快照同一处理口径：告警后继续。
                    tracing::warn!(
                        workflow = %wf_id_str,
                        error = ?e,
                        "recover: corrupt signal buffer snapshot, buffered signals dropped"
                    );
                }
            }
        }

        // 确定性上下文值快照：与 exec/pending 同批恢复，重放才能返回同一组值。
        let ctx_entries = store.scan_prefix(STORE_KEY_CONTEXT).await?;
        for (key, data) in ctx_entries {
            let wf_id_str = key.strip_prefix(STORE_KEY_CONTEXT).unwrap_or(&key);
            let wf_id = WorkflowId::from(wf_id_str.to_string());
            match postcard::from_bytes::<HashMap<String, ContextEntry>>(&data) {
                Ok(table) => {
                    state
                        .context_values
                        .insert(wf_id, table.into_iter().collect());
                }
                Err(e) => {
                    // 上下文快照损坏：事务状态恢复不受影响（历史是事实源，
                    // 水位后事件会重建；水位前的值已随快照丢失，仅告警）。
                    tracing::warn!(
                        workflow = %wf_id_str,
                        error = ?e,
                        "recover: corrupt context value snapshot, recorded values dropped"
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
                signal_buf_key(wf_id),
                context_key(wf_id),
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
            node_id: None,
            hlc: Arc::new(HybridLogicalClock::with_max_drift_ms(
                config.network.hlc_max_drift_ms,
            )),
            network: None,
        };

        // 快照加载完成后重放其后的事件，恢复语义唯一化（快照 + 事件重放）。
        orchestrator
            .replay_events_after_watermarks(event_log.as_deref(), &watermarks)
            .await;

        Ok(orchestrator)
    }

    /// 重放各工作流水位之后的事件：recover = 快照 + 事件重放。
    ///
    /// 事件按追加顺序重放，逐条按语义幂等推进：
    /// - 任务事件复用 `WorkflowExecution::mark_task_completed` / `fail_task` /
    ///   `cancel_task` 的既有守卫（终态守卫 + attempt fencing），重复/迟到
    ///   事件被拒绝，不产生二次状态变更；
    /// - 等待点事件幂等：已注册的 wait_key 跳过、已 Signaled 的等待点跳过；
    /// - 节点事件：DAG 中已存在的节点跳过（快照加速），缺失节点从事件重建
    ///   （增量提交的前置能力）。
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
                match postcard::from_bytes::<JournalEvent>(&entry.payload) {
                    Ok(payload) => {
                        self.apply_replayed_event(payload).await;
                        replayed += 1;
                    }
                    Err(e) => {
                        // 解码失败的事件跳过：旧枚举布局与新枚举布局不兼容
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
    pub(crate) async fn apply_replayed_event(&self, payload: JournalEvent) {
        match payload {
            JournalEvent::TaskCompleted {
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
                    // 与 compute_ready_successors 一致地递减后继 pending，
                    // 保证 recover_ready_tasks 的 "pending == 0 且 Pending"
                    // 过滤与崩溃前状态一致。
                    for succ in slot.dag.successor_ids(&task_id) {
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
            JournalEvent::TaskFailed {
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
            JournalEvent::TaskCancelled {
                workflow_id,
                task_id,
            } => {
                let mut slot = match self.state.slots.get_mut(&workflow_id) {
                    Some(slot) if slot.state == SlotState::Ready => slot,
                    _ => return,
                };
                let was_terminal = slot.execution.is_terminal();
                if slot.execution.cancel_task(&task_id) {
                    self.state.mark_dirty(&workflow_id);
                }
                // 取消可终结工作流（末节点被取消）：与 TaskFailed 回放对称，
                // 需在恢复期落盘终态快照，否则重启后工作流状态回退为非终态。
                let became_terminal = !was_terminal && slot.execution.is_terminal();
                if became_terminal {
                    drop(slot);
                    self.persist_terminal_after_replay(&workflow_id).await;
                }
            }
            JournalEvent::NodeAdded { workflow_id, node } => {
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
            JournalEvent::Started { workflow_id } => {
                let mut slot = match self.state.slots.get_mut(&workflow_id) {
                    Some(slot) if slot.state == SlotState::Ready => slot,
                    _ => return,
                };
                if !slot.execution.is_terminal() {
                    slot.execution.mark_running();
                    self.state.mark_dirty(&workflow_id);
                }
            }
            JournalEvent::WaitPointRegistered {
                workflow_id,
                wait_key,
                condition,
            } => {
                // 幂等：同 wait_key 已存在（快照已含）则跳过。
                if self
                    .state
                    .waitpoints
                    .get(&workflow_id)
                    .is_some_and(|t| t.contains_key(&wait_key))
                {
                    return;
                }
                // 与 `register_wait_point` 同款：注册时消费缓冲（若信号先到）。
                // 重放顺序里 `SignalReceived` 可能早于 `WaitPointRegistered`
                //（缓冲场景），不消费就会把已收到的信号丢掉。
                let buffered = matches!(condition, WaitCondition::Signal { .. })
                    .then(|| {
                        self.state
                            .pending_signals
                            .get(&workflow_id)
                            .and_then(|buf| buf.remove(&wait_key).map(|(_, p)| p))
                    })
                    .flatten();
                self.state
                    .waitpoints
                    .entry(workflow_id)
                    .or_default()
                    .entry(wait_key)
                    .or_insert_with(|| WaitPoint {
                        condition,
                        state: match buffered {
                            Some(payload) => WaitPointState::Signaled { payload },
                            None => WaitPointState::Waiting,
                        },
                    });
            }
            JournalEvent::SignalReceived {
                workflow_id,
                wait_key,
                payload,
            } => {
                // 一次取表到底，不写成"先判存在、再 get_mut().expect()"——
                // 那两步之间有窗口，并发移除会让 expect panic（先取后验，避免 TOCTOU）。
                // 等待点不存在 ⇒ 信号先到：入缓冲，等后续 `WaitPointRegistered`
                // 重放时消费；直接丢弃会让"缓冲跨重启失效"
                // （缓冲与等待点同批落盘，是保证跨重启一致性的兜底路径）。
                let Some(table) = self.state.waitpoints.get_mut(&workflow_id) else {
                    self.state
                        .pending_signals
                        .entry(workflow_id)
                        .or_default()
                        .entry(wait_key)
                        .or_insert(payload);
                    return;
                };
                let Some(mut wp) = table.get_mut(&wait_key) else {
                    self.state
                        .pending_signals
                        .entry(workflow_id)
                        .or_default()
                        .entry(wait_key)
                        .or_insert(payload);
                    return;
                };
                if wp.state == WaitPointState::Waiting {
                    wp.state = WaitPointState::Signaled { payload };
                }
            }
            JournalEvent::TimerFired {
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
            JournalEvent::ContextValue {
                workflow_id,
                key,
                kind,
                value,
            } => {
                // 幂等：快照已含同 key 则保留快照值（二者应当一致；不一致说明
                // 快照与历史不同代，保留快照是因为水位已确认其被吸收）。
                self.state
                    .context_values
                    .entry(workflow_id)
                    .or_default()
                    .entry(key)
                    .or_insert(ContextEntry { kind, value });
            }
            // 派发/运行事件对状态机无增量语义（Running 会在快照加载时被重置
            // 为 Pending 以便重派发）；Submitted/Completed/Cancelled/Failed/
            // Recovered 的状态已由快照吸收。
            JournalEvent::TaskDispatched { .. }
            | JournalEvent::TaskRunning { .. }
            | JournalEvent::Submitted { .. }
            | JournalEvent::Completed { .. }
            | JournalEvent::Cancelled { .. }
            | JournalEvent::Failed { .. }
            | JournalEvent::Recovered { .. } => {}
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

    /// 记录 [`JournalEvent::Recovered`] 事件到 event_log（若存在）。
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
        let payload = JournalEvent::Recovered {
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
        let event_log = self.event_log.clone();
        let config = self.config.clone();
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

                        let plan = FlushPlan::collect(state.as_ref(), &dirty_ids);
                        if plan.snapshots.is_empty() {
                            plan.mark_retry(state.as_ref(), false);
                            continue;
                        }
                        let batch = plan.entries();
                        if let Err(e) = store.put_batch(&batch).await {
                            // 写入失败时把本轮 drained 的 workflow 重新加入脏集合，
                            // 避免状态丢失直到下一轮 flush 重试。
                            tracing::error!("persist flush failed: {}", e);
                            plan.mark_retry(state.as_ref(), true);
                        } else {
                            // 水位已落盘：此刻裁剪"已进快照"的历史才是安全的。
                            plan.trim_persisted(event_log.as_ref(), &config);
                        }
                    }
                }
            }
        });

        cancel_tx
    }

    /// 立即把所有脏工作流状态落盘（测试与优雅关闭使用）。
    pub async fn flush_dirty(&self) -> Result<()> {
        let Some(ref store) = self.store else {
            return Ok(());
        };
        let dirty_ids = self.state.drain_dirty();
        if dirty_ids.is_empty() {
            return Ok(());
        }

        let plan = FlushPlan::collect(self.state.as_ref(), &dirty_ids);
        if plan.snapshots.is_empty() {
            // 序列化全失败（或工作流已被淘汰）：重新标脏后返回，不当作错误。
            plan.mark_retry(self.state.as_ref(), false);
            return Ok(());
        }
        // 写入失败时必须先整批重标脏再上抛：drain 已经把这批移出脏集合，
        // 直接 `?` 会让它们永久停在「内存最新、磁盘缺失」的状态。
        if let Err(e) = store.put_batch(&plan.entries()).await {
            plan.mark_retry(self.state.as_ref(), true);
            return Err(e);
        }
        plan.mark_retry(self.state.as_ref(), false);
        plan.trim_persisted(self.event_log.as_ref(), &self.config);
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
                signal_buf_key(old_id),
                context_key(old_id),
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
