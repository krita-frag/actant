//! 时间驱动的后台扫描：工作流级硬超时 + `Timer` 等待点到期。
//!
//! 与 `execution.rs` 的状态迁移逻辑分离：本模块只做周期轮询，把到期事实
//! 转成 `Failed` / `TimerFired` 事件与取消广播，不参与 admission / readiness。

use crate::common::serialization::serialize_rkyv;
use crate::common::wire::TOPIC_CANCEL;
use crate::common::{TaskId, WorkflowId};
use crate::runtime::network::{NetworkEvent, NetworkMessage};
use crate::runtime::workflow::{Phase, Terminal};

use super::super::keys::exec_key;
use super::super::persistence::append_event;
use super::super::types::JournalEvent;
use super::super::Orchestrator;

impl Orchestrator {
    /// Spawns a background task that periodically checks for expired workflows
    /// and marks them failed. Returns a watch sender for shutdown signaling.
    ///
    /// 当 `network` 已注入时，超时处理路径会主动广播 `CancelBroadcast`
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
                        // 收集 (workflow_id, task_id) 对，统一在持久化后广播取消。
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
                                    if let Some(id) = append_event(
                                        event_log.as_ref(),
                                        JournalEvent::Failed {
                                            workflow_id: wf_id.clone(),
                                            error: "workflow timeout exceeded".into(),
                                        },
                                    ) {
                                        state.record_event_seq(&wf_id, id);
                                    }
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
                        // 广播取消消息。即使持久化失败也要尝试取消，否则运行中的
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

                        // 扫描到期的 `Timer` 等待点。
                        //
                        // Timer 类等待点必须自动到期：生产路径若无定时任务调用
                        // `poll_expired_timers`，等待点**永不自动到期**，
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
}
