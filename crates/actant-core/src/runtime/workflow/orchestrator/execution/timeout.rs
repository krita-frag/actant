//! 时间驱动的后台扫描：工作流级硬超时 + `Timer` 等待点到期。
//!
//! 与 `execution.rs` 的状态迁移逻辑分离：本模块只做周期轮询，把到期事实
//! 转成 `Failed` / `TimerFired` 事件与取消广播（双腿广播实现在
//! [`Orchestrator::broadcast_task_cancel`]），不参与 admission / readiness。

use crate::common::{TaskId, WorkflowId};
use crate::runtime::workflow::{Phase, Terminal};

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
                        // 收集 (workflow_id, task_id) 对，统一在终态收尾后广播取消。
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
                        // 先在写锁内翻转状态并克隆快照；complete_terminal 内部
                        // 会再次读取同一 slot，持锁调用会造成 DashMap 重入死锁。
                        let mut terminal_snapshots: Vec<(
                            WorkflowId,
                            crate::runtime::workflow::WorkflowExecution,
                        )> = Vec::new();
                        for wf_id in expired {
                            if let Some(mut slot) = state.slots.get_mut(&wf_id) {
                                if !slot.execution.is_terminal() {
                                    slot.execution.mark_workflow_failed("workflow timeout exceeded".into());
                                    crate::metrics::inc_workflow_timeouts();
                                    tracing::warn!("workflow {} timed out, marked as failed", wf_id.as_str());
                                    terminal_snapshots.push((wf_id.clone(), slot.execution.clone()));
                                }
                            }
                        }
                        // 统一终态收尾：终态事件、指标、等待者释放、淘汰与
                        // 落盘失败容错与其它入口共用一处实现。
                        for (wf_id, snapshot) in terminal_snapshots {
                            if let Err(e) = orchestrator
                                .complete_terminal(&wf_id, None, &snapshot)
                                .await
                            {
                                tracing::error!(
                                    workflow = %wf_id.as_str(),
                                    error = %e,
                                    "failed to settle timed-out workflow terminal state"
                                );
                            }
                        }
                        // 广播取消消息。即使持久化失败也要尝试取消，否则运行中的
                        // 任务会继续占用 Worker 槽位直到自身完成或超时。
                        // 双腿广播（gossip + 本地自投递）的实现与语义说明见
                        // [`Orchestrator::broadcast_task_cancel`]——与用户级
                        // `cancel` 共用同一实现，避免两类入口行为漂移。
                        for (wf_id, task_id) in &cancels_to_broadcast {
                            orchestrator
                                .broadcast_task_cancel(wf_id, task_id)
                                .await;
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
