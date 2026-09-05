//! Orchestrator 的 `waitpoint` 职责子模块（S1 持久化等待点原语）。
//!
//! 等待点是 orchestrator 状态机的挂起原语：持久化
//! `(workflow_id, wait_key, 条件)` 三元组，条件满足（信号递交 / 定时到期）
//! 时追加唤醒事件进入同一工作流历史。事实源是事件历史；随快照落盘的
//! 等待点条目是重放加速缓存（见 [`super::persistence`]）。
//!
//! 等待点 API 天然幂等以支撑 S7 重放语义：同 key 重复注册为 no-op，
//! 重复 signal 直接返回已收到的 payload（重放体"已收到 → 直接返回"）。

use crate::common::{ActantError, Result, WorkflowId};
use crate::runtime::workflow::{WaitCondition, WaitPoint, WaitPointState};

use super::{types::*, Orchestrator};

impl Orchestrator {
    /// 注册等待点（幂等：同 `wait_key` 已注册则直接返回 `Ok(())`）。
    ///
    /// 注册即追加 [`WorkflowEventPayload::WaitPointRegistered`] 事件进入
    /// 工作流历史，并标记工作流脏（等待点随下一次 flush 并入快照）。
    pub fn register_wait_point(
        &self,
        workflow_id: &WorkflowId,
        wait_key: &str,
        condition: WaitCondition,
    ) -> Result<()> {
        if wait_key.is_empty() {
            return Err(ActantError::Config("wait_key must not be empty".into()));
        }
        {
            let slot = self.state.slots.get(workflow_id).ok_or_else(|| {
                ActantError::NotFound(format!("workflow {} not found", workflow_id.as_str()))
            })?;
            if slot.state != SlotState::Ready {
                return Err(ActantError::InvalidState(format!(
                    "workflow {} is still loading (placeholder), cannot register wait point",
                    workflow_id.as_str()
                )));
            }
        }
        let table = self
            .state
            .waitpoints
            .entry(workflow_id.clone())
            .or_default();
        if table.contains_key(wait_key) {
            // 幂等：同 wait_key 已注册 → 不重复追加事件、不改写条件。
            return Ok(());
        }
        self.log_event(WorkflowEventPayload::WaitPointRegistered {
            workflow_id: workflow_id.clone(),
            wait_key: wait_key.to_string(),
            condition: condition.clone(),
        });
        table.insert(
            wait_key.to_string(),
            WaitPoint {
                condition,
                state: WaitPointState::Waiting,
            },
        );
        self.state.mark_dirty(workflow_id);
        Ok(())
    }

    /// 递交信号，唤醒条件为 `Signal` 的等待点。
    ///
    /// - 无该 `wait_key` 的等待点 → `Ok(None)`；
    /// - 已被唤醒 → `Ok(Some(payload))`（重放体幂等：历史中已收到 → 直接返回）；
    /// - 等待中 → 追加 [`WorkflowEventPayload::SignalReceived`] 事件、标记
    ///   Signaled、唤醒 oneshot 等待者，返回 `Ok(Some(payload))`。
    ///
    /// payload 当前为空（预留 S2 Signals capability 携带信号数据）。
    pub fn signal_wait_point(
        &self,
        workflow_id: &WorkflowId,
        wait_key: &str,
    ) -> Result<Option<Vec<u8>>> {
        let Some(table) = self.state.waitpoints.get(workflow_id) else {
            return Ok(None);
        };
        let Some(mut wp) = table.get_mut(wait_key) else {
            return Ok(None);
        };
        match &wp.state {
            // 重放体幂等：历史中已收到 → 直接返回 payload。
            WaitPointState::Signaled { payload } => Ok(Some(payload.clone())),
            WaitPointState::Waiting => {
                let payload = Vec::new();
                wp.state = WaitPointState::Signaled {
                    payload: payload.clone(),
                };
                drop(wp);
                self.log_event(WorkflowEventPayload::SignalReceived {
                    workflow_id: workflow_id.clone(),
                    wait_key: wait_key.to_string(),
                    payload: payload.clone(),
                });
                self.state.mark_dirty(workflow_id);
                self.state.fire_wait_waiter(workflow_id, wait_key, payload);
                Ok(Some(Vec::new()))
            }
        }
    }

    /// 扫描并触发所有到期的 `Timer` 等待点（复用超时 watcher 的轮询模式；
    /// 生产环境由定时任务周期调用）。
    ///
    /// 到期的等待点追加 [`WorkflowEventPayload::TimerFired`] 事件并标记
    /// Signaled。返回本轮触发的 `(workflow_id, wait_key)` 列表；重复调用
    /// 幂等（已 Signaled 的等待点不会再次触发）。
    pub async fn poll_expired_timers(&self) -> Result<Vec<(WorkflowId, String)>> {
        let now_ms = crate::common::epoch_millis();
        let mut fired = Vec::new();
        for entry in self.state.waitpoints.iter() {
            let workflow_id = entry.key().clone();
            let due_keys: Vec<String> = entry
                .value()
                .iter()
                .filter_map(|wp| match (&wp.state, &wp.condition) {
                    (WaitPointState::Waiting, WaitCondition::Timer { deadline_ms }) => {
                        (now_ms >= *deadline_ms).then(|| wp.key().clone())
                    }
                    _ => None,
                })
                .collect();
            for wait_key in due_keys {
                let Some(mut wp) = entry.value().get_mut(&wait_key) else {
                    continue;
                };
                if wp.state != WaitPointState::Waiting {
                    continue;
                }
                wp.state = WaitPointState::Signaled {
                    payload: Vec::new(),
                };
                drop(wp);
                self.log_event(WorkflowEventPayload::TimerFired {
                    workflow_id: workflow_id.clone(),
                    wait_key: wait_key.clone(),
                });
                self.state.mark_dirty(&workflow_id);
                self.state
                    .fire_wait_waiter(&workflow_id, &wait_key, Vec::new());
                fired.push((workflow_id.clone(), wait_key));
            }
        }
        Ok(fired)
    }

    /// 注册等待点唤醒句柄（oneshot），条件满足时收到 payload。
    ///
    /// 扩展 [`TerminalWaiterRegistry`] 模式，供 S7 flow 线程在等待点 park：
    /// 先注册后检查——若等待点已 Signaled，句柄立即被触发，关闭竞态窗口。
    pub fn register_wait_point_waiter(
        &self,
        workflow_id: WorkflowId,
        wait_key: &str,
    ) -> tokio::sync::oneshot::Receiver<Vec<u8>> {
        let rx = self
            .state
            .register_wait_waiter(workflow_id.clone(), wait_key);
        // "先注册后检查"：已 Signaled 的等待点立即触发刚注册的句柄。
        let signaled_payload: Option<Vec<u8>> =
            self.state.waitpoints.get(&workflow_id).and_then(|table| {
                table.get(wait_key).and_then(|wp| match &wp.state {
                    WaitPointState::Signaled { payload } => Some(payload.clone()),
                    _ => None,
                })
            });
        if let Some(payload) = signaled_payload {
            self.state.fire_wait_waiter(&workflow_id, wait_key, payload);
        }
        rx
    }
}
