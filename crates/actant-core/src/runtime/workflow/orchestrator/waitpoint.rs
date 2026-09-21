//! Orchestrator 的 `waitpoint` 职责子模块（持久化等待点原语）。
//!
//! 等待点是 orchestrator 状态机的挂起原语：持久化
//! `(workflow_id, wait_key, 条件)` 三元组，条件满足（信号递交 / 定时到期）
//! 时追加唤醒事件进入同一工作流历史。事实源是事件历史；随快照落盘的
//! 等待点条目是重放加速缓存（见 [`super::persistence`]）。
//!
//! 等待点 API 天然幂等以支撑重放语义：同 key 重复注册为 no-op，
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
        // 缓冲命中：注册前已抵达的信号（与等待点同批恢复）。
        // 只有 `Signal` 条件的等待点消费缓冲——`Suspend` 是操作员指令、`Timer`
        // 是时钟事件，都不该被一个业务信号顶替。
        let buffered = matches!(condition, WaitCondition::Signal { .. })
            .then(|| {
                self.state
                    .pending_signals
                    .get(workflow_id)
                    .and_then(|buf| buf.remove(wait_key).map(|(_, payload)| payload))
            })
            .flatten();

        let table = self
            .state
            .waitpoints
            .entry(workflow_id.clone())
            .or_default();
        if table.contains_key(wait_key) {
            // 幂等：同 wait_key 已注册 → 不重复追加事件、不改写条件。
            // 缓冲已出队（若有）不回填：已注册的等待点按其自身状态为准。
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
                // 缓冲命中 → 直接生成为已唤醒态：历史里那条 `SignalReceived`
                // 早于本条 `WaitPointRegistered` 存在，重放会按同样
                // 顺序收敛到同一结果（"查历史——已收到 → 直接返回 payload"）。
                state: match buffered {
                    Some(payload) => WaitPointState::Signaled { payload },
                    None => WaitPointState::Waiting,
                },
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
    /// payload 当前为空（预留 Signals capability 携带信号数据）。
    ///
    /// 契约：**信号缓冲**。返回值含义：
    ///
    /// - `Ok(Some(payload))`：有等待点被本次递交唤醒（或历史中已收到）；
    /// - `Ok(None)`：此刻**没有等待点可被唤醒**，信号已入缓冲，将来
    ///   [`Self::register_wait_point`] 注册同 `wait_key` 时会立即命中；
    /// - `Err(NotFound)` / `Err(InvalidState)`：工作流不存在 / 已终态——
    ///   递交错误，信号无处可去（不再静默返回 `None`）。
    ///
    /// 缓冲是**闩锁**：同一 `wait_key` 已有缓冲时重复递交不再追加事件、不覆盖
    /// （当前 payload 恒为空，重复递交不携带新信息），这让"按返回值重试"变安全。
    ///
    /// **持久性边界**：缓冲写 event_log（独立 `SignalReceived` 历史条目）+ 内存表，
    /// 缓冲与等待点同批落盘（`orch:sigbuf:` 与 `orch:wait:`），故**跨重启存活**；
    /// 重放路径同样会缓冲"先到的信号"（`apply_replayed_event` 的 `SignalReceived`
    /// 分支），两条路互为纵深防御。唯一会丢的窗口是"缓冲写入后、尚未落盘就崩溃"。
    /// 信号缓冲与等待点同批落盘，故跨重启存活；唯一丢失窗口是落盘前的崩溃。
    pub fn signal_wait_point(
        &self,
        workflow_id: &WorkflowId,
        wait_key: &str,
    ) -> Result<Option<Vec<u8>>> {
        // 递交错误的显式路径：让"递错 id"与"递给已结束的工作流"
        // 不再与"缓冲成功"同形于 `None`。
        if wait_key.is_empty() {
            return Err(ActantError::Config("wait_key must not be empty".into()));
        }
        if !self.state.slots.contains_key(workflow_id) {
            return Err(ActantError::NotFound(format!(
                "workflow {} not found",
                workflow_id.as_str()
            )));
        }
        // 终态**不拒绝**。曾在此处拒绝（"已终态 ⇒ 信号无处可去"），实测被推翻：
        // 递交方重试时，前一次递交可能已被缓冲命中、flow 已跑完并让工作流进入终态，
        // 此时重试会撞上"已终态"而报错——**明明送到了却报错**，这恰好破坏本项
        // 想保证的"重试安全"（终态不拒绝递交，缓冲边界由随工作流移除清除承担）。
        // 缓冲的边界改由"随工作流移除而清除"承担，不靠拒绝递交。

        // 已缓冲：闩锁，不重复入历史。
        if let Some(buf) = self.state.pending_signals.get(workflow_id) {
            if buf.contains_key(wait_key) {
                return Ok(None);
            }
        }

        // **不要**写成"先判存在、再 get().expect()"：两次取表之间有窗口，
        // 并发的 `evict_workflow` / `remove_workflow` 会让第二次拿到 `None`
        // 然后 panic（审查发现的 TOCTOU）。这里一次取表到底，不存在就走缓冲。
        // `buffer_signal` 只碰 `pending_signals`（另一张表），故持着本表 guard
        // 调用它不会死锁。
        let Some(table) = self.state.waitpoints.get(workflow_id) else {
            return self.buffer_signal(workflow_id, wait_key);
        };
        let Some(mut wp) = table.get_mut(wait_key) else {
            return self.buffer_signal(workflow_id, wait_key);
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

    /// 缓冲一个"等待点尚未注册"的信号（与等待点同批恢复）。
    ///
    /// 追加**独立** `SignalReceived` 历史条目（`SignalReceived` 因此可作为早于
    /// 任何等待点的历史事件存在，满足"查历史"的前置要求），并写入内存
    /// 缓冲表供 `register_wait_point` 命中。
    ///
    /// 调用方保证：工作流存在且非终态，且同 `wait_key` 尚无缓冲。
    fn buffer_signal(&self, workflow_id: &WorkflowId, wait_key: &str) -> Result<Option<Vec<u8>>> {
        let payload = Vec::new();
        self.log_event(WorkflowEventPayload::SignalReceived {
            workflow_id: workflow_id.clone(),
            wait_key: wait_key.to_string(),
            payload: payload.clone(),
        });
        self.state
            .pending_signals
            .entry(workflow_id.clone())
            .or_default()
            .insert(wait_key.to_string(), payload);
        tracing::debug!(
            workflow = %workflow_id.as_str(),
            wait_key = %wait_key,
            "signal buffered: no wait point registered yet"
        );
        Ok(None)
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

    /// 恢复挂起：唤醒该工作流所有 `Waiting` 的 [`WaitCondition::Suspend`] 等待点。
    ///
    /// 返回本次唤醒的等待点数量；`0` 表示该工作流当前没有处于挂起中的挂起点
    /// （幂等——重复调用第二次返回 0，不重复追加事件）。
    ///
    /// **只匹配 `Suspend` 条件**，不触碰 `Signal` / `Timer`：`resume` 是操作员的
    /// 恢复指令，不得冒名顶替一个业务信号。
    /// 唤醒事件复用 [`WorkflowEventPayload::SignalReceived`]——其载荷语义本就是
    /// "等待点被外部满足"，由 `condition` 字段区分是 signal 还是 resume，故
    /// 不新增事件变体、重放路径零改动。
    ///
    /// 按 `workflow_id` 而非按键唤醒，是"多周期挂起/恢复"成立的前提：调用方
    /// 无需知道 flow 内部给挂起点分配了什么键。
    pub fn resume_suspended(&self, workflow_id: &WorkflowId) -> Result<usize> {
        let Some(table) = self.state.waitpoints.get(workflow_id) else {
            return Ok(0);
        };
        let pending: Vec<String> = table
            .iter()
            .filter_map(|wp| match (&wp.state, &wp.condition) {
                (WaitPointState::Waiting, WaitCondition::Suspend) => Some(wp.key().clone()),
                _ => None,
            })
            .collect();

        let mut resumed = 0usize;
        for wait_key in pending {
            let Some(mut wp) = table.get_mut(&wait_key) else {
                continue;
            };
            // 双重检查：收集与改写之间等待点可能已被唤醒（信号/取消路径）。
            if wp.state != WaitPointState::Waiting {
                continue;
            }
            wp.state = WaitPointState::Signaled {
                payload: Vec::new(),
            };
            drop(wp);
            self.log_event(WorkflowEventPayload::SignalReceived {
                workflow_id: workflow_id.clone(),
                wait_key: wait_key.clone(),
                payload: Vec::new(),
            });
            self.state.mark_dirty(workflow_id);
            self.state
                .fire_wait_waiter(workflow_id, &wait_key, Vec::new());
            resumed += 1;
        }
        drop(table);
        Ok(resumed)
    }

    /// 释放所有等待点等待者（运行时关停时调用）。
    ///
    /// `wait_wait_point` 的无限 park 语义（`timeout_ms = 0`）意味着等待者可能
    /// 永远不返回；关停时必须主动唤醒它们，否则 park 线程（可能是主线程）会挂住
    /// 进程退出。丢弃 sender 后 receiver 立即收到 `Err` → park 方返回"未唤醒"。
    pub fn release_all_wait_point_waiters(&self) {
        self.state.clear_wait_waiters();
    }

    /// 注册等待点唤醒句柄（oneshot），条件满足时收到 payload。
    ///
    /// 扩展 [`TerminalWaiterRegistry`] 模式，供 flow 线程在等待点 park：
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
