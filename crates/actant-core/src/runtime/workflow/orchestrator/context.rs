//! 确定性上下文值：flow 体的 wall-clock / 随机数 / UUID。
//!
//! 重放模型要求"同一 `workflow_id` 的同一序号调用返回同一值"：首次执行时
//! Python 侧生成候选值，本模块把它写入工作流历史并返回；重放时命中历史，
//! 返回**记录值**（候选值被丢弃）。事实源是事件历史（`ContextValue`），
//! 内存表随快照落盘作为重放加速缓存——与等待点、信号缓冲同一套恢复语义。
//!
//! **漂移检测**：`key` 由调用序号决定，`kind` 记录调用种类。同 `key` 以不同
//! `kind` 重放说明 flow 体的调用序列已改变，返回 [`ActantError::Replay`]
//! 显式失败，避免"时间/随机值静默变化"这一最难排查的失真。

use crate::common::{ActantError, Result, WorkflowId};

use super::{types::*, Orchestrator};

impl Orchestrator {
    /// 解析（首次记录 / 重放命中）一个确定性上下文值。
    ///
    /// - `key` 已记录且 `kind` 一致 → 返回记录值；
    /// - `key` 已记录但 `kind` 不同 → [`ActantError::Replay`]（调用序列漂移）；
    /// - `key` 未记录 → 写入 `candidate`、追加历史事件、标记脏，返回
    ///   `candidate`。
    ///
    /// 幂等性：重复调用同 `(key, kind)` 恒返回首次记录值，故重放体可以安全
    /// 重复执行到同一行。
    pub fn resolve_context_value(
        &self,
        workflow_id: &WorkflowId,
        key: &str,
        kind: &str,
        candidate: Vec<u8>,
    ) -> Result<Vec<u8>> {
        if key.is_empty() {
            return Err(ActantError::Config("context key must not be empty".into()));
        }
        if kind.is_empty() {
            return Err(ActantError::Config("context kind must not be empty".into()));
        }
        {
            let slot = self.state.slots.get(workflow_id).ok_or_else(|| {
                ActantError::NotFound(format!("workflow {} not found", workflow_id.as_str()))
            })?;
            if slot.state != SlotState::Ready {
                return Err(ActantError::InvalidState(format!(
                    "workflow {} is still loading (placeholder), cannot resolve context value",
                    workflow_id.as_str()
                )));
            }
        }

        let table = self
            .state
            .context_values
            .entry(workflow_id.clone())
            .or_default();
        if let Some(entry) = table.get(key) {
            if entry.kind != kind {
                return Err(ActantError::Replay(format!(
                    "context value {} in workflow {} was recorded as kind {:?}, \
                     replay requested {:?}; the flow body call sequence diverged",
                    key,
                    workflow_id.as_str(),
                    entry.kind,
                    kind
                )));
            }
            return Ok(entry.value.clone());
        }
        table.insert(
            key.to_string(),
            ContextEntry {
                kind: kind.to_string(),
                value: candidate.clone(),
            },
        );
        // 释放分片锁后再写事件/脏标记：二者会触碰其他 DashMap，避免不必要的
        // 跨表持锁。
        drop(table);

        self.log_event(JournalEvent::ContextValue {
            workflow_id: workflow_id.clone(),
            key: key.to_string(),
            kind: kind.to_string(),
            value: candidate.clone(),
        });
        self.state.mark_dirty(workflow_id);
        Ok(candidate)
    }

    /// 只读查询一个已记录的上下文值（`None` = 未记录）。
    ///
    /// 与 [`Self::resolve_context_value`] 的区别是**不写历史、不标记脏**：
    /// 调用方需要"读一个可能不存在的值"（如续跑时读取 continue-as-new 的
    /// 携带参数）而不想为缺失值制造一条记录时用它。
    pub fn get_context_value(
        &self,
        workflow_id: &WorkflowId,
        key: &str,
    ) -> Result<Option<Vec<u8>>> {
        {
            let slot = self.state.slots.get(workflow_id).ok_or_else(|| {
                ActantError::NotFound(format!("workflow {} not found", workflow_id.as_str()))
            })?;
            if slot.state != SlotState::Ready {
                return Err(ActantError::InvalidState(format!(
                    "workflow {} is still loading (placeholder), cannot read context value",
                    workflow_id.as_str()
                )));
            }
        }
        Ok(self
            .state
            .context_values
            .get(workflow_id)
            .and_then(|table| table.get(key).map(|entry| entry.value.clone())))
    }
}
