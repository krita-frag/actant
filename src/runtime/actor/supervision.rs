//! Actor 监督事件统一发布入口。
//!
//! `SupervisionTree` 持有 `EventBus` 引用，所有监督事件通过
//! `Topic::Supervision` 广播——订阅者（Worker、外部观测者、单测）
//! 通过 `EventBus::subscribe` 统一消费，保证监督事件只有单一发布路径。

use crate::common::ActorId;
use crate::runtime::event_bus::{BusEvent, EventBus, Topic};

#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
#[non_exhaustive]
pub enum SupervisionEvent {
    ActorStarted { actor_id: ActorId },
    ActorFailed { actor_id: ActorId, error: String },
    ActorStopped { actor_id: ActorId },
}

/// Actor 监管事件的统一发布入口。
///
/// 现版本 `SupervisionTree` 持有 `EventBus` 引用，`emit()` 直接发布
/// `BusEvent::SupervisionEvent` 到 `Topic::Supervision`。所有订阅者
/// （包括跨模块的 Worker、外部观测者）通过 `EventBus::subscribe` 统一消费，
/// 消除双路径。
///
/// `subscribe()` 返回 `mpsc::Receiver<BusEvent>`，便于单测与外部观测。
pub struct SupervisionTree {
    event_bus: EventBus,
}

impl SupervisionTree {
    pub fn with_event_bus(event_bus: EventBus) -> Self {
        Self { event_bus }
    }

    /// 返回内部 `EventBus` 引用（用于订阅 `Topic::Supervision` 或其他 topic）。
    pub fn event_bus(&self) -> &EventBus {
        &self.event_bus
    }

    /// 订阅 `Topic::Supervision`，返回 `mpsc::Receiver<BusEvent>`。
    pub fn subscribe(&self) -> tokio::sync::mpsc::Receiver<BusEvent> {
        self.event_bus.subscribe(Topic::Supervision)
    }

    /// 同步发布监督事件到 EventBus。
    ///
    /// `publish` 是 async，但监督事件通常在同步上下文（如 `cleanup_actor`）
    /// 中触发。通过 `tokio::spawn` 发布避免阻塞调用方；EventBus 内部
    /// 对无订阅者场景已做 best-effort 处理，事件丢失可接受（监督事件
    /// 仅为观测信号，不驱动状态机）。
    ///
    /// # 投递语义
    ///
    /// `BusEvent::SupervisionEvent` 在 EventBus 中映射为
    /// `DeliveryGuarantee::BestEffort`（`try_send`，通道满即丢并记
    /// `event_bus_dropped_events` 指标）。这与「监督事件仅为观测信号」
    /// 的定位一致：即使丢失，下一次心跳/重试也会重新覆盖。
    ///
    /// 注意：不可恢复的 actor 错误（panic / 状态机非法转换 / 持久化失败）
    /// 走 `BusEvent::ActorLifecycleError`，其在 EventBus 中为
    /// `DeliveryGuarantee::Reliable`（`send_timeout`，可修剪慢订阅者），
    /// 不经本方法发布——确保关键告警不被丢弃。
    pub fn emit(&self, event: SupervisionEvent) {
        let bus = self.event_bus.clone();
        tokio::spawn(async move {
            bus.publish(BusEvent::SupervisionEvent(event)).await;
        });
    }
}
