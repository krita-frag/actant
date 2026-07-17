//! 基于 OpenTelemetry 的 Actant 运行时指标。
//!
//! 所有指标以类型化的 OpenTelemetry instrument 定义，通过兼容 Prometheus 的注册表导出。
//!
//! # 架构
//!
//! ```text
//! Rust 核心 ──(OTel API)──► SdkMeterProvider ──(PrometheusExporter)──► prometheus::Registry
//!                                                                                    │
//! Python HTTP 服务器 ◄──(prometheus_text())──────────────────────────────────────────┘
//! ```
//!
//! 指标完全在 Rust 内流转，无 FFI 回调到 Python。

use std::sync::Arc;

use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use parking_lot::Mutex;
use prometheus::{Registry, TextEncoder};

/// 所有 Actant 指标的类型化 OTel instrument。
struct Instruments {
    // -- 任务计数器 --
    tasks_submitted: Counter<u64>,
    tasks_completed: Counter<u64>,
    tasks_failed: Counter<u64>,
    tasks_timeout: Counter<u64>,
    tasks_retried: Counter<u64>,
    tasks_cancelled: Counter<u64>,

    // -- 工作流计数器 --
    workflows_submitted: Counter<u64>,
    workflows_completed: Counter<u64>,
    workflows_failed: Counter<u64>,
    workflow_timeouts: Counter<u64>,
    workflows_recovered_corrupt: Counter<u64>,
    retry_scheduled: Counter<u64>,

    // -- Gossip 计数器 --
    gossip_updates_sent: Counter<u64>,
    gossip_updates_received: Counter<u64>,
    gossip_updates_dropped: Counter<u64>,

    // -- 故障转移计数器 --
    heartbeats_sent: Counter<u64>,
    failover_claims: Counter<u64>,
    failover_reschedules: Counter<u64>,

    // -- Actor 计数器 --
    actors_spawned: Counter<u64>,
    actors_stopped: Counter<u64>,
    actors_failed: Counter<u64>,

    // -- 网络 / 事件总线计数器 --
    direct_requests_capacity_exceeded: Counter<u64>,
    event_bus_subscriber_pruned: Counter<u64>,
    event_bus_publish_timeout: Counter<u64>,
    event_bus_dropped_events: Counter<u64>,

    // -- 任务转发计数器 --
    task_forward_succeeded: Counter<u64>,
    task_forward_failed: Counter<u64>,
    task_forward_fallback_local: Counter<u64>,
    task_forward_reroute: Counter<u64>,

    // -- UpDownCounter（可增可减的仪表）--
    running_tasks: UpDownCounter<i64>,
    active_workflows: UpDownCounter<i64>,
    active_actors: UpDownCounter<i64>,
    connected_peers: UpDownCounter<i64>,
    cancelled_tasks_pending: UpDownCounter<i64>,

    // -- 直方图 --
    task_duration_ms: Histogram<u64>,
    cancel_latency_ms: Histogram<u64>,
    workflow_duration_ms: Histogram<u64>,
    scheduling_latency_ms: Histogram<u64>,
    payload_serialize_ms: Histogram<u64>,
    payload_deserialize_ms: Histogram<u64>,
    python_handler_ms: Histogram<u64>,
    event_bridge_ms: Histogram<u64>,
    actor_handle_message_ms: Histogram<u64>,
    actor_save_state_ms: Histogram<u64>,
    actor_load_state_ms: Histogram<u64>,
    direct_request_ms: Histogram<u64>,
}

impl Instruments {
    fn from_meter(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            // -- 任务计数器 --
            tasks_submitted: meter
                .u64_counter("actant.tasks.submitted")
                .with_description("Total tasks submitted")
                .build(),
            tasks_completed: meter
                .u64_counter("actant.tasks.completed")
                .with_description("Total tasks completed")
                .build(),
            tasks_failed: meter
                .u64_counter("actant.tasks.failed")
                .with_description("Total tasks failed")
                .build(),
            tasks_timeout: meter
                .u64_counter("actant.tasks.timeout")
                .with_description("Total tasks timed out")
                .build(),
            tasks_retried: meter
                .u64_counter("actant.tasks.retried")
                .with_description("Total tasks retried")
                .build(),
            tasks_cancelled: meter
                .u64_counter("actant.tasks.cancelled")
                .with_description("Total tasks cancelled")
                .build(),

            // -- 工作流计数器 --
            workflows_submitted: meter
                .u64_counter("actant.workflows.submitted")
                .with_description("Total workflows submitted")
                .build(),
            workflows_completed: meter
                .u64_counter("actant.workflows.completed")
                .with_description("Total workflows completed")
                .build(),
            workflows_failed: meter
                .u64_counter("actant.workflows.failed")
                .with_description("Total workflows failed")
                .build(),
            workflow_timeouts: meter
                .u64_counter("actant.workflows.timeouts")
                .with_description("Workflows timed out")
                .build(),
            workflows_recovered_corrupt: meter
                .u64_counter("actant.workflows.recovered_corrupt")
                .with_description("Workflows dropped during recovery due to corrupt persisted data")
                .build(),
            retry_scheduled: meter
                .u64_counter("actant.retry.scheduled")
                .with_description("Retry tasks scheduled")
                .build(),

            // -- Gossip 计数器 --
            gossip_updates_sent: meter
                .u64_counter("actant.gossip.updates.sent")
                .with_description("Gossip updates sent")
                .build(),
            gossip_updates_received: meter
                .u64_counter("actant.gossip.updates.received")
                .with_description("Gossip updates received")
                .build(),
            gossip_updates_dropped: meter
                .u64_counter("actant.gossip.updates.dropped")
                .with_description("Gossip updates dropped (stale)")
                .build(),

            // -- 故障转移计数器 --
            heartbeats_sent: meter
                .u64_counter("actant.failover.heartbeats.sent")
                .with_description("Heartbeats sent")
                .build(),
            failover_claims: meter
                .u64_counter("actant.failover.claims")
                .with_description("Failover claims made")
                .build(),
            failover_reschedules: meter
                .u64_counter("actant.failover.reschedules")
                .with_description("Failover task reschedules")
                .build(),

            // -- Actor 计数器 --
            actors_spawned: meter
                .u64_counter("actant.actors.spawned")
                .with_description("Total actors spawned")
                .build(),
            actors_stopped: meter
                .u64_counter("actant.actors.stopped")
                .with_description("Total actors stopped")
                .build(),
            actors_failed: meter
                .u64_counter("actant.actors.failed")
                .with_description("Total actors failed")
                .build(),

            // -- 网络 / 事件总线计数器 --
            direct_requests_capacity_exceeded: meter
                .u64_counter("actant.network.direct_requests.capacity_exceeded")
                .with_description("Direct requests rejected due to capacity limit")
                .build(),
            event_bus_subscriber_pruned: meter
                .u64_counter("actant.event_bus.subscriber.pruned")
                .with_description("Event bus subscribers pruned due to slow consumption")
                .build(),
            event_bus_publish_timeout: meter
                .u64_counter("actant.event_bus.publish.timeout")
                .with_description("Event bus publish attempts that timed out")
                .build(),
            event_bus_dropped_events: meter
                .u64_counter("actant.event_bus.events.dropped")
                .with_description("Event bus events dropped due to full channel")
                .build(),

            // -- 任务转发计数器 --
            task_forward_succeeded: meter
                .u64_counter("actant.tasks.forward.succeeded")
                .with_description("Tasks successfully forwarded to remote nodes")
                .build(),
            task_forward_failed: meter
                .u64_counter("actant.tasks.forward.failed")
                .with_description("Task forwarding attempts that failed")
                .build(),
            task_forward_fallback_local: meter
                .u64_counter("actant.tasks.forward.fallback_local")
                .with_description("Tasks that fell back to local execution after forwarding failed")
                .build(),
            task_forward_reroute: meter
                .u64_counter("actant.tasks.forward.reroute")
                .with_description("Tasks re-routed to a different node")
                .build(),

            // -- UpDownCounter --
            running_tasks: meter
                .i64_up_down_counter("actant.tasks.running")
                .with_description("Currently running tasks")
                .build(),
            active_workflows: meter
                .i64_up_down_counter("actant.workflows.active")
                .with_description("Currently active workflows")
                .build(),
            active_actors: meter
                .i64_up_down_counter("actant.actors.active")
                .with_description("Currently active actors")
                .build(),
            connected_peers: meter
                .i64_up_down_counter("actant.network.connected_peers")
                .with_description("Connected peer count")
                .build(),
            cancelled_tasks_pending: meter
                .i64_up_down_counter("actant.tasks.cancelled_pending")
                .with_description(
                    "Currently pending cancelled tasks (registered but not yet resolved)",
                )
                .build(),

            // -- 直方图 --
            task_duration_ms: meter
                .u64_histogram("actant.tasks.duration_ms")
                .with_description("Task duration in ms")
                .build(),
            cancel_latency_ms: meter
                .u64_histogram("actant.tasks.cancel_latency_ms")
                .with_description("Time from cancel request to task termination in ms")
                .build(),
            workflow_duration_ms: meter
                .u64_histogram("actant.workflows.duration_ms")
                .with_description("Workflow duration in ms")
                .build(),
            scheduling_latency_ms: meter
                .u64_histogram("actant.scheduler.latency_ms")
                .with_description("Scheduling latency in ms")
                .build(),
            payload_serialize_ms: meter
                .u64_histogram("actant.payload.serialize_ms")
                .with_description("Payload serialization latency in ms")
                .build(),
            payload_deserialize_ms: meter
                .u64_histogram("actant.payload.deserialize_ms")
                .with_description("Payload deserialization latency in ms")
                .build(),
            python_handler_ms: meter
                .u64_histogram("actant.python.handler_ms")
                .with_description("Python task handler execution latency in ms")
                .build(),
            event_bridge_ms: meter
                .u64_histogram("actant.event_bridge.latency_ms")
                .with_description("Event bridge conversion and callback latency in ms")
                .build(),
            actor_handle_message_ms: meter
                .u64_histogram("actant.actor.handle_message_ms")
                .with_description("Actor handle_message latency in ms")
                .build(),
            actor_save_state_ms: meter
                .u64_histogram("actant.actor.save_state_ms")
                .with_description("Actor save_state latency in ms")
                .build(),
            actor_load_state_ms: meter
                .u64_histogram("actant.actor.load_state_ms")
                .with_description("Actor load_state latency in ms")
                .build(),
            direct_request_ms: meter
                .u64_histogram("actant.network.direct_request_ms")
                .with_description("Direct request round-trip latency in ms")
                .build(),
        }
    }
}

/// 包装为 `Arc`，使 `Instruments` 句柄可从锁中克隆并在不持有锁的情况下使用。
/// OTel instrument 句柄克隆开销很小。
static INSTRUMENTS: Mutex<Option<Arc<Instruments>>> = Mutex::new(None);
static REGISTRY: Mutex<Option<Arc<Registry>>> = Mutex::new(None);

fn instruments() -> Arc<Instruments> {
    let guard = INSTRUMENTS.lock();
    match guard.clone() {
        Some(inst) => inst,
        None => {
            // 兜底：从全局 meter 创建（若未调用 init() 则为 no-op）。
            drop(guard);
            let meter = opentelemetry::global::meter("actant");
            let inst = Arc::new(Instruments::from_meter(&meter));
            *INSTRUMENTS.lock() = Some(inst.clone());
            inst
        }
    }
}

/// 初始化带 Prometheus exporter 的 OpenTelemetry 指标管道。
///
/// 由 `PyRuntimeCore::start()` 调用。可多次调用 — 每次调用替换之前的管道
/// （用于在一个进程中创建多个 `_Node` 实例的测试）。
///
/// 返回 `Err` 而非 panic，使调用方能在指标管道无法构建时以适当的错误类型
/// 上报（映射为 `ActantError::Metrics` → `MetricsError`）。
pub fn init() -> Result<(), crate::common::ActantError> {
    let registry = Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .map_err(|e| {
            crate::common::ActantError::Metrics(format!("failed to build Prometheus exporter: {e}"))
        })?;

    let provider = SdkMeterProvider::builder().with_reader(exporter).build();

    opentelemetry::global::set_meter_provider(provider);

    // 从已配置的 provider 创建所有 instrument。
    let meter = opentelemetry::global::meter("actant");
    *INSTRUMENTS.lock() = Some(Arc::new(Instruments::from_meter(&meter)));
    *REGISTRY.lock() = Some(Arc::new(registry));
    Ok(())
}

/// 生成所有已注册指标的 Prometheus 文本展示格式。
///
/// 若未调用 `init()`，返回空字符串。
pub fn prometheus_text() -> String {
    let guard = REGISTRY.lock();
    match guard.as_ref() {
        Some(registry) => {
            let encoder = TextEncoder::new();
            let metric_families = registry.gather();
            encoder
                .encode_to_string(&metric_families)
                .unwrap_or_default()
        }
        None => String::new(),
    }
}

pub fn inc_tasks_submitted() {
    instruments().tasks_submitted.add(1, &[]);
}

pub fn inc_tasks_completed() {
    instruments().tasks_completed.add(1, &[]);
}

pub fn inc_tasks_failed() {
    instruments().tasks_failed.add(1, &[]);
}

pub fn inc_tasks_timeout() {
    instruments().tasks_timeout.add(1, &[]);
}

pub fn inc_tasks_retried() {
    instruments().tasks_retried.add(1, &[]);
}

pub fn inc_tasks_cancelled() {
    instruments().tasks_cancelled.add(1, &[]);
}

pub fn inc_workflows_submitted() {
    instruments().workflows_submitted.add(1, &[]);
}

pub fn inc_workflows_completed() {
    instruments().workflows_completed.add(1, &[]);
}

pub fn inc_workflows_failed() {
    instruments().workflows_failed.add(1, &[]);
}

pub fn inc_workflow_timeouts() {
    instruments().workflow_timeouts.add(1, &[]);
}

/// 因持久化数据损坏而在恢复期间被整体丢弃的 workflow 计数。
pub fn inc_workflows_recovered_corrupt() {
    instruments().workflows_recovered_corrupt.add(1, &[]);
}

pub fn inc_retry_scheduled() {
    instruments().retry_scheduled.add(1, &[]);
}

pub fn inc_gossip_updates_sent() {
    instruments().gossip_updates_sent.add(1, &[]);
}

pub fn inc_gossip_updates_received() {
    instruments().gossip_updates_received.add(1, &[]);
}

pub fn inc_gossip_updates_dropped() {
    instruments().gossip_updates_dropped.add(1, &[]);
}

pub fn inc_heartbeats_sent() {
    instruments().heartbeats_sent.add(1, &[]);
}

pub fn inc_failover_claims() {
    instruments().failover_claims.add(1, &[]);
}

pub fn inc_failover_reschedules() {
    instruments().failover_reschedules.add(1, &[]);
}

pub fn inc_actors_spawned() {
    instruments().actors_spawned.add(1, &[]);
}

pub fn inc_actors_stopped() {
    instruments().actors_stopped.add(1, &[]);
}

pub fn inc_actors_failed() {
    instruments().actors_failed.add(1, &[]);
}

pub fn inc_direct_requests_capacity_exceeded() {
    instruments().direct_requests_capacity_exceeded.add(1, &[]);
}

pub fn inc_event_bus_subscriber_pruned() {
    instruments().event_bus_subscriber_pruned.add(1, &[]);
}

pub fn inc_event_bus_publish_timeout() {
    instruments().event_bus_publish_timeout.add(1, &[]);
}

pub fn inc_event_bus_dropped_events() {
    instruments().event_bus_dropped_events.add(1, &[]);
}

pub fn inc_task_forward_succeeded() {
    instruments().task_forward_succeeded.add(1, &[]);
}

pub fn inc_task_forward_failed() {
    instruments().task_forward_failed.add(1, &[]);
}

pub fn inc_task_forward_fallback_local() {
    instruments().task_forward_fallback_local.add(1, &[]);
}

pub fn inc_task_forward_reroute() {
    instruments().task_forward_reroute.add(1, &[]);
}

pub fn inc_running_tasks() {
    instruments().running_tasks.add(1, &[]);
}

pub fn dec_running_tasks() {
    instruments().running_tasks.add(-1, &[]);
}

pub fn inc_active_workflows() {
    instruments().active_workflows.add(1, &[]);
}

pub fn dec_active_workflows() {
    instruments().active_workflows.add(-1, &[]);
}

pub fn inc_active_actors() {
    instruments().active_actors.add(1, &[]);
}

pub fn dec_active_actors() {
    instruments().active_actors.add(-1, &[]);
}

pub fn inc_connected_peers() {
    instruments().connected_peers.add(1, &[]);
}

pub fn dec_connected_peers() {
    instruments().connected_peers.add(-1, &[]);
}

pub fn inc_cancelled_tasks_pending() {
    instruments().cancelled_tasks_pending.add(1, &[]);
}

pub fn dec_cancelled_tasks_pending() {
    instruments().cancelled_tasks_pending.add(-1, &[]);
}

pub fn dec_cancelled_tasks_pending_by(n: i64) {
    instruments().cancelled_tasks_pending.add(-n, &[]);
}

pub fn observe_task_duration_ms(value: u64) {
    instruments().task_duration_ms.record(value, &[]);
}

pub fn observe_cancel_latency_ms(value: u64) {
    instruments().cancel_latency_ms.record(value, &[]);
}

pub fn observe_workflow_duration_ms(value: u64) {
    instruments().workflow_duration_ms.record(value, &[]);
}

pub fn observe_scheduling_latency_ms(value: u64) {
    instruments().scheduling_latency_ms.record(value, &[]);
}

pub fn observe_payload_serialize_ms(value: u64) {
    instruments().payload_serialize_ms.record(value, &[]);
}

pub fn observe_payload_deserialize_ms(value: u64) {
    instruments().payload_deserialize_ms.record(value, &[]);
}

pub fn observe_python_handler_ms(value: u64) {
    instruments().python_handler_ms.record(value, &[]);
}

pub fn observe_event_bridge_ms(value: u64) {
    instruments().event_bridge_ms.record(value, &[]);
}

pub fn observe_actor_handle_message_ms(value: u64) {
    instruments().actor_handle_message_ms.record(value, &[]);
}

pub fn observe_actor_save_state_ms(value: u64) {
    instruments().actor_save_state_ms.record(value, &[]);
}

pub fn observe_actor_load_state_ms(value: u64) {
    instruments().actor_load_state_ms.record(value, &[]);
}

pub fn observe_direct_request_ms(value: u64) {
    instruments().direct_request_ms.record(value, &[]);
}

#[cfg(test)]
#[path = "../tests/rust/unit/metrics.rs"]
mod tests;
