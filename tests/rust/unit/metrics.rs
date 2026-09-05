//! Unit tests extracted from `src/metrics.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;

// metrics 模块依赖全局状态（opentelemetry global meter provider + 静态
// INSTRUMENTS/REGISTRY）。并行测试会因全局 set_meter_provider 的竞争
// 产生不确定行为，因此用一个进程级 Mutex 将所有 metrics 测试串行化。
static METRICS_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// 获取串行守卫；守卫持有直到测试结束，确保 init() / prometheus_text()
/// 的全局状态不被并发修改。
fn lock() -> std::sync::MutexGuard<'static, ()> {
    METRICS_TEST_GUARD.lock().unwrap_or_else(|e| e.into_inner())
}

#[test]
fn init_returns_ok() {
    let _g = lock();
    // init() 必须返回 Ok，而非 panic（这是 P0 修复的核心契约）。
    let result = init();
    assert!(result.is_ok(), "init() should return Ok, got: {:?}", result);
}

#[test]
fn init_is_idempotent() {
    let _g = lock();
    // 文档约定：init() 可多次调用（用于多 _Node 实例的测试场景）。
    assert!(init().is_ok(), "first init() should succeed");
    assert!(init().is_ok(), "second init() should succeed");
}

#[test]
fn prometheus_text_contains_counter_after_inc() {
    let _g = lock();
    init().expect("init for test");
    inc_tasks_submitted();
    inc_tasks_submitted();
    inc_tasks_completed();

    let text = prometheus_text();
    assert!(
        !text.is_empty(),
        "prometheus_text should be non-empty after init"
    );
    assert!(
        text.contains("actant_tasks_submitted"),
        "prometheus_text should contain actant_tasks_submitted counter, got: {text}"
    );
    assert!(
        text.contains("actant_tasks_completed"),
        "prometheus_text should contain actant_tasks_completed counter, got: {text}"
    );
}

#[test]
fn up_down_counter_reflected_in_output() {
    let _g = lock();
    init().expect("init for test");
    inc_running_tasks();
    inc_running_tasks();
    dec_running_tasks();
    inc_active_workflows();
    inc_connected_peers();
    dec_connected_peers();

    let text = prometheus_text();
    assert!(
        text.contains("actant_tasks_running"),
        "prometheus_text should contain actant_tasks_running gauge, got: {text}"
    );
    assert!(
        text.contains("actant_workflows_active"),
        "prometheus_text should contain actant_workflows_active gauge, got: {text}"
    );
}

#[test]
fn histogram_observe_reflected_in_output() {
    let _g = lock();
    init().expect("init for test");
    observe_task_duration_ms(150);
    observe_task_duration_ms(300);
    observe_workflow_duration_ms(2000);
    observe_scheduling_latency_ms(5);

    let text = prometheus_text();
    assert!(
        text.contains("actant_tasks_duration_ms"),
        "prometheus_text should contain actant_tasks_duration_ms histogram, got: {text}"
    );
    assert!(
        text.contains("actant_workflows_duration_ms"),
        "prometheus_text should contain actant_workflows_duration_ms histogram, got: {text}"
    );
    assert!(
        text.contains("actant_scheduler_latency_ms"),
        "prometheus_text should contain actant_scheduler_latency_ms histogram, got: {text}"
    );
}

#[test]
fn all_counter_helpers_do_not_panic() {
    let _g = lock();
    init().expect("init for test");
    // 烟雾测试：所有公共计数器辅助函数应可无 panic 调用。
    inc_tasks_submitted();
    inc_tasks_completed();
    inc_tasks_failed();
    inc_tasks_timeout();
    inc_tasks_retried();
    inc_workflows_submitted();
    inc_workflows_completed();
    inc_workflows_failed();
    inc_workflow_timeouts();
    inc_retry_scheduled();
    inc_gossip_updates_sent();
    inc_gossip_updates_received();
    inc_gossip_updates_dropped();
    inc_heartbeats_sent();
    inc_failover_claims();
    inc_failover_reschedules();
    inc_actors_spawned();
    inc_actors_stopped();
    inc_actors_failed();
    inc_direct_requests_capacity_exceeded();
    inc_event_bus_publish_dropped();
    inc_task_forward_succeeded();
    inc_task_forward_failed();
    inc_task_forward_fallback_local();
    inc_task_forward_reroute();
    // 若执行到此行，所有辅助函数均未 panic。
}

#[test]
fn all_up_down_counter_helpers_do_not_panic() {
    let _g = lock();
    init().expect("init for test");
    inc_running_tasks();
    dec_running_tasks();
    inc_active_workflows();
    dec_active_workflows();
    inc_active_actors();
    dec_active_actors();
    inc_connected_peers();
    dec_connected_peers();
    // 若执行到此行，所有 UpDownCounter 辅助函数均未 panic。
}
