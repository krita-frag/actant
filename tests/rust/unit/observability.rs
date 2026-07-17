//! Unit tests extracted from `src/observability.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;

#[test]
fn init_and_shutdown_noop_without_env() {
    // 默认无环境变量时，init / shutdown 不应 panic。
    init();
    shutdown();
}

#[test]
fn init_tracing_with_actant_tracing_env() {
    // 设置 ACTANT_TRACING=1，验证 init_tracing 不 panic。
    // 注意：tracing global subscriber 只能设置一次，因此本测试必须在其他
    // 可能设置 global subscriber 的测试之前运行，或依赖 has_been_set 检查。
    unsafe {
        std::env::set_var("ACTANT_TRACING", "1");
    }
    init_tracing();
    unsafe {
        std::env::remove_var("ACTANT_TRACING");
    }
}

#[test]
fn init_console_subscriber_without_feature_is_noop() {
    // 未启用 tokio-console feature 时，无论环境变量如何都应为空操作。
    unsafe {
        std::env::set_var("ACTANT_TOKIO_CONSOLE", "1");
    }
    init_console_subscriber();
    unsafe {
        std::env::remove_var("ACTANT_TOKIO_CONSOLE");
    }
}
