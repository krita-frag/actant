//! 可观测性入口：环境变量驱动的 tracing / tokio-console 初始化。
//!
//! 默认不安装任何 subscriber，避免干扰 pyo3-log 桥接与 Python 侧日志配置。
//! 在 `_Node.start()` 之前调用 [`init()`]。
//!
//! # 环境变量
//!
//! - `ACTANT_TRACING=1` — 安装 `tracing-subscriber`（带 `env-filter`）。
//! - `ACTANT_TOKIO_CONSOLE=1` — 启用 `console-subscriber`（需 `tokio-console` feature）。

#[cfg(feature = "tokio-console")]
use tracing_subscriber::prelude::*;

/// 初始化可观测性子系统。幂等：重复调用无额外副作用。
pub fn init() {
    init_tracing();
    init_console_subscriber();
}

fn init_tracing() {
    if std::env::var_os("ACTANT_TRACING").is_none() {
        return;
    }

    // 若环境中已存在 tracing subscriber，避免重复安装导致 panic。
    if tracing::dispatcher::has_been_set() {
        return;
    }

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .finish();

    // set_global_default 在进程生命周期内只能成功一次；后续调用返回 Err 是
    // 已被初始化的正常情况（如多 Runtime 实例依次调用 init_logger）。
    let _ = tracing::subscriber::set_global_default(subscriber);
}

#[cfg(feature = "tokio-console")]
fn init_console_subscriber() {
    if std::env::var_os("ACTANT_TOKIO_CONSOLE").is_none() {
        return;
    }

    if tracing::dispatcher::has_been_set() {
        return;
    }

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .finish();

    let console_layer = console_subscriber::spawn();
    let layered = subscriber.with(console_layer);

    // set_global_default 在进程生命周期内只能成功一次；后续调用返回 Err 是
    // 已被初始化的正常情况（如多 Runtime 实例依次调用 init_logger）。
    let _ = tracing::subscriber::set_global_default(layered);
}

#[cfg(not(feature = "tokio-console"))]
fn init_console_subscriber() {
    // feature 未启用时忽略环境变量；纯 tracing 已在 init_tracing 中处理。
}

#[cfg(test)]
#[path = "../tests/rust/unit/observability.rs"]
mod tests;
