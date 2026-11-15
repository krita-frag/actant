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
///
/// 两个环境变量同时设置时，console subscriber 需要在同一个全局 dispatcher 上
/// 与 fmt layer 组合；分开初始化会让先安装的 subscriber 独占 dispatcher，
/// 后一个环境变量静默失效。
pub fn init() {
    #[cfg(feature = "tokio-console")]
    {
        if console_requested() {
            init_console_subscriber();
        } else {
            init_tracing();
        }
    }
    #[cfg(not(feature = "tokio-console"))]
    init_tracing();
}

fn tracing_requested() -> bool {
    std::env::var_os("ACTANT_TRACING").is_some()
}

#[cfg(feature = "tokio-console")]
fn console_requested() -> bool {
    std::env::var_os("ACTANT_TOKIO_CONSOLE").is_some()
}

fn init_tracing() {
    if !tracing_requested() {
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
    if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
        tracing::debug!(error = %e, "tracing subscriber already installed");
    }
}

#[cfg(feature = "tokio-console")]
fn init_console_subscriber() {
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
    if let Err(e) = tracing::subscriber::set_global_default(layered) {
        tracing::debug!(error = %e, "tracing subscriber already installed");
    }
}

#[cfg(test)]
#[path = "../../../tests/rust/unit/observability.rs"]
mod tests;
