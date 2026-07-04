//! 可观测性入口：环境变量驱动的 tracing / pprof / tokio-console 初始化。
//!
//! 默认不安装任何 subscriber 或 profiler，避免干扰 pyo3-log 桥接与 Python
//! 侧日志配置。在 `_Node.start()` 之前调用 [`init()`]，在 shutdown 时调用
//! [`shutdown()`] 写出 profiling 工件。
//!
//! # 环境变量
//!
//! - `ACTANT_TRACING=1` — 安装 `tracing-subscriber`（带 `env-filter`）。
//! - `ACTANT_TOKIO_CONSOLE=1` — 启用 `console-subscriber`（需 `tokio-console` feature）。
//! - `ACTANT_PPROF=/path/to/flamegraph.svg` — 启动 pprof CPU profiler，shutdown
//!   时写出火焰图（需 `pprof` feature）。

#[cfg(feature = "tokio-console")]
use tracing_subscriber::prelude::*;

#[cfg(feature = "pprof")]
mod pprof_support {
    use std::sync::Mutex;

    pub(super) struct PprofState {
        pub(super) guard: pprof::ProfilerGuard<'static>,
        pub(super) path: String,
    }

    pub(super) static PPROF_GUARD: Mutex<Option<PprofState>> = Mutex::new(None);
}

/// 初始化可观测性子系统。幂等：重复调用无额外副作用。
pub fn init() {
    init_tracing();
    init_console_subscriber();
    init_pprof();
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

    let _ = tracing::subscriber::set_global_default(layered);
}

#[cfg(not(feature = "tokio-console"))]
fn init_console_subscriber() {
    // feature 未启用时忽略环境变量；纯 tracing 已在 init_tracing 中处理。
}

#[cfg(feature = "pprof")]
fn init_pprof() {
    use pprof_support::*;

    let path = match std::env::var("ACTANT_PPROF") {
        Ok(p) if !p.is_empty() => p,
        _ => return,
    };

    let guard = match pprof::ProfilerGuardBuilder::default()
        .frequency(100)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
    {
        Ok(g) => g,
        Err(e) => {
            tracing::warn!(error = %e, "failed to start pprof CPU profiler");
            return;
        }
    };

    let state = PprofState { guard, path };
    if let Ok(mut slot) = PPROF_GUARD.lock() {
        *slot = Some(state);
    }
}

#[cfg(not(feature = "pprof"))]
fn init_pprof() {
    if std::env::var_os("ACTANT_PPROF").is_some() {
        tracing::warn!(
            "ACTANT_PPROF is set but the 'pprof' cargo feature is not enabled; ignoring"
        );
    }
}

/// 关闭可观测性子系统并写出 profiling 工件。
pub fn shutdown() {
    #[cfg(feature = "pprof")]
    {
        use pprof_support::*;

        let mut slot = match PPROF_GUARD.lock() {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "pprof guard mutex poisoned; skipping shutdown");
                return;
            }
        };

        if let Some(state) = slot.take() {
            match state.guard.report().build() {
                Ok(report) => {
                    let parent = std::path::Path::new(&state.path)
                        .parent()
                        .unwrap_or_else(|| std::path::Path::new(""));
                    if let Err(e) = std::fs::create_dir_all(parent) {
                        tracing::warn!(
                            error = %e,
                            path = %state.path,
                            "failed to create pprof output directory"
                        );
                        return;
                    }

                    let file = match std::fs::File::create(&state.path) {
                        Ok(f) => f,
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                path = %state.path,
                                "failed to create pprof output file"
                            );
                            return;
                        }
                    };

                    if let Err(e) = report.flamegraph(file) {
                        tracing::warn!(
                            error = %e,
                            path = %state.path,
                            "failed to write pprof flamegraph"
                        );
                    } else {
                        tracing::info!(path = %state.path, "wrote pprof flamegraph");
                    }
                }
                Err(e) => {
                    tracing::warn!(error = ?e, "failed to build pprof report");
                }
            }
        }
    }
}
