//! # Actant PyO3 绑定壳
//!
//! Python 与 Actant Rust 核心的唯一边界：把 `actant-core` 的原语封装为
//! Python 对象（`_RuntimeCore` / `_ActantConfig` / effect 桥 / 异常镜像）。
//! cdylib 经 maturin 映射为 `actant.actant`，`import actant` 即加载本 crate。
//!
//! 模块布局与边界约束见 `docs/architecture/overview.md`。

pub use actant_core::common;
pub use actant_core::metrics;
pub use actant_core::observability;
pub use actant_core::runtime;

#[cfg(feature = "python")]
pub mod py;
