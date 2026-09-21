//! # Actant PyO3 绑定壳
//!
//! Python 与 Actant Rust 核心的唯一边界：把 `actant-core` 的原语封装为
//! Python 对象（`_RuntimeCore` / `_ActantConfig` / effect 桥 / 异常镜像）。
//!
//! ## 边界约束
//!
//! - `actant-core`（框架主体）不依赖 Python 类型或 GIL；本 crate 单向依赖它。
//! - Python handler 只能通过 [`py`] 模块桥接进 capability/dispatcher。
//! - 跨节点消息必须经过 `common::WireEnvelope` 或 payload signing/verification。
//! - cdylib 导出名经 maturin 映射为 `actant.actant`（`pyproject.toml` 的
//!   `module-name`），Python 侧 `import actant` 即加载本 crate。
//!
//! ## workspace 布局
//!
//! | crate | 位置 | 职责 |
//! |-------|------|------|
//! | `actant`（本 crate） | `src/` | PyO3 绑定（`py` 模块），maturin 编译入口 |
//! | `actant-common` | `crates/actant-common` | 共享类型层 |
//! | `actant-core` | `crates/actant-core` | 框架主体 |

pub use actant_core::common;
pub use actant_core::metrics;
pub use actant_core::observability;
pub use actant_core::runtime;

#[cfg(feature = "python")]
pub mod py;
