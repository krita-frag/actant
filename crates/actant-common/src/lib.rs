//! Actant 共享类型层：协议、ID、配置、wire message 与错误类型。
//!
//! 依赖图的根，不依赖任何其他 actant crate。全部类型位于 [`common`
//! ](common) 模块下——模块路径与拆分前 `actant::common::*` 一致：
//! `actant_common::common::{backoff, config, error, model, payload,
//! serialization, wire}`。

pub mod common;
