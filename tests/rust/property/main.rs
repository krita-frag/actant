//! Property-based 测试入口。
//!
//! 将 payload 和 DAG 的 property-based 测试聚合为一个集成测试目标，
//! 便于统一运行：`cargo test --test property`

mod dag;
mod payload;
