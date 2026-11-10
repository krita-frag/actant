# 代码约定

## 通用

- **注释原则**：说明“当前实现是什么”和“为何如此设计”，不记录变更历史、个人备忘或与逻辑无关的内容。历史由版本控制承担。
- 允许 `TODO` / `FIXME` / `HACK`，简洁说明问题本质与预期方向。
- 生产代码禁止桩函数、占位实现、模拟数据、`pass` 或 `NotImplementedError`，不吞错误。
- 未经明确要求不修改公共 API。

## Rust

- 遵循 `cargo clippy` 与 `cargo fmt`，不抑制告警。
- 所有日志使用 `tracing`，不使用 `println!`。
- 错误处理：`Result<T, ActantError>` + `thiserror` 派生枚举。库代码不 panic。
- 生产代码禁止 `unwrap()` / `expect()`；用 `?` 传播错误。Mutex 优先 `parking_lot::Mutex`。
- PyO3 `#[new]` 与 fallible 方法返回 `PyResult`。
- 禁止 `let _ =` 静默丢弃错误；必须 `?` 传播、记录日志或 `if let Err(e)` 处理。确无需处理时加注释说明。
- 异步测试用 `#[tokio::test]`。

## Python

- 公共 API 必须有类型注解，使用 `from __future__ import annotations`。
- 私有实现使用 `_` 前缀。
- 避免延迟导入，除非为打破循环依赖。
- 异常层级镜像 Rust `ActantError`，通过 `raise_for_kind()` / `raise_for_state()` 抛出。
- 禁止裸 `except Exception` 吞错误。`emit` 等批量回调允许捕获但须提供可配置错误策略（`"log"` / `"raise"` / `"collect"`）；`Runtime.stop()` 等关键路径捕获后必须向上传播。
- 资源对象须实现 `__enter__` / `__exit__` 或使用 `try/finally`。

## 文档

- 公共 API 的 docstring 是 API 语义的唯一权威源；长设计说明放 `docs/architecture/`，docstring 用链接指向。
- 同一事实只在一个页面维护，其他页面只链接。
- 新增/修改公共 API 时同步更新 docstring；参考页由生成器产出。

## 分层

- 策略放第 3 层；能用现有原语组合实现则不新增原语。
- `crates/actant-core` 不感知 Python；`crates/actant-common` 不依赖 `actant-core`。
- 仅对部分用户有用的能力放仓库外。
