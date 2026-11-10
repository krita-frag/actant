# Actant

> 基于 Actor 模型的跨平台通用分布式任务编排引擎

> ⚠️ **实验性项目** — API 尚未冻结，1.0.0 前不保证向后兼容。0.x 的任何升级都可能破坏调用方。

本文件只保留 agent 必须遵守的硬规则与入口。完整的架构、API 与运维说明见 [`docs/`](docs/index.md)。

## 先读什么

- 架构总览与分层：[docs/architecture/overview.md](docs/architecture/overview.md)
- ERH 扩展模型：[docs/architecture/erh.md](docs/architecture/erh.md)
- Worker 与进程隔离：[docs/architecture/worker-model.md](docs/architecture/worker-model.md)
- 框架契约与 Rust 注入缝：[docs/architecture/framework-contract.md](docs/architecture/framework-contract.md)
- 全部扩展点、执行域与验收：[docs/reference/extension-points.md](docs/reference/extension-points.md)；维护流程：[docs/contributing/extensions.md](docs/contributing/extensions.md)
- 任务 / 工作流 / 能力扩展：[docs/guide/](docs/guide/)
- 代码约定全文：[docs/contributing/code-style.md](docs/contributing/code-style.md)
- 文档该写在哪：[docs/contributing/doc-map.md](docs/contributing/doc-map.md)

## 架构硬边界

| 层级 | 位置 | 约束 |
| --- | --- | --- |
| Rust 核心 | `crates/actant-common` + `crates/actant-core` | 不感知任何 Python 概念；载荷是不透明 `Vec<u8>` |
| PyO3 边界 | `src/`（`src/py/`） | Rust 与 Python 的唯一通道 |
| Python 封装 | `actant/` | cloudpickle 序列化 payload，Rust 不解析语义 |
| 用户扩展 | 仓库外 | 业务策略、路由、定时任务等由用户实现 |

依赖方向单向：`common ← core ← 门面`。`crates/actant-core` 不得引用 PyO3；`crates/actant-common` 不得依赖 `actant-core`。

**核心原则**：简单是可靠的前提。新功能引入前必须能回答“为什么放这一层”：策略 → 第 3 层；能用现有原语组合 → 不新增；只对部分用户有用 → 第 3 层。公开 API 只收缩不净扩张。

## 硬性规则

- 生产代码禁止桩函数、占位实现、模拟数据、`pass`、`NotImplementedError`，不吞错误。
- Rust 生产代码禁止 `unwrap()` / `expect()`；用 `?` 传播错误。Mutex 优先 `parking_lot::Mutex`。
- 禁止 `let _ =` 静默丢弃错误；必须 `?`、记录日志或 `if let Err(e)` 处理，确无需处理时加注释说明。
- Python 禁止裸 `except Exception` 吞错误。批量回调允许捕获但须提供可配置策略（`"log"` / `"raise"` / `"collect"`）。
- 注释只说明“当前实现是什么”和“为何如此设计”，禁止记录变更历史、个人备忘或与逻辑无关的内容。
- 未经用户明确要求不修改公共 API。
- 仅删除因本次改动而失效的 import / 变量 / 函数；不“顺手改进”相邻代码。

## 代码约定

### Rust

- 遵循 `cargo clippy` 与 `cargo fmt`，不抑制告警。
- 日志用 `tracing`，不用 `println!`。
- 错误用 `Result<T, ActantError>` + `thiserror`；库代码不 panic。
- PyO3 `#[new]` 与 fallible 方法返回 `PyResult`。
- 异步测试用 `#[tokio::test]`；单元测试放同文件 `#[cfg(test)] mod tests`。

### Python

- 公共 API 必须有类型注解，使用 `from __future__ import annotations`。
- 私有实现用 `_` 前缀。
- 异常层级镜像 Rust `ActantError`，经 `raise_for_kind()` / `raise_for_state()` 抛出。
- 资源对象实现 `__enter__` / `__exit__` 或使用 `try/finally`。
- 避免延迟导入，除非为打破循环依赖。

## 测试

- 改动后运行受影响测试：`uv run pytest tests/python/<module>/`。
- 测试必须确定性；外部依赖需 mock。
- 端到端测试用 `threading.Event` 做就绪信号。
- 接线类改动不看「单测全绿」：必须有真实驱动生产路径的 e2e，且不直接调用被接线函数。
- 新增或保留扩展点必须登记 `actant/extension_points.json`（含 `resolution` / `hot_path`），并交齐「清单登记 + 能证伪的门禁 + 验收测试」。热路径点必须 `build_time`，热路径白名单文件不得出现 capability 动态分派调用或 GIL 附加；只禁调用形态，不禁类型名与网络帧序列化。门禁见 `tests/python/unit/test_extension_contract.py`，流程见 [扩展点维护](docs/contributing/extensions.md)。
- 全量命令见 [docs/contributing/testing.md](docs/contributing/testing.md)。

## 常用命令

```bash
uv run maturin develop
uv run pytest tests/python/ -v
cargo test --workspace
cargo test -p actant-core -p actant-common --no-default-features
cargo clippy --all-targets -- -D warnings
cargo fmt --check
ruff check actant tests/python
mypy actant
uv run mkdocs build --strict
```

## 文档维护

- **一个事实只有一个权威源**：API 在 docstring / `.pyi` / `///`，架构在 `docs/architecture/`，行为承诺在测试与 `docs/operations/sla-baseline.md`。
- 改 API 只改 docstring / `.pyi`，参考页由生成器更新，不手写签名。
- 同一事实不要在两处复述；第二处只放链接。
- 内部计划与一次性分析留在 `plans/` 或 `.trae/`，不进入发布站点。
