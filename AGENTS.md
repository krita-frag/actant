# Actant

> 基于 Actor 模型的跨平台通用分布式任务编排引擎

> ⚠️ **实验性项目** — API 尚未冻结，**1.0.0 前不保证向后兼容**。每次更新都可能造成破坏性变更，请不要用于生产环境。版本号 0.x 的任何升级都应视为潜在破坏性变更，需查阅变更日志并相应调整调用方代码。

Actant 采用 **Rust + iroh** 构建核心运行时，通过 **PyO3** 暴露给 Python 用户。P2P 对等节点混合架构，每个节点同时具备编排、执行、Actor 管理、持久化完整能力。

## 架构

三层架构：Rust 核心原语（第 1 层）→ PyO3 边界（`src/py/`）→ Python 轻量封装（第 2 层）。用户可直接调用 PyO3 导出接口，或完全绕过第 2 层自行扩展（第 3 层）。

| 层级 | 位置 | 职责 | 边界约束 |
|------|------|------|----------|
| **第 1 层** Rust 核心 | `src/`（除 `src/py/`） | DAG 协议、调度、持久化、网络、Actor、事件总线；载荷为不透明 `Vec<u8>` | **不感知任何 Python 概念** |
| **PyO3 边界** | `src/py/` | Rust 与 Python 的唯一通道 | 封装 Rust 原语为 Python 对象 |
| **第 2 层** Python 封装 | `actant/` | `@task` / `@flow`、顶层 API、异常镜像、Python 侧编排循环、CLI | 通过 `cloudpickle` 序列化 Python 可调用对象为字节后交给 Rust，Rust 不解析其语义 |
| **第 3 层** 用户扩展 | 仓库外 | 业务路由、定时任务、资源管理、监控面板、DSL 风格变体、工作流模板 | 由用户自行实现 |

**核心原则**：简单是可靠的前提。新功能引入前必须能回答"为什么放这一层"。
- 判定：策略 → 第 3 层；能用现有原语组合实现 → 不新增；只对部分用户有用 → 第 3 层。
- 绕过路径：用户可直接基于 `actant.actant`（PyO3 导出）写自己的封装，第 2 层**不是必经之路**。

## 目录结构

```
actant/
├── src/                          # Rust 核心（第 1 层）
│   ├── common/                   # 共享类型：TaskId、WorkflowId、RetryPolicy 等
│   ├── orchestrator/             # DAG 编排、调度器、故障转移、gossip
│   ├── worker/                   # Worker 运行时、任务分发器
│   ├── actor/                    # Actor 系统、邮箱、监督
│   ├── network/                  # iroh 网络、发现、协议
│   ├── store/                    # LMDB/Heed 持久化、WAL、检查点、HLC
│   ├── event_bus.rs              # 内部异步事件总线
│   ├── metrics.rs                # Rust 侧指标
│   ├── observability.rs          # Rust 侧 tracing/pprof/console 观测
│   └── py/                       # PyO3 绑定（唯一边界）
├── actant/                       # Python 轻量封装（第 2 层）
│   ├── __init__.py               # 公共 API 导出
│   ├── _api.py                   # 模块级 API
│   ├── _node.py                  # 内部 P2P 节点运行时
│   ├── task.py / flow.py         # Task / Flow / @flow / parallel()
│   ├── result.py                 # AsyncResult / 工作流结果查询
│   ├── _serialization.py         # 载荷编码、Rust 枚举编解码
│   ├── _orchestration.py         # Python 侧编排循环
│   ├── _annotations.py           # Task 注解上下文（批量选项注入）
│   ├── _components.py            # 可组合内部组件接口（扩展点）
│   ├── _dag.py                   # DAG 工具：循环检测与路径格式化
│   ├── _events.py                # 全局事件订阅系统
│   ├── _observability.py         # viztracer 集成
│   ├── router.py / actor.py / supervision.py
│   ├── config.py / exceptions.py / _logging.py
│   ├── cli/                      # CLI 子命令
│   ├── _task_context.py          # 协作式取消
│   ├── actant.pyi                # PyO3 类型存根
│   └── py.typed                  # PEP 561 标记
├── benches/                      # Rust 基准测试（criterion）
├── tests/                        # 测试套件
├── examples/                     # 用法示例
├── Cargo.toml                    # Rust 依赖
└── pyproject.toml                # Python 项目配置
```

## 技术栈

- **Rust**：tokio、iroh、heed/LMDB、rkyv、postcard、PyO3
- **Python**：>=3.10、cloudpickle、prometheus_client

## 常用命令

```bash
uv sync
uv run maturin develop                    # 构建 Rust 核心 + 安装 Python 包

uv run pytest tests/ -v                   # Python 测试
uv run pytest tests/ -n 4 -v              # 并行 Python 测试
cargo nextest run                         # Rust 测试（含计时）
cargo clippy                              # Rust 静态检查
cargo fmt                                 # Rust 格式化
ruff check actant tests                   # Python 静态检查
mypy actant                               # Python 类型检查

cargo bench --bench scheduler              # Rust 基准测试
cargo audit                               # 依赖漏洞扫描

actant worker start                       # 启动 worker 节点
actant worker start --daemon              # 后台守护进程
actant status workflows                   # 列出活跃工作流
```

## 工作准则

1. **编码前先思考**：显式声明假设；不确定时主动询问；歧义时列出所有解释，不要默默选一种；存在更简单方案时主动提出。
2. **简洁优先**：不实现超出需求的内容；若 200 行能精简为 50 行，重写。
3. **精确变更**：不"顺手改进"相邻代码；匹配既有风格；仅删除因你的改动而失效的 import / 变量 / 函数；每一行变更都必须能直接追溯到用户请求。
4. **目标驱动**：用可验证目标替代模糊指令——"修复 Bug" → 编写复现测试，修复，验证测试通过。
5. **生产代码禁止桩函数与 Mock**：不允许桩函数、占位实现、模拟数据、`pass` 或 `NotImplementedError`，不吞掉错误。`actant/` 中的每个函数都必须在生产环境中正确工作。

## 代码约定

### Rust
- 遵循 `cargo clippy` 与 `cargo fmt`，不抑制告警。
- 所有日志使用 `tracing`（不使用 `println!`）。
- 错误处理：使用 `Result<T, ActantError>` + `thiserror` 派生枚举。库代码不 panic。
- 异步测试用 `#[tokio::test]`，单元测试放在同文件 `#[cfg(test)] mod tests` 中。

### Python
- 所有公共 API 必须有类型注解，使用 `from __future__ import annotations`。
- 私有实现使用 `_` 前缀。
- 未经用户明确要求不修改公共 API。
- 避免延迟导入，除非为打破循环依赖。
- 异常层级镜像 Rust `ActantError`，通过 `raise_for_kind()` / `raise_for_state()` 抛出。

## 测试

- 每次改动后运行受影响测试：`uv run pytest tests/<module>/`
- 测试必须确定性；外部依赖需 mock。
- 端到端测试用 `threading.Event` 做就绪信号。
- Rust 基准测试：`cargo bench --bench <name>`

## 按任务速查关键文件

| 任务 | 阅读顺序 |
|------|----------|
| 理解调度模型 | `src/orchestrator/` → `src/worker/` → `actant/_orchestration.py` |
| 理解 DAG 构造 | `src/orchestrator/dag.rs` → `actant/flow.py` → `actant/task.py` |
| 理解 PyO3 边界 | `src/py/runtime.rs` → `actant/_node.py` → `actant/actant.pyi` |
| 理解持久化 | `src/store/` → `src/common/config.rs` |
| 理解网络与发现 | `src/network/` → `actant/config.py`（NetworkConfig） |
| 理解 Actor 系统 | `src/actor/` → `actant/actor.py` → `actant/supervision.py` |
| 添加 Python DSL 语法糖 | `actant/task.py` → `actant/flow.py` → `actant/_api.py` |
| 配置 | `actant/config.py` → `src/common/config.rs` |
| CLI | `actant/cli/` → `actant/_node.py` |

## API 稳定性策略

- **当前 MSRV：Rust 1.75**（声明于 `Cargo.toml` 的 `rust-version`）。
- CI 必须在 MSRV + stable + beta 三个工具链上通过。
- **0.1.0 发布后 Rust 核心 API 进入冻结状态**；新增功能优先在 Python 层实现，仅当涉及调度、持久化、网络等核心运行时再修改 Rust 核心。
- **1.0.0 前所有层级的公共 API 均不保证向后兼容**，破坏性变更通过版本号 0.x 递增体现。
