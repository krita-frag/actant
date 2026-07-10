# Actant

> 基于 Actor 模型的跨平台通用分布式任务编排引擎

> ⚠️ **实验性项目** — API 尚未冻结，**1.0.0 前不保证向后兼容**。每次更新都可能造成破坏性变更，请不要用于生产环境。版本号 0.x 的任何升级都应视为潜在破坏性变更，需查阅变更日志并相应调整调用方代码。

Actant 采用 **Rust + iroh** 构建核心运行时，通过 **PyO3** 暴露给 Python 用户。P2P 对等节点混合架构，每个节点同时具备编排、执行、Actor 管理、持久化完整能力。

## 架构

四盒（Actor、State、Effect/Capability、Iroh）+ ERH（Effect-Resource-Handler）扩展层。

| 层级 | 位置 | 职责 | 边界约束 |
|------|------|------|----------|
| **第 1 层** Rust 核心 | `src/`（除 `src/py/`） | Actor 运行时、DAG 协议、调度、持久化、网络、Capability/Effect、事件总线；载荷为不透明 `Vec<u8>` | **不感知任何 Python 概念** |
| **PyO3 边界** | `src/py/` | Rust 与 Python 的唯一通道 | 封装 Rust 原语为 Python 对象 |
| **第 2 层** Python 封装 | `actant/` | `Runtime` / `Layer` / effect 原语（`ask`/`perform`/`emit`）、capability 声明、异常镜像 | 通过 `cloudpickle` 序列化任务 payload 为字节后交给 Rust，Rust 不解析其语义 |
| **第 3 层** 用户扩展 | 仓库外 | 业务路由、定时任务、资源管理、监控面板、DSL 风格变体、工作流模板 | 由用户自行实现 |

**核心原则**：简单是可靠的前提。新功能引入前必须能回答"为什么放这一层"。
- 判定：策略 → 第 3 层；能用现有原语组合实现 → 不新增；只对部分用户有用 → 第 3 层。
- 绕过路径：用户可直接基于 `actant.actant`（PyO3 导出）写自己的封装，第 2 层**不是必经之路**。

## 目录结构

```
actant/
├── src/                          # Rust 核心（第 1 层）
│   ├── common/                   # 共享类型与协议：config、error、model、payload、serialization、wire
│   ├── runtime/                  # 运行时四盒
│   │   ├── actor.rs              # Actor 模型统一执行引擎：Actor trait、ActorSystem、监督、邮箱、持久化
│   │   ├── capability.rs         # ERH 核心：Capability、Handler、Layer、Runtime、capability_registry!
│   │   ├── capability/           # Capability 子模块
│   │   │   ├── actor.rs          # Actor 相关 capability（ActorMessaging/Supervision/Lifecycle）codec
│   │   │   ├── builtins.rs       # 内置 capability 注册：register_defaults、StoreHandler、ExecuteHandler
│   │   │   └── gossip.rs         # Capability 元信息 Gossip 同步：CapabilityGossipActor
│   │   ├── context.rs            # ActantRuntime 上下文：CapabilityRuntime + ActorSystem + State + Iroh
│   │   ├── event_bus.rs          # 内部异步事件总线（同文件测试）
│   │   ├── builder.rs            # RuntimeBuilder：组装四盒
│   │   ├── network.rs            # iroh 网络、发现、直连协议
│   │   ├── dispatcher.rs         # TaskDispatcher / TaskRegistry / CancelFlag
│   │   ├── state.rs              # 统一持久化：Store、HLC、Checkpoint、WAL
│   │   ├── state/                # State 子模块
│   │   │   ├── crdt.rs           # CRDT 状态合并
│   │   │   └── event_log.rs      # 事件日志与序号恢复
│   │   └── workflow/             # 工作流运行时
│   │       ├── actor.rs          # WorkflowActor / SchedulerActor / FailoverActor / DagGossipActor
│   │       ├── dag.rs            # DAG 数据结构与拓扑计算
│   │       ├── failover.rs       # 故障转移：心跳、租约、任务回收
│   │       ├── gossip.rs         # DAG 状态 Gossip 同步
│   │       ├── messaging.rs      # 工作流消息类型与 Actor 间协议
│   │       ├── orchestrator.rs   # 编排器：DAG 提交、任务就绪计算、完成处理
│   │       ├── runtime.rs        # WorkflowRuntime：装配 Actor 与调度循环
│   │       └── scheduler.rs      # 任务调度器抽象与 priority/fifo 实现
│   ├── py/                       # PyO3 绑定（唯一边界）
│   │   ├── runtime.rs            # _RuntimeCore / _CapabilityRuntime
│   │   ├── capability.rs         # capability PyO3 桥
│   │   ├── actor.rs / actor_ops.rs  # Actor 系统绑定
│   │   ├── handler.rs            # chain_python_handler：Python handler → Rust
│   │   ├── config.rs             # NetworkConfig / RetryPolicy / ActantConfig
│   │   ├── types.rs              # awaitable/cancel/event/registry 合并模块
│   │   ├── error.rs              # ActantError 镜像
│   │   ├── gil_thread.rs         # GIL 管理
│   │   └── mod.rs
│   ├── metrics.rs                # Rust 侧指标
│   ├── observability.rs          # tracing 初始化与日志回调桥
│   ├── test_support.rs           # 测试辅助（仅 cfg(test)）
│   └── lib.rs
├── actant/                       # Python 封装（第 2 层）
│   ├── __init__.py               # 顶层 re-export：Runtime / Layer / ask/perform/emit / capability / ctx 类型
│   ├── _runtime.py               # Runtime + Layer + effect dispatcher + 默认 handler（LocalRouter/FifoScheduler/NoRetryPolicy）
│   ├── _effects.py               # ask / perform / emit / effect / impossible
│   ├── capabilities.py           # 内置 13 capability 声明、ctx dataclass、Handler Protocol
│   ├── exceptions.py             # ActantError 层级，kind 镜像 Rust
│   ├── cli.py                    # `actant worker` CLI 入口
│   ├── actant.pyi                # PyO3 模块类型存根
│   └── py.typed
├── examples/                     # 可运行示例
│   ├── quickstart.py             # ERH 全流程：Runtime/ask/perform/emit/impossible
│   ├── custom_capability.py      # 自定义 capability、handler 链组合、Protocol 约定
│   └── github_analyzer/          # 大型示例：真实 GitHub issues/PRs 分析流水线
│       ├── __init__.py / __main__.py
│       ├── github_client.py      # urllib 真实 HTTP + 本地缓存
│       ├── models.py             # IssueRecord / RepoStats / AnalysisReport
│       ├── handlers.py           # 10 个 capability handler（路由/调度/重试/限流/拉取/聚合/存储/指标/审计）
│       └── pipeline.py           # 编排：限流→重试拉取→路由→优先级调度→聚合→持久化
├── benches/                      # Rust 基准测试（criterion）
├── tests/                        # 测试套件（unit / integration / e2e）
├── Cargo.toml                    # Rust 依赖
└── pyproject.toml                # Python 项目配置
```

## Worker 运行模型

Actant 是 P2P 对等混合架构：每个节点同时是编排器与执行器，启动一个 `Runtime` 即在该节点上自动启动一个 `Worker`（含 `SchedulerActor`，负责并发槽位、任务超时、优雅 drain）。与 ray/prefect/celery 等中心化系统不同，**无需连接中心服务器**——节点通过 iroh P2P 自动发现对端。

`Runtime.with_defaults()` 注册内置 handler（本地路由 / FIFO 调度 / 无重试）并以默认配置构造 `_RuntimeCore`，Worker 随之启动。`rt.serve()` 在 tokio 后台 spawn worker 守护循环（订阅 P2P topic + 任务执行循环），非阻塞——调用方线程可继续执行编排逻辑。开发期 P2P 发现默认走 `local` preset（本地网络自动发现对端节点），无需手动配置 bootstrap。

### Worker 可调参数

Worker 行为由 `actant.actant._ActantConfig` 控制。当前高层 `Runtime` 使用默认值；需要调优时通过 PyO3 直建路径传入自定义 config（高层 API 暂未暴露 config 入口，属 P7 待同步项）：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `max_concurrent_tasks` | CPU 核数 | 单节点最大并发任务数 |
| `default_task_timeout_ms` | 30000 | 任务默认超时（毫秒） |
| `drain_timeout_secs` | 30 | 退出时等待在途任务的最长时间 |
| `remote_fallback_delay_ms` | 500 | 本地无法执行的任务重新入队前的延迟 |
| `scheduler` | `"priority"` | 调度器类型（`"priority"` / `"fifo"`） |

任务超时、重试、远程回退由 Worker 与 `FailoverActor`、`SchedulerActor` 协同处理；Python 层通过 `Execute` capability 覆盖执行行为，通过 `RetryPolicy` 覆盖重试决策。`WorkerError` 异常镜像 Rust 侧 Worker 运行时错误。

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
- **生产代码禁止 `unwrap()`/`expect()`**：用 `?` 传播错误；Mutex 优先使用 `parking_lot::Mutex`（无 poison）；PyO3 `#[new]` 和 fallible 方法返回 `PyResult`。
- 异步测试用 `#[tokio::test]`，单元测试放在同文件 `#[cfg(test)] mod tests` 中。

### Python
- 所有公共 API 必须有类型注解，使用 `from __future__ import annotations`。
- 私有实现使用 `_` 前缀。
- 未经用户明确要求不修改公共 API。
- 避免延迟导入，除非为打破循环依赖。
- 异常层级镜像 Rust `ActantError`，通过 `raise_for_kind()` / `raise_for_state()` 抛出。
- **禁止裸 `except Exception` 吞错误**：`emit` 等批量回调允许捕获但须提供可配置的错误策略（`"log"` / `"raise"` / `"collect"`）；`Runtime.stop()` 等关键路径捕获异常后必须向上传播。
- 资源对象（文件句柄、连接）须实现 `__enter__`/`__exit__` 上下文管理器或使用 `try/finally` 确保释放。

## 测试

- 每次改动后运行受影响测试：`uv run pytest tests/<module>/`
- 测试必须确定性；外部依赖需 mock。
- 端到端测试用 `threading.Event` 做就绪信号。
- Rust 基准测试：`cargo bench --bench <name>`
