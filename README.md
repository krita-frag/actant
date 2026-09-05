# Actant

> 基于 Actor 模型的跨平台通用分布式任务编排引擎

Actant 采用 **Rust + iroh** 构建核心运行时，通过 **PyO3** 暴露给 Python 用户。P2P 对等节点混合架构，每个节点同时具备编排、执行、Actor 管理、持久化完整能力。

> ⚠️ **实验性项目** — API 尚未冻结，**1.0.0 前不保证向后兼容**。请勿用于生产环境。

---

## 目录

- [从源码构建](#从源码构建)
- [快速开始](#快速开始)
  - [定义与提交任务](#定义与提交任务)
  - [任务依赖](#任务依赖)
  - [工作流编排](#工作流编排)
- [核心概念](#核心概念)
  - [P2P 发现 Preset](#p2p-发现-preset)
  - [超时与取消最佳实践](#超时与取消最佳实践)
- [启动 Worker 节点](#启动-worker-节点)
- [示例](#示例)
- [开发](#开发)
  - [目录结构](#目录结构)
  - [测试](#测试)
  - [基准测试](#基准测试)
  - [代码质量](#代码质量)
  - [可观测性](#可观测性)

---

## 从源码构建

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
curl -LsSf https://astral.sh/uv/install.sh | sh

git clone https://github.com/actant/actant.git
cd actant
uv sync
uv run maturin develop

uv run python -c "import actant; print(actant.__version__)"
```

---

## 快速开始

### 定义与提交任务

```python
import actant
from actant import Runtime, task

@task
def add(a: int, b: int) -> int:
    return a + b

@task(retries=3, retry_delay_ms=100, timeout_ms=5000)
def fetch(url: str) -> str:
    ...  # 自动重试 3 次，每次间隔 100ms，单次超时 5s

with Runtime.with_defaults() as rt:
    handle = add.submit(2, 3)    # 异步提交，返回 AsyncResult
    print(handle.result())        # 5（阻塞等待结果）

    # 直接调用（同步，不走执行池）
    print(add(2, 3))              # 5
```

### 任务依赖

`AsyncResult` 作为下游任务的参数时自动阻塞等待：

```python
@task
def double(x: int) -> int:
    return x * 2

with Runtime.with_defaults():
    a = add.submit(10)        # 11
    b = double.submit(a)      # 自动等待 a 完成，传入 11，得 22
    print(b.result())          # 22
```

依赖解析递归处理嵌套容器（`list` / `tuple` / `dict`）。

### 工作流编排

```python
from actant import flow

@flow
def pipeline(a: int, b: int) -> int:
    result = add.submit(a, b)
    return double.submit(result).result()

with Runtime.with_defaults():
    print(pipeline(2, 3))  # (2+3)*2 = 10
```

`@flow` 广播生命周期事件（`submitted` → `started` → `completed`/`failed`），可通过 `rt.layer("WorkflowLifecycle").chain(handler)` 订阅。

---

## 核心概念

### P2P 发现 Preset

节点发现策略通过 `_NetworkConfig(preset=...)` 配置，内置 5 种 preset：

| preset      | 用途                                       | 行为                                                                                |
| ----------- | ------------------------------------------ | ----------------------------------------------------------------------------------- |
| `local`     | **默认**。互联网节点的通用模式。           | iroh N0 preset：DNS Pkarr 发现 + relay 兜底，NAT 穿透能力强。                       |
| `none`      | 无自动发现。用于测试 / CI / 单节点部署。   | 仅 loopback，无 DNS Pkarr，无 relay。须通过 `bootstrap_nodes` 显式拨号才能连到对端。 |
| `mdns`      | 仅局域网。                                 | N0 preset 但禁用 relay，仅 mDNS 广播发现同网段节点。                                |
| `dns`       | 仅 DNS endpoint 发现（无 relay）。         | 配合 `dns_origin_domain` 使用自定义 DNS 起源域。                                    |
| `relay`     | 强制启用 iroh relay 中继。                 | 适合严格 NAT 穿透场景，但引入外部中继依赖。                                         |

```python
from actant import Runtime
from actant.actant import _ActantConfig, _NetworkConfig

# 局域网部署：禁用 relay，仅 mDNS
rt = Runtime(_ActantConfig(network=_NetworkConfig(preset="mdns")))

# 测试 / CI：完全离线
rt = Runtime(_ActantConfig(network=_NetworkConfig(preset="none")))

# 自定义 DNS 发现域
rt = Runtime(_ActantConfig(network=_NetworkConfig(
    preset="dns",
    dns_origin_domain="actant.example.com",
)))
```

**环境变量覆盖**：`ACTANT_DISCOVERY=<preset>` 优先于配置中的 `preset`，用于在不动代码的前提下切换发现策略（例如 CI 中强制 `ACTANT_DISCOVERY=none` 避免联网）。

**未知 preset 硬失败**：启动时通过 `DiscoveryMode::validate` 校验名称，未知名称返回 `Config` 错误，无静默回退。自定义发现策略须先在 Rust 侧通过 `Discovery` trait 注册。

### 超时与取消最佳实践

Actant 的超时治理分两层，理解它们的协作方式有助于编写可预测的任务：

**任务级超时（`@task(timeout_ms=...)`）**

- 任务在 **worker 子进程** 中执行（进程级隔离，每进程单任务）。超时由 Rust `ProcessTaskDispatcher` 通过 `tokio::time::timeout` 强制执行：超时后对对应 worker 进程**立即 `kill()`**（`kill_and_replace`，不等取消宽限期），**真正终止任务并释放计算资源**，槽位即时回收并自动拉起替补进程。这是硬超时，无需业务代码配合。
- 因此长任务无需主动轮询取消标志也能被强制回收。若希望在超时前优雅收尾（如释放外部资源、写入中间结果），可在任务内配合协作式检查点：

```python
from actant import task
from actant.task import get_task_context
from actant.exceptions import TaskCancelledError

@task(timeout_ms=5000)
def long_pipeline(items: list) -> list:
    ctx = get_task_context()
    out = []
    for i, x in enumerate(items):
        # 协作检查点：取消时抛出并清理（可选，硬超时仍是兜底）
        if ctx is not None and ctx.is_cancelled():
            raise TaskCancelledError(f"cancelled at item {i}")
        out.append(expensive_step(x))
    return out
```

- 匹配取消消息时协作式退出可以避免强杀，但进程终止始终兜底，语义不因业务代码是否配合而丢失。

**Flow 级超时（`@flow(timeout_ms=...)`）**

- 在主线程上用 `threading.Event` 等待子线程，超时后设置 `cancel_event`。
- 子线程中后续的 `Task.submit` 调用会检查该事件并立即抛出 `ActantTimeoutError`，**阻止 orphan 任务继续创建**。
- Flow 级超时是软超时：flow 函数体在子线程中执行，超时后使已提交任务经进程池终态化，但函数体线程可能继续运行——Python 无法强制中断线程。长时间运行的 flow 应拆分为多个 `Task.submit`，由任务级硬超时兜底。

**何时不用 `timeout_ms`**

- 任务级硬超时已能强制回收计算资源，无需为了"释放 CPU"而回避 `timeout_ms`。
- I/O 任务的超时建议优先使用原生异步超时（如 `httpx.Timeout`、`socket.settimeout`），`timeout_ms` 作为兜底，避免依赖进程强杀的相对较重回收路径。

---

## 启动 Worker 节点

```bash
# CLI（前台常驻，SIGINT/SIGTERM 触发优雅 drain）
uv run actant worker --log-level info

# 自定义 Worker 参数
uv run actant worker --max-concurrent-tasks 8 --scheduler priority
```

节点通过 iroh P2P 自动发现对端，**无需连接中心服务器**。

任务在 worker 子进程（进程池，大小默认 CPU 核数）中执行，每个 worker 一次执行一个任务。任务崩溃仅失败该任务、节点存活；超时通过终止 worker 进程硬回收资源；崩溃任务会被自动重路由（受 `crash_failover_max_attempts` 上限约束）。

---

## 示例

```bash
uv run python examples/quickstart.py            # ERH 全流程：ask/perform/emit/impossible
uv run python examples/custom_capability.py     # 自定义 capability、handler 链组合
uv run python examples/flow_pipeline.py         # 真实 DAG 流水线：分支/汇合/依赖/失败路径
uv run python -m examples.github_analyzer       # 大型示例：真实 GitHub 仓库分析
```

---

## 开发

### 目录结构

- `actant/` — Python 公共 API 与 ERH 扩展模型
- `src/` — Rust 核心运行时
- `tests/python/` — Python 测试（`unit/` / `integration/` / `e2e/` / `_helpers/`）
- `tests/rust/` — Rust 测试（`unit/` / `property/` / `test_support.rs`）
- `benches/python/` — Python 基准测试（pytest-benchmark）
- `benches/rust/` — Rust 基准测试（criterion）
- `examples/` — 可运行示例

架构分层、ERH 扩展模型、目录结构、Worker 运行模型、代码约定详见 [AGENTS.md](AGENTS.md)。

### 测试

```bash
# Python 测试
uv run pytest tests/python/ -v

# Rust 测试（默认启用 PyO3 模块，需 Python 开发库）
cargo test

# Rust 测试（禁用 PyO3，适用于无 Python 环境的 CI）
cargo test --no-default-features
```

### 基准测试

```bash
# Python 基准测试
uv run pytest benches/python/ --benchmark-only

# Rust 基准测试
cargo bench
```

### 代码质量

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
ruff check actant tests/python
mypy actant
```

### 可观测性

Actant 内置三种可观测能力，均通过环境变量开关，无需改动业务代码：

- `ACTANT_TRACING=1`：启用 Rust 侧 `tracing` 结构化日志（默认已初始化）。
- `ACTANT_VIZTRACER=/tmp/actant.json`：启用 Python 调用追踪（需安装 `actant[observability]`）。
- `ACTANT_TOKIO_CONSOLE=1`：启用 tokio-console 异步运行时可视化（需使用 `cargo build --features tokio-console` 重新编译 Rust 核心）。
