# Actant

> 基于 Actor 模型的跨平台通用分布式任务编排引擎

Actant 采用 **Rust + iroh** 构建核心运行时，通过 **PyO3** 暴露给 Python 用户。P2P 对等节点混合架构，每个节点同时具备编排、执行、Actor 管理、持久化完整能力。

> ⚠️ **实验性项目** — API 尚未冻结，**1.0.0 前不保证向后兼容**。请勿用于生产环境。

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

### 启动 Worker 节点

```bash
# CLI（前台常驻，SIGINT/SIGTERM 触发优雅 drain）
uv run actant worker --log-level info

# 自定义 Worker 参数
uv run actant worker --max-concurrent-tasks 8 --scheduler priority
```

节点通过 iroh P2P 自动发现对端，**无需连接中心服务器**。

---

## 示例

```bash
uv run python examples/quickstart.py            # ERH 全流程：ask/perform/emit/impossible
uv run python examples/custom_capability.py     # 自定义 capability、handler 链组合
uv run python -m examples.github_analyzer       # 大型示例：真实 GitHub 仓库分析
```

---

## 开发

```bash
uv run pytest tests/ -v          # Python 测试
cargo nextest run                # Rust 测试
cargo clippy && ruff check actant tests
mypy actant
```

架构分层、ERH 扩展模型、目录结构、Worker 运行模型、代码约定详见 [AGENTS.md](AGENTS.md)。

### 可观测性

Actant 内置三种可观测能力，均通过环境变量开关，无需改动业务代码：

- `ACTANT_TRACING=1`：启用 Rust 侧 `tracing` 结构化日志（默认已初始化）。
- `ACTANT_VIZTRACER=/tmp/actant.json`：启用 Python 调用追踪（需安装 `actant[observability]`）。
- `ACTANT_TOKIO_CONSOLE=1`：启用 tokio-console 异步运行时可视化（需使用 `cargo build --features tokio-console` 重新编译 Rust 核心）。
