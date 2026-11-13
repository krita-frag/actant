# Actant

> 基于 Actor 模型的跨平台通用分布式任务编排引擎

Actant 采用 **Rust + iroh** 构建核心运行时，通过 **PyO3** 暴露给 Python 用户。P2P 对等节点混合架构，每个节点同时具备编排、执行、Actor 管理与持久化能力。

> ⚠️ **实验性项目** — API 尚未冻结，**1.0.0 前不保证向后兼容**。请勿用于生产环境。

## 从源码构建

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
curl -LsSf https://astral.sh/uv/install.sh | sh

git clone https://github.com/actant/actant.git
cd actant
uv sync --extra dev
uv run maturin develop

uv run python -c "import actant; print(actant.__version__)"
```

## 快速开始

```python
import actant
from actant import Runtime, task

@task
def add(a: int, b: int) -> int:
    return a + b

@task(retries=3, retry_delay_ms=100, timeout_ms=5000)
def fetch(url: str) -> str:
    ...  # 自动重试 3 次，每次间隔 100ms，单次超时 5s

with Runtime() as rt:
    handle = add.submit(2, 3)    # 异步提交，返回 AsyncResult
    print(handle.result())        # 5（阻塞等待）

    print(add(2, 3))              # 5（同步执行，不走执行池）
```

`AsyncResult` 作为下游任务参数时自动等待，依赖解析递归处理 `list` / `tuple` / `dict`：

```python
@task
def double(x: int) -> int:
    return x * 2

with Runtime():
    a = add.submit(10, 1)     # 11
    b = double.submit(a)      # 自动等待 a 完成，传入 11，得 22
    print(b.result())          # 22
```

工作流编排：

```python
from actant import flow

@flow
def pipeline(a: int, b: int) -> int:
    result = add.submit(a, b)
    return double.submit(result).result()

with Runtime():
    print(pipeline(2, 3))  # (2+3)*2 = 10
```

`@flow` 广播 `submitted` → `started` → `completed` / `failed` 生命周期事件。工作流惰性创建：函数体第一次 `Task.submit` 或进入等待点时建槽。

## 示例

```bash
uv run python examples/quickstart.py            # ERH 全流程：ask/perform/emit/impossible
uv run python examples/custom_capability.py     # 自定义 capability、handler 链组合
uv run python examples/flow_pipeline.py         # 真实 DAG 流水线：分支/汇合/依赖/失败路径
```

## 文档

完整文档见 `docs/`（本地预览：`uv run mkdocs serve`）。

| 主题 | 页面 |
| --- | --- |
| 安装与快速开始 | [docs/start/quickstart.md](docs/start/quickstart.md) |
| 任务、工作流、能力扩展 | [docs/guide/](docs/guide/tasks.md) |
| Python / Rust API | [docs/reference/](docs/reference/python-api.md) |
| 扩展缝与执行域边界 | [docs/architecture/framework-contract.md](docs/architecture/framework-contract.md) |
| 分层、ERH、Worker、框架契约 | [docs/architecture/](docs/architecture/overview.md) |
| SLA、排障 | [docs/operations/](docs/operations/sla-baseline.md) |
| 开发、测试、代码约定 | [docs/contributing/](docs/contributing/dev-setup.md) |

## 开发

```bash
uv run pytest tests/python/ -v            # Python 测试
cargo test --workspace                    # Rust 全量测试（含 PyO3）
cargo test -p actant-core -p actant-common --no-default-features  # 框架构建

cargo fmt --check
cargo clippy --all-targets -- -D warnings
ruff check actant tests/python
mypy actant

uv run pytest benches/python/ --benchmark-only
cargo bench
```

架构分层、ERH 扩展模型、Worker 运行模型与代码约定见 [AGENTS.md](AGENTS.md) 和 `docs/architecture/`。

## 可观测性

Rust 侧 `tracing` 结构化日志与 tokio-console 的环境变量开关统一见[配置项 · 环境变量](docs/reference/configuration.md)。

## 启动 Worker 节点

```bash
uv run actant worker --log-level info
uv run actant worker --max-concurrent-tasks 8 --scheduler priority
uv run actant worker --resume
```

显式指定 `--data-dir`（或 `Runtime(data_dir=...)`）时，节点默认使用 `preset="local"`（公网 DNS + relay 发现）；不指定时使用临时目录并默认 `preset="none"`（仅 loopback）。内网部署应显式使用 `--discovery none` 并配置 bootstrap。部署与发现配置见 `docs/guide/deployment.md`。
