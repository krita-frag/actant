# Actant

> 基于 Actor 模型的跨平台通用分布式任务编排引擎

Actant 采用 **Rust + iroh** 构建核心运行时，通过 **PyO3** 暴露给 Python 用户。P2P 对等节点混合架构，每个节点同时具备编排、执行、Actor 管理、持久化完整能力。

> ⚠️ **实验性项目** — API 尚未冻结，**1.0.0 前不保证向后兼容**。版本号 0.x 的任何升级都应视为潜在破坏性变更，需查阅变更日志并相应调整调用方代码。请勿用于生产环境。

---

## ERH 扩展架构

0.2.0 起，所有扩展点（路由、调度、重试、存储、传输、执行、Actor、生命周期）统一建模为 **Capability**（能力声明）+ **Handler**（能力实现）+ **Layer**（handler 组合）+ **Effect**（请求能力）。

| Effect | 语义 | 调用方式 | 返回值 |
|--------|------|----------|--------|
| `ask` | 决策型 | 逆序调用，首个非 `None` 决定结果 | `Optional[Any]` |
| `perform` | 副作用型 | 调用最后注册的 handler | handler 返回值 |
| `emit` | 反应型 | 顺序调用所有 handler | `None` |

后注册的 handler 优先级更高（`ask` 逆序决策，自定义覆盖默认）。内置 13 个 capability：策略型 `Routing`/`Scheduling`/`RetryPolicy`（纯 Python），其余由 Rust 核心提供 codec 与默认 handler、Python 可覆盖。

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
# 0.2.0-alpha.1
```

---

## 启动 Worker 节点

```bash
# CLI（前台常驻，SIGINT/SIGTERM 触发优雅 drain）
uv run actant worker

# 或嵌入应用
import actant
with actant.Runtime.with_defaults() as rt:
    rt.layer("Routing").chain(my_router)
    rt.serve()                       # 启动 worker 守护循环（非阻塞）
    result = actant.ask("Routing", ctx)
```

节点通过 iroh P2P 自动发现对端，**无需连接中心服务器**。退出 `with` 块时 `Runtime.stop()` 自动 drain 在途任务并关闭 iroh endpoint。

---

## 快速开始

```python
import actant
from actant import Runtime, RouteCtx, ScheduleCtx, ask, emit

with Runtime.with_defaults() as rt:
    # ask：决策型，请求 Routing capability（按 task_name 稳定哈希到 peer）
    target = ask("Routing", RouteCtx(task_name="etl-1", peers=["node-a", "node-b"], local_node="me"))
    print(target)  # node-a

    # 追加自定义 router：后注册=高优先级，覆盖默认
    rt.layer("Routing").chain(lambda ctx: "gpu-node" if "gpu" in ctx.tags else None)
    print(ask("Routing", RouteCtx(task_name="train", peers=["a", "b"], tags=["gpu"])))  # gpu-node

    # emit：反应型，自定义 capability 多播
    rt.layer("Audit", "emit").chain(lambda e: print(f"audit: {e}"))
    emit("Audit", {"action": "login", "user": "alice"})
```

核心 API：`Runtime` / `Runtime.with_defaults()` / `Layer`（`rt.layer(name).chain(handler)`）/ `ask` / `perform` / `emit` / `effect` / `impossible` / `capability`。请求/事件类型（`RouteCtx`、`ScheduleCtx`、`RetryCtx`、`StoreReq` 等）与 Handler Protocol 定义于 `actant.capabilities`。

---

## 示例

```bash
uv run python examples/quickstart.py            # ERH 全流程：ask/perform/emit/impossible
uv run python examples/custom_capability.py     # 自定义 capability、handler 链组合
uv run python -m examples.github_analyzer       # 大型示例：真实 GitHub 仓库分析
```

[`examples/github_analyzer/`](examples/github_analyzer/) 演示真实拉取 GitHub issues/PRs、按 owner 内容路由、PR 优先调度、指数退避重试、令牌桶限流、分仓库聚合、文件持久化等生产级关注点。

```bash
# 默认分析 tokio-rs/tokio 与 astral-sh/uv（首次真实拉取，之后读缓存）
uv run python -m examples.github_analyzer

# 自定义仓库列表（可选 GITHUB_TOKEN 提升速率限制到 5000 req/hour）
ACTANT_GH_REPOS=owner1/repo1,owner2/repo2 uv run python -m examples.github_analyzer
```

---

## 开发

```bash
uv run pytest tests/ -v          # Python 测试
cargo nextest run                # Rust 测试
cargo clippy && ruff check actant tests
```

架构分层、目录结构、Worker 运行模型、重构路线图、代码约定与测试规范详见 [AGENTS.md](AGENTS.md)。
