# 能力扩展

## ERH 模型

ERH 只覆盖 `resolution=runtime` 的扩展域。Rust 热路径上的路由、调度、分发、
传输与事件日志在构造期定稿为 trait 句柄，不走 handler 链；全部扩展点及其
解析时机见[扩展点清单](../reference/extension-points.md)。

| Effect | 语义 | 调用方式 | 返回值 |
| --- | --- | --- | --- |
| `ask` | 决策型 | 逆序调用，首个非 `None` 决定结果 | `Optional[Any]` |
| `perform` | 副作用型 | 调用最后注册的 handler | handler 返回值 |
| `emit` | 反应型 | 顺序调用所有 handler | `None` |

后注册的 handler 优先级更高（`ask` 逆序决策，自定义覆盖默认）。

## 内置 capability

内置 4 个 capability，每一项都有生产发射点或默认 handler；`tests/python/unit/test_capability_wiring.py` 会扫描 `actant/` 生产代码校验这一点，新增只有声明、没有调用点的 capability 会让门禁失败。

| 名称 | 类型 | 说明 | 默认是否可用 |
| --- | --- | --- | --- |
| `Store` | `perform` | 持久化键值存储 | 是：Rust `StoreHandler`，可用 `rt.layer("Store").chain(...)` 覆盖 |
| `ValueStore` | `perform` | Python → Rust blob 桥 | 是：Python 默认 handler，库内由 `Ref` 使用 |
| `TaskLifecycle` | `emit` | 任务生命周期事件 | 引擎发射，用户以 handler 订阅 |
| `WorkflowLifecycle` | `emit` | 工作流生命周期事件 | 引擎发射，用户以 handler 订阅 |

接线状态以[核心概念](../start/concepts.md)为准。

放置类策略（路由 / 调度 / 重试）**不是**内置 capability。它们由 Rust `RoutePolicy` / `Scheduler` 或第 3 层外部调度器承担，避免进程内闭包在 workflow failover 后失效。

## 两条 Python 扩展路径

均经 `rt.layer(...)` 登记 handler，只作用于 Python 发起的 `actant.effects.ask` / `actant.perform` / `actant.emit`：

```python
from actant import Runtime

def my_value_store_handler(req):
    return None  # 示例占位：实际实现应处理请求

def my_router(ctx):
    return None  # 弃权，交给后续 handler

with Runtime() as rt:
    # 覆盖内置 capability 的 handler
    rt.layer("ValueStore").chain(my_value_store_handler)

    # 新增自定义 capability
    rt.layer("MyRouter", "ask").chain(my_router)
```

| 路径 | 入口 | 适用 |
| --- | --- | --- |
| 覆盖内置 | `rt.layer(name).chain(handler)` 或 `rt.chain(name, handler)` | 替换/追加数据面 handler（如 `ValueStore` 换后端） |
| 新增自定义 | `rt.layer(name, kind).chain(handler)` | 声明新的 `ask` / `perform` / `emit` 扩展点 |

## 双分发规则

- Python 侧：`_dispatch_ask/perform/emit` 先走 Python handler 链；缺失或弃权时，仅 `RUST_BACKED_CAPABILITIES`（`Store` / `TaskLifecycle` / `WorkflowLifecycle`）转发到 Rust capability runtime。只有 `Store` 有 Rust `StoreHandler`；两个 lifecycle capability 在 Rust 侧只有 codec 与空 layer，emit 为空操作，实际订阅者是 Python handler。
- Rust 内部：Worker 执行、Orchestrator 持久化等路径**不咨询** Python `_layers`；Python handler 永不进入 Rust 内部 dispatch。
- `ValueStore` 没有 Rust 内部分发参与，只有 Python 一条路径。

要覆盖 **Rust 内部行为**（任务执行、存储、传输等），只能用 Rust 侧 `RuntimeBuilder` 的注入缝，不能靠 `layer().chain()`。

## 自定义 handler 示例

```python
from actant import Runtime

def my_router(ctx):
    if ctx.tags.get("region") == "cn":
        return "node-cn"
    return None  # 弃权，交给后续 handler

with Runtime() as rt:
    rt.layer("MyRouter", "ask").chain(my_router)
```

`with Runtime() as rt` 进入时已启动运行时，不要再调用 `rt.start()`。

`actant.capabilities.RouteCtx` / `ScheduleCtx` / `RetryCtx` 与对应 Protocol 仅作为自定义 capability 的 payload / 签名助手保留，**不会自动生效**。

## 下一步

- [扩展点清单](../reference/extension-points.md)：全部扩展点、解析时机与验收强度。
- [框架契约](../architecture/framework-contract.md)：Rust 注入缝与双分发边界。
- [架构总览](../architecture/overview.md)：ERH 在整体分层中的位置。
