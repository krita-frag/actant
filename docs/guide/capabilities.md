# 能力扩展

## ERH 模型

ERH 只覆盖 `resolution=runtime` 的扩展域。Rust 热路径上的路由、调度、分发、
传输与事件日志在构造期定稿为 trait 句柄，不走 handler 链；两层边界见
[框架契约](../architecture/framework-contract.md)。

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

均经 `rt.layer(...)` 登记，只作用于 Python 发起的 `ask` / `perform` / `emit`：

```python
from actant import Runtime

def my_value_store_handler(req):
    return None  # 示例占位：实际实现应处理请求

def my_router(ctx):
    if ctx.tags.get("region") == "cn":
        return "node-cn"
    return None  # 弃权，交给后续 handler

with Runtime() as rt:
    rt.layer("ValueStore").chain(my_value_store_handler)  # 覆盖内置
    rt.layer("MyRouter", "ask").chain(my_router)          # 新增自定义
```

`with Runtime()` 进入时已启动运行时，不要再调用 `rt.start()`。入口形状、双分发
边界与回退规则见[框架契约](../architecture/framework-contract.md)；覆盖 Rust 内部
行为只能经 `RuntimeBuilder` 注入缝，不能靠 `layer().chain()`。

`actant.capabilities.RouteCtx` / `ScheduleCtx` / `RetryCtx` 与对应 Protocol 仅作为
自定义 capability 的 payload / 签名助手保留，**不会自动生效**。

## 下一步

- [框架契约](../architecture/framework-contract.md)：Rust 注入缝、双分发边界与两个执行域。
- [架构总览](../architecture/overview.md)：ERH 在整体分层中的位置。
