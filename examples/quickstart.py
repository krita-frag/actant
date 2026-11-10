"""快速开始：Actant ERH（Effect-Resource-Handler）架构。

本示例演示统一扩展语法：

- ``Runtime`` / ``Runtime()``：运行时入口
- ``rt.layer(name, kind).chain(handler)``：向自定义 capability 注册 handler
- ``ask`` / ``perform`` / ``emit``：三种 effect 原语
- handler 覆盖优先级（后注册 = 高优先级）
- ``Runtime`` 作为 context manager 的生命周期

放置类策略（路由/调度/重试）由 Rust ``RoutePolicy`` / ``Scheduler`` 或
第 3 层外部调度器承担，不在 Python 层预装 handler。这里用自定义 capability
演示 ERH 语法。

运行::

    uv run python examples/quickstart.py
"""

from __future__ import annotations

from actant import Runtime, emit
from actant.capabilities import RetryCtx, RouteCtx, ScheduleCtx
from actant.effects import ask, impossible
from actant.exceptions import InternalError, InvalidStateError


def main() -> None:
    # ``Runtime()`` 直接构造：Rust 侧已内置数据面默认策略，
    # Python 层不再注册路由/调度/重试 handler。
    with Runtime() as rt:
        print("=== ask 决策型（用户自定义 capability）===")

        # 自定义路由 capability：后注册的 handler 在 ask 中优先决策。
        rt.layer("MyRouter", "ask").chain(
            lambda ctx: "dedicated-node" if "gpu" in ctx.tags else None
        )
        route = ask("MyRouter", RouteCtx(task_name="train", peers=["a", "b"], tags=["gpu"]))
        print(f"  routing(tag=gpu)            -> {route}")

        # 自定义 handler 弃权（None）时继续向前查找；无 handler 则返回 None。
        route = ask("MyRouter", RouteCtx(task_name="etl", peers=["a", "b"]))
        print(f"  routing(no gpu)             -> {route}")

        # 自定义调度 capability。
        rt.layer("MyScheduler", "ask").chain(
            lambda ctx: ctx.pending[0] if ctx.pending else None
        )
        nxt = ask("MyScheduler", ScheduleCtx(workflow_id="wf-1", pending=["t1", "t2", "t3"]))
        print(f"  scheduling(FIFO)            -> {nxt}")

        # 自定义重试 capability。
        rt.layer("MyRetry", "ask").chain(
            lambda ctx: True if ctx.attempt < ctx.max_retries else None
        )
        print(f"  retry(attempt=0, max=3)     -> {ask('MyRetry', RetryCtx('t', 0, 'boom', 3))}")
        print(f"  retry(attempt=3, max=3)     -> {ask('MyRetry', RetryCtx('t', 3, 'boom', 3))}")

        print("\n=== emit 反应型（自定义 capability）===")
        # 内置 lifecycle capability（TaskLifecycle 等）由 Rust 事件总线提供，
        # 需完整工作流运行时才绑定。这里用自定义 capability 演示 emit 多播语义。
        audit: list[str] = []
        rt.layer("Audit", "emit").chain(lambda e: audit.append(f"stdout:{e['action']}"))
        rt.layer("Audit", "emit").chain(lambda e: audit.append(f"metric:{e['user']}"))
        emit("Audit", {"action": "login", "user": "alice"})
        emit("Audit", {"action": "logout", "user": "bob"})
        for line in audit:
            print(f"  {line}")

        print("\n=== 决策型 effect ===")
        # ``ask`` 逆序询问 handler 链，首个非 None 的结果生效。
        result = ask("MyRouter", RouteCtx(task_name="x", tags=["gpu"]))
        print(f"  ask(MyRouter)               -> {result}")

        print("\n=== 运行时查询 ===")
        print(f"  capabilities: {sorted(rt.capabilities)}")
        print(f"  MyRouter handlers: {len(rt.handlers('MyRouter'))}")
        print(f"  Audit handlers:    {len(rt.handlers('Audit'))}")

    # 退出 with 块后 Runtime 已 stop，此时调用 ask 会得到明确错误。
    print("\n=== 退出后的错误路径 ===")
    try:
        ask("MyRouter", RouteCtx(task_name="x"))
    except InvalidStateError as exc:
        print(f"  预期错误 (InvalidStateError): {exc}")

    # impossible() 用于标记"此 effect 必须有 handler 处理"的不可达分支。
    try:
        impossible("此处不应到达")
    except InternalError as exc:
        print(f"  impossible() (InternalError): {exc}")


if __name__ == "__main__":
    main()
