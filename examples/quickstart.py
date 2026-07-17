"""快速开始：Actant 0.2 ERH（Effect-Resource-Handler）架构。

本示例演示 0.2.0 的统一扩展语法：

- ``Runtime`` / ``Runtime.with_defaults()``：运行时入口
- ``rt.layer(name).chain(handler)``：向 capability 注册 handler
- ``ask`` / ``perform`` / ``emit``：三种 effect 原语
- handler 覆盖优先级（后注册 = 高优先级）
- ``Runtime`` 作为 context manager 的生命周期

运行::

    uv run python examples/quickstart.py
"""

from __future__ import annotations

import actant
from actant import (
    RetryCtx,
    RouteCtx,
    Runtime,
    ScheduleCtx,
    ask,
    emit,
    impossible,
)


def main() -> None:
    # ``with_defaults()`` 预装 Python 策略层默认 handler（LocalRouter /
    # FifoScheduler / DefaultRetryPolicy）。也可用 ``Runtime()`` 从零开始。
    with Runtime.with_defaults() as rt:
        print("=== ask 决策型 ===")

        # Routing：无 peer 时路由到本地；有 peer 时按 task_name 稳定哈希。
        route = ask(
            "Routing",
            RouteCtx(task_name="etl-1", peers=["node-a", "node-b"], local_node="me"),
        )
        print(f"  routing(etl-1, peers=[a,b]) -> {route}")

        # 追加自定义 router：后注册的 handler 在 ask 中优先决策，覆盖默认。
        rt.layer("Routing").chain(lambda ctx: "dedicated-node" if "gpu" in ctx.tags else None)
        route = ask("Routing", RouteCtx(task_name="train", peers=["a", "b"], tags=["gpu"]))
        print(f"  routing(tag=gpu, override)  -> {route}")
        # 标签不匹配时自定义 router 弃权（返回 None），回退到默认 LocalRouter。
        route = ask("Routing", RouteCtx(task_name="etl-2", peers=["a", "b"]))
        print(f"  routing(no gpu, fallback)   -> {route}")

        # Scheduling：默认 FIFO，返回 pending 队首。
        nxt = ask("Scheduling", ScheduleCtx(workflow_id="wf-1", pending=["t1", "t2", "t3"]))
        print(f"  scheduling(FIFO)            -> {nxt}")

        # RetryPolicy：attempt < max_retries 返回 True，否则弃权返回 None。
        print(f"  retry(attempt=0, max=3)     -> {ask('RetryPolicy', RetryCtx('t', 0, 'boom', 3))}")
        print(f"  retry(attempt=3, max=3)     -> {ask('RetryPolicy', RetryCtx('t', 3, 'boom', 3))}")

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

        print("\n=== 通用 effect 入口 ===")
        # ``actant.effect`` 按 kind 字符串分发到 ask/perform/emit。
        result = actant.effect("Routing", "ask", RouteCtx(task_name="x", tags=["gpu"]))
        print(f"  effect(Routing, ask)        -> {result}")

        print("\n=== 运行时查询 ===")
        print(f"  capabilities: {sorted(rt.capabilities)}")
        print(f"  Routing handlers: {rt.handler_count('Routing')}")
        print(f"  Audit handlers:   {rt.handler_count('Audit')}")

    # 退出 with 块后 Runtime 已 stop，此时调用 effect 会得到明确错误。
    print("\n=== 退出后的错误路径 ===")
    try:
        ask("Routing", RouteCtx(task_name="x"))
    except actant.InvalidStateError as exc:
        print(f"  预期错误 (InvalidStateError): {exc}")

    # impossible() 用于标记"此 effect 必须有 handler 处理"的不可达分支。
    try:
        impossible("此处不应到达")
    except actant.InternalError as exc:
        print(f"  impossible() (InternalError): {exc}")


if __name__ == "__main__":
    main()
