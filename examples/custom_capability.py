"""自定义 Capability 与 handler 链组合。

演示 0.2.0 的扩展能力：

- 用 ``rt.layer(name, kind)`` 直接注册自定义 capability（自动创建 meta）
- 用 Protocol 约定 handler 签名（``RoutingHandler`` / ``StoreHandler`` 等）
- 多 handler 链式组合：ask 逆序决策、emit 顺序多播、perform 取末位
- 用自定义 Store capability 替换内置副作用，无需 Rust 介入

运行::

    uv run python examples/custom_capability.py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from actant import (
    RouteCtx,
    Runtime,
    ScheduleCtx,
    ask,
    emit,
    perform,
)

# Handler Protocol 类型从 capabilities 子模块导入（未在顶层 re-export）。

# ---------------------------------------------------------------------------
# 自定义请求类型：限流决策
# ---------------------------------------------------------------------------


@dataclass
class ThrottleCtx:
    """``Throttle`` capability 的请求上下文。"""

    key: str
    tokens: int = 1
    capacity: int = 10


# 自定义 capability 通过 ``rt.layer(name, kind)`` 直接注册，
# layer() 会自动创建 CapabilityMeta 并存入 Runtime._metas，无需单独声明。


# ---------------------------------------------------------------------------
# 自定义 handler：实现内置 Protocol
# ---------------------------------------------------------------------------


class TagAwareRouter:
    """按标签路由的 RoutingHandler。

    实现 ``RoutingHandler`` Protocol：``__call__(ctx) -> str | None``。
    返回 None 表示弃权，交由链中下一个 handler 决策。
    """

    def __call__(self, ctx: RouteCtx) -> str | None:
        if "gpu" in ctx.tags:
            return "gpu-node"
        if "io-heavy" in ctx.tags:
            return "io-node"
        return None  # 弃权 → 回退到默认 LocalRouter


class PriorityScheduler:
    """按优先级前缀选任务的 SchedulingHandler。

    pending 中以 ``P0:`` 开头的任务优先；无则弃权，回退到默认 FIFO。
    """

    def __call__(self, ctx: ScheduleCtx) -> str | None:
        for task_id in ctx.pending:
            if task_id.startswith("P0:"):
                return task_id
        return None


# ---------------------------------------------------------------------------
# 自定义 perform capability：内存 KV 存储
# ---------------------------------------------------------------------------


class InMemoryStore:
    """``KVStore`` capability 的 handler（perform 副作用型）。

    perform 取链中最后注册的 handler 执行，返回值直接作为结果。
    """

    def __init__(self) -> None:
        self._data: dict[bytes, bytes] = {}

    def __call__(self, req: dict[str, Any]) -> bytes | None:
        op = req["op"]
        if op == "put":
            self._data[req["key"]] = req["value"]
            return None
        if op == "get":
            return self._data.get(req["key"])
        if op == "delete":
            self._data.pop(req.get("key"), None)
            return None
        raise ValueError(f"unknown op: {op!r}")


def main() -> None:
    with Runtime.with_defaults() as rt:
        # 注册自定义 handler：chain 顺序决定 ask 决策优先级（后注册=高优先级）。
        rt.layer("Routing").chain(TagAwareRouter())
        rt.layer("Scheduling").chain(PriorityScheduler())

        print("=== ask：多 handler 决策链 ===")
        # tag=gpu → TagAwareRouter 命中 "gpu-node"。
        print(f"  gpu task    -> {ask('Routing', RouteCtx('train', peers=['a', 'b'], tags=['gpu']))}")
        # 无标签 → TagAwareRouter 弃权，回退默认 LocalRouter（哈希到 peer）。
        print(f"  plain task  -> {ask('Routing', RouteCtx('etl', peers=['a', 'b']))}")
        # P0 优先级 → PriorityScheduler 命中 "P0:urgent"。
        sched = ask("Scheduling", ScheduleCtx("wf", pending=["t1", "P0:urgent", "t2"]))
        print(f"  P0 priority -> {sched}")
        # 无 P0 → PriorityScheduler 弃权，回退默认 FIFO（返回 "t1"）。
        print(f"  no P0       -> {ask('Scheduling', ScheduleCtx('wf', pending=['t1', 't2']))}")

        print("\n=== 自定义 capability：Throttle ===")
        # 用 dataclass 作为请求 payload，注册自定义 ask capability。
        buckets: dict[str, int] = {}

        def token_bucket(ctx: ThrottleCtx) -> bool | None:
            used = buckets.get(ctx.key, 0)
            if used + ctx.tokens <= ctx.capacity:
                buckets[ctx.key] = used + ctx.tokens
                return True  # 放行
            return False  # 拒绝

        rt.layer("Throttle", "ask").chain(token_bucket)
        for i in range(4):
            ok = ask("Throttle", ThrottleCtx(key="api", tokens=3, capacity=10))
            print(f"  request {i + 1} (tokens=3) -> {'放行' if ok else '拒绝'}")

        print("\n=== 自定义 perform capability：KVStore ===")
        store = InMemoryStore()
        rt.layer("KVStore", "perform").chain(store)
        perform("KVStore", {"op": "put", "key": b"user:1", "value": b"alice"})
        perform("KVStore", {"op": "put", "key": b"user:2", "value": b"bob"})
        print(f"  get user:1 -> {perform('KVStore', {'op': 'get', 'key': b'user:1'})}")
        print(f"  get user:2 -> {perform('KVStore', {'op': 'get', 'key': b'user:2'})}")
        perform("KVStore", {"op": "delete", "key": b"user:1"})
        print(f"  after del  -> {perform('KVStore', {'op': 'get', 'key': b'user:1'})}")

        print("\n=== 自定义 emit capability：MetricsPipeline ===")
        # emit 反应型：所有 handler 顺序执行，互不阻塞。
        rt.layer("Metrics", "emit").chain(lambda e: print(f"  counter: {e['name']} += {e['value']}"))
        rt.layer("Metrics", "emit").chain(lambda e: print(f"  log:     {e['name']}={e['value']}"))
        emit("Metrics", {"name": "tasks_completed", "value": 1})
        emit("Metrics", {"name": "tasks_failed", "value": 0})

        print("\n=== capability 元数据 ===")
        meta = rt.capability_meta("Throttle")
        print(f"  Throttle: name={meta.name!r} kind={meta.kind!r}")
        print(f"  registered: {sorted(rt.capabilities)}")

    print("\n完成。")


if __name__ == "__main__":
    main()
