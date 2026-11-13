"""自定义 Capability 与 handler 链组合。

演示 ``ask`` 逆序决策、``perform`` 取末位、``emit`` 顺序多播。

.. note::
    自定义 capability 只由用户代码自己驱动，引擎不会自动调用。示例里的
    ``KVStore`` 是独立名字，**不会替换内置 ``Store``**；覆盖数据面请用
    ``rt.layer("Store").chain(...)``。真实的放置由 Rust ``RoutePolicy`` /
    ``Scheduler`` 承担。

运行::

    uv run python examples/custom_capability.py
"""

from __future__ import annotations

from typing import Any

from actant import Runtime, emit, perform
from actant.capabilities import RouteCtx, ScheduleCtx
from actant.effects import ask


class TagAwareRouter:
    """按标签路由的自定义 ask handler。

    实现 ``RoutingHandler`` Protocol：``__call__(ctx) -> str | None``。
    返回 None 表示弃权，交由链中下一个 handler 决策。
    """

    def __call__(self, ctx: RouteCtx) -> str | None:
        if "gpu" in ctx.tags:
            return "gpu-node"
        if "io-heavy" in ctx.tags:
            return "io-node"
        return None  # 弃权 → 无后续 handler 时 ask 返回 None


class PriorityScheduler:
    """按优先级前缀选任务的自定义 ask handler。

    pending 中以 ``P0:`` 开头的任务优先；无则弃权。
    """

    def __call__(self, ctx: ScheduleCtx) -> str | None:
        for task_id in ctx.pending:
            if task_id.startswith("P0:"):
                return task_id
        return None

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
    with Runtime() as rt:
        # 注册自定义 handler：chain 顺序决定 ask 决策优先级（后注册=高优先级）。
        rt.layer("MyRouter", "ask").chain(TagAwareRouter())
        rt.layer("MyScheduler", "ask").chain(PriorityScheduler())

        print("=== ask：多 handler 决策链 ===")
        # tag=gpu → TagAwareRouter 命中 "gpu-node"。
        print(f"  gpu task    -> {ask('MyRouter', RouteCtx('train', peers=['a', 'b'], tags=['gpu']))}")
        # 无标签 → TagAwareRouter 弃权，无后续 handler 时返回 None。
        print(f"  plain task  -> {ask('MyRouter', RouteCtx('etl', peers=['a', 'b']))}")
        # P0 优先级 → PriorityScheduler 命中 "P0:urgent"。
        sched = ask("MyScheduler", ScheduleCtx("wf", pending=["t1", "P0:urgent", "t2"]))
        print(f"  P0 priority -> {sched}")
        # 无 P0 → PriorityScheduler 弃权，返回 None。
        print(f"  no P0       -> {ask('MyScheduler', ScheduleCtx('wf', pending=['t1', 't2']))}")

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
        meta = rt.capability_meta("MyRouter")
        print(f"  MyRouter: name={meta.name!r} kind={meta.kind!r}")
        print(f"  registered: {sorted(rt.capabilities)}")

    print("\n完成。")


if __name__ == "__main__":
    main()
