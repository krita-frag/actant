"""追踪驱动断言的基础设施。

``TraceRecorder`` 把三类异步可观测信号归一为带节点归属与单调序号的
``TraceEvent``：

1. ``TaskLifecycle``（started/completed/failed/retried/cancelled）；
2. 任务日志（``on_task_log``，best-effort tap）；
3. Worker 状态（``on_worker_state``）。

跨节点事件的**接收顺序**不等于因果顺序（时钟不同源），因此每个事件同时
记录全局 ``seq`` 与节点内 ``node_seq``；生命周期不变量只在同一节点内做
顺序判断，跨节点只做集合/终态判断。
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .cluster import TestCluster


TERMINAL_KINDS = frozenset({"completed", "failed", "cancelled"})
LIFECYCLE_KINDS = frozenset(
    {
        "started",
        "completed",
        "failed",
        "retried",
        "cancelled",
    }
)


@dataclass(frozen=True)
class TracePolicy:
    """被测应用的事件语义策略。

    衍生应用可以自定义终态/lifecycle 名称，断言与不变量通过该策略判断，
    不需要修改 testkit。
    """

    terminal_kinds: frozenset[str] = TERMINAL_KINDS
    lifecycle_kinds: frozenset[str] = LIFECYCLE_KINDS


DEFAULT_TRACE_POLICY = TracePolicy()


@dataclass(frozen=True)
class TraceEvent:
    """一条追踪事件。"""

    seq: int
    node_seq: int
    at: float
    node: str
    kind: str
    task_id: str = ""
    workflow_id: str = ""
    attempt: int = 0
    next_attempt: int = 0
    error: str = ""
    payload_size: int = 0
    data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "node_seq": self.node_seq,
            "at": self.at,
            "node": self.node,
            "kind": self.kind,
            "task_id": self.task_id,
            "workflow_id": self.workflow_id,
            "attempt": self.attempt,
            "next_attempt": self.next_attempt,
            "error": self.error,
            "payload_size": self.payload_size,
            "data": dict(self.data),
        }


class TraceRecorder:
    """线程安全的追踪聚合器。

    用法::

        trace = TraceRecorder()
        trace.attach_cluster(cluster)
        ...
        assert trace.terminal_state(task_id) == "completed"

    ``partial=True`` 表示该 trace 是**观察者视角**（只记录本端可见事件）。
    远端执行的任务，``started`` 由执行节点的 worker 产生，不会回流到提交方，
    因此这种情况下"终态缺少 started"不是缺陷。需要完整生命周期时保持默认
    ``partial=False``（同进程/同节点执行）。
    """

    def __init__(
        self,
        *,
        partial: bool = False,
        policy: TracePolicy = DEFAULT_TRACE_POLICY,
    ) -> None:
        self.partial = partial
        self.policy = policy
        self._events: list[TraceEvent] = []
        self._seq = 0
        self._node_seq: Counter[str] = Counter()
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._attached: set[str] = set()

    # ------------------------------------------------------------------
    # 采集
    # ------------------------------------------------------------------

    def record(self, node: str, kind: str, **fields: Any) -> TraceEvent:
        """记录一条事件（线程安全，非阻塞）。"""
        with self._cond:
            self._seq += 1
            self._node_seq[node] += 1
            event = TraceEvent(
                seq=self._seq,
                node_seq=self._node_seq[node],
                at=time.time(),
                node=node,
                kind=kind,
                task_id=str(fields.get("task_id", "")),
                workflow_id=str(fields.get("workflow_id", "")),
                attempt=int(fields.get("attempt", 0) or 0),
                next_attempt=int(fields.get("next_attempt", 0) or 0),
                error=str(fields.get("error", "") or ""),
                payload_size=int(fields.get("payload_size", 0) or 0),
                data=dict(fields.get("data", {})),
            )
            self._events.append(event)
            self._cond.notify_all()
            return event

    def attach(self, handle: Any) -> None:
        """订阅一个节点句柄的事件流（幂等）。"""
        name = str(getattr(handle, "name", ""))
        if not name:
            raise ValueError("node handle must expose .name")
        with self._lock:
            if name in self._attached:
                return
            self._attached.add(name)

        subscribe = getattr(handle, "subscribe", None)
        if callable(subscribe):
            subscribe(lambda event: self._on_external(name, event))
            return

        add_sink = getattr(handle, "add_trace_sink", None)
        if callable(add_sink):
            add_sink(lambda event: self._on_external(name, event))
            return

        runtime = getattr(handle, "runtime", None)
        if runtime is None:
            raise ValueError(f"node {name!r} exposes neither add_trace_sink nor runtime")

        def lifecycle(event: Any) -> None:
            self._on_external(name, _event_fields(event))

        runtime.layer("TaskLifecycle").chain(lifecycle)

        def task_log(event: dict[str, Any]) -> None:
            self.record(
                name,
                "log",
                task_id=str(event.get("task_id", "")),
                data={
                    "level": event.get("level", ""),
                    "message": event.get("message", ""),
                },
            )

        runtime.on_task_log(task_log)

        def worker_state(event: dict[str, Any]) -> None:
            self.record(
                name,
                "worker",
                data={"state": event.get("state", ""), "node_id": event.get("node_id", "")},
            )

        runtime.on_worker_state(worker_state)

    def attach_cluster(self, cluster: TestCluster) -> None:
        """订阅集群内全部节点。"""
        for handle in cluster.handles():
            self.attach(handle)

    def _on_external(self, node: str, event: dict[str, Any]) -> None:
        kind = str(event.get("kind", ""))
        if not kind:
            return
        self.record(
            node,
            kind,
            task_id=event.get("task_id", ""),
            workflow_id=event.get("workflow_id", ""),
            attempt=event.get("attempt", 0),
            next_attempt=event.get("next_attempt", 0),
            error=event.get("error", ""),
            payload_size=event.get("payload_size", 0),
            data=dict(event.get("data", {})),
        )

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    @property
    def events(self) -> list[TraceEvent]:
        with self._lock:
            return list(self._events)

    def query(
        self,
        *,
        kind: str | None = None,
        kinds: set[str] | frozenset[str] | None = None,
        node: str | None = None,
        task_id: str | None = None,
        workflow_id: str | None = None,
    ) -> list[TraceEvent]:
        """按维度过滤事件（保持 seq 顺序）。"""
        with self._lock:
            events = list(self._events)
        result = []
        for event in events:
            if kind is not None and event.kind != kind:
                continue
            if kinds is not None and event.kind not in kinds:
                continue
            if node is not None and event.node != node:
                continue
            if task_id is not None and event.task_id != task_id:
                continue
            if workflow_id is not None and event.workflow_id != workflow_id:
                continue
            result.append(event)
        return result

    def is_lifecycle(self, event: TraceEvent) -> bool:
        return event.kind in self.policy.lifecycle_kinds

    def is_terminal(self, event: TraceEvent) -> bool:
        return event.kind in self.policy.terminal_kinds

    def tasks(self) -> list[str]:
        """出现过生命周期事件的 task_id（按首次出现排序）。"""
        seen: dict[str, None] = {}
        for event in self.events:
            if self.is_lifecycle(event) and event.task_id:
                seen.setdefault(event.task_id, None)
        return list(seen)

    def for_task(self, task_id: str) -> list[TraceEvent]:
        return self.query(task_id=task_id)

    def for_node(self, node: str) -> list[TraceEvent]:
        return self.query(node=node)

    def terminal_state(self, task_id: str) -> str | None:
        """返回任务的终态；无终态返回 ``None``（不判断是否重复）。"""
        terminals = [
            e.kind
            for e in self.query(task_id=task_id, kinds=self.policy.terminal_kinds)
        ]
        return terminals[-1] if terminals else None

    def wait_for(
        self,
        predicate: Callable[[TraceEvent], bool],
        *,
        timeout_s: float = 10.0,
        description: str = "trace event",
    ) -> TraceEvent:
        """等待首个满足 ``predicate`` 的事件。"""
        deadline = time.monotonic() + timeout_s
        seen = 0
        while True:
            with self._cond:
                for event in self._events[seen:]:
                    if predicate(event):
                        return event
                seen = len(self._events)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"timeout after {timeout_s}s waiting for {description}")
                self._cond.wait(timeout=min(remaining, 0.2))

    def summary(self) -> dict[str, Any]:
        """报告用摘要（不包含逐条事件，避免报告膨胀）。"""
        events = self.events
        by_kind = Counter(e.kind for e in events)
        by_node = Counter(e.node for e in events)
        tasks = self.tasks()
        terminals: Counter[str] = Counter()
        for event in events:
            if self.is_terminal(event) and event.task_id:
                terminals[event.task_id] += 1
        duplicate_terminals = {k: v for k, v in terminals.items() if v > 1}
        return {
            "event_count": len(events),
            "by_kind": dict(sorted(by_kind.items())),
            "by_node": dict(sorted(by_node.items())),
            "task_count": len(tasks),
            "tasks": tasks,
            "terminal_count": len(terminals),
            "duplicate_terminal_tasks": duplicate_terminals,
            "first_at": events[0].at if events else None,
            "last_at": events[-1].at if events else None,
        }


def _event_fields(event: Any) -> dict[str, Any]:
    """把 ``TaskEvent`` 数据类或 dict 归一为普通字段。"""
    if isinstance(event, dict):
        data = dict(event)
        result_payload = data.get("result_payload", b"")
    else:
        data = {
            "kind": getattr(event, "kind", ""),
            "task_id": getattr(event, "task_id", ""),
            "workflow_id": getattr(event, "workflow_id", ""),
            "attempt": getattr(event, "attempt", 0),
            "next_attempt": getattr(event, "next_attempt", 0),
            "error": getattr(event, "error", ""),
        }
        result_payload = getattr(event, "result_payload", b"")
    data["payload_size"] = len(result_payload) if isinstance(result_payload, (bytes, bytearray)) else 0
    return data
