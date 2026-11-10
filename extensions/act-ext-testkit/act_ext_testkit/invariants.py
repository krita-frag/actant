"""生命周期不变量：把"分布式正确性"显式化为可断言的规则。

不变量只依赖 trace 中的生命周期事件，不依赖实现细节：

- 每个任务最多一个终态（completed/failed/cancelled）——重复终态通常意味着
  结果被重复上报或状态机回退；
- 终态必须晚于 started；
- 没有 ``retried`` 事件时，同一任务不得出现多次 ``started``（重复执行）；
- ``retried.next_attempt > attempt``。

跨节点因果顺序无法从接收时间推断，因此顺序检查限制在同一节点内。
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from .trace import DEFAULT_TRACE_POLICY, TraceEvent, TracePolicy, TraceRecorder


@dataclass(frozen=True)
class Violation:
    """一条不变量违反。"""

    code: str
    message: str
    task_id: str = ""
    node: str = ""
    seq: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "task_id": self.task_id,
            "node": self.node,
            "seq": self.seq,
        }

    def __str__(self) -> str:
        location = f"task={self.task_id} node={self.node} seq={self.seq}"
        return f"[{self.code}] {self.message} ({location})"


class InvariantViolation(AssertionError):
    """不变量断言失败（携带全部违反项）。"""

    def __init__(self, violations: list[Violation]) -> None:
        self.violations = list(violations)
        detail = "\n".join(f"  - {v}" for v in self.violations[:10])
        if len(self.violations) > 10:
            detail += f"\n  ... and {len(self.violations) - 10} more"
        super().__init__(f"{len(self.violations)} invariant violation(s):\n{detail}")


def check_trace(
    trace: TraceRecorder | Iterable[TraceEvent],
    *,
    allow_incomplete: bool = True,
    allow_partial: bool | None = None,
    policy: TracePolicy | None = None,
) -> list[Violation]:
    """检查 trace 的全部不变量，返回违反列表（空 = 通过）。

    ``allow_partial`` 省略时沿用 ``TraceRecorder.partial``：观察者视角的
    trace 允许"终态缺少 started"（远端任务的开跑事件不回流提交方）。
    """
    if allow_partial is None:
        allow_partial = trace.partial if isinstance(trace, TraceRecorder) else False
    active_policy = policy or (
        trace.policy if isinstance(trace, TraceRecorder) else DEFAULT_TRACE_POLICY
    )
    events = trace.events if isinstance(trace, TraceRecorder) else list(trace)
    lifecycle = [
        e for e in events if e.kind in active_policy.lifecycle_kinds and e.task_id
    ]
    grouped: dict[str, list[TraceEvent]] = defaultdict(list)
    for event in lifecycle:
        grouped[event.task_id].append(event)

    violations: list[Violation] = []
    for task_id, task_events in grouped.items():
        violations.extend(
            _check_task(
                task_id,
                task_events,
                allow_incomplete=allow_incomplete,
                allow_partial=allow_partial,
                policy=active_policy,
            )
        )
    return violations


def _check_task(
    task_id: str,
    events: list[TraceEvent],
    *,
    allow_incomplete: bool,
    allow_partial: bool,
    policy: TracePolicy,
) -> list[Violation]:
    violations: list[Violation] = []
    terminals = [e for e in events if e.kind in policy.terminal_kinds]
    starts = [e for e in events if e.kind == "started"]
    retries = [e for e in events if e.kind == "retried"]

    if len(terminals) > 1:
        first = terminals[0]
        violations.append(
            Violation(
                "terminal_duplicate",
                f"task has {len(terminals)} terminal events: "
                + ", ".join(e.kind for e in terminals),
                task_id=task_id,
                node=first.node,
                seq=first.seq,
            )
        )

    if terminals and not starts and not allow_partial:
        first = terminals[0]
        violations.append(
            Violation(
                "terminal_without_start",
                f"terminal event {first.kind!r} without a preceding started",
                task_id=task_id,
                node=first.node,
                seq=first.seq,
            )
        )

    if not terminals and not allow_incomplete:
        first = events[0]
        violations.append(
            Violation(
                "not_terminal",
                "task has no terminal event",
                task_id=task_id,
                node=first.node,
                seq=first.seq,
            )
        )

    # 同一节点内：terminal 之后不应再出现 started（除非有 retried 解释）。
    by_node: dict[str, list[TraceEvent]] = defaultdict(list)
    for event in events:
        by_node[event.node].append(event)
    for node, node_events in by_node.items():
        terminal_seen = False
        for event in sorted(node_events, key=lambda e: e.node_seq):
            if event.kind in policy.terminal_kinds:
                terminal_seen = True
            elif event.kind == "started" and terminal_seen:
                violations.append(
                    Violation(
                        "started_after_terminal",
                        "started observed after terminal on the same node",
                        task_id=task_id,
                        node=node,
                        seq=event.seq,
                    )
                )

    # 无 retried 事件时，多次 started 视为重复执行。
    if len(starts) > 1 and not retries:
        first = starts[0]
        violations.append(
            Violation(
                "duplicate_execution",
                f"task started {len(starts)} times without a retried event",
                task_id=task_id,
                node=first.node,
                seq=first.seq,
            )
        )

    for retry in retries:
        if retry.next_attempt <= retry.attempt:
            violations.append(
                Violation(
                    "retry_order",
                    f"retried next_attempt={retry.next_attempt} <= attempt={retry.attempt}",
                    task_id=task_id,
                    node=retry.node,
                    seq=retry.seq,
                )
            )

    return violations


def check_duplicate_execution(
    trace: TraceRecorder | Iterable[TraceEvent],
    *,
    allow_partial: bool | None = None,
    policy: TracePolicy | None = None,
) -> list[Violation]:
    """只检查重复执行相关不变量。"""
    return [
        v
        for v in check_trace(
            trace,
            allow_incomplete=True,
            allow_partial=allow_partial,
            policy=policy,
        )
        if v.code == "duplicate_execution"
    ]


def check_cross_node_duplicates(
    traces: Iterable[TraceRecorder | Iterable[TraceEvent]],
    *,
    policy: TracePolicy | None = None,
) -> list[Violation]:
    """合并多个节点的 trace，检查同一任务是否被重复执行。

    单节点 trace 只能看到本节点视角；P2P 场景下重复执行通常表现为
    "同一 task_id 在两个节点各有一个 started"。合并后仍要求有
    ``retried`` 事件解释多次 started，否则视为跨节点重复执行。
    """
    items = list(traces)
    active_policy = policy or next(
        (item.policy for item in items if isinstance(item, TraceRecorder)),
        DEFAULT_TRACE_POLICY,
    )
    merged: list[TraceEvent] = []
    for item in items:
        merged.extend(item.events if isinstance(item, TraceRecorder) else list(item))
    return [
        v
        for v in check_trace(
            merged,
            allow_incomplete=True,
            allow_partial=True,
            policy=active_policy,
        )
        if v.code == "duplicate_execution"
    ]


def assert_single_execution(
    traces: Iterable[TraceRecorder | Iterable[TraceEvent]],
    *,
    policy: TracePolicy | None = None,
) -> None:
    """断言跨节点视角下每个任务最多执行一次（重试除外）。"""
    violations = check_cross_node_duplicates(traces, policy=policy)
    if violations:
        raise InvariantViolation(violations)


def assert_invariants(
    trace: TraceRecorder | Iterable[TraceEvent],
    *,
    allow_incomplete: bool = True,
    allow_partial: bool | None = None,
    policy: TracePolicy | None = None,
) -> None:
    """断言全部不变量成立；失败抛 ``InvariantViolation``。"""
    violations = check_trace(
        trace,
        allow_incomplete=allow_incomplete,
        allow_partial=allow_partial,
        policy=policy,
    )
    if violations:
        raise InvariantViolation(violations)
