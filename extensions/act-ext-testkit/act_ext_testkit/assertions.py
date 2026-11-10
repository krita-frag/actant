"""事件记录与追踪驱动断言。

``TraceRecorder`` 负责事件采集；本模块把生命周期不变量与收敛条件转化为
可读失败信息。断言只做"最终一致"等待，不对事件到达顺序做强保证。
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from .cluster import wait_until as testkit_wait_until
from .invariants import (
    InvariantViolation,
    Violation,
    assert_invariants,
    check_duplicate_execution,
)
from .trace import (
    DEFAULT_TRACE_POLICY,
    TERMINAL_KINDS,
    TraceEvent,
    TracePolicy,
    TraceRecorder,
)


def assert_eventually(
    predicate: Callable[[], bool],
    *,
    timeout_s: float = 10.0,
    description: str = "condition",
    interval_s: float = 0.2,
) -> None:
    """等待 ``predicate`` 为真；超时抛 ``ClusterError``。"""
    testkit_wait_until(
        predicate,
        timeout_s=timeout_s,
        description=description,
        interval_s=interval_s,
    )


# ----------------------------------------------------------------------
# 追踪驱动断言
# ----------------------------------------------------------------------


def assert_no_duplicate_execution(
    trace: TraceRecorder | Iterable[TraceEvent],
    *,
    allow_partial: bool | None = None,
) -> None:
    """断言没有"无 retried 解释的重复 started"。"""
    violations = check_duplicate_execution(trace, allow_partial=allow_partial)
    if violations:
        raise InvariantViolation(violations)


def assert_lifecycle_valid(
    trace: TraceRecorder | Iterable[TraceEvent],
    *,
    allow_incomplete: bool = True,
    allow_partial: bool | None = None,
) -> None:
    """断言生命周期不变量全部成立。"""
    assert_invariants(
        trace,
        allow_incomplete=allow_incomplete,
        allow_partial=allow_partial,
    )


def assert_all_terminal(
    trace: TraceRecorder | Iterable[TraceEvent],
    task_ids: Iterable[str] | None = None,
    *,
    kinds: frozenset[str] = TERMINAL_KINDS,
    policy: TracePolicy = DEFAULT_TRACE_POLICY,
) -> None:
    """断言指定任务（默认全部）恰好有一个终态事件。"""
    events = trace.events if isinstance(trace, TraceRecorder) else list(trace)
    ids = list(task_ids) if task_ids is not None else _task_ids(events, policy)
    problems: list[str] = []
    for task_id in ids:
        terminals = [e for e in events if e.task_id == task_id and e.kind in kinds]
        if len(terminals) == 0:
            problems.append(f"{task_id}: no terminal event")
        elif len(terminals) > 1:
            problems.append(
                f"{task_id}: {len(terminals)} terminal events "
                f"({', '.join(e.kind for e in terminals)})"
            )
    if problems:
        raise AssertionError("terminal state assertions failed:\n  - " + "\n  - ".join(problems))


def assert_trace_order(
    trace: TraceRecorder | Iterable[TraceEvent],
    *,
    allow_partial: bool | None = None,
    policy: TracePolicy = DEFAULT_TRACE_POLICY,
) -> None:
    """断言同一任务至少有一个 started 早于终态（全局 seq 顺序）。

    ``allow_partial`` 省略时沿用 ``TraceRecorder.partial``：观察者视角下
    远端任务只有终态，缺少 started 不算顺序违规。
    """
    if allow_partial is None:
        allow_partial = trace.partial if isinstance(trace, TraceRecorder) else False
    events = trace.events if isinstance(trace, TraceRecorder) else list(trace)
    violations: list[Violation] = []
    for task_id in _task_ids(events, policy):
        task_events = [e for e in events if e.task_id == task_id]
        starts = [e for e in task_events if e.kind == "started"]
        terminals = [e for e in task_events if e.kind in policy.terminal_kinds]
        if not terminals:
            continue
        if not starts:
            if allow_partial:
                continue
            terminal = terminals[0]
            violations.append(
                Violation(
                    "terminal_without_start",
                    "terminal before any started",
                    task_id=task_id,
                    node=terminal.node,
                    seq=terminal.seq,
                )
            )
            continue
        first_start = min(e.seq for e in starts)
        for terminal in terminals:
            if terminal.seq < first_start:
                violations.append(
                    Violation(
                        "terminal_before_start",
                        "terminal observed before first started",
                        task_id=task_id,
                        node=terminal.node,
                        seq=terminal.seq,
                    )
                )
    if violations:
        raise InvariantViolation(violations)


def assert_converged(
    cluster: Any,
    *,
    timeout_s: float = 20.0,
    expected_labels: dict[str, dict[str, str]] | None = None,
    expected_peers: dict[str, set[str]] | None = None,
) -> None:
    """断言集群在超时内收敛到预期 peer 集。"""
    cluster.wait_converged(
        timeout_s=timeout_s,
        expected_labels=expected_labels,
        expected_peers=expected_peers,
    )


def _task_ids(
    events: Iterable[TraceEvent],
    policy: TracePolicy = DEFAULT_TRACE_POLICY,
) -> list[str]:
    seen: dict[str, None] = {}
    for event in events:
        if event.kind in policy.lifecycle_kinds and event.task_id:
            seen.setdefault(event.task_id, None)
    return list(seen)
