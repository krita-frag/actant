"""基准：事件发布批量化与 ``silent`` 跳过开销。

测量：
- 普通任务（发布 TaskLifecycle 事件）vs silent 任务（跳过事件）
- 事件批量化开关对吞吐的影响

理论基线：
- 直接 emit 单事件 ≈ 5-20μs（thread-local dispatch）
- 批量化 100 事件 ≈ 1ms（10x 提升）
"""

from __future__ import annotations

import pytest

import actant
from actant.task._helpers import _EventBatcher, _EventBatcherScope


@pytest.mark.benchmark(group="events", min_rounds=20, warmup=5)
def test_bench_normal_task_events(benchmark, runtime):
    """普通任务（每个 emit started/completed 事件）100 次。"""
    @actant.task
    def _normal() -> int:
        return 42

    def run():
        for _ in range(100):
            h = _normal.submit()
            h.result(timeout=5)
    benchmark(run)


@pytest.mark.benchmark(group="events", min_rounds=20, warmup=5)
def test_bench_silent_task_events(benchmark, runtime):
    """silent 任务（跳过事件）100 次。"""
    @actant.task(silent=True)
    def _silent() -> int:
        return 42

    def run():
        for _ in range(100):
            h = _silent.submit()
            h.result(timeout=5)
    benchmark(run)


@pytest.mark.benchmark(group="events", min_rounds=10, warmup=3)
def test_bench_event_batching_on(benchmark, runtime):
    """开启事件批量化 100 个任务。"""
    @actant.task
    def _batched() -> int:
        return 42

    def run():
        # 使用 _EventBatcherScope 作用域批量化
        with _EventBatcherScope(flush_interval_ms=1, flush_threshold=100):
            for _ in range(100):
                h = _batched.submit()
                h.result(timeout=5)
    benchmark(run)


@pytest.mark.benchmark(group="events", min_rounds=10, warmup=3)
def test_bench_event_batching_off(benchmark, runtime):
    """关闭事件批量化 100 个任务（基线）。"""
    @actant.task
    def _unbatched() -> int:
        return 42

    def run():
        for _ in range(100):
            h = _unbatched.submit()
            h.result(timeout=5)
    benchmark(run)
