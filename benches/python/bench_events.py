"""基准：任务生命周期事件发布开销。

测量：
- 普通任务（发布 TaskLifecycle 事件）vs silent 任务（跳过事件）

理论基线：直接 emit 单事件 ≈ 5-20μs。事件批处理由 ``Runtime.start()``
统一启用，不再是用户可切换的开关。
"""

from __future__ import annotations

import pytest

import actant


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
