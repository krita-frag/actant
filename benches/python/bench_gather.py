"""基准：``actant.gather`` 并行等待吞吐。

测量并行等待 N 个 AsyncResult 的端到端延迟：
- 单个 result() 等待 N 次（串行基线）
- gather() 等待 N 次（intrusively-linked futures 优化）

理论基线：
- asyncio.gather: 100 个 future ≈ 0.5ms
- Ray.get: 100 个 object refs ≈ 1.5ms
- actant.gather 目标：100 个 handle ≈ 1ms
"""

from __future__ import annotations

import pytest

import actant


@pytest.mark.benchmark(group="gather", min_rounds=10, warmup=3)
def test_bench_gather_10(benchmark, runtime, noop_task):
    """gather 10 个任务端到端延迟。"""
    def run():
        handles = [noop_task.submit() for _ in range(10)]
        return actant.gather(*handles, timeout=10)
    result = benchmark(run)
    assert len(result) == 10


@pytest.mark.benchmark(group="gather", min_rounds=5, warmup=2)
def test_bench_gather_100(benchmark, runtime, noop_task):
    """gather 100 个任务端到端延迟。"""
    def run():
        handles = [noop_task.submit() for _ in range(100)]
        return actant.gather(*handles, timeout=20)
    result = benchmark(run)
    assert len(result) == 100


@pytest.mark.benchmark(group="gather", min_rounds=3, warmup=1)
def test_bench_sequential_result_100(benchmark, runtime, noop_task):
    """串行 result() 100 次基线，用于对比 gather 加速比。"""
    def run():
        handles = [noop_task.submit() for _ in range(100)]
        return [h.result(timeout=10) for h in handles]
    result = benchmark(run)
    assert len(result) == 100


@pytest.mark.benchmark(group="gather", min_rounds=3, warmup=1)
def test_bench_gather_with_slow_tasks(benchmark, runtime, slow_task):
    """gather 10 个 50ms 慢任务：应 ~50ms 而非 ~500ms。"""
    def run():
        handles = [slow_task.submit(50) for _ in range(10)]
        return actant.gather(*handles, timeout=10)
    result = benchmark(run)
    assert len(result) == 10
