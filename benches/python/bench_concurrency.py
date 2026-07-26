"""基准：并发任务数扩展性。

测量不同规模并发任务的端到端延迟：
- 10 / 100 / 1000 个 noop 任务
- 串行 submit + gather 等待所有完成

理论基线：
- Ray 1000 个 noop 任务：~50ms
- Dask 1000 个 noop 任务：~30ms
- actant 目标：1000 个 noop 任务 < 100ms（含 Python 序列化开销）
"""

from __future__ import annotations

import pytest

import actant


@pytest.mark.parametrize("n", [10, 100, 1000])
@pytest.mark.benchmark(group="concurrency", min_rounds=3, warmup=1)
def test_bench_concurrent_noop(benchmark, runtime, n):
    """N 个 noop 任务并发执行（gather）。"""
    @actant.task
    def _noop() -> int:
        return 42

    def run():
        handles = [_noop.submit() for _ in range(n)]
        return actant.gather(*handles, timeout=30)
    result = benchmark(run)
    assert len(result) == n


@pytest.mark.parametrize("n", [10, 100, 1000])
@pytest.mark.benchmark(group="concurrency", min_rounds=3, warmup=1)
def test_bench_concurrent_silent(benchmark, runtime, n):
    """N 个 silent noop 任务并发执行（对比 silent 提升比）。"""
    @actant.task(silent=True)
    def _silent() -> int:
        return 42

    def run():
        handles = [_silent.submit() for _ in range(n)]
        return actant.gather(*handles, timeout=30)
    result = benchmark(run)
    assert len(result) == n


@pytest.mark.parametrize("n", [10, 100, 1000])
@pytest.mark.benchmark(group="concurrency", min_rounds=3, warmup=1)
def test_bench_serial_submit_result(benchmark, runtime, n):
    """N 个任务串行 submit+result（基线，无 gather 优化）。"""
    @actant.task
    def _noop() -> int:
        return 42

    def run():
        for _ in range(n):
            h = _noop.submit()
            h.result(timeout=5)
    benchmark(run)
