"""基准：``@flow`` 命令式编排执行延迟。

测量 fan-out / fan-in 模式 flow 的端到端延迟：N 个并行任务 + gather。
"""

from __future__ import annotations

import time

import pytest

import actant


@pytest.fixture
def fanout_imperative():
    """命令式 fan-out/fan-in flow：N 个并行任务 + gather。"""
    @actant.task
    def _compute(x: int) -> int:
        # 轻量计算（10ms 模拟 IO）
        time.sleep(0.01)
        return x * 2

    @actant.flow
    def _flow(n: int) -> list[int]:
        handles = [_compute.submit(i) for i in range(n)]
        return actant.gather(*handles, timeout=10)

    return _flow, _compute


@pytest.mark.benchmark(group="flow", min_rounds=5, warmup=2)
def test_bench_flow_imperative_10(benchmark, runtime, fanout_imperative):
    """命令式 flow：10 个并行任务。"""
    flow, _ = fanout_imperative

    def run():
        return flow(10)
    result = benchmark(run)
    assert len(result) == 10


@pytest.mark.benchmark(group="flow", min_rounds=3, warmup=1)
def test_bench_flow_imperative_50(benchmark, runtime, fanout_imperative):
    """命令式 flow：50 个并行任务（扩展性测量）。"""
    flow, _ = fanout_imperative

    def run():
        return flow(50)
    result = benchmark(run)
    assert len(result) == 50
