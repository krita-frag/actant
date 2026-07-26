"""基准：``@flow`` 命令式 vs ``@flow(compiled=True)`` DAG 编译式对比。

测量 fan-out / fan-in 模式 flow 的端到端延迟：
- 命令式：每次执行都按代码顺序 submit
- 编译式：首次 trace 编译为 DAG，后续按拓扑层并行 submit

预期编译式对 IO bound 任务有显著加速（拓扑层内并行）。
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


@pytest.fixture
def fanout_compiled():
    """编译式 fan-out/fan-in flow。"""
    @actant.task
    def _compute(x: int) -> int:
        time.sleep(0.01)
        return x * 2

    @actant.flow(compiled=True)
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


@pytest.mark.benchmark(group="flow", min_rounds=5, warmup=2)
def test_bench_flow_compiled_10(benchmark, runtime, fanout_compiled):
    """编译式 flow：10 个并行任务（首调编译 + 后续复用）。"""
    flow, _ = fanout_compiled

    # 预热：触发编译，后续 benchmark 测量缓存命中路径
    flow(10)

    def run():
        return flow(10)
    result = benchmark(run)
    assert len(result) == 10


@pytest.mark.benchmark(group="flow", min_rounds=3, warmup=1)
def test_bench_flow_first_compile(benchmark, runtime, fanout_compiled):
    """编译式 flow 首次调用：包含 trace + 编译开销。

    每次重新定义 flow 以触发首次编译，测量编译开销。
    """
    @actant.task
    def _compute(x: int) -> int:
        time.sleep(0.001)
        return x * 2

    def run():
        @actant.flow(compiled=True)
        def _flow(n: int) -> list[int]:
            handles = [_compute.submit(i) for i in range(n)]
            return actant.gather(*handles, timeout=10)
        return _flow(5)

    result = benchmark(run)
    assert len(result) == 5
