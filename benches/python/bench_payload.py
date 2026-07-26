"""基准：不同 payload 大小对调度延迟的影响。

测量任务输入/输出大小对端到端延迟的影响：
- 输入 0 / 1KB / 10KB / 100KB / 1MB
- 输出 0 / 1KB / 10KB / 100KB / 1MB

理论基线：
- cloudpickle 序列化 1MB ≈ 5ms
- 跨 worker 传输 1MB ≈ 1-2ms（loopback）
- actant 目标：100KB 输入总延迟 < 3ms
"""

from __future__ import annotations

import pytest

import actant


@pytest.fixture
def echo_task():
    """回显任务：返回输入。"""
    @actant.task
    def _echo(data: bytes) -> bytes:
        return data
    return _echo


@pytest.mark.parametrize("size", [0, 1024, 10 * 1024, 100 * 1024])
@pytest.mark.benchmark(group="payload", min_rounds=10, warmup=3)
def test_bench_payload_input_size(benchmark, runtime, echo_task, size):
    """不同输入 payload 大小对调度延迟的影响。"""
    data = b"x" * size

    def run():
        h = echo_task.submit(data)
        return h.result(timeout=10)
    result = benchmark(run)
    assert len(result) == size


@pytest.mark.parametrize("size", [0, 1024, 10 * 1024, 100 * 1024])
@pytest.mark.benchmark(group="payload", min_rounds=10, warmup=3)
def test_bench_payload_output_size(benchmark, runtime, size):
    """不同输出 payload 大小对调度延迟的影响。"""
    @actant.task
    def _make(size: int) -> bytes:
        return b"x" * size

    def run():
        h = _make.submit(size)
        return h.result(timeout=10)
    result = benchmark(run)
    assert len(result) == size
