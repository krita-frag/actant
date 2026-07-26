"""基准：单任务端到端调度延迟。

测量 ``task.submit(args).result()`` 端到端延迟，包括：
- Python 端 payload 序列化（cloudpickle）
- Rust 调度器 enqueue → dequeue
- Python handler 反序列化 + 调用
- 结果回传 + Future 完成

理论基线：
- Ray: ~1.2ms (单机 in-process)
- Celery: ~5-15ms (Redis broker)
- Dask distributed: ~1-2ms
- actant 目标：单机零负载任务 < 1ms
"""

from __future__ import annotations

import pytest


@pytest.mark.benchmark(group="task_dispatch", min_rounds=50, warmup=10)
def test_bench_single_task_dispatch(benchmark, runtime, noop_task):
    """单任务 submit→result 端到端延迟。"""
    def run():
        h = noop_task.submit()
        return h.result(timeout=5)
    result = benchmark(run)
    assert result == 42


@pytest.mark.benchmark(group="task_dispatch", min_rounds=20, warmup=5)
def test_bench_task_with_args(benchmark, runtime, add_task):
    """带参数任务调度延迟。"""
    def run():
        h = add_task.submit(3, 4)
        return h.result(timeout=5)
    result = benchmark(run)
    assert result == 7


@pytest.mark.benchmark(group="task_dispatch", min_rounds=20, warmup=5)
def test_bench_silent_task_dispatch(benchmark, runtime):
    """silent 任务（跳过事件发布）调度延迟。"""
    @actant.task(silent=True)
    def _silent() -> int:
        return 42

    def run():
        h = _silent.submit()
        return h.result(timeout=5)
    result = benchmark(run)
    assert result == 42


@pytest.mark.benchmark(group="task_dispatch", min_rounds=10, warmup=3)
def test_bench_direct_call(benchmark, runtime, noop_task):
    """直接调用基准（Python 同步调用开销，无调度）。"""
    def run():
        return noop_task()
    result = benchmark(run)
    assert result == 42


# 必须在测试函数之后（fixture 已 import actant）
import actant  # noqa: E402
