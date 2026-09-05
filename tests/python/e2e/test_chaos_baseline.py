"""混沌与压力基线 e2e（H2）：打满队列 / 大 fan-out / 慢消费者。

单节点、真实 worker 子进程池，验证并记录压力场景下的**行为契约**：
1. ``test_queue_saturation_backpressure_zero_loss``：提交量远超
   ``max_concurrent_tasks``，背压（信号量）生效，零丢失全部完成。
2. ``test_large_fanout_throughput``：200 任务 fan-out/fan-in，全部正确
   聚合；吞吐与内存作为 sideline 观测值打印（不作为断言）。
3. ``test_slow_consumer_no_loss``：慢任务占满 worker，后续任务排队
   不丢、慢任务结束后全部收敛。

断言只覆盖 SLA 契约（正确性、零丢失、最终收敛）；吞吐/内存为观测值，
写入 ``docs/SLA_BASELINE.md`` 供人工对比，不设阈值门禁。任务函数来自
规范模块 ``tests.python._helpers.reliability_tasks``（原因见其 docstring）。
"""

from __future__ import annotations

import os
import platform
import resource
import time
from typing import TYPE_CHECKING

import pytest

import actant
from actant.task import gather, task
from tests.python._helpers import reliability_tasks as rt_tasks

if TYPE_CHECKING:
    from collections.abc import Iterator

    from actant._runtime import Runtime

# 用例总时长上限（秒）。
CASE_BUDGET_S = 60.0

_noop = task(rt_tasks.noop)
_quick = task(rt_tasks.quick)
_sleep_mult = task(rt_tasks.sleep_mult)


def _rss_peak_mib() -> float:
    """进程峰值 RSS（MiB）。macOS 单位为字节，Linux 为 KiB。"""
    raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    scale = 1 if platform.system() == "Darwin" else 1024
    return raw * scale / (1024 * 1024)


def _env_line() -> str:
    return (
        f"platform={platform.platform()} python={platform.python_version()} "
        f"cpus={os.cpu_count()}"
    )


@pytest.fixture
def single_node(tmp_path) -> Iterator[Runtime]:
    """单节点 Runtime，测试后确保停止（避免残留 worker 子进程）。"""
    rt = actant.Runtime.with_defaults(
        name="chaos-node",
        data_dir=str(tmp_path / "data"),
        max_concurrent_tasks=2,
    )
    rt.start()
    try:
        yield rt
    finally:
        rt.stop()


@pytest.fixture
def default_node(tmp_path) -> Iterator[Runtime]:
    """默认并发度的单节点 Runtime（max_concurrent_tasks = CPU 核数）。"""
    rt = actant.Runtime.with_defaults(name="chaos-node-wide", data_dir=str(tmp_path / "data"))
    rt.start()
    try:
        yield rt
    finally:
        rt.stop()


class TestQueueSaturation:
    """打满队列：提交量远超并发度，背压生效、零丢失。"""

    def test_queue_saturation_backpressure_zero_loss(self, single_node) -> None:
        """50x 超额提交（并发度 2，提交 100），全部完成且结果零丢失。

        SLA 契约：背压排队不丢弃任务——超额提交只增加延迟，不产生失败。
        """
        started = time.monotonic()
        with actant.use_runtime(single_node):
            handles = [_noop.submit() for _ in range(100)]
            results = gather(*handles, timeout=55.0)
        elapsed = time.monotonic() - started
        assert elapsed < CASE_BUDGET_S
        # 零丢失：100/100 返回且值正确（无静默丢任务/错位）。
        assert results == [42] * 100, (
            f"backpressure must not drop or corrupt tasks, got {len(results)} results"
        )
        ops = 100 / elapsed
        print(
            f"\n[chaos-baseline] queue_saturation: n=100 concurrency=2 "
            f"elapsed={elapsed:.2f}s ops={ops:.1f}/s"
        )


class TestLargeFanout:
    """大 fan-out：单节点 200 任务 fan-out/fan-in。"""

    def test_large_fanout_throughput(self, default_node) -> None:
        """200 个 noop 任务一次性提交，全部正确聚合。

        吞吐与内存峰值是 sideline 观测值：打印供 docs/SLA_BASELINE.md
        记录，不参与断言（CI 机器噪声大，阈值门禁待基线稳定后再加）。
        """
        rss_before = _rss_peak_mib()
        started = time.monotonic()
        with actant.use_runtime(default_node):
            handles = [_noop.submit() for _ in range(200)]
            results = gather(*handles, timeout=55.0)
        elapsed = time.monotonic() - started
        assert elapsed < CASE_BUDGET_S
        assert results == [42] * 200, (
            f"fan-out/fan-in must aggregate all results losslessly, got {len(results)}"
        )
        rss_after = _rss_peak_mib()
        ops = 200 / elapsed
        print(
            f"\n[chaos-baseline] large_fanout: n=200 concurrency=default "
            f"elapsed={elapsed:.2f}s ops={ops:.1f}/s "
            f"rss_peak={rss_after:.1f}MiB (delta={rss_after - rss_before:+.1f}) "
            f"| {_env_line()}"
        )


class TestSlowConsumer:
    """慢消费者：慢任务占满 worker，后续任务排队不丢。"""

    def test_slow_consumer_no_loss(self, single_node) -> None:
        """2 个慢任务占满并发度 2 的 worker，20 个快速任务排队等待。

        SLA 契约：排队任务在慢任务结束后全部收敛，零丢失；
        快速任务的排队等待时间 ≈ 慢任务时长（1 个排队批次深度）。
        """
        slow_s = 1.5
        started = time.monotonic()
        with actant.use_runtime(single_node):
            # 先提交 2 个慢任务占满 worker，再提交 20 个快速任务排队。
            slow = [
                _sleep_mult.submit(i, slow_s) for i in range(2)
            ]
            queued = [_quick.submit(i) for i in range(20)]
            slow_results = gather(*slow, timeout=55.0)
            queued_results = gather(*queued, timeout=55.0)
        elapsed = time.monotonic() - started
        assert elapsed < CASE_BUDGET_S
        assert slow_results == [0, 10]
        assert queued_results == [i * 2 for i in range(20)], (
            "queued tasks behind slow consumers must complete without loss"
        )
        print(
            f"\n[chaos-baseline] slow_consumer: slow=2x({slow_s}s) queued=20 "
            f"concurrency=2 elapsed={elapsed:.2f}s"
        )
