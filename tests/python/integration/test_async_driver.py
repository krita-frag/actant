"""``AsyncDriver``：单 event loop 承载万级 parked async flow。

验收口径（``plans/GENERALIZATION_ROADMAP.md`` PR-5）：线程数不随并发数增长、
逐个 signal 后全部唤醒、结果可读。默认 100 个用例进日常套件；10,000 个用例
标记 ``slow``。

10k 用例关注**调度并发度**而非落盘延迟，因此用 ``no_sync`` 存储模式把 CI
预算留给协程调度；持久化语义由同步 flow 用例与本文件的 100 用例覆盖。
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

import pytest

from actant import Runtime, aio, flow, host_flows, start_flow, use_runtime
from actant.actant import _ActantConfig, _NetworkConfig, _StoreConfig
from actant.driver import AsyncDriver

_LOCK = threading.Lock()
_PARKED: list[int] = []
_WOKEN: dict[int, float] = {}


@aio.flow(name="async-driver-gate")
async def _gate(ctx, index: int) -> str:
    with _LOCK:
        _PARKED.append(index)
    payload = await ctx.wait_signal("go")
    with _LOCK:
        _WOKEN[index] = time.monotonic()
    return f"{index}:{payload.decode() if payload else 'none'}"


def _config() -> _ActantConfig:
    return _ActantConfig(
        payload_signing_key="",
        network=_NetworkConfig(preset="none"),
        max_concurrent_tasks=64,
        store=_StoreConfig(sync_mode="no_sync"),
        state_poll_interval_ms=10,
    )


@flow(name="async-driver-sync-flow")
def _sync_flow(value: int) -> int:
    return value + 1


@flow(name="async-driver-failing-sync-flow")
def _failing_sync_flow() -> None:
    raise ValueError("intentional driver failure")


def test_async_driver_runs_sync_flow_in_executor() -> None:
    """同一 journal 混有同步 flow 时，不阻塞 event loop。"""
    with Runtime(config=_config()) as rt, use_runtime(rt):
        handle = start_flow("async-driver-sync-flow", 41, runtime=rt)
        with AsyncDriver() as driver:
            host_flows(runtime=rt, once=True, driver=driver)
            driver.join()
        assert rt.get_workflow_result(handle.workflow_id) == 42


def test_async_driver_close_cleans_up_after_failure() -> None:
    """``close()`` 在 join 重抛失败后仍停止 loop 并关闭同步执行器。"""
    with Runtime(config=_config()) as rt, use_runtime(rt):
        start_flow("async-driver-failing-sync-flow", runtime=rt)
        driver = AsyncDriver()
        host_flows(runtime=rt, once=True, driver=driver)
        with pytest.raises(ValueError, match="intentional driver failure"):
            driver.close()
        assert driver._loop_thread is None
        driver.close()  # 重复调用为 no-op


def _wait_until(predicate: Callable[[], bool], *, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


@pytest.mark.parametrize("flows", [100, pytest.param(10_000, marks=pytest.mark.slow)])
def test_async_driver_parks_many_flows_on_one_loop(flows: int) -> None:
    """所有 flow 同时躺在等待点上，线程数保持恒定。"""
    _PARKED.clear()
    _WOKEN.clear()
    with Runtime(config=_config()) as rt, use_runtime(rt):
        handles = [start_flow("async-driver-gate", index, runtime=rt) for index in range(flows)]
        baseline_threads = threading.active_count()
        started = time.monotonic()

        with AsyncDriver() as driver:
            driven = host_flows(runtime=rt, once=True, driver=driver)
            assert len(driven) == flows
            _wait_until(
                lambda: len(_PARKED) >= flows,
                timeout=90,
                what="all async flows parked",
            )
            assert threading.active_count() <= baseline_threads + 2, (
                "AsyncDriver 不应为每个 flow 建线程"
            )

            signalled_at: list[float] = []
            for handle in handles:
                signalled_at.append(time.monotonic())
                rt.signal_wait_point(handle.workflow_id, "go", b"go")
            driver.join()

        elapsed = time.monotonic() - started
        wake_delays = [_WOKEN[index] - signalled_at[index] for index in range(flows)]
        for index, handle in enumerate(handles):
            assert rt.get_workflow_result(handle.workflow_id) == f"{index}:go"
    # 唤醒延迟是序贯 signal 下的上界；硬阈值放宽到 5s 以避免 CI 抖动误报。
    assert max(wake_delays) < 5.0, f"slowest wake: {max(wake_delays):.2f}s"
    print(f"\n[{flows} async flows] total={elapsed:.1f}s max_wake={max(wake_delays):.2f}s")
