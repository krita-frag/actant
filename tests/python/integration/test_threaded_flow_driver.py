"""``ThreadedDriver``：大量 flow 可同时 park，容量超限显式报错。"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

import pytest

from actant import Runtime, flow, host_flows, start_flow, task, use_runtime
from actant.driver import ThreadedDriver
from actant.exceptions import InvalidStateError
from actant.workflow import current_workflow_id, wait_signal

_LOCK = threading.Lock()
_PARKED: list[str] = []


@task(name="threaded_driver_noop")
def _noop(value: int) -> int:
    return value


@flow(name="threaded-driver-park")
def _park_flow(index: int) -> str:
    """完成一个节点后 park，验证多个 flow 能同时驻留在等待点。"""
    _noop.submit(index).result()
    workflow_id = current_workflow_id()
    with _LOCK:
        _PARKED.append(workflow_id)
    payload = wait_signal("go", timeout_ms=60_000)
    return payload.decode() if payload else "timeout"


@flow(name="threaded-driver-hold")
def _hold_flow() -> str:
    """不提交任务，直接 park；用于确定性地占住驱动线程。"""
    with _LOCK:
        _PARKED.append(current_workflow_id())
    payload = wait_signal("go", timeout_ms=60_000)
    return payload.decode() if payload else "timeout"


def _wait_until(predicate: Callable[[], bool], *, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


@pytest.mark.parametrize("flows", [50, pytest.param(1000, marks=pytest.mark.slow)])
def test_flows_park_concurrently(flows: int) -> None:
    """所有 flow 同时进入等待点，而不是被一个 park 的 flow 串行阻塞。"""
    _PARKED.clear()
    driver = ThreadedDriver(max_workers=flows)
    with (
        Runtime(max_concurrent_tasks=min(flows, 64)) as rt,
        use_runtime(rt),
    ):
        handles = [
            start_flow("threaded-driver-park", index, runtime=rt)
            for index in range(flows)
        ]
        driven = host_flows(runtime=rt, once=True, driver=driver)
        assert len(driven) == flows
        _wait_until(lambda: len(_PARKED) >= flows, timeout=90, what="all flows parked")
        assert threading.active_count() <= flows + 64, "线程数随 flow 数失控增长"

        for handle in handles:
            rt.signal_wait_point(handle.workflow_id, "go", b"go")
        driver.join()

        for handle in handles:
            assert rt.get_workflow_result(handle.workflow_id) == "go"


def test_capacity_overflow_raises_and_keeps_running_flows_clean() -> None:
    """并发数超过 ``max_workers`` 时显式报错，不无限建线程。"""
    _PARKED.clear()
    driver = ThreadedDriver(max_workers=2)
    with Runtime() as rt, use_runtime(rt):
        handles = [
            start_flow("threaded-driver-hold", runtime=rt).workflow_id
            for _ in range(3)
        ]
        with pytest.raises(InvalidStateError, match="max_workers"):
            host_flows(runtime=rt, once=True, driver=driver)

        _wait_until(lambda: len(_PARKED) >= 2, timeout=30, what="two flows parked")
        for handle in handles:
            rt.signal_wait_point(handle, "go", b"go")
        driver.join()
