"""驱动协议：``host_flows`` 委托可注入的 ``FlowDriver``，语义不变。"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from actant import Runtime, flow, host_flows, start_flow, task, use_runtime
from actant.driver import BlockingDriver, FlowDriver


@task(name="driver_unit_add_one")
def _add_one(value: int) -> int:
    return value + 1


@flow(name="driver-unit-injected")
def _injected_flow(value: int) -> int:
    return _add_one.submit(value).result()  # type: ignore[no-any-return]


class _RecordingDriver:
    """记录被驱动的工作流，并委托同步执行。"""

    def __init__(self) -> None:
        self.spawned: list[str] = []
        self._delegate = BlockingDriver()

    def spawn(self, workflow_id: str, run: Callable[[], None]) -> bool:
        self.spawned.append(workflow_id)
        return self._delegate.spawn(workflow_id, run)

    def join(self) -> None:
        self._delegate.join()


def test_blocking_driver_executes_callable_synchronously() -> None:
    seen: list[str] = []

    BlockingDriver().spawn("wf-1", lambda: seen.append("ran"))

    assert seen == ["ran"]


def test_blocking_driver_propagates_error() -> None:
    def _boom() -> None:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        BlockingDriver().spawn("wf-1", _boom)


def test_host_flows_delegates_to_injected_driver() -> None:
    recorder = _RecordingDriver()
    driver: FlowDriver = recorder
    with Runtime() as rt, use_runtime(rt):
        handle = start_flow("driver-unit-injected", 41, runtime=rt)
        driven = host_flows(runtime=rt, once=True, driver=driver)
        assert rt.get_workflow_result(handle.workflow_id) == 42

    assert recorder.spawned == [handle.workflow_id]
    assert driven == [handle.workflow_id]


def test_driver_error_propagates_from_host_flows() -> None:
    class _FailingDriver:
        def spawn(self, workflow_id: str, run: Callable[[], None]) -> bool:
            raise RuntimeError("driver failed")

        def join(self) -> None:
            return None

    with Runtime() as rt, use_runtime(rt):
        start_flow("driver-unit-injected", 1, runtime=rt)
        with pytest.raises(RuntimeError, match="driver failed"):
            host_flows(runtime=rt, once=True, driver=_FailingDriver())
