"""``actant._internal.tasks.task`` 单元测试（不启动 Runtime）。"""
from __future__ import annotations

import pytest

from actant.exceptions import InvalidStateError
from actant.tasks import AsyncResult, Task, task


class _CapturingCore:
    def __init__(self) -> None:
        self.submitted: object | None = None

    def submit_task(self, task_def: object) -> None:
        self.submitted = task_def


class _FakeRuntime:
    def __init__(self) -> None:
        self._rust_core = _CapturingCore()
        self.registered: dict[str, AsyncResult] = {}

    def _register_task(self, task_id: str, handle: AsyncResult) -> None:
        self.registered[task_id] = handle

    def _unregister_task(self, task_id: str) -> None:
        self.registered.pop(task_id, None)


def test_task_call_runs_original_function() -> None:
    @task
    def add(a: int, b: int) -> int:
        return a + b

    assert add(1, 2) == 3


def test_task_preserves_metadata() -> None:
    @task
    def sample() -> int:
        """docstring"""
        return 1

    assert sample.__name__ == "sample"
    assert sample.__doc__ == "docstring"
    assert isinstance(sample, Task)


def test_task_name_defaults_to_qualified_name() -> None:
    @task
    def sample() -> None:
        return None

    assert "test_task_obj" in sample.name
    assert "sample" in sample.name


def test_task_name_override() -> None:
    @task(name="custom-name")  # type: ignore[untyped-decorator]
    def sample() -> None:
        return None

    assert sample.name == "custom-name"


def test_task_repr() -> None:
    @task
    def sample() -> None:
        return None

    assert "Task" in repr(sample)
    assert "sample" in repr(sample)


def test_task_submit_requires_runtime() -> None:
    @task
    def sample() -> None:
        return None

    with pytest.raises(InvalidStateError):
        sample.submit()


def test_task_submit_to_sets_remote_target(monkeypatch: pytest.MonkeyPatch) -> None:
    from actant._internal.tasks import task as task_mod

    fake_runtime = _FakeRuntime()
    monkeypatch.setattr(task_mod, "get_current_runtime", lambda: fake_runtime)

    @task
    def add(a: int, b: int) -> int:
        return a + b

    handle = add.submit_to("worker-node", 1, 2, endpoint_addr="peer-id")
    submitted = fake_runtime._rust_core.submitted

    assert isinstance(handle, AsyncResult)
    assert submitted is not None
    assert submitted.target_node == "worker-node"
    assert submitted.target_endpoint_addr == "peer-id"


def test_task_options_preserved() -> None:
    @task(timeout_ms=1000, retries=2, retry_delay_ms=500, tags=["io"], priority=5)  # type: ignore[untyped-decorator]
    def sample() -> None:
        return None

    assert sample._timeout_ms == 1000
    assert sample._retries == 2
    assert sample._retry_delay_ms == 500
    assert sample._tags == ["io"]
    assert sample._priority == 5
