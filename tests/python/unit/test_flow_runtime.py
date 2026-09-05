"""``actant.flow`` 运行时单元测试（依赖 Runtime）。"""
from __future__ import annotations

import importlib
import time

import pytest

from actant import Runtime, flow, task
from actant.exceptions import ActantTimeoutError, InvalidStateError

# `actant.flow` 属性被 actant/__init__ 的 re-export 覆盖为装饰器函数，
# 需经 importlib 取模块对象。
_flow_module = importlib.import_module("actant.flow")


@task
def _add_one(x: int) -> int:
    return x + 1


@task
def _slow_task() -> str:
    import time

    time.sleep(2)
    return "done"


_flow_retry_count: dict[str, int] = {"count": 0}


def _bump_flow_retry() -> None:
    _flow_retry_count["count"] += 1


def test_flow_requires_runtime() -> None:
    @flow
    def my_flow() -> None:
        return None

    with pytest.raises(InvalidStateError):
        my_flow()


def test_flow_success() -> None:
    @flow
    def my_flow(x: int) -> int:
        return _add_one.submit(x).result()  # type: ignore[no-any-return]

    with Runtime.with_defaults() as rt:
        events: list[str] = []
        rt.chain("WorkflowLifecycle", lambda e: events.append(e.kind))
        assert my_flow(5) == 6
    assert "submitted" in events
    assert "started" in events
    assert "completed" in events


def test_flow_failure() -> None:
    @flow
    def my_flow() -> None:
        raise ValueError("boom")

    with Runtime.with_defaults() as rt:
        events: list[str] = []
        rt.chain("WorkflowLifecycle", lambda e: events.append(e.kind))
        with pytest.raises(ValueError, match="boom"):
            my_flow()
    assert "failed" in events


def test_flow_retry_then_success() -> None:
    _flow_retry_count["count"] = 0

    @flow(retries=2, retry_delay_ms=0)  # type: ignore[untyped-decorator]
    def my_flow() -> str:
        _bump_flow_retry()
        if _flow_retry_count["count"] < 3:
            raise RuntimeError("not yet")
        return "ok"

    with Runtime.with_defaults():
        assert my_flow() == "ok"
        assert _flow_retry_count["count"] == 3


def test_flow_timeout() -> None:
    @flow(timeout_ms=100)  # type: ignore[untyped-decorator]
    def my_flow() -> str:
        return _slow_task().result()  # type: ignore[no-any-return]

    with Runtime.with_defaults(), pytest.raises(ActantTimeoutError):
        my_flow()


def test_flow_with_name() -> None:
    @flow(name="custom-flow")  # type: ignore[untyped-decorator]
    def my_flow() -> int:
        return 42

    with Runtime.with_defaults():
        assert my_flow() == 42


def test_flow_no_parentheses() -> None:
    @flow
    def my_flow() -> int:
        return 42

    with Runtime.with_defaults():
        assert my_flow() == 42


def test_flow_invalid_retries() -> None:
    with pytest.raises(ValueError, match="retries"):
        @flow(retries=-1)  # type: ignore[untyped-decorator]
        def my_flow() -> None:
            return None


def test_flow_workflow_id_in_context() -> None:
    from actant.flow import current_workflow_id

    @flow
    def my_flow() -> str | None:
        return current_workflow_id()

    with Runtime.with_defaults():
        wid = my_flow()
        assert wid is not None
        assert "my_flow" in wid


def _spy_submit_dag(
    rt: Runtime,
    captured: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """用 spy 替换 ``rt.submit_dag``，记录 kwargs 后委托原实现。"""
    orig = rt.submit_dag

    def spy(workflow_id: str, nodes: object, edges: object, **kw: object) -> None:
        captured.update(kw)
        orig(workflow_id, nodes, edges, **kw)  # type: ignore[arg-type]

    monkeypatch.setattr(rt, "submit_dag", spy)


def test_flow_failure_strategy_default_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """默认（未传 failure_strategy）不向 Rust 传递该参数。

    Rust 侧 ``submit_dag`` 对 ``None`` 应用 ``FailureStrategy::FailFast``
    默认值，因此默认行为与旧版硬编码 ``"fail_fast"`` 一致。
    """
    captured: dict[str, object] = {}
    with Runtime.with_defaults() as rt:
        _spy_submit_dag(rt, captured, monkeypatch)

        @flow  # type: ignore[untyped-decorator]
        def my_flow() -> int:
            return _add_one.submit(1).result()  # type: ignore[no-any-return]

        assert my_flow() == 2
    assert "failure_strategy" not in captured


def test_flow_failure_strategy_continue_passthrough(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """显式 ``failure_strategy="continue"`` 原样透传给 ``submit_dag``。"""
    captured: dict[str, object] = {}
    with Runtime.with_defaults() as rt:
        _spy_submit_dag(rt, captured, monkeypatch)

        @flow(failure_strategy="continue")  # type: ignore[untyped-decorator]
        def my_flow() -> int:
            return _add_one.submit(1).result()  # type: ignore[no-any-return]

        assert my_flow() == 2
    assert captured["failure_strategy"] == "continue"


def test_flow_timeout_body_not_reexecuted_while_orphan_alive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """超时后孤儿线程未结束时不得重试——流程体不并发重复执行。

    孤儿线程睡眠 1s 远超缩短后的 join 上限（0.2s）：若旧实现直接重试，
    计数会达到 2；修复后放弃等待并直接抛出，函数体只执行一次。
    """
    monkeypatch.setattr(_flow_module, "_FLOW_ORPHAN_JOIN_TIMEOUT_S", 0.2)
    calls = {"count": 0}

    @flow(retries=3, timeout_ms=100)  # type: ignore[untyped-decorator]
    def my_flow() -> str:
        calls["count"] += 1
        time.sleep(1.0)
        return "done"

    with Runtime.with_defaults(), pytest.raises(ActantTimeoutError):
        my_flow()
    assert calls["count"] == 1


def test_flow_timeout_retry_after_orphan_joined() -> None:
    """孤儿线程在 join 上限内正常结束后允许重试（非超时路径语义不变）。"""
    calls = {"count": 0}

    @flow(retries=1, timeout_ms=100)  # type: ignore[untyped-decorator]
    def my_flow() -> str:
        calls["count"] += 1
        time.sleep(0.3)
        return "done"

    with Runtime.with_defaults(), pytest.raises(ActantTimeoutError):
        my_flow()
    # 第一次超时后孤儿线程已结束（0.3s < 5s 上限）→ 重试一次；
    # 第二次超时后重试耗尽，抛 ActantTimeoutError。
    assert calls["count"] == 2
