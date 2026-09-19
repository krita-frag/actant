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


def test_flow_body_error_does_not_retry() -> None:
    """flow 不重试：函数体抛错直接传播，不重跑。

    重试语义由任务级 ``@task(retries=...)`` 承载（orchestrator 驱动）；
    flow 体本身的失败恢复走续跑/重放。
    """
    _flow_retry_count["count"] = 0

    @flow(retries=2, retry_delay_ms=0)  # type: ignore[untyped-decorator]
    def my_flow() -> str:
        _bump_flow_retry()
        raise RuntimeError("not yet")

    with Runtime.with_defaults(), pytest.raises(RuntimeError, match="not yet"):
        my_flow()
    assert _flow_retry_count["count"] == 1


def test_flow_timeout() -> None:
    """强还原：函数体阻塞在任务等待上 → deadline 到期后取消本地在途任务，
    函数体被唤醒并以 ``ActantTimeoutError`` 呈现给调用方。

    注意必须走 ``submit()``：``@task`` 直接调用是**同步执行**，不产生编排
    节点，工作流不存在，deadline 也就没有宿主。
    """

    @flow(timeout_ms=100)  # type: ignore[untyped-decorator]
    def my_flow() -> str:
        return _slow_task.submit().result()  # type: ignore[no-any-return]

    with Runtime.with_defaults(), pytest.raises(ActantTimeoutError) as excinfo:
        my_flow()

    # 抛错源必须是工作流 deadline 归一，而不是别的路径恰好抛了同类异常。
    assert "deadline" in str(excinfo.value), str(excinfo.value)


def test_task_free_flow_deadline_has_no_host() -> None:
    """无 ``Task.submit`` 的函数体不产生编排外壳 → deadline 无宿主、不生效。

    这是**文档化约束**：超时的判定与
    强还原都挂在工作流上，而空 flow 沿用既有的"惰性创建"语义、不创建工作流。
    函数体因此自己跑完并正常返回——纯 CPU 段本就不可中断，报超时也没有意义。
    """

    @flow(timeout_ms=50)  # type: ignore[untyped-decorator]
    def my_flow() -> str:
        time.sleep(0.2)
        return "done"

    with Runtime.with_defaults():
        assert my_flow() == "done"


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


def _spy_submit_workflow(
    rt: Runtime,
    captured: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """spy 工作流外壳创建（提交路径第一步），记录随外壳传递的参数。"""
    orig = rt.submit_workflow

    def spy(workflow_id: str, **kw: object) -> None:
        captured.update(kw)
        orig(workflow_id, **kw)  # type: ignore[arg-type]

    monkeypatch.setattr(rt, "submit_workflow", spy)


def test_flow_failure_strategy_default_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """默认（未传 failure_strategy）外壳不携带策略——Rust 应用 FailFast 默认。"""
    captured: dict[str, object] = {}
    with Runtime.with_defaults() as rt:
        _spy_submit_workflow(rt, captured, monkeypatch)

        @flow  # type: ignore[untyped-decorator]
        def my_flow() -> int:
            return _add_one.submit(1).result()  # type: ignore[no-any-return]

        assert my_flow() == 2
    assert captured.get("failure_strategy") is None, "default must pass None (Rust applies FailFast)"


def test_flow_failure_strategy_continue_passthrough(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """显式 ``failure_strategy="continue"`` 原样透传给 ``submit_workflow``。"""
    captured: dict[str, object] = {}
    with Runtime.with_defaults() as rt:
        _spy_submit_workflow(rt, captured, monkeypatch)

        @flow(failure_strategy="continue")  # type: ignore[untyped-decorator]
        def my_flow() -> int:
            return _add_one.submit(1).result()  # type: ignore[no-any-return]

        assert my_flow() == 2
    assert captured["failure_strategy"] == "continue"


def test_flow_timeout_fails_without_reexecution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """超时即失败终态：函数体不重试、不并发重复执行。

    函数体阻塞在任务等待上被 deadline 强还原唤醒。本用例断言"强还原
    不引发重放/重跑"——函数体调用计数保持 1。
    """
    calls = {"count": 0}

    @flow(timeout_ms=100)  # type: ignore[untyped-decorator]
    def my_flow() -> str:
        calls["count"] += 1
        return _slow_task.submit().result()  # type: ignore[no-any-return]

    with Runtime.with_defaults(), pytest.raises(ActantTimeoutError):
        my_flow()
    assert calls["count"] == 1


def test_flow_body_error_propagates_without_retry() -> None:
    """函数体抛错：直接传播（无 flow 级重试），工作流转失败终态。"""
    calls = {"count": 0}

    @flow  # type: ignore[untyped-decorator]
    def my_flow() -> str:
        calls["count"] += 1
        raise RuntimeError("not yet")

    with Runtime.with_defaults(), pytest.raises(RuntimeError, match="not yet"):
        my_flow()
    assert calls["count"] == 1


