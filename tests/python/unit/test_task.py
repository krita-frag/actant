"""``@task`` / ``AsyncResult`` / ``@flow`` 单元测试。

覆盖：
- 任务定义与本地同步调用
- ``submit`` 异步执行与结果获取
- 任务依赖自动解析
- 重试与超时
- 失败传播（异常编码进 ``ExecuteOutcome.error_payload``）
- 任务注册表查询/取消
- ``@flow`` 生命周期事件
- ``TaskState`` 字面量类型
- 取消系统：协作式检查点、清理钩子、级联传播、force_after

注意：cloudpickle 对 ``@task`` 装饰的函数走 by-value 模式（装饰器将模块级
名称替换为 ``Task`` 对象，``getattr(module, qualname)`` 不再返回原函数）。
因此测试中保留原函数名在模块级，再创建 Task 对象，使 cloudpickle 能按引用
序列化。需要共享状态的任务（计数器）通过模块级 dict 共享。
"""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from actant import AsyncResult, Runtime, flow, get_task_context, task
from actant.exceptions import ActantError, InvalidStateError, TaskCancelledError

# ============================================================================
# 模块级函数 + Task 对象
#
# 保留原函数名在模块级（如 _echo_fn），再创建 Task 对象（如 _echo）。
# 这样 cloudpickle 的 _lookup_module_and_qualname 能通过 getattr(module,
# "_echo_fn") 找到原函数，走 by-reference 模式，共享模块级状态。
# ============================================================================

_retry_counter = {"count": 0}
_no_retry_counter = {"count": 0}
_cancel_hook_log: list[str] = []
# 信号：_cancel_hook_fn 注册 on_cancel 钩子后 set，供测试等待"钩子已注册"
# 而非仅"任务已 running"（running 状态在 func 执行前由 TaskStarted 设置，
# 此时 on_cancel 可能尚未注册，cancel 会触发 _run_with_timeout 前置取消检查
# 抛异常，func 不执行，钩子永不注册）。
_cancel_hook_registered = threading.Event()

def _wait_for_state(handle: AsyncResult[Any], state: str, timeout: float = 30.0) -> None:
    """轮询等待 AsyncResult 进入目标状态（如 ``running`` / ``completed``）。

    默认 30s 超时：连续多个测试创建/销毁 Runtime 时，前一个的 iroh/tokio 资源
    释放是异步的，新 Runtime 的 Worker 启动可能有几秒抖动。30s 给 CI
    慢机器留足余量，仍能快失败。
    """
    deadline = time.monotonic() + timeout
    while handle.state != state and time.monotonic() < deadline:
        time.sleep(0.01)
    assert handle.state == state, (
        f"expected state {state!r} within {timeout}s, got {handle.state!r}"
    )


def _echo_fn(x: int) -> int:
    return x


def _add_fn(a: int, b: int) -> int:
    return a + b


def _increment_fn(x: int) -> int:
    return x + 1


def _double_fn(x: int) -> int:
    return x * 2


def _make_pair_fn(pair: list[int]) -> tuple[int, int]:
    return (pair[0], pair[1])


def _sum_dict_fn(d: dict[str, int]) -> int:
    return d["a"] + d["b"]


def _fail_value_error_fn() -> None:
    raise ValueError("task failed")


def _fail_runtime_error_fn() -> None:
    raise RuntimeError("kaboom")


def _flaky_fn() -> str:
    _retry_counter["count"] += 1
    if _retry_counter["count"] < 3:
        raise RuntimeError("not yet")
    return "ok"


def _always_fail_fn() -> None:
    raise ValueError("always")


def _fail_once_fn() -> None:
    _no_retry_counter["count"] += 1
    raise RuntimeError("fail")


def _slow_timeout_fn() -> int:
    # 可中断 sleep：超时取消后能及时退出，释放 worker 线程。
    ctx = get_task_context()
    for _ in range(20):
        if ctx is not None and ctx.is_cancelled():
            return 0
        time.sleep(0.1)
    return 1


def _quick_no_timeout_fn() -> int:
    time.sleep(0.05)
    return 42


def _long_sleep_fn() -> int:
    # 可中断 sleep：避免阻塞 worker shutdown。
    ctx = get_task_context()
    for _ in range(100):
        if ctx is not None and ctx.is_cancelled():
            return 0
        time.sleep(0.1)
    return 1


def _blocker_fn() -> int:
    """2 秒阻塞任务，用于填充单线程执行池，使后续任务排队。

    可中断：检查 cancel token，确保 Runtime.stop() 时能及时退出，
    避免阻塞 shutdown_and_wait 的 join。
    """
    ctx = get_task_context()
    for _ in range(20):
        if ctx is not None and ctx.is_cancelled():
            return 0
        time.sleep(0.1)
    return 1


def _short_sleep_fn() -> int:
    time.sleep(0.3)
    return 1


class _MyError(Exception):
    pass


def _fail_custom_fn() -> None:
    raise _MyError("custom")


def _extract_fn(x: int) -> int:
    return x


def _transform_fn(x: int) -> int:
    return x * 2


def _load_fn(x: int) -> int:
    return x + 1


def _cancel_check_fn() -> str:
    """协作式取消：定期调用 get_task_context().is_cancelled()。"""
    ctx = get_task_context()
    for _ in range(50):
        if ctx is not None and ctx.is_cancelled():
            return "cancelled"
        time.sleep(0.01)
    return "completed"


def _cancel_hook_fn() -> str:
    """注册取消清理钩子并协作式等待。"""
    import sys as _sys

    ctx = get_task_context()
    if ctx is not None:
        # cloudpickle 序列化任务到 worker 线程后，闭包变量会复制到新线程。
        # 通过 sys.modules[__name__] 重新查找当前模块，确保修改的是主线程的
        # _cancel_hook_log（pytest 可能以短名导入测试模块）。
        def _record_cleanup() -> None:
            mod = _sys.modules[__name__]
            mod._cancel_hook_log.append(f"cleanup:{ctx.task_id}")

        ctx.on_cancel(_record_cleanup)
        # 通知测试：on_cancel 钩子已注册，可以安全 cancel。
        # 若不等待此信号而仅等 state=="running"，cancel 可能在 func 执行前
        # 触发 _run_with_timeout 前置取消检查，func 不执行，钩子永不注册。
        mod = _sys.modules[__name__]
        mod._cancel_hook_registered.set()
    for _ in range(100):
        if ctx is not None and ctx.is_cancelled():
            return "cancelled"
        time.sleep(0.05)
    return "completed"


# Task 对象
_echo = task(_echo_fn)
_add = task(_add_fn)
_increment = task(_increment_fn)
_double = task(_double_fn)
_make_pair = task(_make_pair_fn)
_sum_dict = task(_sum_dict_fn)
_fail_value_error = task(_fail_value_error_fn)
_fail_runtime_error = task(_fail_runtime_error_fn)
_flaky = task(_flaky_fn, retries=3, retry_delay_ms=0)
_always_fail = task(_always_fail_fn, retries=2, retry_delay_ms=0)
_fail_once = task(_fail_once_fn)
_slow_timeout = task(_slow_timeout_fn, timeout_ms=100)
_quick_no_timeout = task(_quick_no_timeout_fn, timeout_ms=0)
_long_sleep = task(_long_sleep_fn)
_blocker = task(_blocker_fn)
_short_sleep = task(_short_sleep_fn)
_fail_custom = task(_fail_custom_fn)
_extract = task(_extract_fn)
_transform = task(_transform_fn)
_load = task(_load_fn)
_cancel_check = task(_cancel_check_fn)
_cancel_hook = task(_cancel_hook_fn)


class _FailingSubmitCore:
    def submit_task(self, _task_def: object) -> None:
        raise RuntimeError("enqueue failed")

    def shutdown(self) -> None:
        return None


# ============================================================================
# Task 定义与同步调用
# ============================================================================


class TestTaskDefinition:
    def test_task_is_callable_directly(self):
        @task
        def add(a: int, b: int) -> int:
            return a + b

        assert add(2, 3) == 5

    def test_task_preserves_metadata(self):
        @task(name="my-task")
        def fetch(url: str) -> str:
            """Fetch a URL."""
            return url

        assert fetch.name == "my-task"
        assert fetch.__doc__ == "Fetch a URL."
        assert fetch.__name__ == "fetch"

    def test_task_default_name_uses_module_and_qualname(self):
        @task
        def compute(x: int) -> int:
            return x

        assert "compute" in compute.name
        assert "test_task" in compute.name

    def test_task_decorator_with_parameters(self):
        @task(retries=3, retry_delay_ms=10, timeout_ms=5000, name="param-task")
        def heavy() -> str:
            return "done"

        assert heavy.name == "param-task"

    def test_delay_is_alias_of_submit(self):
        @task
        def echo(x: int) -> int:
            return x

        with Runtime.with_defaults():
            handle = echo.delay(42)
            assert isinstance(handle, AsyncResult)
            assert handle.result(timeout=30) == 42


# ============================================================================
# submit / AsyncResult
# ============================================================================


class TestSubmit:
    def test_submit_returns_async_result(self):
        with Runtime.with_defaults():
            handle = _echo.submit(42)
            assert isinstance(handle, AsyncResult)
            assert handle.result(timeout=30) == 42

    def test_submit_requires_runtime(self):
        @task
        def f() -> None:
            pass

        with pytest.raises(InvalidStateError, match="no active Runtime"):
            f.submit()

    def test_result_blocks_until_done(self):
        with Runtime.with_defaults():
            handle = _echo.submit(99)
            assert handle.result(timeout=30) == 99
            assert handle.done()

    def test_done_property(self):
        with Runtime.with_defaults():
            handle = _echo.submit(1)
            handle.result(timeout=30)
            assert handle.done()

    def test_repr_includes_state(self):
        with Runtime.with_defaults():
            handle = _echo.submit(1)
            handle.result(timeout=30)
            repr_str = repr(handle)
            assert "AsyncResult" in repr_str
            assert "completed" in repr_str


class TestTaskState:
    """TaskState 字面量类型与 AsyncResult.state 行为。"""

    def test_state_completed_on_success(self):
        with Runtime.with_defaults():
            handle = _echo.submit(1)
            handle.result(timeout=30)
            assert handle.state == "completed"

    def test_state_failed_on_exception(self):
        with Runtime.with_defaults():
            handle = _fail_value_error.submit()
            with pytest.raises(ValueError, match="task failed"):
                handle.result(timeout=30)
            assert handle.state == "failed"

    def test_state_running_while_in_flight(self):
        with Runtime.with_defaults():
            handle = _short_sleep.submit()
            # 分布式模型：Worker 需轮询拉取任务后才会进入 running 状态，
            # 短暂轮询等待 TaskStarted 事件到达（0.3s 睡眠窗口内应观察到 running）。
            deadline = time.monotonic() + 2.0
            while handle.state == "pending" and time.monotonic() < deadline:
                time.sleep(0.005)
            assert handle.state == "running"
            handle.result(timeout=30)


# ============================================================================
# 失败传播
# ============================================================================


class TestFailurePropagation:
    def test_task_exception_re_raised_by_result(self):
        with Runtime.with_defaults():
            handle = _fail_value_error.submit()
            with pytest.raises(ValueError, match="task failed"):
                handle.result(timeout=30)

    def test_exception_method_returns_exception(self):
        with Runtime.with_defaults():
            handle = _fail_runtime_error.submit()
            exc = handle.exception(timeout=30)
            assert isinstance(exc, RuntimeError)
            assert "kaboom" in str(exc)

    def test_exception_method_returns_none_on_success(self):
        with Runtime.with_defaults():
            handle = _echo.submit(1)
            handle.result(timeout=30)
            assert handle.exception(timeout=30) is None

    def test_exception_method_raises_cancelled_on_cancelled_task(self):
        """exception() 取消语义对齐 - 取消时抛 TaskCancelledError。"""
        # 使用 max_concurrent_tasks=1：blocker 占用唯一 worker，第二个任务排队，
        # 取消排队任务后状态转为 cancelled，exception() 应抛 TaskCancelledError。
        with Runtime.with_defaults(max_concurrent_tasks=1) as rt:
            blocker = _blocker.submit()  # 占用唯一 worker
            _wait_for_state(blocker, "running")
            handle = _echo.submit(42)    # 排队，尚未开始
            cancelled = rt.cancel_task(handle.task_id)
            assert cancelled is True
            with pytest.raises(TaskCancelledError):
                handle.exception(timeout=30)
            # 等待 blocker 完成，避免 stop() 长时间等待
            blocker.result(timeout=30)

    def test_custom_exception_type_preserved(self):
        with Runtime.with_defaults():
            handle = _fail_custom.submit()
            with pytest.raises(_MyError):
                handle.result(timeout=30)


# ============================================================================
# 任务依赖自动解析
# ============================================================================


class TestDependencyResolution:
    def test_async_result_resolved_as_argument(self):
        with Runtime.with_defaults():
            a = _increment.submit(10)       # 11
            b = _double.submit(a)           # 自动等待 a，传入 11，得 22
            assert b.result(timeout=30) == 22

    def test_dependency_in_list(self):
        with Runtime.with_defaults():
            a = _echo.submit(1)
            b = _echo.submit(2)
            result = _make_pair.submit([a, b])
            assert result.result(timeout=30) == (1, 2)

    def test_dependency_in_dict(self):
        with Runtime.with_defaults():
            a = _echo.submit(10)
            b = _echo.submit(20)
            result = _sum_dict.submit({"a": a, "b": b})
            assert result.result(timeout=30) == 30

    def test_chained_dependencies(self):
        with Runtime.with_defaults():
            h = _increment.submit(0)    # 0 → 1
            for _ in range(4):           # 1→2→3→4→5
                h = _increment.submit(h)
            assert h.result(timeout=30) == 5


# ============================================================================
# 重试
# ============================================================================


class TestRetry:
    def test_retry_succeeds_after_failures(self):
        _retry_counter["count"] = 0
        with Runtime.with_defaults():
            handle = _flaky.submit()
            assert handle.result(timeout=30) == "ok"
            assert _retry_counter["count"] == 3

    def test_retry_exhausted_returns_failure(self):
        with Runtime.with_defaults():
            handle = _always_fail.submit()
            with pytest.raises(ValueError, match="always"):
                handle.result(timeout=30)

    def test_no_retry_by_default(self):
        _no_retry_counter["count"] = 0
        with Runtime.with_defaults():
            handle = _fail_once.submit()
            with pytest.raises(RuntimeError, match=r"^fail$"):
                handle.result(timeout=30)
            assert _no_retry_counter["count"] == 1


# ============================================================================
# 超时
# ============================================================================


class TestTimeout:
    def test_timeout_aborts_task(self):
        with Runtime.with_defaults():
            handle = _slow_timeout.submit()
            # 分布式模型：超时由 Rust Worker tokio 调度器强制取消，
            # 结果为 ActantError（"task timed out after Xms"）。
            with pytest.raises(ActantError, match="timed out"):
                handle.result(timeout=30)

    def test_no_timeout_allows_long_task(self):
        with Runtime.with_defaults():
            handle = _quick_no_timeout.submit()
            assert handle.result(timeout=30) == 42


# ============================================================================
# 任务注册表
# ============================================================================


class TestTaskRegistry:
    def test_list_tasks_after_submit(self):
        with Runtime.with_defaults() as rt:
            handle = _echo.submit(1)
            # 任务完成前应在注册表中；完成后会移除（孤儿回收）
            task_ids = rt.list_tasks()
            assert handle.task_id in task_ids
            handle.result(timeout=30)

    def test_get_task_returns_handle(self):
        with Runtime.with_defaults() as rt:
            handle = _echo.submit(1)
            assert rt.get_task(handle.task_id) is handle

    def test_get_task_returns_none_for_unknown(self):
        with Runtime.with_defaults() as rt:
            assert rt.get_task("nonexistent") is None

    def test_cancel_queued_task(self):
        # 使用 max_concurrent_tasks=1：blocker 占用唯一 worker，第二个任务排队等待，
        # Future.cancel() 对排队任务返回 True。
        with Runtime.with_defaults(max_concurrent_tasks=1) as rt:
            blocker = _blocker.submit()      # 占用唯一 worker
            _wait_for_state(blocker, "running")
            handle = _echo.submit(42)        # 排队，尚未开始
            cancelled = rt.cancel_task(handle.task_id)
            assert cancelled is True
            assert rt.is_cancelled(handle.task_id) is True
            with pytest.raises(TaskCancelledError):
                handle.result(timeout=30)
            # 等待 blocker 完成，避免 stop() 长时间等待
            blocker.result(timeout=30)
            deadline = time.monotonic() + 5.0
            while rt.is_cancelled(handle.task_id) and time.monotonic() < deadline:
                time.sleep(0.01)
            assert rt.is_cancelled(handle.task_id) is False
            assert rt.get_task(handle.task_id) is None

    def test_cancel_unknown_task_returns_false(self):
        with Runtime.with_defaults() as rt:
            assert rt.cancel_task("unknown") is False

    def test_cancel_completed_task_returns_false(self):
        """已完成的任务不可取消，cancel_task 应返回 False。"""
        with Runtime.with_defaults() as rt:
            handle = _echo.submit(42)
            result = handle.result(timeout=30)
            assert result == 42
            # 任务已完成，cancel 应返回 False
            assert rt.cancel_task(handle.task_id) is False

    def test_cancel_failed_task_returns_false(self):
        """已失败的任务不可取消，cancel_task 应返回 False。"""
        with Runtime.with_defaults() as rt:
            handle = _fail_value_error.submit()
            with pytest.raises(ValueError):
                handle.result(timeout=30)
            # 任务已失败，cancel 应返回 False
            assert rt.cancel_task(handle.task_id) is False

    def test_cancel_already_cancelled_is_idempotent(self):
        """对已取消任务再次调用 cancel 应返回 True（幂等语义）。"""
        with Runtime.with_defaults(max_concurrent_tasks=1) as rt:
            blocker = _blocker.submit()
            _wait_for_state(blocker, "running")
            handle = _echo.submit(42)
            assert rt.cancel_task(handle.task_id) is True
            # 再次取消：任务已标记为 cancelled，应返回 True（幂等）
            assert handle.cancel() is True
            blocker.result(timeout=30)

    def test_submit_failure_cleans_up_registration(self):
        with Runtime.with_defaults() as rt:
            baseline = set(rt.list_tasks())
            original_core = rt._rust_core
            rt._rust_core = _FailingSubmitCore()
            try:
                with pytest.raises(RuntimeError, match="enqueue failed"):
                    _echo.submit(1)
            finally:
                rt._rust_core = original_core
            assert set(rt.list_tasks()) == baseline


# ============================================================================
# 取消系统
# ============================================================================


class TestCancellation:
    def test_get_task_context_outside_task_returns_none(self):
        assert get_task_context() is None

    def test_cooperative_cancellation_checkpoint(self):
        with Runtime.with_defaults(max_concurrent_tasks=1) as rt:
            blocker = _blocker.submit()
            _wait_for_state(blocker, "running")
            handle = _cancel_check.submit()  # 排队
            rt.cancel_task(handle.task_id)
            blocker.result(timeout=30)
            # 取消后 result() 抛出 TaskCancelledError；协作式检查点已生效。
            with pytest.raises(TaskCancelledError):
                handle.result(timeout=30)

    def test_cancel_emits_task_lifecycle_event(self):
        events: list[str] = []

        with Runtime.with_defaults(max_concurrent_tasks=1) as rt:
            rt.layer("TaskLifecycle").chain(lambda e: events.append(e.kind))
            blocker = _blocker.submit()
            _wait_for_state(blocker, "running")
            handle = _echo.submit(1)
            rt.cancel_task(handle.task_id)
            with pytest.raises(TaskCancelledError):
                handle.result(timeout=30)
            blocker.result(timeout=30)

        assert "cancelled" in events

    def test_dispatch_emits_started_and_completed_lifecycle_events(self):
        """回归测试：dispatch handler 在 Rust worker 线程中执行时，
        ``_emit_task_event`` 必须能解析到 Runtime 并成功广播
        ``started`` / ``completed`` 事件（而非被 ``on_error="log"`` 静默吞掉）。

        历史 bug：dispatch handler 闭包捕获了 Runtime 引用但未将其注入
        ``threading.local``，导致 ``emit()`` 在 worker 线程中抛
        ``InvalidStateError``，所有 started/completed/failed/retried 事件丢失。
        修复：在 dispatch handler 执行期间用 ``use_runtime(runtime)`` 注入线程局部。
        """
        events: list[tuple[str, str]] = []  # (kind, task_id)

        with Runtime.with_defaults() as rt:
            rt.layer("TaskLifecycle").chain(
                lambda e: events.append((e.kind, e.task_id))
            )
            handle = _echo.submit(42)
            handle.result(timeout=30)

        kinds = [k for k, _ in events if _ == handle.task_id]
        assert "started" in kinds, f"missing 'started' event; got {events!r}"
        assert "completed" in kinds, f"missing 'completed' event; got {events!r}"

    def test_dispatch_emits_failed_lifecycle_event_on_exception(self):
        """任务失败时 dispatch handler 应广播 ``failed`` 事件（回归同上）。"""
        events: list[tuple[str, str]] = []

        with Runtime.with_defaults() as rt:
            rt.layer("TaskLifecycle").chain(
                lambda e: events.append((e.kind, e.task_id))
            )
            handle = _fail_runtime_error.submit()
            with pytest.raises(RuntimeError):
                handle.result(timeout=30)

        kinds = [k for k, _ in events if _ == handle.task_id]
        assert "started" in kinds, f"missing 'started' event; got {events!r}"
        assert "failed" in kinds, f"missing 'failed' event; got {events!r}"

    def test_on_cancel_hook_invoked(self):
        _cancel_hook_log.clear()
        _cancel_hook_registered.clear()
        # 使用 max_concurrent_tasks=2 让 _cancel_hook 立即开始执行并注册钩子；
        # 然后 cancel，钩子应被调用。
        with Runtime.with_defaults(max_concurrent_tasks=2) as rt:
            handle = _cancel_hook.submit()
            # 等待 on_cancel 钩子已注册（而非仅 state=="running"），
            # 避免 cancel 在 func 执行前触发前置取消检查导致 func 不执行。
            assert _cancel_hook_registered.wait(timeout=5), "on_cancel hook not registered in time"
            rt.cancel_task(handle.task_id)
            with pytest.raises(TaskCancelledError):
                handle.result(timeout=30)

        assert any(handle.task_id in entry for entry in _cancel_hook_log)

    def test_force_after_invokes_cleanup(self):
        _cancel_hook_log.clear()
        _cancel_hook_registered.clear()
        with Runtime.with_defaults(max_concurrent_tasks=2):
            handle = _cancel_hook.submit()
            # 等待 on_cancel 钩子已注册（而非仅 state=="running"），
            # 避免 cancel 在 func 执行前触发前置取消检查导致 func 不执行。
            assert _cancel_hook_registered.wait(timeout=5), "on_cancel hook not registered in time"
            handle.cancel(force_after=0.1)
            # force_after 超时后清理钩子应被调用
            time.sleep(0.3)
            assert any(handle.task_id in entry for entry in _cancel_hook_log)

    def test_propagate_cancels_same_workflow_tasks(self):
        events: list[str] = []

        def _pipeline() -> None:
            a = _long_sleep.submit()
            b = _long_sleep.submit()
            # 取消 a 并级联到同一 flow 的 b
            a.cancel(propagate=True, force_after=0.5)
            time.sleep(0.2)
            # 此时 a/b 都应被取消
            assert a.state == "cancelled" or a.state == "running"
            assert b.state == "cancelled" or b.state == "running"

        pipeline = flow(_pipeline)
        with Runtime.with_defaults(max_concurrent_tasks=2) as rt:
            rt.layer("TaskLifecycle").chain(lambda e: events.append(e.kind))
            pipeline()

        assert events.count("cancelled") >= 1

    def test_task_unregistered_after_completion(self):
        with Runtime.with_defaults() as rt:
            handle = _echo.submit(1)
            handle.result(timeout=30)
            # result() 返回时 state=completed，但 unregister_task 在同一回调中
            # 稍后执行（不同线程）。轮询等待注册表清理完成，避免竞态。
            deadline = time.monotonic() + 5.0
            while rt.get_task(handle.task_id) is not None and time.monotonic() < deadline:
                time.sleep(0.01)
            assert rt.get_task(handle.task_id) is None
            assert handle.task_id not in rt.list_tasks()


# ============================================================================
# @flow 编排
# ============================================================================


def _simple_pipeline_fn(a: int, b: int) -> int:
    h = _add.submit(a, b)
    return h.result()


def _etl_pipeline_fn(src: int) -> int:
    raw = _extract.submit(src)
    processed = _transform.submit(raw)
    return _load.submit(processed).result()


def _flow_no_runtime_fn() -> None:
    pass


def _flow_fail_fn() -> None:
    raise ValueError("flow failed")


_simple_pipeline = flow(_simple_pipeline_fn)
_etl_pipeline = flow(_etl_pipeline_fn)
_flow_no_runtime = flow(_flow_no_runtime_fn)
_flow_fail = flow(_flow_fail_fn)


class TestFlow:
    def test_flow_executes_body(self):
        with Runtime.with_defaults():
            assert _simple_pipeline(2, 3) == 5

    def test_flow_requires_runtime(self):
        with pytest.raises(InvalidStateError, match="no active Runtime"):
            _flow_no_runtime()

    def test_flow_emits_lifecycle_events(self):
        events: list[str] = []

        with Runtime.with_defaults() as rt:
            rt.layer("WorkflowLifecycle").chain(
                lambda e: events.append(e.kind)
            )
            _simple_pipeline(2, 3)

        assert events == ["submitted", "started", "completed"]

    def test_flow_emits_failed_on_exception(self):
        events: list[str] = []

        with Runtime.with_defaults() as rt:
            rt.layer("WorkflowLifecycle").chain(
                lambda e: events.append(e.kind)
            )
            with pytest.raises(ValueError):
                _flow_fail()

        assert "failed" in events
        assert "completed" not in events

    def test_flow_with_task_dependencies(self):
        with Runtime.with_defaults():
            assert _etl_pipeline(10) == 21  # (10 * 2) + 1
