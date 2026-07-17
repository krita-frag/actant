"""任务序列化 / 执行 / 事件广播辅助函数。

依赖 ``_context``（``get_task_context``），不依赖 ``AsyncResult`` / ``Task``，
避免循环导入。
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any, Literal, cast

import cloudpickle

from actant.capabilities import ExecuteCtx, ExecuteOutcome
from actant.exceptions import ActantError, SerializationError, TaskCancelledError
from actant.task._context import get_task_context

CallbackErrorPolicy = Literal["log", "raise", "collect"]

_logger = logging.getLogger("actant.task")


def _run_with_timeout(
    func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    timeout_ms: int,
) -> Any:
    """执行 func，超时由 Rust Worker 的 tokio 调度器强制取消。

    分布式模式下，dispatch handler 已在 Rust 线程池的独立线程中执行，
    Rust Worker 通过 ``tokio::time::timeout`` 监控超时。超时后 Rust 设置
    cancel_flag，dispatch handler 在下次协作检查点退出。

    Python 侧不再使用嵌套 ThreadPoolExecutor（会与 Rust 线程池竞争 GIL，
    导致超时无法及时触发）。``timeout_ms`` 仍传入 options 供 Rust 侧使用。

    取消检查点：函数执行前检查 ``get_task_context().is_cancelled()``。
    """
    ctx = get_task_context()
    if ctx is not None and ctx.is_cancelled():
        raise TaskCancelledError(f"task {ctx.task_id!r} was cancelled before start")
    result = func(*args, **kwargs)
    if ctx is not None and ctx.is_cancelled():
        raise TaskCancelledError(f"task {ctx.task_id!r} was cancelled during execution")
    return result


def _interruptible_sleep(
    duration: float,
    cancel_token: Any,
    *,
    interval: float = 0.05,
) -> None:
    """可中断的 sleep：在 duration 期间分段轮询 cancel_token。

    若 ``cancel_token`` 报告已取消，立即返回，使调用方能在下次循环顶部
    的取消检查点退出。避免重试延迟期间无法响应取消请求。

    Args:
        duration: 总睡眠时长（秒）。
        cancel_token: 拥有 ``is_cancelled() -> bool`` 方法的取消令牌。
        interval: 轮询间隔（秒），默认 50ms。
    """
    if duration <= 0:
        return
    deadline = time.monotonic() + duration
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        if cancel_token is not None and cancel_token.is_cancelled():
            return
        time.sleep(min(interval, remaining))


def _safe_serialize(
    func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    options: dict[str, Any],
    *,
    task_id: str,
) -> bytes:
    """序列化任务 payload，失败时定位不可序列化的参数。

    cloudpickle 失败时原始错误消息（如 ``"can't pickle _thread.lock object"``）
    不指明是哪个参数。此函数逐个尝试序列化 func/各 args/各 kwargs values，
    在错误消息中包含参数索引/名称与 repr，帮助用户定位问题。
    """
    try:
        return cast(bytes, cloudpickle.dumps((func, args, kwargs, options)))
    except (TypeError, AttributeError, ValueError, RecursionError) as overall:
        # 整体失败：逐个定位不可序列化的项
        bad: list[str] = []
        try:
            cloudpickle.dumps(func)
        except (TypeError, AttributeError, ValueError, RecursionError) as e:
            bad.append(f"func {func!r}: {e}")
        for i, a in enumerate(args):
            try:
                cloudpickle.dumps(a)
            except (TypeError, AttributeError, ValueError, RecursionError) as e:
                bad.append(f"args[{i}]={a!r}: {e}")
        for k, v in kwargs.items():
            try:
                cloudpickle.dumps(v)
            except (TypeError, AttributeError, ValueError, RecursionError) as e:
                bad.append(f"kwargs[{k!r}]={v!r}: {e}")
        detail = "; ".join(bad) if bad else f"unknown component: {overall}"
        raise SerializationError(
            f"task {task_id!r}: cannot serialize payload. Unserializable components: {detail}"
        ) from overall


def _pickle_exception(exc: BaseException) -> bytes:
    """序列化异常，不可序列化时退化为携带类型与消息的 RuntimeError。"""
    try:
        return cast(bytes, cloudpickle.dumps(exc))
    except Exception:
        return cast(bytes, cloudpickle.dumps(RuntimeError(f"{type(exc).__name__}: {exc}")))


def _invoke_callback(
    fn: Callable[..., Any],
    *args: Any,
    label: str,
    policy: CallbackErrorPolicy = "log",
) -> Exception | None:
    """统一调用用户回调，按 ``policy`` 处理异常。

    - ``log``（默认）：记录 debug 日志并返回异常，不向上抛出。
    - ``raise``：立即向上抛出异常。
    - ``collect``：记录并返回异常，由调用方决定是否聚合抛出。

    返回捕获到的异常（若有）。
    """
    try:
        fn(*args)
    except Exception as e:
        if policy == "raise":
            raise
        _logger.debug("callback %s raised", label, exc_info=True)
        if policy == "collect":
            return e
    return None


def _invoke_callbacks(
    callbacks: list[Callable[..., Any]],
    *args: Any,
    label: str,
    policy: CallbackErrorPolicy = "log",
) -> list[Exception]:
    """批量调用回调，按 ``policy`` 收集/处理异常。

    ``collect`` 策略下收集所有异常并返回；``raise`` 策略下遇到首个异常即抛出；
    ``log`` 策略下记录并继续执行后续回调。
    """
    errors: list[Exception] = []
    for fn in callbacks:
        caught = _invoke_callback(fn, *args, label=f"{label}/{getattr(fn, '__name__', fn)}", policy=policy)
        if caught is not None:
            errors.append(caught)
    if policy == "collect" and errors:
        raise ActantError(
            f"{label}: {len(errors)} callback(s) failed; first: {errors[0]!r}"
        )
    return errors


class _suppress_pickle_errors:
    """cloudpickle 反序列化失败时静默返回（用于 exception() 的 best-effort 解码）。

    仅抑制 ``Exception`` 子类（如 ``pickle.UnpicklingError``），
    不抑制 ``KeyboardInterrupt`` / ``SystemExit`` 等 ``BaseException``。
    """

    def __enter__(self) -> _suppress_pickle_errors:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        return exc_type is not None and issubclass(exc_type, Exception)


def _emit_task_event(
    kind: str,
    task_id: str,
    workflow_id: str,
    *,
    attempt: int = 0,
    next_attempt: int = 0,
    error: str = "",
    on_error: CallbackErrorPolicy = "log",
) -> Exception | None:
    """广播 ``TaskLifecycle`` 事件，失败时按 ``on_error`` 策略处理。

    generic handler 在工作线程中执行，``emit`` 需要活跃 Runtime——已由
    ``Task.submit`` 的 ``use_runtime(rt)`` 传播。若 Runtime 未注册
    ``TaskLifecycle`` handler，emit 静默失败（``_dispatch_emit`` 对空 handler
    列表 + Rust-backed capability 会调用 Rust emit，无 Python handler 时不报错）。

    Args:
        on_error: ``log``（默认）仅记录 warning；``raise`` 立即抛出；
            ``collect`` 记录并返回异常。

    Returns:
        捕获到的异常（仅当 ``on_error="collect"`` 时）。
    """
    try:
        from actant._effects import emit as _emit
        from actant.capabilities import TASK_LIFECYCLE, TaskEvent

        _emit(
            TASK_LIFECYCLE,
            TaskEvent(
                kind=kind,  # type: ignore[arg-type]
                task_id=task_id,
                workflow_id=workflow_id,
                attempt=attempt,
                next_attempt=next_attempt,
                error=error,
            ),
        )
    except Exception as e:
        if on_error == "raise":
            raise
        _logger.warning(
            "task %s: TaskLifecycle emit (%s) failed", task_id, kind, exc_info=True,
        )
        if on_error == "collect":
            return e
    return None


__all__ = [
    "CallbackErrorPolicy",
    "ExecuteCtx",
    "ExecuteOutcome",
    "_emit_task_event",
    "_interruptible_sleep",
    "_invoke_callback",
    "_invoke_callbacks",
    "_pickle_exception",
    "_run_with_timeout",
    "_safe_serialize",
    "_suppress_pickle_errors",
]
