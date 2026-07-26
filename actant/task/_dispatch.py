"""Execute / dispatch capability handler。

由 ``Runtime.start()`` 注册到 ``Execute`` capability 链末尾。依赖 ``_context``
（``_DispatchTaskContext`` / ``_task_context_scope``）与 ``_helpers``
（``_run_with_timeout`` / ``_interruptible_sleep`` / ``_emit_task_event`` /
``_pickle_exception``）。
"""

from __future__ import annotations

import logging
import pickle
from collections.abc import Callable
from contextlib import nullcontext
from typing import Any, cast

import cloudpickle

from actant._runtime import use_runtime
from actant.capabilities import ExecuteCtx, ExecuteOutcome
from actant.exceptions import ActantError, TaskCancelledError
from actant.task._context import _DispatchTaskContext, _task_context_scope
from actant.task._helpers import (
    _emit_task_event,
    _ensure_picklable,
    _interruptible_sleep,
    _run_with_timeout,
)

_logger = logging.getLogger("actant.task")


class _NoopCancelToken:
    def is_cancelled(self) -> bool:
        return False


def _generic_execute_handler(ctx: ExecuteCtx) -> ExecuteOutcome:
    """Execute capability 的 generic handler。

    反序列化 cloudpickle payload ``(func, args, kwargs, options)``，调用
    ``func(*args, **kwargs)``，将返回值序列化为 ``ExecuteOutcome``。

    失败处理：任务异常**不**通过 ``perform`` 抛出，而是 cloudpickle 序列化
    进 ``ExecuteOutcome.error_payload``。这使失败可跨节点传播，且与 Rust
    ``ExecuteHandler`` 的"成功返回 outcome、失败编码"契约一致。``AsyncResult.result``
    据此重新抛出原异常。

    重试：``options["retries"]`` 控制重试次数，``retry_delay_ms`` 控制间隔。
    超时：``options["timeout_ms"]`` 限制每次尝试的执行时长。

    生命周期事件：在每个关键节点 emit ``TaskLifecycle`` 事件
    （``started``/``completed``/``failed``/``retried``），使注册到
    ``TaskLifecycle`` 的 handler（如 CLI 的 task 事件输出）真正收到事件。
    若 ``options["silent"]`` 为 ``True``，跳过事件发布。

    该 handler 在 ``Runtime.start()`` 时注册到 ``Execute`` capability 链末尾。
    """
    try:
        func, args, kwargs, options = cloudpickle.loads(ctx.payload)
    except (pickle.UnpicklingError, ValueError, TypeError) as e:
        _logger.error("task %s: cloudpickle.loads failed", ctx.task_id, exc_info=True)
        _emit_task_event("failed", ctx.task_id, ctx.workflow_id, error=f"deserialization: {e}")
        raise ActantError(
            f"task {ctx.task_id!r}: failed to deserialize payload: {e}",
            kind="serialization",
        ) from e

    retries = int(options.get("retries", 0))
    retry_delay_ms = int(options.get("retry_delay_ms", 0))
    timeout_ms = int(options.get("timeout_ms", ctx.timeout_ms))
    silent = bool(options.get("silent", False))

    # 复用 _execute_with_retries 的重试/取消/超时/事件循环，
    # 将其 ``(success, payload_obj)`` 结果转换为 ``ExecuteOutcome``。
    # payload_obj 是未序列化的对象（result 或 exc），需 dumps 一次放入
    # ExecuteOutcome.result_payload / error_payload（bytes 字段，跨 Rust 边界）。
    _emit_task_event("started", ctx.task_id, ctx.workflow_id, attempt=0, silent=silent)
    success, payload_obj = _execute_with_retries(
        func, args, kwargs, timeout_ms, retries, retry_delay_ms,
        ctx.task_id, ctx.workflow_id, _NoopCancelToken(), silent=silent,
    )
    payload_bytes = cast(
        bytes, cloudpickle.dumps(payload_obj)
    )
    if success:
        return ExecuteOutcome(
            task_id=ctx.task_id,
            result_payload=payload_bytes,
            error_payload=b"",
        )
    return ExecuteOutcome(
        task_id=ctx.task_id,
        result_payload=b"",
        error_payload=payload_bytes,
    )


def _bind_dispatch_handler(runtime: Any) -> Callable[[bytes, Any], bytes]:
    """构造一个绑定了 Runtime 引用的 dispatch handler 闭包。

    Rust Worker 在其线程池中调用此 handler，无需依赖 ``threading.local``
    或全局变量即可访问 Runtime 的任务注册表（取消检查、context 关联）。

    Args:
        runtime: 注册 handler 时所属的 ``Runtime`` 实例。

    Returns:
        签名为 ``handler(payload: bytes, cancel_token: Any) -> bytes`` 的可调用对象。
    """

    def _handler(payload: bytes, cancel_token: Any) -> bytes:
        """Worker dispatch handler：由 Rust Worker 调用执行任务。

        签名: ``handler(payload: bytes, cancel_token: CancelToken) -> bytes``

        反序列化 cloudpickle payload ``(func, args, kwargs, options)``，调用
        ``func(*args, **kwargs)``。返回值编码为 ``(success, payload_bytes)`` 元组
        的 cloudpickle 序列化字节：

        - 成功: ``(True, cloudpickle.dumps(result))``
        - 失败: ``(False, cloudpickle.dumps(exc))``（异常序列化传播）

        Runtime._on_task_result 收到 TaskCompletion 后，根据 success 标志调用
        AsyncResult._set_result 或 _set_error。

        重试：``options["retries"]`` 控制重试次数。
        超时：``options["timeout_ms"]`` 限制每次尝试时长。
        取消：检查 ``cancel_token.is_cancelled()``，已取消时抛 ``TaskCancelledError``。
        """
        try:
            func, args, kwargs, options = cloudpickle.loads(payload)
        except (pickle.UnpicklingError, ValueError, TypeError) as e:
            _logger.error("dispatch: cloudpickle.loads failed", exc_info=True)
            # 返回 (False, exc_obj) 统一格式，由调用方 loads 后直接 _set_error(obj)。
            exc_obj = _ensure_picklable(ActantError(
                f"failed to deserialize payload: {e}", kind="serialization",
            ))
            return cast(bytes, cloudpickle.dumps((False, exc_obj)))

        retries = int(options.get("retries", 0))
        retry_delay_ms = int(options.get("retry_delay_ms", 0))
        timeout_ms = int(options.get("timeout_ms", 0))
        task_id = options.get("task_id", "unknown")
        workflow_id = options.get("workflow_id", "")
        silent = bool(options.get("silent", False))

        # Python 端预取消检查：若任务在排队期间被 cancel_task 标记，
        # 立即返回 TaskCancelledError，不执行任务函数。
        if runtime is not None and runtime.is_cancelled(task_id):
            exc = TaskCancelledError(f"task {task_id!r} was cancelled")
            _emit_task_event("cancelled", task_id, workflow_id, attempt=0,
                             error=f"{type(exc).__name__}: {exc}", silent=silent)
            # 返回 (False, exc_obj) 统一格式，callback 端可直接 isinstance 检查。
            return cast(bytes, cloudpickle.dumps((False, _ensure_picklable(exc))))

        # 创建 TaskContext 并桥接 Rust cancel_token，使任务函数内
        # get_task_context().is_cancelled() / on_cancel() 可用。
        task_ctx = _DispatchTaskContext(task_id, workflow_id, cancel_token)
        # 将 dispatch context 关联到 AsyncResult，使 handle.cancel(force_after=...)
        # 能触发 dispatch 端的 on_cancel 钩子。
        # 同时迁移 submit 阶段在初始 TaskContext 上预注册的回调，避免取消发生在
        # worker 拉取任务前时清理钩子漏执行。
        if runtime is not None:
            handle = runtime.get_task(task_id)
            if handle is not None:
                old_ctx = handle._context
                if old_ctx is not None and old_ctx is not task_ctx:
                    old_ctx._migrate_callbacks_to(task_ctx)
                handle._context = task_ctx
        # 在 Rust worker 线程中执行任务期间，将 Runtime 注入当前线程的
        # threading.local，使任务函数内调用的 ``emit``/``ask``/``perform``
        # （包括 ``_emit_task_event`` 广播 TaskLifecycle 事件）能解析到 Runtime。
        # 否则 ``get_current_runtime()`` 返回 None，``emit`` 抛 InvalidStateError
        # 被 ``_emit_task_event`` 静默吞掉，导致 started/completed/failed/retried
        # 事件丢失——破坏可观测性契约。退出 with 块后恢复原值（通常为 None）。
        rt_ctx = use_runtime(runtime) if runtime is not None else nullcontext()
        with rt_ctx, _task_context_scope(task_ctx):
            _emit_task_event("started", task_id, workflow_id, attempt=0, silent=silent)
            success, payload_obj = _execute_with_retries(
                func, args, kwargs, timeout_ms, retries, retry_delay_ms,
                task_id, workflow_id, cancel_token, silent=silent,
            )
            # 单次 dumps：(success, payload_obj) 序列化为 bytes 跨 Rust 边界。
            # payload_obj 是未序列化的 result/exc 对象，避免双层 dumps。
            return cast(bytes, cloudpickle.dumps((success, payload_obj)))

    return _handler


def _execute_with_retries(
    func: Any,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    timeout_ms: int,
    retries: int,
    retry_delay_ms: int,
    task_id: str,
    workflow_id: str,
    cancel_token: Any,
    *,
    silent: bool = False,
) -> tuple[bool, Any]:
    """执行任务函数并处理重试/取消/超时。

    重试循环：每次尝试前检查取消，调用 ``_run_with_timeout`` 执行函数。
    失败时若仍有重试次数，则等待可中断的 ``retry_delay_ms`` 后重试；
    否则返回异常对象。成功时返回结果对象。

    Args:
        silent: ``True`` 时跳过所有 TaskLifecycle 事件发布。

    Returns:
        ``(success, payload)`` 元组：
        - 成功：``(True, result_obj)`` —— result_obj 是任务返回值（未序列化）。
        - 失败：``(False, exc_obj)`` —— exc_obj 是异常对象（已通过
          ``_pickle_exception`` 处理，确保可序列化）。

        调用方负责序列化（``cloudpickle.dumps``）以跨 Rust 边界传递。
        此设计避免了「先 dumps result 再 dumps (success, result_bytes)」
        的双层序列化开销，成功路径减少 1 次 dumps + 1 次 loads。
    """
    attempt = 0
    while True:
        # 协作式取消检查
        if cancel_token.is_cancelled():
            cancel_exc = TaskCancelledError(f"task {task_id!r} was cancelled")
            _emit_task_event("cancelled", task_id, workflow_id, attempt=attempt,
                             error=f"{type(cancel_exc).__name__}: {cancel_exc}",
                             silent=silent)
            return False, _ensure_picklable(cancel_exc)
        try:
            result = _run_with_timeout(func, args, kwargs, timeout_ms)
        except TaskCancelledError as exc:
            _logger.warning("task %s: cancelled", task_id)
            _emit_task_event("cancelled", task_id, workflow_id, attempt=attempt,
                             error=f"{type(exc).__name__}: {exc}", silent=silent)
            return False, _ensure_picklable(exc)
        except Exception as exc:
            if attempt < retries:
                _logger.warning(
                    "task %s: attempt %d failed (%s: %s), retrying",
                    task_id, attempt, type(exc).__name__, exc,
                )
                _emit_task_event("retried", task_id, workflow_id, attempt=attempt,
                                 next_attempt=attempt + 1,
                                 error=f"{type(exc).__name__}: {exc}", silent=silent)
                if retry_delay_ms > 0:
                    # 可中断的重试延迟：分段轮询 cancel_token，
                    # 避免重试延迟期间无法响应取消请求。
                    _interruptible_sleep(
                        retry_delay_ms / 1000,
                        cancel_token,
                        interval=0.05,
                    )
                attempt += 1
                continue
            _logger.error("task %s: failed after %d attempt(s)", task_id, attempt + 1,
                          exc_info=True)
            _emit_task_event("failed", task_id, workflow_id, attempt=attempt,
                             error=f"{type(exc).__name__}: {exc}", silent=silent)
            return False, _ensure_picklable(exc)
        _emit_task_event("completed", task_id, workflow_id, attempt=attempt, silent=silent)
        return True, result


__all__ = [
    "_bind_dispatch_handler",
    "_execute_with_retries",
    "_generic_execute_handler",
]
