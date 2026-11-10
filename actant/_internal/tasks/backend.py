"""任务提交接缝。

``Task.submit`` 只依赖 :class:`SubmitBackend`，不再直接解析全局 ``Runtime``。
默认实现 :class:`EmbeddedSubmitBackend` 把提交转交给进程内 Runtime 与 Rust
调度器；跨进程提交只需在同一接缝上提供远程实现，``Task`` 与 ``@task``
的调用语义保持不变。
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Protocol, cast

from actant._internal.runtime import get_current_runtime
from actant.exceptions import InvalidStateError

if TYPE_CHECKING:
    from actant._internal.tasks.async_result import AsyncResult


class SubmitBackend(Protocol):
    """任务提交接缝。"""

    def submit_task(
        self,
        task: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        target_node: str | None,
        target_endpoint_addr: str | None,
        idempotency_key: str | None,
    ) -> AsyncResult:
        """提交一次任务调用并返回 ``AsyncResult``。"""
        ...


class EmbeddedSubmitBackend:
    """进程内实现：解析参数、处理幂等、登记句柄并提交 Rust 调度器。"""

    def __init__(self, runtime: Any) -> None:
        self._runtime = runtime

    def submit_task(
        self,
        task: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        target_node: str | None,
        target_endpoint_addr: str | None,
        idempotency_key: str | None,
    ) -> AsyncResult:
        runtime = self._runtime
        if idempotency_key is not None:
            if not isinstance(idempotency_key, str) or not idempotency_key:
                raise ValueError("idempotency_key must be a non-empty string")
            return cast(
                "AsyncResult",
                runtime._idempotent_submit(
                    idempotency_key,
                    lambda: self.submit_task(
                        task,
                        args,
                        kwargs,
                        target_node=target_node,
                        target_endpoint_addr=target_endpoint_addr,
                        idempotency_key=None,
                    ),
                ),
            )

        from actant._internal.flow import _current_flow_state
        from actant._internal.tasks.async_result import _resolve_args_with_deps

        resolved_args, resolved_kwargs, deps = _resolve_args_with_deps(args, kwargs)

        flow_state = _current_flow_state()
        if flow_state is not None:
            return cast(
                "AsyncResult",
                task._flow_submit(
                    runtime, flow_state, resolved_args, resolved_kwargs, deps
                ),
            )

        task_id, _workflow_id, _payload, _task_ctx, handle, task_def = (
            task._prepare_task_def(
                resolved_args,
                resolved_kwargs,
                target_node=target_node,
                target_endpoint_addr=target_endpoint_addr,
                runtime=runtime,
            )
        )
        core = runtime._rust_core
        if core is None:
            runtime.unregister_task(task_id)
            raise InvalidStateError("Runtime not started: rust_core is None")
        try:
            core.submit_task(task_def)
        except Exception:
            runtime.unregister_task(task_id)
            raise
        return cast("AsyncResult", handle)


_backend_local = threading.local()


def resolve_submit_backend() -> SubmitBackend | None:
    """返回当前线程的提交后端；无显式后端且无活跃 Runtime 时返回 ``None``。"""
    override: SubmitBackend | None = getattr(_backend_local, "backend", None)
    if override is not None:
        return override

    runtime = get_current_runtime()
    return EmbeddedSubmitBackend(runtime) if runtime is not None else None


@contextmanager
def use_submit_backend(backend: SubmitBackend | None) -> Iterator[None]:
    """临时替换当前线程的提交后端。"""
    previous = getattr(_backend_local, "backend", None)
    _backend_local.backend = backend
    try:
        yield
    finally:
        _backend_local.backend = previous
