"""异步工作流：``@aio.flow`` 与协程化的等待点 API。

``@aio.flow`` 与 ``@flow`` 共享同一套 journal、重放与等待点语义，差别只在
**谁在等**：同步 flow 在等待点上阻塞线程，异步 flow 在 event loop 上
``await``。因此单进程可用一个事件循环承载万级 parked workflow，线程数不随
并发数增长。

函数体接收一个 :class:`AsyncFlowContext`::

    from actant import aio, task
    from actant.aio import AsyncFlowContext

    @task
    def write_audit(doc, decision):
        print("audit:", doc, decision)

    @aio.flow
    async def approve(ctx: AsyncFlowContext, doc: str) -> str:
        await ctx.sleep(60)
        decision = (await ctx.wait_signal("approval")).decode()
        await ctx.submit(write_audit, doc, decision)
        return decision

驱动：``from actant.aio import AsyncDriver`` 后
``actant.host_flows(driver=AsyncDriver())``。等待点的键与序号
规则与同步 API 完全一致（``timer-{n}`` / 信号名），故重放、信号早到缓冲、
跨重启恢复均复用既有实现，不新增 journal 原语。
"""

from __future__ import annotations

import asyncio
import inspect
import time
import uuid
from collections.abc import Callable
from functools import partial, wraps
from typing import Any, cast

import cloudpickle

from actant._internal.flow import (
    _FLOW_TERMINAL_POLL_INTERVAL_S,
    _VALID_FAILURE_STRATEGIES,
    FLOW_REGISTRY,
    FlowDefinition,
    _ContinueAsNew,
    _current_flow_state,
    _deadline_failure,
    _emit_terminal,
    _FlowContext,
    _FlowState,
    _raise_if_workflow_terminal,
    _settle_flow_failure,
    _terminal_kind,
    _wait_context,
    _workflow_deadline_expired,
    _workflow_timeout_error,
)
from actant._internal.runtime import _set_current_runtime, get_current_runtime
from actant.exceptions import ActantTimeoutError, InvalidStateError
from actant.tasks import AsyncResult


class AsyncFlowContext:
    """异步 flow 体的确定性上下文（由 ``aio.flow`` 注入）。

    所有方法都只能在 ``aio.flow`` 函数体内调用。等待语义与同步 API
    一一对应：

    - :meth:`sleep` ↔ ``actant.workflow.sleep_until`` / 持久化 timer 等待点；
    - :meth:`wait_signal` ↔ ``actant.workflow.wait_signal``；
    - :meth:`submit` ↔ ``Task.submit``（节点先持久化再派发）。
    """

    async def sleep(self, seconds: float) -> None:
        """挂起指定秒数（``seconds`` 为非负数）。

        复用持久化 timer 等待点，键由**等待序号**决定（``timer-{n}``）。重放
        时若该等待点已到期，立即返回；否则等到原 deadline，不会重新计时。

        Raises:
            ValueError: ``seconds`` 为负。
            ActantTimeoutError: 工作流级 deadline 到期，或等待点被释放。
        """
        if seconds < 0:
            raise ValueError(f"ctx.sleep: seconds must be >= 0, got {seconds}")
        runtime, state = _wait_context()
        wait_key = f"timer-{state.next_wait_seq()}"
        deadline_ms = int(time.time() * 1000) + int(seconds * 1000)
        runtime._register_wait_point(
            state.workflow_id,
            wait_key,
            kind="timer",
            deadline_ms=deadline_ms,
        )
        bound = state.timeout_ms
        if await _park_payload_async(runtime, state.workflow_id, wait_key, bound_ms=bound) is None:
            raise ActantTimeoutError(
                f"flow {state.workflow_id!r}: ctx.sleep({seconds}) was not woken up"
            )

    async def wait_signal(
        self,
        name: str,
        *,
        timeout_ms: int = 0,
    ) -> bytes | None:
        """等待具名信号，返回递交时携带的字节（``None`` 表示超时）。

        信号早于等待点注册时由 Rust 侧缓冲，注册瞬间即生成为已唤醒态；
        重放体重复执行到同一 ``wait_signal`` 时直接返回历史 payload。

        Args:
            name: 信号语义名，同时作为等待点键。
            timeout_ms: 本地等待上界（0=沿用工作流级 deadline）。

        Raises:
            ActantTimeoutError: 工作流级 deadline 到期。
        """
        runtime, state = _wait_context()
        runtime._register_wait_point(state.workflow_id, name, kind="signal", name=name)
        if timeout_ms > 0:
            return await _park_payload_async(runtime, state.workflow_id, name, bound_ms=timeout_ms)
        bound = state.timeout_ms
        payload = await _park_payload_async(runtime, state.workflow_id, name, bound_ms=bound)
        if payload is None and bound > 0:
            raise ActantTimeoutError(
                f"flow {state.workflow_id!r}: wait_signal({name!r}) "
                f"exceeded the workflow deadline ({bound}ms)"
            )
        return payload

    async def submit(self, target: Any, /, *args: Any, **kwargs: Any) -> Any:
        """提交任务并等待其完成，返回任务结果。

        语义等价于同步 flow 里的 ``task.submit(...).result()``：节点先持久化
        再派发，重放命中时直接返回记录结果。参数中的 :class:`AsyncResult`
        通过 ``await`` 等待（而不是阻塞线程）；等待完成后仍把上游 ``task_id``
        作为 DAG 依赖传给 orchestrator，失败传播与重放命中语义不变。

        Args:
            target: ``@task`` 装饰后的任务对象。
            *args: 任务位置参数（可含 ``AsyncResult``）。
            **kwargs: 任务关键字参数（可含 ``AsyncResult``）。

        Returns:
            任务返回值。大结果（``Ref``）已透明拉取反序列化。
        """
        runtime = get_current_runtime()
        if runtime is None:
            raise InvalidStateError(
                "ctx.submit: no active Runtime; wrap your code in `with actant.Runtime() as rt:`"
            )
        state = _current_flow_state()
        if state is None:
            raise InvalidStateError("ctx.submit 只能在 aio.flow 函数体内调用")
        resolved_args, resolved_kwargs, deps = await _resolve_args_async(args, kwargs)
        handle = cast(
            AsyncResult,
            target._flow_submit(runtime, state, resolved_args, resolved_kwargs, deps),
        )
        return await handle


async def _park_payload_async(
    runtime: Any,
    workflow_id: str,
    wait_key: str,
    *,
    bound_ms: int,
) -> bytes | None:
    """异步版 :func:`actant._internal.flow._park_payload`：await 等待点而非阻塞线程。"""
    payload = await runtime._wait_wait_point_async(workflow_id, wait_key, timeout_ms=bound_ms)
    if payload is None:
        # 与同步路径同源：``None`` 既可能是超时，也可能是工作流终态释放了
        # 等待者。先排除后者，否则会把 abort 当成空 payload。
        _raise_if_workflow_terminal(runtime, workflow_id)
    return cast("bytes | None", payload)


async def _resolve_value_async(
    value: Any,
    seen: set[str],
    deps: list[str],
) -> Any:
    """递归 ``await`` 参数树中的 ``AsyncResult``，并收集 DAG 依赖 id。"""
    if isinstance(value, AsyncResult):
        if value.task_id not in seen:
            seen.add(value.task_id)
            deps.append(value.task_id)
        await value
        ref = value.ref()
        return ref if ref is not None else value.result(timeout=0)
    if isinstance(value, list):
        return [await _resolve_value_async(v, seen, deps) for v in value]
    if isinstance(value, tuple):
        return tuple([await _resolve_value_async(v, seen, deps) for v in value])
    if isinstance(value, dict):
        return {k: await _resolve_value_async(v, seen, deps) for k, v in value.items()}
    return value


async def _resolve_args_async(
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> tuple[tuple[Any, ...], dict[str, Any], list[str]]:
    """异步解析提交参数，返回 ``(args, kwargs, deps)``。"""
    seen: set[str] = set()
    deps: list[str] = []
    resolved_args = tuple([await _resolve_value_async(v, seen, deps) for v in args])
    resolved_kwargs = {k: await _resolve_value_async(v, seen, deps) for k, v in kwargs.items()}
    return resolved_args, resolved_kwargs, deps


async def _run_async_body(
    func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    state: _FlowState,
) -> Any:
    """在 flow 上下文中执行协程函数体。

    ``_FlowContext`` 的 ``ContextVar`` 随当前 asyncio Task 隔离，``with`` 块
    跨越 ``await`` 也不会被同一 loop 上的其他 flow 污染。
    """
    ctx = AsyncFlowContext()
    with _FlowContext(state):
        return await func(ctx, *args, **kwargs)


async def _wait_terminal_and_emit_async(
    runtime: Any,
    workflow_id: str,
    *,
    on_completed: Callable[[], None] | None = None,
) -> dict[str, Any] | None:
    """异步版 :func:`actant._internal.flow._wait_terminal_and_emit`。

    终态分类复用 :func:`actant._internal.flow._terminal_kind`；轮询间隔内 ``await``
    让出 loop，使其他 parked flow 继续推进。
    """
    while True:
        state = cast(
            "dict[str, Any] | None",
            runtime.get_workflow_state(workflow_id),
        )
        if _emit_terminal(
            workflow_id,
            state,
            _terminal_kind(state),
            on_completed=on_completed,
        ):
            return state
        await asyncio.sleep(_FLOW_TERMINAL_POLL_INTERVAL_S)


async def _replay_async_flow(
    func: Callable[..., Any],
    workflow_id: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    runtime: Any = None,
    timeout_ms: int = 0,
    failure_strategy: str | None = None,
    version: int = 0,
    flow_name: str = "",
    announce: bool = False,
) -> Any:
    """续跑入口：与 :func:`actant._internal.flow._replay_flow` 同构的协程版本。

    逐代执行函数体，遇 ``continue_as_new`` 时封口旧代并推进到新代；所有
    journal 语义（节点重放命中、等待点幂等、版本比对）都由被复用的同步
    原语承担，本函数只把阻塞点换成 ``await``。
    """
    if runtime is None:
        runtime = get_current_runtime()
    if runtime is None:
        raise InvalidStateError(
            f"async flow replay {workflow_id!r}: no active Runtime; "
            "wrap your code in `with actant.Runtime() as rt:`"
        )
    run_args = args
    run_kwargs = dict(kwargs)
    while True:
        state = _FlowState(
            workflow_id,
            flow_name=flow_name,
            failure_strategy=failure_strategy,
            timeout_ms=timeout_ms,
            version=version,
            invocation_args=cloudpickle.dumps((run_args, run_kwargs)),
            announce=announce,
            existing=not announce,
        )
        try:
            result = await _run_async_body(func, run_args, run_kwargs, state)
        except _ContinueAsNew as signal:
            if state.workflow_created or state.existing:
                runtime._seal_workflow(workflow_id)
            terminal = await _wait_terminal_and_emit_async(runtime, workflow_id)
            if _deadline_failure(terminal):
                raise _workflow_timeout_error(workflow_id) from signal
            workflow_id = signal.workflow_id
            run_args = signal.args
            run_kwargs = signal.kwargs
            continue
        except BaseException as exc:
            if _workflow_deadline_expired(runtime, state):
                timeout_exc = _workflow_timeout_error(workflow_id)
                _settle_flow_failure(runtime, state, timeout_exc)
                raise timeout_exc from exc
            _settle_flow_failure(runtime, state, exc)
            raise
        if state.workflow_created or state.existing:
            runtime._seal_workflow(workflow_id)
        terminal = await _wait_terminal_and_emit_async(
            runtime,
            workflow_id,
            on_completed=partial(runtime._persist_flow_result, workflow_id, result),
        )
        if _deadline_failure(terminal):
            raise _workflow_timeout_error(workflow_id)
        return result


async def _replay_async_in_context(
    runtime: Any,
    func: Callable[..., Any],
    workflow_id: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    timeout_ms: int = 0,
    failure_strategy: str | None = None,
    version: int = 0,
    flow_name: str = "",
) -> None:
    """host loop 入口：把 loop 线程绑定到 ``runtime`` 后重放。

    event loop 线程在驱动生命周期内固定服务一个 Runtime，故这里直接写线程
    局部绑定而不恢复；协程共享同一 Runtime 时不会互相干扰。
    """
    _set_current_runtime(runtime)
    await _replay_async_flow(
        func,
        workflow_id,
        args,
        kwargs,
        runtime=runtime,
        timeout_ms=timeout_ms,
        failure_strategy=failure_strategy,
        version=version,
        flow_name=flow_name,
        announce=False,
    )


def flow(
    func: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    timeout_ms: int = 0,
    failure_strategy: str | None = None,
    version: int = 0,
) -> Any:
    """装饰器：把 ``async def`` 函数标记为可重放的异步工作流。

    与 :func:`actant.flow` 的差异只有两点：函数体必须是 ``async def``，
    且首个参数是 :class:`AsyncFlowContext`。调用返回协程，``await`` 它即执行
    工作流并得到返回值；分离式执行仍用 ``start_flow`` + ``host_flows``。

    Args:
        func: 被装饰的协程函数（无参装饰器时由 ``aio.flow`` 自动填充）。
        name: 工作流名称（默认 ``func.__qualname__``）。
        timeout_ms: 工作流级 deadline 毫秒（0=不限制）。
        failure_strategy: ``"fail_fast"`` / ``"continue"`` / ``None``。
        version: flow 定义版本（非负整数）。

    Raises:
        TypeError: 被装饰对象不是 ``async def`` 函数。
        ValueError: ``failure_strategy`` 非法或 ``version`` 为负。
    """

    def _make(f: Callable[..., Any]) -> Callable[..., Any]:
        if not inspect.iscoroutinefunction(f):
            raise TypeError("aio.flow: decorated function must be declared with `async def`")
        if version < 0:
            raise ValueError(f"aio.flow: version must be >= 0, got {version}")
        if failure_strategy is not None and failure_strategy not in _VALID_FAILURE_STRATEGIES:
            raise ValueError(
                f"aio.flow: failure_strategy must be one of "
                f"{_VALID_FAILURE_STRATEGIES} or None, "
                f"got {failure_strategy!r}"
            )
        flow_name = name or f.__qualname__
        FLOW_REGISTRY.register(
            FlowDefinition(
                name=flow_name,
                func=f,
                version=version,
                timeout_ms=timeout_ms,
                failure_strategy=failure_strategy,
                is_async=True,
            )
        )

        @wraps(f)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            runtime = get_current_runtime()
            if runtime is None:
                raise InvalidStateError(
                    f"async flow {flow_name!r}: no active Runtime; "
                    "wrap your code in `with actant.Runtime() as rt:`"
                )
            return await _replay_async_flow(
                f,
                f"{flow_name}-{uuid.uuid4().hex[:8]}",
                args,
                dict(kwargs),
                runtime=runtime,
                timeout_ms=timeout_ms,
                failure_strategy=failure_strategy,
                version=version,
                flow_name=flow_name,
                announce=True,
            )

        setattr(wrapper, "__actant_flow_name__", flow_name)  # noqa: B010
        setattr(wrapper, "__actant_flow_version__", version)  # noqa: B010
        return wrapper

    if func is None:
        return _make
    return _make(func)


__all__ = ["AsyncFlowContext", "flow"]
