"""工作流编排：``@flow`` 装饰器。

`@flow` 提供与 Prefect ``@flow`` 等价的编排入口：在函数体内调用 ``task.submit()``
即可组合任务，``AsyncResult`` 作为下游 ``submit`` 参数时自动解析依赖。

设计说明
========

当前实现为 **Python 侧编排**：flow 函数直接在调用线程执行，任务依赖通过
``Task.submit`` 的 ``AsyncResult`` 自动解析（阻塞等待上游结果）实现。

与 Prefect/Ray 的差异：
- Prefect 的 ``@flow`` 构建显式 DAG 并由调度器执行；Actant 当前为命令式编排，
  依赖关系由 ``AsyncResult`` 传递隐式表达。
- 后续接入 Rust ``submit_dag`` 后，``@flow`` 可选择编译为 DAG 提交，API 不变。

flow 生命周期通过 ``WorkflowLifecycle`` capability 广播（``submitted``/``started``/
``completed``/``failed``），可由用户订阅做监控/审计。

**Flow 级重试与超时**：
- ``retries``：flow 函数体抛异常时整体重试，最多 ``retries`` 次。
- ``retry_delay_ms``：重试间隔。
- ``timeout_ms``：flow 函数体的总执行时长上限，超时抛 ``ActantTimeoutError``。
  超时为**软超时**（子线程协作）：flow 函数在子线程中执行，主线程等待结果或超时；
  超时后子线程仍可能继续运行（Python 无法强制中断线程），但 ``@flow`` 调用立即返回超时错误。

**Flow 上下文（workflow_id 传播）**：
flow 执行期间设置 ``_flow_local.workflow_id``，``Task.submit`` 读取它作为任务的
``workflow_id``，使 ``TaskEvent`` 携带正确的归属信息。

用法
====

::

    import actant

    @actant.task
    def extract(src):
        return open(src).read()

    @actant.task
    def transform(raw):
        return raw.upper()

    @actant.task
    def load(data):
        print("loaded:", data)

    @actant.flow
    def pipeline(src):
        raw = extract.submit(src)        # AsyncResult
        upper = transform.submit(raw)    # 自动等待 extract 完成后取结果传入
        load.submit(upper)               # 自动等待 transform
        return upper.result()

    with actant.Runtime.with_defaults() as rt:
        rt.layer("WorkflowLifecycle", "emit").chain(
            lambda e: print(f"flow {e.kind}: {e.workflow_id}")
        )
        print(pipeline("data.txt"))
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from functools import wraps
from typing import Any, ParamSpec, TypeVar, cast

from actant._effects import emit
from actant._runtime import get_current_runtime, use_runtime
from actant.capabilities import WORKFLOW_LIFECYCLE, WorkflowEvent
from actant.exceptions import ActantTimeoutError, InvalidStateError
from actant.task._helpers import CallbackErrorPolicy

_logger = logging.getLogger("actant.flow")

P = ParamSpec("P")
R = TypeVar("R")

# 线程局部：当前 flow 的上下文状态，供 Task.submit 读取。
# None 表示不在任何 flow 上下文中。
_flow_local = threading.local()


class _FlowState:
    """单个 flow 实例的状态：workflow_id + 取消协作标记。

    ``cancel_event`` 用于超时协作：主线程超时后设置它，子线程中的
    ``Task.submit`` 在提交新任务前检查它，若已设置则抛出
    ``ActantTimeoutError``，阻止 orphan 任务继续创建。
    """

    def __init__(self, workflow_id: str) -> None:
        self.workflow_id = workflow_id
        self.cancel_event = threading.Event()

    def is_cancelled(self) -> bool:
        return self.cancel_event.is_set()


def current_workflow_id() -> str | None:
    """返回当前线程活跃的 workflow_id（若在 ``@flow`` 上下文中）。"""
    state = getattr(_flow_local, "state", None)
    return state.workflow_id if state is not None else None


def is_flow_cancelled() -> bool:
    """当前 flow 是否已被取消（超时或显式取消）。

    由 ``Task.submit`` 在提交新任务前检查；若返回 ``True``，
    调用方应抛出 ``ActantTimeoutError`` 以阻止 orphan 任务创建。
    不在 flow 上下文中时返回 ``False``。
    """
    state = getattr(_flow_local, "state", None)
    return state is not None and state.is_cancelled()


def _cancel_flow_tasks(workflow_id: str) -> None:
    """取消指定 workflow 下的所有本地任务（flow 失败/超时时调用）。"""
    from actant._runtime import get_current_runtime

    runtime = get_current_runtime()
    if runtime is None:
        return
    for tid in list(runtime.list_tasks()):
        handle = runtime.get_task(tid)
        if handle is not None and handle.workflow_id == workflow_id:
            try:
                handle.cancel(propagate=False)
            except Exception:
                # 批量取消：单个任务取消失败不应阻止后续任务被取消，
                # 记录 warning 后继续（最终由 flow 失败异常向上传播）。
                _logger.warning(
                    "flow %s: failed to cancel task %s", workflow_id, tid,
                    exc_info=True,
                )


class _FlowContext:
    """``with`` 上下文：设置/恢复 ``_flow_local.state``。

    可接受外部 ``cancel_event``，使主线程能在超时后设置子线程的取消信号。
    """

    def __init__(
        self,
        workflow_id: str,
        *,
        cancel_event: threading.Event | None = None,
    ) -> None:
        self._state = _FlowState(workflow_id)
        if cancel_event is not None:
            self._state.cancel_event = cancel_event
        self._prev: _FlowState | None = None

    def __enter__(self) -> _FlowState:
        self._prev = getattr(_flow_local, "state", None)
        _flow_local.state = self._state
        return self._state

    def __exit__(self, *exc: object) -> None:
        _flow_local.state = self._prev


def flow(
    func: Callable[P, R] | None = None,
    *,
    name: str | None = None,
    retries: int = 0,
    retry_delay_ms: int = 0,
    timeout_ms: int = 0,
    compiled: bool = False,
    mode: str = "imperative",
) -> Any:
    """装饰器：将函数标记为工作流，提供生命周期事件、重试、超时与上下文校验。

    被装饰的函数行为：

    1. 调用前校验存在活跃 ``Runtime``（否则抛 ``InvalidStateError``）。
    2. 生成 ``workflow_id`` 并设置 flow 上下文，使函数体内的 ``task.submit()``
       自动携带该 ``workflow_id``（配套 ``TaskEvent`` 归属）。
    3. 广播 ``WorkflowLifecycle`` 事件（submitted → started → completed/failed）。
    4. Flow 级重试（``retries``）：函数体抛异常时整体重试。
    5. Flow 级超时（``timeout_ms``）：函数体在子线程执行，超时抛 ``ActantTimeoutError``。
    6. 可选 DAG 编译（``compiled=True`` 或 ``mode="dag"``）：首次执行时捕获
       提交序列编译为静态 DAG，后续调用复用 DAG 调度，避免重复执行 flow
       函数体的解释开销。仅适用于"纯提交"型 flow（不在体内调用 ``result()``
       做条件分支）；检测到无法编译时自动回退命令式执行。

    Args:
        func: 被装饰的编排函数（无参装饰器时由 ``flow`` 自动填充）。
        name: 工作流名称（默认 ``func.__qualname__``），用于日志与事件。
        retries: 失败后的重试次数（0=不重试）。
        retry_delay_ms: 重试间隔毫秒。
        timeout_ms: flow 函数体的总执行超时毫秒（0=不限制）。
        compiled: ``True`` 启用 DAG 编译（兼容旧参数）。首选 ``mode`` 参数。
        mode: 执行模式，``"imperative"``（默认，命令式编排）或 ``"dag"``
            （编译为静态 DAG 提交）。``mode="dag"`` 等价于 ``compiled=True``。
            两者同时设置时以 ``mode`` 为准。

    Raises:
        InvalidStateError: 无活跃 Runtime。
        ActantTimeoutError: flow 执行超时。
        ValueError: ``mode`` 不是合法值。
    """
    # 统一 mode 参数到 compiled 布尔：mode 优先，compiled 兼容。
    if mode not in ("imperative", "dag"):
        raise ValueError(
            f"flow: mode must be 'imperative' or 'dag', got {mode!r}"
        )
    use_compiled = compiled or (mode == "dag")

    def _make(f: Callable[P, R]) -> Callable[P, R]:
        if retries < 0:
            raise ValueError(f"flow: retries must be >= 0, got {retries}")
        if retry_delay_ms < 0:
            raise ValueError(
                f"flow: retry_delay_ms must be >= 0, got {retry_delay_ms}"
            )
        flow_name = name or f.__qualname__

        @wraps(f)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            runtime = get_current_runtime()
            if runtime is None:
                raise InvalidStateError(
                    f"flow {flow_name!r}: no active Runtime; "
                    "wrap your code in `with actant.Runtime() as rt:`"
                )
            workflow_id = f"{flow_name}-{uuid.uuid4().hex[:8]}"
            _safe_emit(workflow_id, "submitted")
            _safe_emit(workflow_id, "started")
            try:
                result = _run_flow_with_retry(
                    f, args, kwargs, workflow_id,
                    retries=retries,
                    retry_delay_ms=retry_delay_ms,
                    timeout_ms=timeout_ms,
                    runtime=runtime,
                    compiled=use_compiled,
                    cache_target=wrapper,
                )
            except BaseException as exc:
                _safe_emit(workflow_id, "failed", error=f"{type(exc).__name__}: {exc}")
                # 级联取消：flow 失败/超时后取消其所有子任务（最佳-effort）。
                _cancel_flow_tasks(workflow_id)
                raise
            _safe_emit(workflow_id, "completed")
            return result

        return wrapper

    if func is None:
        return _make
    return _make(func)


def _run_flow_with_retry(
    func: Callable[..., R],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    workflow_id: str,
    *,
    retries: int,
    retry_delay_ms: int,
    timeout_ms: int,
    runtime: Any = None,
    compiled: bool = False,
    cache_target: Any = None,
) -> R:
    """在 flow 上下文中执行函数体，支持重试与超时。

    Args:
        compiled: ``True`` 启用 DAG 编译路径。首次执行 trace flow 体，
            编译为静态 DAG 后缓存复用；后续调用按拓扑序并行 submit。
            编译失败（体内调用 result() 等）自动回退命令式。
        cache_target: DAG 缓存目标对象（通常是装饰器 wrapper），
            传给 ``run_compiled_flow`` 使外部可通过 wrapper 访问缓存的 DAG。
    """
    attempt = 0
    while True:
        try:
            return _run_with_timeout_in_context(
                func, args, kwargs, workflow_id, timeout_ms, runtime,
                compiled=compiled, cache_target=cache_target,
            )
        except Exception as exc:
            if attempt < retries:
                _logger.warning(
                    "flow %s: attempt %d failed (%s: %s), retrying",
                    workflow_id, attempt, type(exc).__name__, exc,
                )
                # 重试前清理上一轮已提交的子任务，避免 orphan 任务继续执行
                # 产生副作用（如重复写库）。
                _cancel_flow_tasks(workflow_id)
                if retry_delay_ms > 0:
                    time.sleep(retry_delay_ms / 1000)
                attempt += 1
                continue
            raise


def _run_with_timeout_in_context(
    func: Callable[..., R],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    workflow_id: str,
    timeout_ms: int,
    runtime: Any = None,
    *,
    compiled: bool = False,
    cache_target: Any = None,
) -> R:
    """在 flow 上下文中执行 func，可选超时。

    无超时（``timeout_ms <= 0``）时直接在当前线程执行，保留 flow 上下文。
    有超时时在子线程执行，主线程等待结果；超时抛 ``ActantTimeoutError``。

    超时治理：
    主线程超时后设置 ``cancel_event``，子线程中后续的 ``Task.submit`` 调用
    会检查该事件并抛出 ``ActantTimeoutError``，阻止 orphan 任务继续创建。
    注意：Python 无法强制中断线程，正在运行的同步代码不受影响，但新任务
    提交会被拦截。

    Args:
        compiled: ``True`` 启用 DAG 编译路径。无超时模式下，编译路径在
            当前线程直接执行（DAG 调度本身不阻塞 submit）。超时模式下
            编译路径在子线程执行（与命令式一致）。
        cache_target: DAG 缓存目标对象，传给 ``run_compiled_flow``。
    """
    if timeout_ms <= 0:
        # 无超时：当前线程执行。若启用 compiled，优先走编译路径。
        # run_compiled_flow 内部已封装 FlowContext 设置（trace 期 + 运行期）。
        if compiled:
            from actant._flow_compiled import run_compiled_flow
            used_compiled, compiled_result = run_compiled_flow(
                func, args, kwargs, workflow_id,
                cache_target=cache_target,
            )
            if used_compiled:
                return cast("R", compiled_result)
            # 编译失败：回退命令式。
        with _FlowContext(workflow_id):
            return func(*args, **kwargs)

    # 超时模式：子线程执行，主线程等待。
    # 共享 cancel_event：主线程超时后 set()，子线程中 Task.submit 检查它。
    cancel_event = threading.Event()
    result_box: list[Any] = [None]
    exc_box: list[BaseException | None] = [None]
    done_event = threading.Event()

    def _runner() -> None:
        # 子线程需显式继承 Runtime 上下文（threading.local 不跨线程传播）。
        # runtime 由主线程在调用 _run_with_timeout_in_context 前通过
        # get_current_runtime() 获取并传入。runtime 为 None 时用 nullcontext。
        rt_ctx = use_runtime(runtime) if runtime is not None else nullcontext()
        try:
            with rt_ctx, _FlowContext(workflow_id, cancel_event=cancel_event) as state:
                if compiled:
                    from actant._flow_compiled import run_compiled_flow
                    used_compiled, compiled_result = run_compiled_flow(
                        func, args, kwargs, workflow_id,
                        cache_target=cache_target,
                    )
                    if used_compiled:
                        result_box[0] = compiled_result
                        return
                result_box[0] = func(*args, **kwargs)
        except BaseException as e:
            # 若主线程已设置 cancel_event，将子线程异常替换为 ActantTimeoutError，
            # 避免将 orphan 线程的后续异常泄漏给调用方。
            if state.cancel_event.is_set():
                exc_box[0] = ActantTimeoutError(
                    f"flow {workflow_id!r} exceeded timeout_ms={timeout_ms}"
                )
            else:
                exc_box[0] = e
        finally:
            done_event.set()

    thread = threading.Thread(
        target=_runner,
        name=f"actant-flow-{workflow_id}",
        daemon=True,
    )
    # 注册到 Runtime 供 stop() join。
    if runtime is not None:
        runtime.register_flow_thread(thread)
    thread.start()
    try:
        completed = done_event.wait(timeout=timeout_ms / 1000)
    finally:
        if runtime is not None:
            runtime.unregister_flow_thread(thread)
    if not completed:
        # 超时：设置 cancel_event，使子线程中后续 Task.submit 抛出 ActantTimeoutError。
        cancel_event.set()
        raise ActantTimeoutError(
            f"flow {workflow_id!r} exceeded timeout_ms={timeout_ms}"
        )
    if exc_box[0] is not None:
        raise exc_box[0]
    return cast("R", result_box[0])


def _safe_emit(
    workflow_id: str,
    kind: str,
    *,
    error: str = "",
    on_error: CallbackErrorPolicy = "log",
) -> Exception | None:
    """广播 WorkflowLifecycle 事件，失败时按 ``on_error`` 策略处理。

    Args:
        on_error: ``log``（默认）仅记录 warning；``raise`` 立即抛出；
            ``collect`` 记录并返回异常。

    Returns:
        捕获到的异常（仅当 ``on_error="collect"`` 时）。
    """
    try:
        emit(
            WORKFLOW_LIFECYCLE,
            WorkflowEvent(
                kind=kind,  # type: ignore[arg-type]
                workflow_id=workflow_id,
                error=error,
            ),
        )
    except Exception as e:
        if on_error == "raise":
            raise
        _logger.warning(
            "flow %r: WorkflowLifecycle emit (%s) failed", workflow_id, kind,
            exc_info=True,
        )
        if on_error == "collect":
            return e
    return None


__all__ = [
    "current_workflow_id",
    "flow",
]
