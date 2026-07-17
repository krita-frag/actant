"""``Task`` 类与 ``@task`` 装饰器。

依赖 ``_async_result``（``AsyncResult`` / ``_resolve_value``）、``_context``
（``TaskContext``）、``_helpers``（``_safe_serialize``）。``actant.flow`` 与
``actant.actant`` 的引用延迟导入，避免循环依赖。
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Iterable
from functools import wraps
from typing import Any

from actant._runtime import get_current_runtime
from actant.exceptions import InvalidStateError
from actant.task._async_result import AsyncResult, _resolve_value
from actant.task._context import TaskContext
from actant.task._helpers import _safe_serialize

_logger = logging.getLogger("actant.task")


class Task:
    """由 ``@task`` 装饰器产生的可执行任务对象。

    支持两种调用方式：
    - 直接调用 ``task(*args, **kwargs)``：本地同步执行（等价于原函数）。
    - ``task.submit(*args, **kwargs)``：异步提交，返回 ``AsyncResult``。

    Args:
        func: 被包装的函数。
        name: 任务名称（默认 ``模块.函数名``）。
        timeout_ms: 每次尝试的超时（毫秒），0 表示无超时。
        retries: 失败后重试次数（默认 0，不重试）。
        retry_delay_ms: 重试间隔（毫秒，默认 0）。
        tags: 任务标签列表，供 Routing capability 的 ``RouteCtx.tags`` 使用。
        priority: 任务优先级（有符号整数，越大越优先）。
    """

    # 注意：不使用 __slots__。functools.wraps 会设置 __name__/__doc__/__wrapped__ 等
    # 属性，__slots__ 会静默丢弃它们，导致 inspect.signature 等工具失效。
    # Task 实例数量少，__slots__ 收益可忽略。

    def __init__(
        self,
        func: Callable[..., Any],
        *,
        name: str | None = None,
        timeout_ms: int = 0,
        retries: int = 0,
        retry_delay_ms: int = 0,
        tags: list[str] | None = None,
        priority: int | None = None,
    ) -> None:
        self._func = func
        self._name = name or f"{func.__module__}.{func.__qualname__}"
        self._timeout_ms = timeout_ms
        self._retries = retries
        self._retry_delay_ms = retry_delay_ms
        self._tags = list(tags) if tags else []
        self._priority = priority
        # 保留原函数的元数据，使 Task 可被 functools.wraps / inspect 等工具识别。
        wraps(func)(self)

    @property
    def name(self) -> str:
        return self._name

    @property
    def func(self) -> Callable[..., Any]:
        return self._func

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """本地同步调用（等价于原函数）。"""
        return self._func(*args, **kwargs)

    def submit(self, *args: Any, **kwargs: Any) -> AsyncResult:
        """异步提交任务到分布式 Worker，返回 ``AsyncResult`` 句柄。

        将函数与参数用 cloudpickle 序列化为 payload，通过
        ``rust_core.submit_task`` 提交到 Rust Worker 调度器。Worker 拉取并
        调用 dispatch handler 执行任务，结果通过 event_bus 回调解析。

        **任务依赖**：参数中的 ``AsyncResult`` 会自动阻塞解析为其结果，
        使 ``b.submit(a.submit(x))`` 表达依赖关系。

        Raises:
            InvalidStateError: 无活跃 Runtime。
        """
        return self._submit(args, kwargs, target_node=None, target_endpoint_addr=None)

    def submit_to(
        self,
        target_node: str,
        *args: Any,
        endpoint_addr: str | None = None,
        **kwargs: Any,
    ) -> AsyncResult:
        """异步提交任务到指定远程 Worker。

        Args:
            target_node: 目标 Actant ``node_id``。
            endpoint_addr: 目标 ``listen_addresses()["endpoint_addr"]``。
                若省略，使用 ``target_node`` 作为直连地址。
        """
        return self._submit(
            args,
            kwargs,
            target_node=target_node,
            target_endpoint_addr=endpoint_addr or target_node,
        )

    def _submit(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        target_node: str | None,
        target_endpoint_addr: str | None,
    ) -> AsyncResult:
        runtime = get_current_runtime()
        if runtime is None:
            raise InvalidStateError(
                "task.submit: no active Runtime; "
                "wrap your code in `with actant.Runtime() as rt:`"
            )

        # 自动解析参数中的 AsyncResult（阻塞等待其结果），表达任务依赖。
        resolved_args = tuple(_resolve_value(a) for a in args)
        resolved_kwargs = {k: _resolve_value(v) for k, v in kwargs.items()}

        task_id = f"{self._name}-{uuid.uuid4().hex[:8]}"
        # 从 flow 上下文继承 workflow_id，使 TaskEvent 归属正确。
        from actant.flow import current_workflow_id, is_flow_cancelled

        workflow_id = current_workflow_id() or ""
        options = {
            "retries": self._retries,
            "retry_delay_ms": self._retry_delay_ms,
            "timeout_ms": self._timeout_ms,
            "task_id": task_id,
            "workflow_id": workflow_id,
            "tags": self._tags,
            "priority": self._priority,
        }
        payload = _safe_serialize(
            self._func, resolved_args, resolved_kwargs, options, task_id=self._name,
        )

        # Flow 超时治理：若当前 flow 已被取消（超时），
        # 拒绝提交新任务，阻止 orphan 线程继续创建任务。
        if is_flow_cancelled():
            from actant.exceptions import ActantTimeoutError
            raise ActantTimeoutError(
                f"flow {workflow_id!r} has been cancelled (timeout); "
                f"cannot submit new task {task_id!r}"
            )

        # 创建任务上下文（取消系统）：关联到 AsyncResult。
        task_ctx = TaskContext(task_id, workflow_id=workflow_id)

        # 创建 AsyncResult 句柄（由 Runtime._on_task_result 回调解析）。
        handle = AsyncResult(
            task_id, context=task_ctx, workflow_id=workflow_id,
        )
        # 注册到 Runtime 任务表，供 list_tasks/get_task/cancel_task 查询。
        runtime.register_task(task_id, handle)

        # 构造 _TaskDef 并提交到 Rust Worker 调度器。
        from actant.actant import _TaskDef

        task_def = _TaskDef(
            task_id=task_id,
            name=self._name,
            payload=payload,
            workflow_id=workflow_id or None,
            target_node=target_node,
            target_endpoint_addr=target_endpoint_addr,
            timeout_ms=self._timeout_ms if self._timeout_ms > 0 else None,
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
        return handle

    def delay(self, *args: Any, **kwargs: Any) -> AsyncResult:
        """``submit`` 的别名（Celery 风格）。"""
        return self.submit(*args, **kwargs)

    def map(self, iterable: Iterable[Any]) -> list[AsyncResult]:
        """对 iterable 中每个元素提交一个任务，返回 ``AsyncResult`` 列表。

        等价于 ``[self.submit(x) for x in iterable]``，但语义更明确，对标
        Prefect ``task.map`` / Ray ``[task.remote(x) for x in xs]``。

        Args:
            iterable: 可迭代对象，每个元素作为 ``submit`` 的唯一位置参数。

        Returns:
            ``AsyncResult`` 列表，顺序与输入一致。

        用法::

            @actant.task
            def square(x): return x * x
            with actant.Runtime.with_defaults():
                handles = square.map([1, 2, 3])  # 并行提交 3 个任务
                results = actant.gather(*handles)  # [1, 4, 9]
        """
        return [self.submit(item) for item in iterable]

    def starmap(self, iterable: Iterable[Any]) -> list[AsyncResult]:
        """对 iterable 中每个元素解包后提交任务。

        等价于 ``[self.submit(*args) for args in iterable]``，对标
        ``itertools.starmap`` / Prefect ``task.map(unpack=True)``。

        Args:
            iterable: 可迭代对象，每个元素为 ``tuple``/``list``，解包后作为
                ``submit`` 的位置参数。

        Returns:
            ``AsyncResult`` 列表。

        用法::

            @actant.task
            def add(a, b): return a + b
            with actant.Runtime.with_defaults():
                handles = add.starmap([(1, 2), (3, 4)])
                results = actant.gather(*handles)  # [3, 7]
        """
        return [self.submit(*item) for item in iterable]

    def __repr__(self) -> str:
        return f"Task(name={self._name!r})"


def task(
    func: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    timeout_ms: int = 0,
    retries: int = 0,
    retry_delay_ms: int = 0,
    tags: list[str] | None = None,
    priority: int | None = None,
) -> Any:
    """装饰器：将函数转换为 ``Task`` 对象。

    用法::

        @actant.task
        def fetch(url):
            ...

        @actant.task(name="heavy-compute", timeout_ms=60000, retries=3, retry_delay_ms=1000)
        def compute(data):
            ...

        @actant.task(tags=["gpu"], priority=10)
        def train(model):
            ...

    Args:
        func: 被装饰的函数。为 ``None`` 时返回装饰器工厂（支持参数化装饰）。
        name: 任务名称（默认为 ``模块.函数名``）。
        timeout_ms: 每次尝试的超时（毫秒），0 表示无超时。
        retries: 失败重试次数（默认 0）。重试由 generic handler 执行。
        retry_delay_ms: 重试间隔（毫秒，默认 0）。
        tags: 任务标签列表，供 Routing capability 的 ``RouteCtx.tags`` 使用。
            路由 handler 可基于 tags 决定任务路由目标节点。
        priority: 任务优先级（有符号整数，越大越优先），供 Scheduling capability
            的优先级调度器使用。``None`` 表示使用调度器默认优先级。
    """

    def _make(f: Callable[..., Any]) -> Task:
        return Task(
            f,
            name=name,
            timeout_ms=timeout_ms,
            retries=retries,
            retry_delay_ms=retry_delay_ms,
            tags=tags,
            priority=priority,
        )

    if func is None:
        # 带参数调用：@actant.task(name="foo")
        return _make
    # 无参数调用：@actant.task
    return _make(func)


__all__ = ["Task", "task"]
