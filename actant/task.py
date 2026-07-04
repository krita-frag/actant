"""Task：延迟任务定义与 TaskRef 结果引用。

Task 是 @actant.task 装饰后的对象，保存函数定义和执行选项。
在 flow 上下文中调用 Task 返回 TaskRef，自动追踪依赖关系。
在 flow 上下文外调用 Task 直接执行函数。

TaskRef 是延迟计算的结果引用，可await 获取结果。
"""

from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import inspect
import threading
import warnings
from collections.abc import Callable, Generator, Sequence
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from actant.config import PriorityInput
from actant.exceptions import InvalidStateError

# 缓存的 flow 上下文访问器 —— 避免每次 Task.__call__ 都进行延迟导入的开销。
# 由 _get_flow_context() 延迟初始化，以打破 task→flow 的循环依赖。
_flow_ctx_getter: Callable[[], Any] | None = None


def _get_flow_context() -> Any:
    """获取当前 FlowContext，或在 flow 上下文外返回 None。

    缓存从 actant.flow 模块导入的 _current_flow_context 函数，避免每次调用都导入模块。
    """
    global _flow_ctx_getter
    if _flow_ctx_getter is None:
        from actant.flow import _current_flow_context
        _flow_ctx_getter = _current_flow_context
    return _flow_ctx_getter()


def _run_sync_or_async(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """同步调用 func；若 func 为 async 则在复用线程池中运行事件循环。

    处理三种场景：
    1. func 为同步函数 → 直接调用
    2. func 为 async 函数，无运行中的事件循环 → asyncio.run()
    3. func 为 async 函数，已有运行中的事件循环 → 在复用线程池中 asyncio.run()
    """
    if not inspect.iscoroutinefunction(func):
        return func(*args, **kwargs)

    coro = func(*args, **kwargs)

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # 无运行中的 loop — 可安全使用 asyncio.run()
        return asyncio.run(coro)

    # 检测到运行中的事件循环 —— 在独立线程中运行以避免死锁。
    # 复用模块级线程池，避免每次调用都创建新线程池。
    future = _ASYNC_EXECUTOR.submit(asyncio.run, coro)
    return future.result()


def _merge_task_options(
    task_obj: Task,
    priority: PriorityInput,
    timeout: float | None,
    retry_policy: dict[str, Any] | None,
    tags: list[str] | None,
    metadata: dict[str, str] | None,
) -> dict[str, Any]:
    """合并 Task 默认值、当前活跃注解和调用参数，返回最终执行选项。

    优先级：调用参数 > actant.annotate() 注解 > Task 注册默认值。

    Args:
        task_obj: Task 实例，提供注册默认值。
        priority/timeout/retry_policy/tags/metadata: __call__/map/reduce
            显式传入的参数，None 表示未传入。

    Returns:
        合并后的选项字典，包含 keys:
        priority, timeout, retry_policy, tags, metadata。
    """
    from actant._annotations import merge_options

    return merge_options(
        defaults={
            "priority": task_obj._priority,
            "timeout": task_obj._timeout,
            "retry_policy": task_obj._retry_policy,
            "tags": task_obj._tags,
            "metadata": task_obj._metadata,
        },
        overrides={
            "priority": priority,
            "timeout": timeout,
            "retry_policy": retry_policy,
            "tags": tags,
            "metadata": metadata,
        },
    )


# 模块级线程池，用于异步任务调度。
_ASYNC_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=4,
    thread_name_prefix="actant-async",
)
# 确保进程退出时关闭线程池，避免在嵌入式 Python（如 PyO3 嵌入）中资源泄漏。
atexit.register(_ASYNC_EXECUTOR.shutdown, wait=False)


# ---------------------------------------------------------------------------
# TaskRef: 延迟计算的结果引用
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TaskRef:
    """延迟计算的结果引用。

    在 flow 上下文中由 Task.__call__ 产生。
    TaskRef 可作为参数传入另一个 Task 调用，自动建立 DAG 依赖边。

    TaskRef 不可 await。Actant 使用 DAG 编译模型：flow 函数体构建依赖图，
    所有 task 在 submit() 后统一执行。要获取结果，请使用：
        result = app.submit(my_workflow)
        value = await result.get()
    """

    task_name: str
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] | None = None
    retry_policy: dict[str, Any] | None = None
    timeout: float | None = None
    priority: PriorityInput = None
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, str] = field(default_factory=dict)
    id: str = field(default_factory=lambda: uuid4().hex)
    # inline_func: 内联 callable(用于 generic worker)。
    # 当为 None 时,worker 通过 task_name 在注册表中查找 handler。
    # 当非 None 时,worker 通过 __actant_generic__ handler 执行内联 callable,
    # 让无业务模块依赖的 worker 也能运行该任务。
    inline_func: Callable[..., Any] | None = None

    def __await__(self) -> Generator[None, None, None]:
        raise TypeError(
            f"cannot await TaskRef '{self.task_name}': "
            "Actant uses DAG compilation — tasks execute after submit(), not during flow construction. "
            "Use `result = app.submit(flow); value = await result.get()` instead."
        )

    def __repr__(self) -> str:
        args_str = ", ".join(repr(a) for a in self.args)
        return f"<TaskRef {self.task_name}({args_str})>"


@dataclass(frozen=True, slots=True)
class Task:
    """表示一个已注册的任务定义。

    职责：保存任务名称、可调用对象和执行选项。
    在 flow 上下文中调用返回 TaskRef；在 flow 上下文外调用直接执行。

    执行选项可通过两种方式指定：
    1. @actant.task() 装饰器参数 — 注册时指定默认选项
    2. task.__call__(priority="high", timeout=5.0) — 调用时覆盖选项
    """

    name: str
    func: Callable[..., Any] | None = None
    _retry_policy: dict[str, Any] | None = None
    _timeout: float | None = None
    _priority: PriorityInput = None
    _tags: list[str] = field(default_factory=list)
    _metadata: dict[str, str] = field(default_factory=dict)

    def __call__(
        self,
        *args: Any,
        priority: PriorityInput = None,
        timeout: float | None = None,
        retry_policy: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, str] | None = None,
        inline: bool = False,
        **kwargs: Any,
    ) -> TaskRef | Any:
        """调用任务。

        在 flow 上下文中返回 TaskRef；在 flow 上下文外直接执行。
        执行选项合并优先级：调用参数 > actant.annotate() 注解 > Task 注册默认值。
        所有时间参数使用秒（float）。

        ``inline=True`` 将 Task 的可调用对象内联到 payload 中,使纯计算节点
        (无业务模块依赖的 worker) 也能通过 ``__actant_generic__`` handler
        执行此任务。默认 False,worker 通过 task_name 在注册表中查找 handler。
        """
        ctx = _get_flow_context()
        if ctx is not None:
            opts = _merge_task_options(
                self, priority, timeout, retry_policy, tags, metadata
            )
            task_ref = TaskRef(
                task_name=self.name,
                args=args,
                kwargs=kwargs if kwargs else None,
                retry_policy=opts["retry_policy"],
                timeout=opts["timeout"],
                priority=opts["priority"],
                tags=opts["tags"],
                metadata=opts["metadata"],
                inline_func=self.func if inline else None,
            )
            ctx.track(task_ref)
            return task_ref
        # flow 外直接执行
        return self.apply(*args, **kwargs)

    def map(
        self,
        items: Sequence[Any],
        *,
        priority: PriorityInput = None,
        timeout: float | None = None,
        retry_policy: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, str] | None = None,
    ) -> list[TaskRef] | list[Any]:
        """并行映射：对 items 中每个元素创建一个 TaskRef。

        在 flow 上下文中返回 list[TaskRef]；在 flow 上下文外直接执行
        每个元素并返回结果列表（用于 Flow.run() 本地调试模式）。
        执行选项合并优先级同 __call__。
        """
        ctx = _get_flow_context()
        if ctx is None:
            # 本地模式：直接执行每个元素
            return [self.apply(item) for item in items]

        opts = _merge_task_options(self, priority, timeout, retry_policy, tags, metadata)
        refs: list[TaskRef] = []
        for item in items:
            task_ref = TaskRef(
                task_name=self.name,
                args=(item,),
                retry_policy=opts["retry_policy"],
                timeout=opts["timeout"],
                priority=opts["priority"],
                tags=opts["tags"],
                metadata=opts["metadata"],
            )
            ctx.track(task_ref)
            refs.append(task_ref)
        return refs

    def reduce(
        self,
        refs: Sequence[TaskRef] | Sequence[Any],
        *,
        priority: PriorityInput = None,
        timeout: float | None = None,
        retry_policy: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, str] | None = None,
    ) -> TaskRef | Any:
        """并行执行 refs 后聚合（chord 语义）。

        在 flow 上下文中返回新的 TaskRef（依赖所有输入 TaskRef）；
        在 flow 上下文外直接调用聚合函数（用于 Flow.run() 本地调试模式）。
        执行选项合并优先级同 __call__。
        """
        ctx = _get_flow_context()
        if ctx is None:
            # 本地模式：refs 是实际值，直接调用
            return self(list(refs))

        opts = _merge_task_options(self, priority, timeout, retry_policy, tags, metadata)
        callback = TaskRef(
            task_name=self.name,
            args=(list(refs),),
            retry_policy=opts["retry_policy"],
            timeout=opts["timeout"],
            priority=opts["priority"],
            tags=opts["tags"],
            metadata=opts["metadata"],
        )
        ctx.track(callback)
        return callback

    def apply(self, *args: Any, **kwargs: Any) -> Any:
        """在本地直接执行该任务（阻塞调用），绕过运行时。"""
        if self.func is None:
            raise InvalidStateError(f"task '{self.name}' has no callable")
        return _run_sync_or_async(self.func, *args, **kwargs)

    def __repr__(self) -> str:
        return f"<Task {self.name}>"


# ---------------------------------------------------------------------------
# 全局任务注册表
# ---------------------------------------------------------------------------

# 模块级全局任务注册表。@actant.task 装饰器自动将 Task 注册到此表。
# _Node.start() 时自动加载此表中的所有任务，让 worker 无需显式 register()。
# 这解耦了任务定义与节点实例：用户只需 @actant.task，节点启动时自动发现。
_global_tasks: dict[str, Task] = {}
_global_tasks_lock = threading.Lock()


def _task_origin(task_obj: Task) -> str:
    """返回任务的来源标识（module.qualname），用于检测同名冲突。

    同一函数被重复导入时 origin 相同，不视为冲突；
    不同函数使用相同 name= 时 origin 不同，触发警告。
    """
    func = task_obj.func
    if func is None:
        return "<no-func>"
    module = getattr(func, "__module__", "?")
    qualname = getattr(func, "__qualname__", "?")
    return f"{module}.{qualname}"


def register_global_task(task_obj: Task) -> None:
    """将 Task 注册到全局表。

    同名任务会被覆盖（后定义生效）。当检测到不同函数注册了相同名称时，
    发出 UserWarning 以帮助定位意外的名称冲突。
    """
    with _global_tasks_lock:
        existing = _global_tasks.get(task_obj.name)
        if existing is not None and existing is not task_obj:
            old_origin = _task_origin(existing)
            new_origin = _task_origin(task_obj)
            if old_origin != new_origin:
                warnings.warn(
                    f"task name '{task_obj.name}' is already registered by "
                    f"{old_origin}; the new definition from {new_origin} "
                    f"will override it. Use a unique name= to avoid silent "
                    f"overrides.",
                    UserWarning,
                    stacklevel=2,
                )
        _global_tasks[task_obj.name] = task_obj


def get_global_tasks() -> dict[str, Task]:
    """返回全局任务注册表的快照（线程安全拷贝）。"""
    with _global_tasks_lock:
        return dict(_global_tasks)


def get_global_task(name: str) -> Task | None:
    """按名称查找全局任务。"""
    with _global_tasks_lock:
        return _global_tasks.get(name)


def clear_global_tasks() -> None:
    """清空全局任务注册表（仅用于测试隔离）。"""
    with _global_tasks_lock:
        _global_tasks.clear()


def _make_task(
    func: Callable[..., Any],
    *,
    name: str | None = None,
    max_retries: int | None = None,
    retry_delay: float | None = None,
    timeout: float | None = None,
    priority: PriorityInput = None,
    tags: list[str] | None = None,
    metadata: dict[str, str] | None = None,
    register: bool = True,
) -> Task:
    """创建 Task 定义。

    Args:
        register: 若 True（默认），自动注册到全局任务表。
    """
    from actant._serialization import get_default_retry_policy

    _defaults = get_default_retry_policy()
    _max_retries: int = max_retries if max_retries is not None else int(_defaults["max_retries"])
    retry_delay_ms = int(retry_delay * 1000 if retry_delay is not None else _defaults["delay_ms"])

    task_name = name or str(getattr(func, "__name__", "unknown_task"))
    task_obj = Task(
        task_name,
        func,
        _retry_policy={
            "max_retries": _max_retries,
            "delay_ms": retry_delay_ms,
            "backoff_multiplier": int(_defaults["backoff_multiplier"]),
            "max_delay_ms": int(_defaults["max_delay_ms"]),
        },
        _timeout=timeout,
        _priority=priority,
        _tags=tags or [],
        _metadata=metadata or {},
    )
    if register:
        register_global_task(task_obj)
    return task_obj
