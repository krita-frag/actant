"""任务定义与异步执行。

提供 ``@task`` 装饰器，将普通函数转换为可本地调用、可异步提交的 ``Task`` 对象。

设计说明
========

**分布式执行**：``Task.submit`` 将 cloudpickle 序列化的 payload 提交到 Rust
``ProcessTaskDispatcher`` 进程池，任务在 worker 子进程（``actant.task._worker``）
中执行，实现进程级隔离。任务完成后结果经 event_bus 回调解析 ``AsyncResult``。

每个 Worker 节点的并发由 ``max_concurrent_tasks`` 控制；需要强制远程执行时，
可使用 ``task.submit_to(node_id, ..., endpoint_addr=endpoint_addr)`` 指定目标节点。

特性
====

- ``task.submit(*args)``：异步提交到分布式 Worker，返回 ``AsyncResult``。
- ``task.submit_to(node_id, *args, endpoint_addr=endpoint_addr)``：显式提交到指定远程 Worker。
- 任务依赖：``AsyncResult`` 可直接作为下游 ``submit`` 的参数，自动阻塞解析。
- 重试：``@task(retries=3, retry_delay_ms=1000)`` 在 worker 子进程内重试。
- 超时：``@task(timeout_ms=5000)`` 由 Rust 进程池硬超时强杀 worker 子进程。
- 失败传播：任务异常序列化进 result bytes，``result()`` 重新抛出，跨节点安全。
- 任务查询：提交的任务注册到 ``Runtime``，可用 ``rt.list_tasks()`` / ``rt.get_task()``
  / ``rt.cancel_task()`` 查询/取消。

用法
====

::

    import actant

    @actant.task
    def fetch(url):
        return requests.get(url).json()

    with actant.Runtime.with_defaults() as rt:
        # 本地同步调用
        data = fetch("https://api.example.com")

        # 异步提交到 Worker
        handle = fetch.submit("https://api.example.com")
        data = handle.result()

        # 任务依赖：AsyncResult 自动解析
        @actant.task
        def parse(raw):
            return raw["data"]
        raw = fetch.submit("https://api.example.com")
        parsed = parse.submit(raw)   # 自动等待 fetch 完成后取结果传入
        print(parsed.result())

- ``_context``：``TaskContext`` / ``get_task_context`` / ``_DispatchTaskContext`` / ``TaskState``
- ``_helpers``：序列化、超时执行、重试执行、事件广播等纯函数辅助
- ``_async_result``：``AsyncResult`` 与 ``_resolve_value``
- ``_worker``：worker 子进程循环（进程池后端的唯一执行入口）
- ``_task_obj``：``Task`` 类与 ``@task`` 装饰器
- ``_gather``：并行等待原语 ``gather``
"""

from __future__ import annotations

from actant.task._async_result import AsyncResult, _resolve_value  # noqa: F401
from actant.task._context import (  # noqa: F401
    TaskContext,
    TaskState,
    _DispatchTaskContext,
    _task_context_scope,
    get_task_context,
)
from actant.task._gather import gather, gather_async
from actant.task._helpers import (  # noqa: F401
    _emit_task_event,
    _interruptible_sleep,
    _pickle_exception,
    _run_with_timeout,
    _safe_serialize,
    _suppress_pickle_errors,
)
from actant.task._ref import REF_INLINE_THRESHOLD, Ref
from actant.task._task_obj import Task, task

__all__ = [
    "REF_INLINE_THRESHOLD",
    "AsyncResult",
    "Ref",
    "Task",
    "TaskContext",
    "TaskState",
    "gather",
    "gather_async",
    "get_task_context",
    "task",
]
