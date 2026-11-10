"""任务定义与异步执行。

提供 ``@task`` 装饰器，将普通函数转换为可本地调用、可异步提交的 ``Task`` 对象。

提交经 cloudpickle 序列化后进入 Rust 进程池，在 worker 子进程中执行；
``AsyncResult`` 作为下游参数时自动解析依赖。

用法
====

::

    import actant

    @actant.task
    def fetch(url):
        return requests.get(url).json()

    with actant.Runtime() as rt:
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

设计细节见 ``docs/guide/tasks.md``（提交、依赖、重试、超时、失败传播）与
``docs/architecture/worker-model.md``（进程隔离与 IPC）。
"""

from __future__ import annotations

from actant._internal.tasks.async_result import AsyncResult, BatchProgress, gather
from actant._internal.tasks.context import TaskContext, TaskState, get_task_context
from actant._internal.tasks.ref import REF_INLINE_THRESHOLD, Ref
from actant._internal.tasks.task import Task, task

__all__ = [
    "REF_INLINE_THRESHOLD",
    "AsyncResult",
    "BatchProgress",
    "Ref",
    "Task",
    "TaskContext",
    "TaskState",
    "gather",
    "get_task_context",
    "task",
]
