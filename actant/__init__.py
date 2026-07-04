"""Actant - 基于 Actor 模型的跨平台通用分布式任务编排引擎。

P2P 对等节点混合架构，每个节点同时具备编排、执行、Actor 管理、持久化完整能力。

典型用法::

    import actant

    @actant.task
    def add(x, y):
        return x + y

    @actant.flow
    def my_flow():
        return add(1, 2)

    # 提交工作流（自动管理瞬态节点）
    result = actant.submit(my_flow)
    value = result.get_sync(timeout=10.0)

    # 或启动常驻节点（接收并执行任务）
    node = actant.start("worker-1")
    # ... 提交工作流 ...
    actant.stop()
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from actant._annotations import annotate
from actant._api import (
    cancel,
    cancel_task,
    get_active_node,
    list_workflows,
    start,
    stop,
    submit,
    workflow_state,
    workflow_status,
)
from actant._components import CapacityProvider
from actant._events import subscribe as on
from actant._orchestration import DefaultCapacityProvider
from actant._serialization import (
    CloudpickleSerializer,
    PayloadSerializer,
)
from actant._task_context import TaskCancelled, is_cancelled
from actant.actant import get_version
from actant.actor import Actor, ActorMethodProxy
from actant.config import (
    FailureStrategy,
    NetworkConfig,
    PriorityInput,
    TaskPriority,
)
from actant.exceptions import (
    ActantError,
    ActantTimeoutError,
    NotFoundError,
    PayloadTooLargeError,
    TaskCancelledError,
    WorkflowCancelledError,
    WorkflowFailedError,
)
from actant.flow import BranchRef, Flow, branch, flow, parallel, switch
from actant.result import AsyncResult, WorkflowResult
from actant.router import (
    LeastLoadedRouter,
    NodeCapacity,
    TaskRouter,
)
from actant.supervision import (
    ActorSupervisor,
    BackoffConfig,
    RestartPolicy,
)
from actant.task import Task, TaskRef, _make_task

__version__ = get_version()

# 通用计算节点 fallback handler 名。
# 与 Rust 端 `src/worker/dispatcher.rs::GENERIC_DISPATCH_NAME` 必须保持一致。
# Worker 启动时总是注册此 handler,用于执行内联 callable payload,
# 让无业务模块依赖的 worker 也能运行任意 cloudpickle 任务。
GENERIC_DISPATCH_NAME: str = "__actant_generic__"


def task(
    func: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    max_retries: int | None = None,
    retry_delay: float | None = None,
    timeout: float | None = None,
    priority: PriorityInput = None,
    tags: list[str] | None = None,
    metadata: dict[str, str] | None = None,
    **kwargs: Any,
) -> Task | Callable[..., Task]:
    """定义一个全局任务，自动注册到全局任务注册表。

    注册后的任务可被所有节点自动发现（worker 启动时加载全局注册表）。
    无需绑定到具体 App 实例。

    用法::

        import actant

        @actant.task
        def add(x, y):
            return x + y

        @actant.task(max_retries=5, retry_delay=2.0, timeout=60.0, priority="high")
        def reliable_task(data):
            ...

    Args:
        func: 被装饰的函数。
        name: 任务名称，默认使用函数名。
        max_retries: 最大重试次数。
        retry_delay: 重试延迟（秒）。
        timeout: 任务超时（秒）。
        priority: 任务优先级（int/str/None）。
        tags: 任务标签列表。
        metadata: 任务元数据（键值对），Rust 透传不解释。
        **kwargs: 保留参数。

    Returns:
        Task 对象，或装饰器函数。
    """
    if func is None:

        def decorator(f: Callable[..., Any]) -> Task:
            return _make_task(
                f,
                name=name,
                max_retries=max_retries,
                retry_delay=retry_delay,
                timeout=timeout,
                priority=priority,
                tags=tags,
                metadata=metadata,
            )

        return decorator

    if not callable(func):
        raise TypeError("func must be callable")
    return _make_task(
        func,
        name=name,
        max_retries=max_retries,
        retry_delay=retry_delay,
        timeout=timeout,
        priority=priority,
        tags=tags,
        metadata=metadata,
    )


__all__ = [
    "GENERIC_DISPATCH_NAME",
    "ActantError",
    "ActantTimeoutError",
    "Actor",
    "ActorMethodProxy",
    "ActorSupervisor",
    "AsyncResult",
    "BackoffConfig",
    "BranchRef",
    "CapacityProvider",
    "CloudpickleSerializer",
    "DefaultCapacityProvider",
    "FailureStrategy",
    "Flow",
    "LeastLoadedRouter",
    "NetworkConfig",
    "NodeCapacity",
    "NotFoundError",
    "PayloadSerializer",
    "PayloadTooLargeError",
    "RestartPolicy",
    "Task",
    "TaskCancelled",
    "TaskCancelledError",
    "TaskPriority",
    "TaskRef",
    "TaskRouter",
    "WorkflowCancelledError",
    "WorkflowFailedError",
    "WorkflowResult",
    "__version__",
    "annotate",
    "branch",
    "cancel",
    "cancel_task",
    "flow",
    "get_active_node",
    "is_cancelled",
    "list_workflows",
    "on",
    "parallel",
    "start",
    "stop",
    "submit",
    "switch",
    "task",
    "workflow_state",
    "workflow_status",
]
