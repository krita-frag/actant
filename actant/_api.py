"""模块级 API：用户面向的简洁接口。

本模块提供 ``actant.submit`` / ``actant.start`` / ``actant.stop`` 等模块级函数，
封装内部 ``_Node`` 的生命周期管理，让用户无需接触 ``_Node`` 类。

两种节点模式：
- **常驻节点**（``actant.start()`` 或 ``actant worker``）：接收并执行任务，
  适合长期运行的工作节点。
- **瞬态节点**（``actant.submit()`` 内部创建）：仅提交工作流不执行任务，
  提交后可立即关闭，适合一次性脚本。
"""

from __future__ import annotations

import os
import threading
from typing import Any

from actant._components import CapacityProvider
from actant._node import _Node
from actant._serialization import PayloadSerializer
from actant.config import NetworkConfig
from actant.result import AsyncResult
from actant.router import TaskRouter

_active_node: _Node | None = None
_active_node_lock = threading.Lock()


def _resolve_signing_key(explicit: str | None) -> str:
    """解析 payload 签名密钥。

    优先级：
        1. 显式传入的 ``signing_key`` 参数
        2. ``ACTANT_SIGNING_KEY`` 环境变量

    Raises:
        ValueError: 显式传入空字符串，或两者均未提供。
    """
    if explicit is not None:
        if not explicit:
            raise ValueError("signing_key must not be empty")
        return explicit
    env_key = os.environ.get("ACTANT_SIGNING_KEY")
    if env_key:
        return env_key
    raise ValueError(
        "signing key required: pass signing_key=... or set ACTANT_SIGNING_KEY environment variable"
    )


def _get_or_create_transient(signing_key: str) -> _Node:
    """获取活跃节点，或创建瞬态提交节点并写入 ``_active_node`` 以便复用。

    若当前进程已有活跃节点（常驻或瞬态，``_runtime is not None``），复用之；
    否则创建一个瞬态节点（``_executing=False``）用于提交，并写入 ``_active_node``
    使后续 ``submit()`` 调用复用同一节点，避免每次调用都新建并启动节点。

    ``stop()`` 会清理 ``_active_node``（无论常驻或瞬态），符合"用户主动停止"语义。
    """
    global _active_node
    with _active_node_lock:
        node = _active_node
        if node is not None and node._runtime is not None:
            return node
        # 创建瞬态提交节点：不执行任务，仅提交工作流。
        node = _Node("actant-transient", _executing=False, signing_key=signing_key)
    # node.start() 可能阻塞（网络初始化），移至锁外避免持锁时间过长。
    node.start()
    with _active_node_lock:
        # 若期间其他线程已 start() 了常驻节点，关闭刚创建的瞬态节点并复用常驻节点。
        existing = _active_node
        if existing is not None and existing._runtime is not None:
            node.shutdown(timeout=5.0)
            return existing
        _active_node = node
    return node


def submit(
    flow: Any,
    *args: Any,
    signing_key: str | None = None,
    **kwargs: Any,
) -> AsyncResult:
    """提交工作流到集群执行。

    自动复用当前进程的活跃常驻节点；若无则创建瞬态提交节点。
    瞬态节点不执行任务（``_executing=False``），仅负责提交，
    任务由集群中的 worker 节点执行。

    Args:
        flow: 要执行的 Flow 实例（由 ``@actant.flow`` 装饰器创建）。
        *args: 传递给 flow 函数的位置参数。
        signing_key: payload 签名密钥。若为 None，则从
            ``ACTANT_SIGNING_KEY`` 环境变量读取；两者均未提供时抛 ``ValueError``。
        **kwargs: 传递给 flow 函数的关键字参数。

    Returns:
        AsyncResult: 异步结果对象，用于查询状态和获取结果。

    Raises:
        ValueError: 未提供 signing_key 且环境变量未设置。

    用法::

        import actant

        @actant.task
        def add(x, y):
            return x + y

        @actant.flow
        def my_flow():
            return add(1, 2)

        # 方式 1：显式传 key
        result = actant.submit(my_flow, signing_key="your-secret")
        # 方式 2：通过环境变量（适合生产部署）
        # export ACTANT_SIGNING_KEY="your-secret"
        result = actant.submit(my_flow)
        value = result.get_sync(timeout=10.0)
    """
    key = _resolve_signing_key(signing_key)
    node = _get_or_create_transient(key)
    return node.submit(flow, *args, **kwargs)


def start(
    name: str = "actant",
    *,
    signing_key: str | None = None,
    max_concurrent_tasks: int | None = None,
    node_id: str | None = None,
    data_dir: str | None = None,
    network: NetworkConfig | dict[str, Any] | None = None,
    router: TaskRouter | None = None,
    serializer: PayloadSerializer | None = None,
    capacity_provider: CapacityProvider | None = None,
    port: int = 0,
    listen_ip: str = "0.0.0.0",
    heartbeat_interval: float | None = None,
    failure_timeout: float | None = None,
    default_task_timeout: float | None = None,
    capabilities: dict[str, Any] | None = None,
    log_level: str | None = None,
) -> _Node:
    """启动常驻节点（非阻塞）。

    常驻节点接收并执行任务，适合长期运行的工作节点。
    启动后可通过 ``actant.submit()`` 提交工作流，节点会自动复用。
    使用 ``actant.stop()`` 关闭。

    Args:
        name: 节点名称，默认 "actant"。
        signing_key: payload 签名密钥。若为 None，则从
            ``ACTANT_SIGNING_KEY`` 环境变量读取；两者均未提供时抛 ``ValueError``。
        max_concurrent_tasks: 最大并发任务数。
        node_id: 节点 ID（默认自动生成）。
        data_dir: 持久化目录（None 表示纯内存模式）。
        network: 网络配置。
        router: 任务路由器。
        serializer: 任务 payload 序列化器（默认 cloudpickle）。
        capacity_provider: 对等节点容量提供者。
        port: 监听端口（0 表示自动分配）。
        listen_ip: 监听 IP。
        heartbeat_interval: 心跳间隔（秒）。
        failure_timeout: 故障检测超时（秒）。
        default_task_timeout: 默认任务超时（秒）。
        capabilities: 节点能力标签。
        log_level: 日志级别。

    Returns:
        已启动的 ``_Node`` 实例（内部对象，通常不需直接操作）。

    Raises:
        RuntimeError: 若当前进程已有一个活跃常驻节点。
        ValueError: 未提供 signing_key 且环境变量未设置。

    用法::

        import actant

        # 显式传 key
        node = actant.start("worker-1", max_concurrent_tasks=4, signing_key="your-secret")
        # 或通过环境变量
        # export ACTANT_SIGNING_KEY="your-secret"
        node = actant.start("worker-1", max_concurrent_tasks=4)
        # ... 提交工作流 ...
        actant.stop()
    """
    global _active_node
    key = _resolve_signing_key(signing_key)
    with _active_node_lock:
        if _active_node is not None and _active_node._runtime is not None:
            raise RuntimeError(
                "an active node already exists in this process. "
                "Call actant.stop() before starting a new one."
            )
        node = _Node(
            name,
            _executing=True,
            max_concurrent_tasks=max_concurrent_tasks,
            node_id=node_id,
            data_dir=data_dir,
            network=network,
            router=router,
            serializer=serializer,
            capacity_provider=capacity_provider,
            port=port,
            listen_ip=listen_ip,
            heartbeat_interval=heartbeat_interval,
            failure_timeout=failure_timeout,
            default_task_timeout=default_task_timeout,
            capabilities=capabilities,
            log_level=log_level,
            signing_key=key,
        )
    # node.start() 可能阻塞（如网络初始化），移至锁外避免持锁时间过长
    node.start()
    with _active_node_lock:
        _active_node = node
    return node


def stop(timeout: float = 10.0) -> None:
    """停止当前进程的活跃常驻节点。

    Args:
        timeout: 等待运行中任务完成的最大时间（秒）。
    """
    global _active_node
    with _active_node_lock:
        node = _active_node
        _active_node = None
    if node is not None:
        node.shutdown(timeout=timeout)


def get_active_node() -> _Node | None:
    """获取当前进程的活跃常驻节点（未启动则返回 None）。"""
    with _active_node_lock:
        return _active_node


def list_workflows() -> list[tuple[str, str]]:
    """列出所有活动 workflow 及其状态。

    Returns:
        list of (workflow_id, state) 元组。

    Raises:
        RuntimeError: 若无活跃节点。
    """
    node = get_active_node()
    if node is None or node._runtime is None:
        raise RuntimeError("no active node. Call actant.start() first.")
    return node.list_workflows()


def workflow_state(workflow_id: str) -> str | None:
    """查询 workflow 的当前状态。

    Args:
        workflow_id: workflow ID。

    Returns:
        状态字符串（如 "Running"、"Completed"），未找到返回 None。

    Raises:
        RuntimeError: 若无活跃节点。
    """
    node = get_active_node()
    if node is None or node._runtime is None:
        raise RuntimeError("no active node. Call actant.start() first.")
    return node.workflow_state(workflow_id)


def cancel(workflow_id: str) -> None:
    """取消运行中的 workflow。

    Args:
        workflow_id: 要取消的 workflow ID。

    Raises:
        RuntimeError: 若无活跃节点。
        NotFoundError: 若 workflow 不存在。
    """
    node = get_active_node()
    if node is None or node._runtime is None:
        raise RuntimeError("no active node. Call actant.start() first.")
    node.cancel(workflow_id)


def cancel_task(workflow_id: str, task_id: str) -> bool:
    """取消 workflow 中的特定任务。

    Returns:
        True 若取消成功。

    Raises:
        RuntimeError: 若无活跃节点。
    """
    node = get_active_node()
    if node is None or node._runtime is None:
        raise RuntimeError("no active node. Call actant.start() first.")
    return node.cancel_task(workflow_id, task_id)


def workflow_status(workflow_id: str) -> dict[str, Any]:
    """获取 workflow 的详细状态。

    Returns:
        包含 workflow_id、state、tasks、result_count、error 的字典。

    Raises:
        RuntimeError: 若无活跃节点。
    """
    node = get_active_node()
    if node is None or node._runtime is None:
        raise RuntimeError("no active node. Call actant.start() first.")
    return node.get_workflow_status(workflow_id)
