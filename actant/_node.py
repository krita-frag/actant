"""_Node：内部节点运行时，负责任务/actor注册与运行时管理。

此类为内部实现细节，用户应通过 ``actant`` 模块级 API（``actant.submit``、
``actant.start`` 等）使用 Actant，不应直接实例化 ``_Node``。
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future as ConcurrentFuture
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from actant import _observability as _obs
from actant._components import CapacityProvider, EventContext
from actant._events import subscribe as _subscribe_event
from actant._orchestration import (
    DefaultCapacityProvider,
    DefaultOrchestrationEventHandler,
    _RouteInfo,
)
from actant._serialization import (
    CloudpickleSerializer,
    PayloadSerializer,
    _dispatch_generic,
    _dispatch_task,
)
from actant.actant import (
    _ActantConfig,
    _FailoverConfig,
    _NetworkConfig,
    _RuntimeCore,
)
from actant.actor import Actor, _ActorDispatcher
from actant.config import NetworkConfig
from actant.exceptions import InvalidStateError, NotFoundError
from actant.result import AsyncResult
from actant.router import LeastLoadedRouter, NodeCapacity, TaskRouter
from actant.supervision import ActorSupervisor, BackoffConfig, RestartPolicy
from actant.task import Task, get_global_tasks

_RUNTIME_NOT_INITIALIZED = (
    "Actant runtime is not running. Call start() first, or use actant.start()."
)

_SHUTDOWN_TIMEOUT_SECS = 10.0

logger = logging.getLogger("actant.app")


def _dispatch_generic_task(payload: bytes, cancel_token: Any = None) -> bytes:
    """通用 fallback handler:执行内联 callable payload。

    handler 名固定为 "__actant_generic__",在 start() 时注册。
    支持 TAG_GENERIC 和 TAG_POSITIONAL 两种 inner tag —
    这两种 tag 的 dispatcher 都从 payload 中提取真正的 fn（而非使用传入的 fn 参数），
    因此可以安全地通过 generic handler 执行。

    payload 可能被 TAG_UPSTREAM_PREFIX 包装（有前驱结果时），_dispatch_task 会先解包。

    若 inner tag 是 TAG_SINGLE/TAG_GROUP 等 "named path" 载荷（fn 直接由 dispatcher 使用），
    说明命名任务未在此 worker 注册、错误地回退到了 generic handler —
    此时抛出清晰错误而非产生令人困惑的参数不匹配异常。
    """
    from actant._serialization import (
        TAG_GENERIC,
        TAG_POSITIONAL,
        TAG_UPSTREAM_PREFIX,
        _dispatch_task,
        unpack_upstream_prefix,
    )

    # 先解包可能的 upstream prefix，检查 inner_payload 的 tag
    if payload and payload[0] == TAG_UPSTREAM_PREFIX:
        _, inner_payload = unpack_upstream_prefix(payload)
    else:
        inner_payload = payload

    if inner_payload and inner_payload[0] not in (TAG_GENERIC, TAG_POSITIONAL):
        from actant.exceptions import SerializationError

        raise SerializationError(
            "task fell back to generic handler but inner payload tag is "
            f"0x{inner_payload[0]:02x}; this worker has not imported the task module "
            "that registers the named task — ensure the worker process imports "
            "the business module before actant.start()"
        )

    # 复用 _dispatch_task 以统一取消 token 设置和异常处理。
    # dispatcher 会从 payload 中反序列化出真正的 fn，传入的 _dispatch_generic 仅占位。
    return _dispatch_task(_dispatch_generic, payload, cancel_token)


class _Node:
    """内部节点运行时。

    单个 ``_Node`` 实例代表集群中的一个对等节点，具备编排、执行、
    Actor 管理、持久化完整能力（P2P 对等架构）。

    此类为内部实现，用户应通过 ``actant`` 模块级 API 使用。
    """

    def __init__(
        self,
        name: str,
        *,
        _executing: bool = True,
        max_concurrent_tasks: int | None = None,
        node_id: str | None = None,
        default_task_timeout: float | None = None,
        data_dir: str | None = None,
        heartbeat_interval: float | None = None,
        failure_timeout: float | None = None,
        network: NetworkConfig | dict[str, Any] | None = None,
        router: TaskRouter | None = None,
        serializer: PayloadSerializer | None = None,
        capacity_provider: CapacityProvider | None = None,
        port: int = 0,
        listen_ip: str = "0.0.0.0",
        capabilities: dict[str, Any] | None = None,
        log_level: str | None = None,
        signing_key: str | None = None,
    ) -> None:
        self.name = name
        # _executing 是内部参数：True 表示节点接收并执行任务（worker/常驻节点），
        # False 表示节点仅提交工作流不执行任务（瞬态提交节点）。
        # 用户不直接接触此参数，由模块级 API（actant.start / actant.submit）控制。
        self._executing = _executing
        self.max_concurrent_tasks = max_concurrent_tasks
        self._node_id = node_id
        self._default_task_timeout = default_task_timeout
        self.data_dir = data_dir
        self._heartbeat_interval = heartbeat_interval
        self._failure_timeout = failure_timeout
        if network is None:
            network = NetworkConfig()
        elif isinstance(network, dict):
            network = NetworkConfig(**network)
        self.network: NetworkConfig = network
        self.port = port
        self.listen_ip = listen_ip
        self._capabilities = capabilities or {}
        self.router: TaskRouter = router or LeastLoadedRouter()
        self.serializer: PayloadSerializer = serializer or CloudpickleSerializer()
        self.capacity_provider: CapacityProvider = capacity_provider or DefaultCapacityProvider()
        if not signing_key:
            raise ValueError("signing_key is required for payload MAC signing")
        self._signing_key = signing_key

        # 统一日志：Python（stdlib logging）+ Rust（tracing）。
        if log_level is not None:
            from actant._logging import configure_logging

            configure_logging(log_level, force=True)

        # Task/actor 注册表
        self._tasks: dict[str, Task] = {}
        self._actors: dict[str, Actor] = {}
        self._runtime: _RuntimeCore | None = None
        self._lock = threading.Lock()
        self._supervisors: list[ActorSupervisor] = []
        self._condition_evaluators: dict[str, Callable[[Any], bool]] = {}

        # 节点生命周期钩子（startup/shutdown 仍走本地 list，
        # 因它们与 _Node 实例强绑定且需在 start/stop 同步触发）。
        self._on_startup: list[Callable[[], None]] = []
        self._on_shutdown: list[Callable[[], None]] = []
        # task_start/task_complete 走全局事件订阅（actant._events），
        # _Node 不再维护本地 hook 列表。
        self._custom_events: dict[str, list[Callable[..., None]]] = {}

        # 编排生命周期
        self._event_loop: asyncio.AbstractEventLoop | None = None
        self._orchestration_loop: Any = None  # OrchestrationLoop
        self._ctx: EventContext | None = None
        self._metrics_server: HTTPServer | None = None

        # 同步
        self._ready_event: threading.Event = threading.Event()
        self._drained_event: threading.Event = threading.Event()
        # _shutdown_event 在 shutdown() 完成时置位，用于让 run() 退出忙等待。
        self._shutdown_event: threading.Event = threading.Event()

        # workflow 重试的提交历史（按 workflow_id 存储 Flow + args）
        self._submission_history: dict[str, tuple[Any, tuple[Any, ...], dict[str, Any]]] = {}

    # ------------------------------------------------------------------
    # Context manager 协议
    # ------------------------------------------------------------------

    def __enter__(self) -> _Node:
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.shutdown()

    def on(self, event: str, handler: Callable[..., None] | None = None) -> Any:
        """注册事件处理函数。

        内建事件：
        - ``"startup"`` / ``"shutdown"``：节点生命周期，本地触发。
        - ``"task_start"`` / ``"task_complete"``：转发到全局事件订阅
          （对应 ``actant.on("task.started")`` / ``actant.on("task.completed")``）。
        其他事件名作为自定义事件存储，由 ``emit`` 触发。

        推荐使用顶层 ``actant.on`` 装饰器以获得更完整的事件主题列表
        （``task.failed``、``actor.failed``、``worker.drained`` 等）。
        """
        if handler is not None:
            self._register_event(event, handler)
            return handler

        def decorator(fn: Callable[..., None]) -> Callable[..., None]:
            self._register_event(event, fn)
            return fn

        return decorator

    def emit(self, event: str, *args: Any, **kwargs: Any) -> None:
        """触发自定义事件，调用所有注册的处理函数。"""
        for handler in self._custom_events.get(event, []):
            try:
                handler(*args, **kwargs)
            except Exception:
                logger.exception("event handler for '%s' raised", event)

    def _register_event(self, event: str, handler: Callable[..., None]) -> None:
        if event == "startup":
            self._on_startup.append(handler)
        elif event == "shutdown":
            self._on_shutdown.append(handler)
        elif event == "task_start":
            # 转发到全局事件订阅，统一与 @actant.on("task.started") 行为。
            _subscribe_event("task.started", handler)
        elif event == "task_complete":
            # task_complete 同时订阅 completed 和 failed 两个主题，
            # 与原行为（success 参数区分）保持一致：用户在 handler 中
            # 接收的 event 对象带有 state 字段（_TaskCompletion）。
            _subscribe_event("task.completed", handler)
            _subscribe_event("task.failed", handler)
        else:
            self._custom_events.setdefault(event, []).append(handler)

    def _fire_startup(self) -> None:
        for hook in self._on_startup:
            try:
                hook()
            except Exception:
                logger.exception("startup hook raised")

    def _fire_shutdown(self) -> None:
        for hook in self._on_shutdown:
            try:
                hook()
            except Exception:
                logger.exception("shutdown hook raised")

    @property
    def available_capacity(self) -> int:
        """当前可用的任务槽位数。"""
        rt = self._runtime
        if rt is None:
            return 0
        max_cap: int | None = rt.max_concurrent_tasks()
        running: int | None = rt.running_task_count()
        if max_cap is None or running is None:
            return 0
        return max(0, max_cap - running)

    @property
    def max_capacity(self) -> int:
        """此节点可以处理的最大并发任务数。"""
        rt = self._runtime
        if rt is None:
            return 0
        val: int | None = rt.max_concurrent_tasks()
        return val if val is not None else 0

    @property
    def running_task_count(self) -> int:
        """当前运行的任务数。"""
        rt = self._runtime
        if rt is None:
            return 0
        val: int | None = rt.running_task_count()
        return val if val is not None else 0

    def _local_node_capacity(self) -> NodeCapacity:
        return NodeCapacity(available=self.available_capacity, max_capacity=self.max_capacity)

    def _peer_capacities_snapshot(self) -> dict[str, NodeCapacity]:
        rt = self._runtime
        if rt is None:
            return {}
        local_node_id = rt.node_id()
        if isinstance(self.capacity_provider, DefaultCapacityProvider):
            return self.capacity_provider.snapshot_with_runtime(
                local_node_id, self._capabilities, list(self._tasks.keys())
            )
        return self.capacity_provider.snapshot(
            local_node_id, self._capabilities, list(self._tasks.keys())
        )

    def _build_route_map(
        self,
        tasks: list[_RouteInfo],
    ) -> dict[str, str] | None:
        """根据 router 和 capacity_provider 为任务选择目标节点。"""
        rt = self._runtime
        if rt is None:
            return None

        local_node = rt.node_id()
        peer_capacities = self._peer_capacities_snapshot()

        local_cap = peer_capacities.get(local_node)
        if local_cap is not None and local_cap.max_capacity == 0:
            peer_capacities.pop(local_node, None)

        target_nodes: dict[str, str] = {}
        for info in tasks:
            task_obj = self._tasks.get(info.task_name)
            task_meta: dict[str, Any] = {
                "name": info.task_name,
                "tags": info.tags,
                "priority": info.priority,
            }
            if task_obj is not None and local_cap is not None and local_cap.max_capacity > 0:
                continue
            target = self.router.route(local_node, info.node_key, task_meta, peer_capacities)
            if target is not None and target != local_node:
                target_nodes[info.node_key] = target

        return target_nodes if target_nodes else None

    def _route_endpoint_addrs(
        self,
        target_nodes: dict[str, str],
    ) -> dict[str, str] | None:
        if not target_nodes:
            return None
        endpoint_addrs: dict[str, str] = {}
        for task_id, node_id in target_nodes.items():
            ep = self.capacity_provider.endpoint_addr(node_id)
            if ep is not None:
                endpoint_addrs[task_id] = ep
        return endpoint_addrs if endpoint_addrs else None

    def _route_orchestration_tasks(
        self,
        tasks: list[Any],
    ) -> dict[str, str] | None:
        """将编排任务转换为 _RouteInfo 并委托给 _build_route_map。"""
        route_infos: list[_RouteInfo] = []
        for t in tasks:
            name = t.name
            task_obj = self._tasks.get(name)
            route_infos.append(_RouteInfo(
                node_key=t.task_id,
                task_name=name,
                tags=task_obj._tags if task_obj else [],
                priority=task_obj._priority if task_obj else None,
            ))
        return self._build_route_map(route_infos)

    def _refresh_peer_capacities(self) -> None:
        """从运行时拉取对等节点容量并更新 capacity_provider。"""
        rt = self._runtime
        if rt is None:
            return
        raw = rt.get_peer_capacities()
        for node_id, cap in raw.items():
            self.capacity_provider.update(
                node_id,
                cap.available,
                cap.max,
                endpoint_addr=cap.endpoint_addr,
            )

    def _on_actor_event(self, event: Any) -> None:
        """Actor 生命周期事件处理器（由 ``actant._events.dispatch`` 调用）。

        转发给所有已注册的 ``ActorSupervisor``，让监督器决定重启/停止策略。
        """
        for supervisor in self._supervisors:
            try:
                supervisor.handle_event(event.event_type, event.actor_id, event.error)
            except Exception:
                logger.exception(
                    "supervisor.handle_event failed for actor %s", event.actor_id
                )

    def _on_worker_drained_event(self, event: Any) -> None:
        """worker.drained 事件处理器：设置内部 drain 信号，让 stop() 不再阻塞。"""
        self._drained_event.set()

    def actor(self, cls: type) -> Actor:
        """注册演员类。"""
        if not isinstance(cls, type):
            raise TypeError("cls must be a Python class")
        name = cls.__name__
        actor = Actor(name, cls)
        with self._lock:
            self._actors[name] = actor
        return actor

    def create_actor(self, actor_or_cls: Actor | type) -> Actor:
        """创建演员实例。"""
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)

        if isinstance(actor_or_cls, type):
            name = actor_or_cls.__name__
            with self._lock:
                if name not in self._actors:
                    self._actors[name] = Actor(name, actor_or_cls)
            actor = self._actors[name]
        elif isinstance(actor_or_cls, Actor):
            actor = actor_or_cls
        else:
            raise TypeError(
                f"expected Actor or class, got {type(actor_or_cls).__name__}. "
                "Pass a class like app.create_actor(Counter)."
            )

        if actor.cls is None:
            raise InvalidStateError("actor has no class registered.")
        instance = actor.cls()
        dispatcher = _ActorDispatcher(instance)
        actor_id = self._runtime.create_actor(actor.name, dispatcher)
        actor._set_proxy(actor_id, self._runtime.actor_core())
        return actor

    def create_supervisor(
        self,
        *,
        policy: RestartPolicy = RestartPolicy.TRANSIENT,
        backoff: dict[str, Any] | None = None,
    ) -> ActorSupervisor:
        """创建并注册演员监督器。"""
        backoff_cfg = BackoffConfig(**backoff) if backoff else BackoffConfig()
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)
        supervisor = ActorSupervisor(
            self._runtime.actor_core(),
            policy=policy,
            backoff=backoff_cfg,
        )
        self._supervisors.append(supervisor)
        return supervisor

    def submit(self, flow: Any, *args: Any, **kwargs: Any) -> AsyncResult:
        """将 Flow 提交到运行时进行分布式执行。

        Args:
            flow: 要执行的 Flow 实例（由 @flow 装饰器创建）。
            *args: 传递给 flow 函数的位置参数。
            **kwargs: 传递给 flow 函数的关键字参数。

        Returns:
            AsyncResult: 一个类似 future 的对象，用于查询工作流状态
                和获取结果。

        Raises:
            TypeError: 如果 *flow* 不是 Flow 实例。
            InvalidStateError: 如果运行时未启动。

        使用示例::

            @flow
            def my_workflow(x, y):
                a = add(x, y)
                b = multiply(a, 2)
                return b

            result = app.submit(my_workflow, 1, 2)
            value = result.get_sync()
        """
        from actant.flow import Flow

        if not isinstance(flow, Flow):
            raise TypeError(
                f"expected Flow, got {type(flow).__name__}. Use app.submit(flow, *args)."
            )
        nodes, edges, condition_evaluators = flow._build_dag(*args, **kwargs)
        result = self._submit_dag(
            nodes,
            edges,
            timeout=flow._timeout,
            failure_strategy=flow._failure_strategy,
            condition_evaluators=condition_evaluators or None,
        )
        # 存储 workflow 重试的提交历史
        self._submission_history[result.workflow_id] = (flow, args, kwargs)
        return result

    def _submit_dag(
        self,
        nodes: list[Any],
        edges: list[tuple[int, int, str | None]],
        *,
        timeout: float | None = None,
        failure_strategy: str | None = None,
        condition_evaluators: dict[str, Callable[[Any], bool]] | None = None,
    ) -> AsyncResult:
        """将 DAG 提交到运行时进行分布式执行。"""
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)

        target_nodes_by_idx: dict[int, str] | None = None
        target_endpoint_addrs_by_idx: dict[int, str] | None = None
        if nodes:
            route_infos = []
            for idx, node in enumerate(nodes):
                name = node.name
                task_obj = self._tasks.get(name)
                route_infos.append(_RouteInfo(
                    node_key=str(idx),
                    task_name=name,
                    tags=task_obj._tags if task_obj else [],
                    priority=task_obj._priority if task_obj else [],
                ))
            target_nodes_by_id = self._build_route_map(route_infos)
            if target_nodes_by_id:
                target_nodes_by_idx = {}
                target_endpoint_addrs_by_id = self._route_endpoint_addrs(target_nodes_by_id)
                target_endpoint_addrs_by_idx = {}
                for idx, info in enumerate(route_infos):
                    if info.node_key in target_nodes_by_id:
                        target_nodes_by_idx[idx] = target_nodes_by_id[info.node_key]
                    if target_endpoint_addrs_by_id and info.node_key in target_endpoint_addrs_by_id:
                        target_endpoint_addrs_by_idx[idx] = target_endpoint_addrs_by_id[info.node_key]

        timeout_ms: int | None = int(timeout * 1000) if timeout is not None else None

        core_result = self._runtime.submit_dag(
            nodes,
            edges,
            timeout_ms,
            None,
            target_nodes_by_idx,
            target_endpoint_addrs_by_idx,
            failure_strategy,
        )

        if condition_evaluators:
            self._condition_evaluators.update(condition_evaluators)

        return AsyncResult(core_result)

    def get_stored_results(self, workflow_id: str) -> list[Any] | None:
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)
        raw_results = self._runtime.get_stored_results(workflow_id)
        if raw_results is None:
            return None
        from actant._serialization import loads

        return [loads(data) for data in raw_results]  # type: ignore[arg-type]

    def cancel(self, workflow_id: str) -> None:
        """根据 ID 取消运行中的工作流。

        Raises:
            NotFoundError: workflow 不存在。
        """
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)
        self._runtime.cancel_workflow(workflow_id)

    def cancel_task(self, workflow_id: str, task_id: str) -> bool:
        """根据 ID 取消工作流中的特定任务。

        Raises:
            NotFoundError: workflow 不存在。
        """
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)
        try:
            return self._runtime.cancel_task(workflow_id, task_id)
        except NotFoundError:
            raise
        except Exception:
            # Rust 抛出其他异常时返回 False
            return False

    def resubmit_workflow(self, workflow_id: str) -> AsyncResult:
        """重新提交之前提交的工作流。

        使用原始提交的 Flow 和参数创建新的 DAG。
        原始工作流必须通过 app.submit() 提交。

        Args:
            workflow_id: The workflow ID to resubmit.

        Returns:
            A new AsyncResult for the resubmitted workflow.

        Raises:
            KeyError: If the workflow_id was not submitted via this app instance.
        """
        entry = self._submission_history.get(workflow_id)
        if entry is None:
            raise KeyError(
                f"workflow {workflow_id} not found in submission history. "
                "Only workflows submitted via app.submit() can be resubmitted."
            )
        flow_obj, args, kwargs = entry
        return self.submit(flow_obj, *args, **kwargs)

    def start(self) -> None:
        """启动运行时，不阻塞调用线程。

        调用 submit() 后，运行时准备接受工作流提交。
        使用 run() 阻塞直到关闭。
        """
        if self._runtime is not None:
            return  # already started

        # 可观测性：在创建运行时前启动 viztracer（若环境变量已设置）。
        # 必须在 worker 线程派生之前启动，否则子线程不会被 trace。
        _obs.start()

        # 重置同步状态以支持 start/shutdown/start 重启场景。
        self._shutdown_event.clear()
        self._ready_event.clear()

        funcs: dict[str, Callable[..., Any]] = {}
        if self._executing:
            # 自动加载全局任务注册表中的所有任务。
            # @actant.task 装饰的函数在模块 import 时自动注册到全局表，
            # worker 启动时（可能预加载了业务模块）自动发现这些任务。
            with self._lock:
                # 合并全局注册表到本地（仅本节点未注册的）
                for gname, gtask in get_global_tasks().items():
                    if gname not in self._tasks:
                        self._tasks[gname] = gtask
                for name, task_obj in self._tasks.items():
                    if task_obj.func is not None:
                        fn = task_obj.func
                        funcs[name] = lambda payload, cancel_token=None, _fn=fn: _dispatch_task(
                            _fn, payload, cancel_token
                        )
                # 注册通用 fallback handler:用于执行内联 callable payload。
                # Rust dispatcher 在 task name 未命中时回退到此 handler。
                # 这让无业务模块依赖的 worker 也能执行任意 cloudpickle 任务。
                # 字符串值与 actant.GENERIC_DISPATCH_NAME / Rust GENERIC_DISPATCH_NAME 保持一致。
                funcs["__actant_generic__"] = _dispatch_generic_task

        heartbeat_ms = (
            int(self._heartbeat_interval * 1000) if self._heartbeat_interval is not None else None
        )
        failure_ms = (
            int(self._failure_timeout * 1000) if self._failure_timeout is not None else None
        )
        timeout_ms = (
            int(self._default_task_timeout * 1000)
            if self._default_task_timeout is not None
            else None
        )

        self._runtime = _RuntimeCore.start(
            name=self.name,
            config=_ActantConfig(
                payload_signing_key=self._signing_key,
                network=_NetworkConfig(
                    preset=self.network.preset,
                    bootstrap_nodes=list(self.network.bootstrap_nodes),
                    max_message_size=self.network.max_message_size,
                    allowed_peer_ids=list(self.network.allowed_peer_ids),
                    gossip_bootstrap_peers=list(self.network.gossip_bootstrap_peers),
                    direct_request_timeout_ms=self.network.direct_request_timeout_ms,
                    listen_port=self.port,
                    listen_ip=self.listen_ip,
                ),
                failover=_FailoverConfig(
                    heartbeat_interval_ms=heartbeat_ms,
                    failure_timeout_ms=failure_ms,
                )
                if heartbeat_ms is not None or failure_ms is not None
                else None,
                max_concurrent_tasks=self.max_concurrent_tasks if self._executing else 0,
                default_task_timeout_ms=timeout_ms,
                data_dir=self.data_dir,
            ),
            node_id=self._node_id,
            tasks=funcs,
        )

        self._start_orchestration()
        self._fire_startup()
        self._ready_event.set()

    def run(self) -> None:
        """启动运行时，阻塞调用线程直到关闭。

        等价于调用 start() 后，等待 Ctrl+C 或另一个线程调用 shutdown()。
        """
        self.start()
        try:
            self._ready_event.wait()
            # 通过 Event 等待 shutdown() 完成，避免忙轮询 _runtime 字段。
            while self._runtime is not None:
                # 短超时让 KeyboardInterrupt 能及时响应；正常路径由 Event 唤醒。
                if self._shutdown_event.wait(timeout=0.5):
                    break
        except KeyboardInterrupt:
            self.shutdown()

    def _start_orchestration(self) -> None:
        """启动编排事件循环，后台线程运行。

        ``OrchestrationLoop.start()`` 必须在事件循环线程内执行（它创建 asyncio
        tasks 并注册事件回调）。因此本方法启动后台线程，在线程内先运行
        ``start()`` 再 ``run_forever()``，并通过 ``ready_event`` 同步：主线程
        阻塞直到 ``start()`` 完成，确保返回时事件回调已注册、后台任务已创建。

        不能在主线程直接 ``run_until_complete(start())``：当调用方处于已有
        运行事件循环中（如 pytest-asyncio 的 async 测试），Python 3.14 会抛出
        ``RuntimeError: Cannot run the event loop while another loop is running``。
        """
        from actant._orchestration import OrchestrationLoop

        rt = self._runtime
        if rt is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)

        if isinstance(self.capacity_provider, DefaultCapacityProvider):
            self.capacity_provider.set_local_runtime(rt)

        # 默认 handler 是 OrchestrationLoop 的内部协作者，
        # 用户定制行为通过 @actant.on 事件订阅实现。
        handler = DefaultOrchestrationEventHandler(self._tasks)

        self._ctx = EventContext(
            node_id=rt.node_id(),
            runtime=rt,
            router=self.router,
            serializer=self.serializer,
            capacity_provider=self.capacity_provider,
            local_tasks=list(self._tasks.keys()),
            condition_evaluators=self._condition_evaluators,
        )

        # worker_drained 事件仍由 _Node 内部 _drained_event 维持，
        # 以便 stop() 能阻塞等待 drain 完成。同时 dispatch 给订阅者。
        _subscribe_event("worker.drained", self._on_worker_drained_event)
        # Actor 生命周期事件转发给监督器。
        _subscribe_event("actor.started", self._on_actor_event)
        _subscribe_event("actor.stopped", self._on_actor_event)
        _subscribe_event("actor.failed", self._on_actor_event)

        self._event_loop = asyncio.new_event_loop()
        self._orchestration_loop = OrchestrationLoop(
            rt,
            handler,
            self._ctx,
            refresh_capacity=self._refresh_peer_capacities,
            capacity_refresh_interval=5.0,
        )

        ready_event = threading.Event()
        start_error: list[BaseException] = []

        def _run_with_start() -> None:
            loop = self._event_loop
            if loop is None:
                ready_event.set()
                return
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._orchestration_loop.start())
            except BaseException as exc:  # 透传给主线程
                start_error.append(exc)
                ready_event.set()
                return
            ready_event.set()
            try:
                loop.run_forever()
            finally:
                try:
                    pending = asyncio.all_tasks(loop)
                    for task in pending:
                        task.cancel()
                    if pending:
                        loop.run_until_complete(
                            asyncio.gather(*pending, return_exceptions=True)
                        )
                except RuntimeError:
                    pass
                loop.close()

        thread = threading.Thread(
            target=_run_with_start,
            name=f"actant-orchestration-{self.name}",
            daemon=True,
        )
        thread.start()

        # 等待 start() 在后台线程完成，确保事件回调已注册后才返回。
        if not ready_event.wait(timeout=_SHUTDOWN_TIMEOUT_SECS):
            raise InvalidStateError(
                "orchestration loop failed to start within timeout"
            )
        if start_error:
            raise start_error[0]

    def start_metrics_server(self, port: int = 9090, bind_address: str = "127.0.0.1") -> int:
        """启动 Prometheus 指标和健康 HTTP 服务器。

        如果端口为 0，操作系统会分配一个可用端口。
        返回实际绑定的端口。
        """
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)

        rt = self._runtime
        node_id = rt.node_id()

        class _MetricsHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path == "/health":
                    status_str, peer_count = rt.get_health_info()
                    body = json.dumps(
                        {
                            "node_id": node_id,
                            "status": status_str,
                            "peers": peer_count,
                        }
                    )
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(body.encode())
                else:
                    body = rt.prometheus_text()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(body.encode())

            def log_message(self, format: str, *args: Any) -> None:
                pass

        class _ReuseHTTPServer(HTTPServer):
            allow_reuse_address = True

        server = _ReuseHTTPServer((bind_address, port), _MetricsHandler)
        actual_port = server.server_address[1]
        self._metrics_server = server
        thread = threading.Thread(
            target=server.serve_forever,
            daemon=True,
            name=f"actant-metrics-{actual_port}",
        )
        thread.start()
        return actual_port

    def has_metrics_server(self) -> bool:
        return self._metrics_server is not None

    def stop_metrics_server(self) -> None:
        """关闭指标服务器（如果运行中）。"""
        if self._metrics_server is not None:
            self._metrics_server.shutdown()
            self._metrics_server = None

    def drain(self, timeout: float | None = None) -> bool:
        """停止接受新任务，等待正在运行的任务完成。

        如果超时时间为 None，则无限等待。
        返回 True 如果所有任务都完成，否则返回 False。
        """
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)
        self._drained_event.clear()
        self._runtime.drain()
        if self._runtime.running_task_count() == 0:
            return True
        result = self._drained_event.wait(timeout=timeout)
        return result

    def shutdown(self, timeout: float = 10.0) -> None:
        """优雅地关闭运行时。

        Args:
            timeout: 强制关闭前等待运行中任务完成的最大时间（秒）。
                该超时时间会传递给 Rust 运行时。
        """
        self._fire_shutdown()

        self.stop_metrics_server()
        self._shutdown_orchestration()

        if self._runtime is not None:
            self._runtime.drain()

            # 等待运行中任务完成。Rust 端目前没有"任务数为 0"的事件回调，
            # 因此在 deadline 内以 50ms 步长轮询 running_task_count()。
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                count = self._runtime.running_task_count()
                if count == 0:
                    break
                time.sleep(0.05)

            remaining_ms = max(0, int((deadline - time.monotonic()) * 1000))
            self._runtime.shutdown(remaining_ms)
            self._runtime = None
            if isinstance(self.capacity_provider, DefaultCapacityProvider):
                self.capacity_provider._cache.clear()
            self._ctx = None
            # 强制 GC 立即释放 Rust Arc<RuntimeContext> 引用。
            # 如果不这样做，Python 的惰性 GC 可能会延迟释放 Arc 引用，
            # 导致 LMDB 锁文件被持续占用，阻止其他进程打开相同的 data_dir。
            import gc

            gc.collect()

        # 可观测性：停止 viztracer 并写出报告（若已启用）。
        # 在运行时关闭后调用，确保 trace 覆盖整个生命周期。
        _obs.stop()

        # 唤醒可能在 run() 中等待的线程。
        self._shutdown_event.set()

    def _shutdown_orchestration(self) -> None:
        """关闭编排循环和事件循环。"""
        loop = self._orchestration_loop
        ev = self._event_loop
        if loop is not None and ev is not None:
            future: ConcurrentFuture[None] = asyncio.run_coroutine_threadsafe(
                loop.stop(),
                ev,
            )
            try:
                future.result(timeout=_SHUTDOWN_TIMEOUT_SECS)
            except TimeoutError:
                future.cancel()
            except Exception:
                pass

            ev.call_soon_threadsafe(ev.stop)
            self._orchestration_loop = None
            self._event_loop = None

    @property
    def task_names(self) -> list[str]:
        with self._lock:
            return list(self._tasks.keys())

    @property
    def capacity(self) -> NodeCapacity:
        """返回本地节点的 NodeCapacity 快照。"""
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)
        return self._local_node_capacity()

    @property
    def node_id(self) -> str:
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)
        return self._runtime.node_id()

    @property
    def capabilities(self) -> dict[str, Any]:
        return self._capabilities

    def list_actors(self) -> list[str]:
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)
        return self._runtime.actor_core().list_actors()

    def list_workflows(self) -> list[tuple[str, str]]:
        """列出所有活动的 workflow 及其状态。"""
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)
        return self._runtime.list_workflows()

    def workflow_state(self, workflow_id: str) -> str | None:
        """查询 workflow 的前状态。"""
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)
        return self._runtime.workflow_state(workflow_id)

    def task_states(self, workflow_id: str) -> list[tuple[str, str]] | None:
        """查询 workflow 中所有任务的状态。"""
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)
        return self._runtime.task_states(workflow_id)

    def get_workflow_status(self, workflow_id: str) -> dict[str, Any]:
        """获取 workflow 的详细状态。

        Returns:
            - workflow_id: str
            - state: str (Pending|Running|Completed|Failed|Cancelled)
            - tasks: list of {task_id, task_name, state, error}
            - result_count: int (number of tasks with results)
            - error: str | None (workflow-level error if Failed)

        Raises:
            - InvalidStateError: 运行时未启动
            - ActantNotFoundError: workflow 不存在
        """
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)

        state = self._runtime.workflow_state(workflow_id)
        if state is None:
            from actant.exceptions import raise_for_kind

            raise_for_kind("not_found", f"workflow {workflow_id} not found")

        raw_tasks = self._runtime.task_states(workflow_id) or []

        # 构造结构化任务信息 — task_states 返回 (task_id, state_str)
        tasks_info: list[dict[str, Any]] = []
        for tid, tstate in raw_tasks:
            tasks_info.append(
                {
                    "task_id": tid,
                    "state": tstate,
                }
            )

        completed_count = sum(1 for _, s in raw_tasks if s == "Completed")

        status: dict[str, Any] = {
            "workflow_id": workflow_id,
            "state": state,
            "tasks": tasks_info,
            "result_count": completed_count,
            "error": None,
        }

        if state == "Failed":
            # 尝试从已存储结果中获取错误
            stored = self._runtime.get_stored_results(workflow_id)
            if stored:
                from actant._serialization import loads

                for result_list in stored:
                    for result_bytes in result_list:
                        try:
                            err = loads(result_bytes)
                            if isinstance(err, Exception):
                                status["error"] = str(err)
                                break
                        except Exception:
                            pass
                    if status["error"]:
                        break

        return status

    def stop_actor(self, actor_id: str) -> None:
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)
        self._runtime.actor_core().stop_actor(actor_id)

    def actor_status(self, actor_id: str) -> str:
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)
        return self._runtime.actor_core().actor_status(actor_id)

    def connect(self, addr: str) -> None:
        """连接到远程 Actant 节点。"""
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)
        loop = self._event_loop
        if loop is None:
            raise InvalidStateError("event loop not running")

        async def _dial() -> None:
            await self._runtime.dial(addr)  # type: ignore[union-attr]

        future: ConcurrentFuture[None] = asyncio.run_coroutine_threadsafe(_dial(), loop)
        future.result(timeout=10)

    def metrics_prometheus(self) -> str:
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)
        return self._runtime.prometheus_text()

    def add_gossip_peer(self, peer_id: str) -> None:
        """显式向当前所有已订阅 gossip 话题添加 peer。"""
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)
        loop = self._event_loop
        if loop is None:
            raise InvalidStateError("event loop not running")

        async def _add() -> None:
            await self._runtime._add_gossip_peer(peer_id)  # type: ignore[union-attr]

        future: ConcurrentFuture[None] = asyncio.run_coroutine_threadsafe(_add(), loop)
        future.result(timeout=10)

    def listen_addresses(self) -> dict[str, Any]:
        if self._runtime is None:
            raise InvalidStateError(_RUNTIME_NOT_INITIALIZED)
        loop = self._event_loop
        if loop is None:
            return {"endpoint_id": "", "relay_url": None, "direct_addrs": []}

        async def _get_addrs() -> dict[str, Any]:
            return await self._runtime.listen_addresses()  # type: ignore[union-attr]

        future: ConcurrentFuture[dict[str, Any]] = asyncio.run_coroutine_threadsafe(
            _get_addrs(), loop
        )
        return future.result(timeout=5)

    def __repr__(self) -> str:
        return f"<_Node {self.name}>"
