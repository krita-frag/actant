"""编排循环：Python 层驱动核心编排逻辑。

事件驱动架构：Rust EventBus 通过 call_soon_threadsafe 直接回调
Python 事件循环，零轮询、零中间队列。
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from actant._components import CapacityProvider, EventContext
from actant._events import dispatch
from actant._serialization import loads
from actant.actant import (
    _OrchestrationEvent,
    _RetryInfo,
    _RuntimeCore,
    _TaskCompletion,
    _TaskDef,
)
from actant.exceptions import NotFoundError
from actant.router import NodeCapacity, TaskRouter

logger = logging.getLogger("actant.orchestration")


class _ContextLogger(logging.LoggerAdapter[logging.Logger]):
    """在日志记录中自动注入 workflow_id / task_id 等结构化上下文。"""

    def process(self, msg: str, kwargs: Any) -> tuple[str, Any]:
        extra: dict[str, Any] = kwargs.get("extra", {}) or {}
        base: dict[str, Any] = dict(self.extra) if self.extra else {}
        merged = {**base, **extra}
        kwargs["extra"] = merged
        # 前缀结构化字段，方便日志收集器解析
        parts = [f"{k}={v}" for k, v in base.items() if v is not None]
        prefix = " ".join(parts)
        return f"[{prefix}] {msg}" if prefix else msg, kwargs


def _log(wf_id: str | None = None, task_id: str | None = None) -> _ContextLogger:
    """创建带上下文的 logger。"""
    return _ContextLogger(logger, {"wf_id": wf_id, "task_id": task_id})


@dataclass(frozen=True, slots=True)
class _RouteInfo:
    """路由决策所需的最小数据。"""

    node_key: str
    task_name: str
    tags: list[str]
    priority: Any


class DefaultCapacityProvider(CapacityProvider):
    """默认容量提供者：缓存 gossip/heartbeat 收到的对等节点容量。"""

    def __init__(self) -> None:
        self._cache: dict[str, NodeCapacity] = {}
        self._last_refresh = 0.0
        self._runtime: Any | None = None

    def update(
        self,
        node_id: str,
        available: int,
        max_capacity: int,
        *,
        endpoint_addr: str | None = None,
    ) -> None:
        existing = self._cache.get(node_id)
        if existing is not None and endpoint_addr is None:
            endpoint_addr = existing.endpoint_addr
        self._cache[node_id] = NodeCapacity(
            available,
            max_capacity,
            endpoint_addr=endpoint_addr,
        )

    def endpoint_addr(self, node_id: str) -> str | None:
        cap = self._cache.get(node_id)
        return cap.endpoint_addr if cap is not None else None

    def snapshot(
        self,
        local_node_id: str,
        local_capabilities: dict[str, Any],
        local_tasks: list[str],
    ) -> dict[str, NodeCapacity]:
        peer_capacities = dict(self._cache)
        if self._runtime is not None:
            local_cap = NodeCapacity(
                self._runtime.available_capacity(),
                self._runtime.max_capacity(),
            )
        else:
            local_cap = self._local_capacity_placeholder(
                local_node_id, local_capabilities, local_tasks
            )
        merged_caps = dict(local_capabilities)
        if local_tasks:
            merged_caps["tasks"] = local_tasks
        if merged_caps:
            local_cap = NodeCapacity(
                local_cap.available,
                local_cap.max_capacity,
                capabilities=merged_caps,
                endpoint_addr=local_cap.endpoint_addr,
            )
        peer_capacities[local_node_id] = local_cap
        return peer_capacities

    def _local_capacity_placeholder(
        self,
        local_node_id: str,
        local_capabilities: dict[str, Any],
        local_tasks: list[str],
    ) -> NodeCapacity:
        """在未持有 RuntimeCore 时构造本地容量占位；调用方应覆盖此值。"""
        # 默认值会被 snapshot 的调用方覆盖。
        caps = dict(local_capabilities)
        if local_tasks:
            caps["tasks"] = local_tasks
        return NodeCapacity(0, 0, capabilities=caps)

    def set_local_runtime(self, runtime: Any) -> None:
        """允许调用方注入运行时以获取真实本地容量。"""
        self._runtime = runtime

    def snapshot_with_runtime(
        self,
        local_node_id: str,
        local_capabilities: dict[str, Any],
        local_tasks: list[str],
    ) -> dict[str, NodeCapacity]:
        peer_capacities = dict(self._cache)
        runtime = getattr(self, "_runtime", None)
        if runtime is not None:
            local_cap = NodeCapacity(
                runtime.available_capacity(),
                runtime.max_capacity(),
            )
        else:
            local_cap = NodeCapacity(0, 0)
        caps = dict(local_capabilities)
        if local_tasks:
            caps["tasks"] = local_tasks
        if caps:
            local_cap = NodeCapacity(
                local_cap.available,
                local_cap.max_capacity,
                capabilities=caps,
                endpoint_addr=local_cap.endpoint_addr,
            )
        peer_capacities[local_node_id] = local_cap
        return peer_capacities


class DefaultOrchestrationEventHandler:
    """默认编排事件处理器（内部）。

    实现任务完成处理、重试调度、条件分支评估、任务路由、
    心跳容量更新等编排循环核心逻辑。

    本类是 ``OrchestrationLoop`` 的内部协作者，不再对外暴露为 ABC。
    用户要定制行为，应使用 ``@actant.on`` 监听事件，而非子类化本类。
    决策型扩展点（路由、序列化、容量）仍通过 ``_Node`` 构造器注入。
    """

    def __init__(self, tasks: dict[str, Any] | None = None) -> None:
        self._tasks = tasks or {}
        self._background_tasks: set[asyncio.Task[None]] = set()

    async def on_task_completed(
        self,
        ctx: EventContext,
        completion: _TaskCompletion,
    ) -> list[Any] | None:
        wf_id = completion.workflow_id
        task_id = completion.task_id
        task_name = completion.task_name or task_id
        log = _log(wf_id, task_id)
        followups: list[Any] = []

        if completion.state == "Completed":
            result = completion.result or b""
            try:
                next_tasks, conditional_edges = ctx.runtime._complete_task_and_broadcast(
                    wf_id,
                    task_id,
                    result,
                    task_name,
                )
                if next_tasks:
                    self._enqueue_routed_tasks(ctx, next_tasks)
                if conditional_edges:
                    followups.append(
                        self._evaluate_conditional_edges(ctx, wf_id, result, conditional_edges)
                    )
            except Exception:
                log.exception("complete_task_and_broadcast failed for %s/%s", wf_id, task_id)
                try:
                    ctx.runtime._broadcast_failure(
                        wf_id,
                        task_id,
                        "internal error: failed to process task completion",
                        task_name,
                    )
                except Exception:
                    log.exception(
                        "broadcast_failure fallback also failed for %s/%s", wf_id, task_id
                    )
            await dispatch("task.completed", completion)
        elif completion.state == "Failed":
            error = completion.error or "unknown error"
            log.debug("task failed: %s", error)
            try:
                retry_info = ctx.runtime._mark_failed_and_get_retry_info(
                    wf_id,
                    task_id,
                    error,
                )
                if (
                    retry_info is not None
                    and retry_info.current_retry_count < retry_info.max_retries
                ):
                    followups.append(
                        self._schedule_retry(ctx, wf_id, task_id, retry_info.next_delay_ms, retry_info)
                    )
                else:
                    ctx.runtime._broadcast_failure(wf_id, task_id, error, task_name)
            except Exception:
                log.exception("on_task_failed failed")
                try:
                    ctx.runtime._broadcast_failure(wf_id, task_id, error, task_name)
                except Exception:
                    log.exception("broadcast_failure also failed")
            await dispatch("task.failed", completion)

        return followups or None

    async def route_tasks(
        self,
        ctx: EventContext,
        tasks: list[Any],
    ) -> list[Any]:
        """对任务列表应用路由决策，返回带 target 和 endpoint 的路由后列表。"""
        route_map = self._build_route_map(ctx, tasks)

        endpoint_map: dict[str, str] = {}
        if route_map:
            for task_id, node_id in route_map.items():
                ep = ctx.capacity_provider.endpoint_addr(node_id)
                if ep is not None:
                    endpoint_map[task_id] = ep

        routed = []
        for t in tasks:
            task_id = t.task_id
            existing_target = t.target_node
            existing_endpoint = t.target_endpoint_addr
            target = None
            target_endpoint = None
            if route_map and task_id in route_map:
                target = route_map[task_id]
                target_endpoint = endpoint_map.get(task_id)
            elif existing_target:
                target = existing_target
                target_endpoint = existing_endpoint
            routed.append(
                _TaskDef(
                    task_id=task_id,
                    name=t.name,
                    payload=t.payload,
                    workflow_id=t.workflow_id,
                    target_node=target,
                    target_endpoint_addr=target_endpoint,
                    timeout_ms=t.timeout_ms,
                    retry_policy=t.retry_policy,
                )
            )
        return routed

    async def on_orchestration_event(
        self,
        ctx: EventContext,
        event: _OrchestrationEvent,
    ) -> list[Any] | None:
        if event.event_type == "DagStateUpdate":
            wf_id = event.workflow_id
            task_id = event.task_id
            state = event.state
            data = event.data or b""
            if wf_id and task_id:
                try:
                    ctx.runtime._apply_dag_state_update(wf_id, task_id, state, data)
                except NotFoundError:
                    pass
                except RuntimeError as e:
                    logger.exception(
                        "apply_dag_state_update failed for %s/%s: %s", wf_id, task_id, e
                    )
                except Exception:
                    logger.exception("apply_dag_state_update failed for %s/%s", wf_id, task_id)
        elif event.event_type == "NodeHeartbeat":
            try:
                node_id = event.node_id or ""
                if node_id:
                    ctx.capacity_provider.update(
                        node_id,
                        event.available_capacity or 0,
                        event.max_capacity or 0,
                    )
            except Exception:
                logger.exception("update_peer_capacity failed for %s", event.node_id)
        elif event.event_type == "HeadsExchange":
            data = event.data or b""
            if data:
                try:
                    ctx.runtime._handle_heads_exchange(data)
                except Exception:
                    logger.exception("handle_heads_exchange failed for %s", event.node_id)
        elif event.event_type == "WorkerDrained":
            logger.info("worker drained: %s", event.node_id)
            await dispatch("worker.drained", event)
        return None

    def _build_route_map(
        self, ctx: EventContext, tasks: list[Any]
    ) -> dict[str, str] | None:
        router = ctx.router
        if router is None or not isinstance(router, TaskRouter):
            return None

        provider = ctx.capacity_provider
        if isinstance(provider, DefaultCapacityProvider):
            peer_capacities = provider.snapshot_with_runtime(
                ctx.node_id, {}, ctx.local_tasks
            )
        else:
            peer_capacities = provider.snapshot(ctx.node_id, {}, ctx.local_tasks)

        local_cap = peer_capacities.get(ctx.node_id)
        if local_cap is not None and local_cap.max_capacity == 0:
            peer_capacities.pop(ctx.node_id, None)

        target_nodes: dict[str, str] = {}
        for t in tasks:
            task_obj = self._tasks.get(t.name)
            task_meta: dict[str, Any] = {
                "name": t.name,
                "tags": task_obj._tags if task_obj else [],
                "priority": task_obj._priority if task_obj else None,
            }
            if task_obj is not None and local_cap is not None and local_cap.max_capacity > 0:
                continue
            target = router.route(ctx.node_id, t.task_id, task_meta, peer_capacities)
            if target is not None and target != ctx.node_id:
                target_nodes[t.task_id] = target

        return target_nodes if target_nodes else None

    async def _schedule_retry(
        self,
        ctx: EventContext,
        wf_id: str,
        task_id: str,
        delay_ms: int,
        retry_info: _RetryInfo,
    ) -> None:
        log = _log(wf_id, task_id)
        try:
            await asyncio.sleep(delay_ms / 1000.0)
            retry_task = ctx.runtime._prepare_retry(wf_id, task_id)
            if retry_task is not None:
                self._enqueue_routed_tasks(ctx, [retry_task])
                log.info(
                    "retrying task (attempt %d/%d)",
                    retry_info.current_retry_count + 1,
                    retry_info.max_retries,
                )
            else:
                ctx.runtime._broadcast_failure(
                    wf_id,
                    task_id,
                    "retry preparation failed",
                    task_id,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("retry scheduling failed")

    async def _evaluate_conditional_edges(
        self,
        ctx: EventContext,
        wf_id: str,
        result_bytes: bytes,
        conditional_edges: list[tuple[str, str]],
    ) -> None:
        log = _log(wf_id)
        try:
            result_value = loads(result_bytes)
        except Exception:
            log.exception("failed to deserialize result for condition evaluation")
            return

        for successor_id, condition_tag in conditional_edges:
            evaluator = ctx.condition_evaluators.get(condition_tag)
            if evaluator is None:
                log.warning("no evaluator for condition tag %r, skipping branch", condition_tag)
                continue

            try:
                should_activate = evaluator(result_value)
            except Exception:
                log.exception("condition evaluator %r failed", condition_tag)
                continue

            if should_activate:
                try:
                    task_def = ctx.runtime._activate_conditional_successor(
                        wf_id,
                        successor_id,
                    )
                    if task_def is not None:
                        self._enqueue_routed_tasks(ctx, [task_def])
                        log.debug("activated conditional branch %r", condition_tag)
                except Exception:
                    log.exception("activate_conditional_successor failed for task %s", successor_id)
            else:
                try:
                    next_tasks = ctx.runtime._skip_conditional_branch(wf_id, successor_id)
                    if next_tasks:
                        self._enqueue_routed_tasks(ctx, next_tasks)
                    log.debug(
                        "skipped conditional branch %r (task %s, %d ready)",
                        condition_tag,
                        successor_id,
                        len(next_tasks),
                    )
                except Exception:
                    log.exception("skip_conditional_branch failed for task %s", successor_id)

    def _enqueue_routed_tasks(self, ctx: EventContext, tasks: list[Any]) -> None:
        """对编排器产生的后续任务进行路由决策后入队。"""
        # route_tasks 是 async，但这里需要同步调用；编排循环会 await
        # 返回的协程，因此直接创建 task 并交给 loop。
        coro = self.route_tasks(ctx, tasks)

        async def _enqueue() -> None:
            routed = await coro
            for t in routed:
                if t.workflow_id:
                    from types import SimpleNamespace

                    await dispatch(
                        "task.started",
                        SimpleNamespace(workflow_id=t.workflow_id, task_name=t.name),
                    )
            ctx.runtime.enqueue_tasks(routed)

        task = asyncio.create_task(_enqueue())
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)


class OrchestrationLoop:
    """Python 侧编排循环，驱动事件处理。

    事件通过 Rust 的 PyEventBridge 直接回调到 Python 事件循环，
    无需 drain thread 或 asyncio.Queue 中间层。

    本类负责事件分派与生命周期管理，具体业务逻辑委托给
    ``DefaultOrchestrationEventHandler``（内部协作者）。用户要定制
    行为，应使用 ``@actant.on`` 监听事件，而非替换 handler。
    决策型扩展点（路由、序列化、容量）仍通过 ``_Node`` 构造器注入。
    """

    DEFAULT_REROUTE_INTERVAL: float = 2.0

    def __init__(
        self,
        runtime: _RuntimeCore,
        handler: DefaultOrchestrationEventHandler,
        ctx: EventContext,
        *,
        reroute_interval: float = DEFAULT_REROUTE_INTERVAL,
        refresh_capacity: Callable[[], None] | None = None,
        capacity_refresh_interval: float = 5.0,
    ) -> None:
        self._runtime = runtime
        self._handler = handler
        self._ctx = ctx
        self._running = False
        # _tracked_tasks 收集所有由编排循环派生的后台 asyncio.Task：
        # completion 处理、retry 调度、条件边评估等。命名 _tracked_tasks
        # 反映其"生命周期跟踪"职责（不再仅限于 retry）。
        self._tracked_tasks: set[asyncio.Task[None]] = set()
        self._last_reroute = 0.0
        self._reroute_interval = reroute_interval
        self._reroute_task: asyncio.Task[None] | None = None
        self._refresh_capacity = refresh_capacity
        self._capacity_refresh_interval = capacity_refresh_interval
        self._capacity_refresh_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True

        self._runtime.set_event_callback(self._on_rust_event)

        self._reroute_task = asyncio.create_task(
            self._reroute_loop(),
            name="reroute-loop",
        )

        if self._refresh_capacity is not None:
            self._capacity_refresh_task = asyncio.create_task(
                self._capacity_refresh_loop(),
                name="capacity-refresh-loop",
            )

        await self._recover_workflows()
        logger.info("orchestration loop started")

    async def _recover_workflows(self) -> None:
        """从持久化存储中恢复未完成的 workflow，重新调度 pending 任务。"""
        try:
            recoverable = self._runtime._recoverable_workflows_with_pending()
        except Exception:
            logger.debug("no recoverable workflows (likely no data_dir)")
            return

        if not recoverable:
            return

        recovered: list[Any] = []
        for wf_id in recoverable:
            try:
                ready = self._runtime._recover_workflow(wf_id)  # type: ignore[attr-defined]
                recovered.extend(ready)
            except Exception:
                logger.exception("failed to recover workflow %s", wf_id)

        if recovered:
            routed = await self._handler.route_tasks(self._ctx, recovered)
            self._runtime.enqueue_tasks(routed)
            logger.info("recovered and enqueued %d ready task(s)", len(routed))

    async def stop(self) -> None:
        """停止编排循环。"""
        if not self._running:
            return
        self._running = False

        if self._reroute_task is not None:
            self._reroute_task.cancel()
            self._reroute_task = None

        if self._capacity_refresh_task is not None:
            self._capacity_refresh_task.cancel()
            self._capacity_refresh_task = None

        for task in list(self._tracked_tasks):
            task.cancel()
        if self._tracked_tasks:
            with contextlib.suppress(Exception):
                await asyncio.wait(
                    self._tracked_tasks,
                    timeout=2.0,
                )
        self._tracked_tasks.clear()
        logger.info("orchestration loop stopped")

    def _on_rust_event(self, event: Any) -> None:
        """由 Rust 的 PyEventBridge 通过 call_soon_threadsafe 调用的回调函数。

        此函数在 Python 事件循环线程上运行。事件已由 Rust 端
        从 BusEvent 转换为 PyEvent。
        """
        if not self._running:
            return

        try:
            if event.kind == "completion" and event.completion is not None:
                async def _handle_completion_event() -> None:
                    followups = await self._handler.on_task_completed(
                        self._ctx, event.completion
                    )
                    for coro in followups or []:
                        self._spawn_tracked(coro)

                self._spawn_tracked(_handle_completion_event())
            elif event.kind == "supervision" and event.supervision is not None:
                # Actor 生命周期事件分发给订阅者。
                # 监督器由 _Node 订阅 actor.* 事件后转发给 ActorSupervisor，
                # 用户自定义逻辑通过 @actant.on("actor.failed") 等订阅。
                # event.event_type 来自 Rust（PascalCase：ActorStarted/ActorFailed/ActorStopped）。
                topic_map = {
                    "ActorStarted": "actor.started",
                    "ActorStopped": "actor.stopped",
                    "ActorFailed": "actor.failed",
                }
                topic = topic_map.get(event.event_type)
                if topic is None:
                    return

                async def _dispatch_supervision() -> None:
                    from types import SimpleNamespace

                    await dispatch(
                        topic,
                        SimpleNamespace(
                            event_type=event.event_type,
                            actor_id=event.actor_id,
                            error=event.error,
                        ),
                    )

                self._spawn_tracked(_dispatch_supervision())
            elif event.orchestration is not None:
                async def _handle_orchestration_event() -> None:
                    followups = await self._handler.on_orchestration_event(
                        self._ctx, event.orchestration
                    )
                    for coro in followups or []:
                        self._spawn_tracked(coro)

                self._spawn_tracked(_handle_orchestration_event())
        except Exception:
            logger.exception("error in _on_rust_event callback")

    def _spawn_tracked(self, coro: Any, name: str | None = None) -> asyncio.Task[None] | None:
        """使用 asyncio.create_task 创建一个跟踪任务。

        若编排循环已停止，返回 None 且不创建任务，避免产生未 await 的协程。
        """
        if not self._running:
            coro.close()  # 显式关闭协程，避免 RuntimeWarning
            return None
        task = asyncio.create_task(coro, name=name)
        self._tracked_tasks.add(task)
        task.add_done_callback(self._tracked_tasks.discard)
        return task

    async def _reroute_loop(self) -> None:
        """定期重新路由未路由任务。"""
        while self._running:
            await asyncio.sleep(self._reroute_interval)
            await self._try_reroute()

    async def _try_reroute(self) -> None:
        """尝试重新路由未路由任务。"""
        try:
            unrouted = self._runtime._drain_unrouted_tasks()
            if not unrouted:
                return

            routed = await self._handler.route_tasks(self._ctx, unrouted)
            if routed:
                self._runtime.enqueue_tasks(routed)
                logger.debug("re-routed %d unrouted task(s)", len(routed))
        except Exception:
            logger.exception("reroute check error")

    async def _capacity_refresh_loop(self) -> None:
        """定期刷新对等体容量缓存。"""
        refresh = self._refresh_capacity
        if refresh is None:
            return
        # 初始刷新
        try:
            refresh()
        except Exception:
            logger.debug("initial capacity refresh failed")

        while self._running:
            await asyncio.sleep(self._capacity_refresh_interval)
            try:
                refresh()
            except Exception:
                logger.debug("capacity refresh failed")
