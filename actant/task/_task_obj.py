"""``Task`` 类与 ``@task`` 装饰器。

依赖 ``_async_result``（``AsyncResult`` / ``_resolve_args_with_deps``）、``_context``
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
from actant.task._async_result import AsyncResult, _resolve_args_with_deps
from actant.task._context import TaskContext
from actant.task._helpers import (
    _is_coroutine_function,
    _run_coroutine_on_worker_thread,
    _safe_serialize,
)

_logger = logging.getLogger("actant.task")


def _unpack_item(
    item: Any,
    unpack: bool,
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """按 ``unpack`` 语义解包 ``submit_batch`` 的单个元素。

    支持 ``(args,)`` / ``(args, kwargs)`` 两种形式（与 ``starmap`` 一致）；
    ``unpack=False`` 时元素整体作为唯一位置参数。
    """
    if not unpack:
        return (item,), {}
    if isinstance(item, tuple) and len(item) == 2 and isinstance(item[1], dict):
        raw_args, raw_kwargs = item
        return raw_args, raw_kwargs
    return tuple(item), {}


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
        silent: ``True`` 时跳过 TaskLifecycle 事件发布（started/completed/
            failed/retried/cancelled 均不 emit）。用于高吞吐批量提交场景，
            避免每个 task 产生两次事件造成 event_bus 噪声。默认 ``False``。
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
        silent: bool = False,
    ) -> None:
        self._func = func
        self._name = name or f"{func.__module__}.{func.__qualname__}"
        self._timeout_ms = timeout_ms
        self._retries = retries
        self._retry_delay_ms = retry_delay_ms
        self._tags = list(tags) if tags else []
        self._priority = priority
        self._silent = silent
        # 保留原函数的元数据，使 Task 可被 functools.wraps / inspect 等工具识别。
        wraps(func)(self)

    @property
    def name(self) -> str:
        return self._name

    @property
    def func(self) -> Callable[..., Any]:
        return self._func

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """本地同步调用（等价于原函数）。

        .. warning::
            **此路径不经过分布式调度**：不进入 Worker 队列、不应用 ``retries`` /
            ``timeout_ms`` / ``priority`` / ``tags`` / ``silent`` 等任务配置，
            不发布 ``TaskLifecycle`` 事件，也不参与 ``cancel_task`` 协作取消。
            参数中的 ``AsyncResult`` **不会**自动解析（直接传入会导致
            cloudpickle 序列化失败或类型错误）。

            分布式执行请使用 :meth:`submit`。``__call__`` 仅适用于：
            - 本地纯函数求值（如 flow 内部已通过 ``submit`` 拿到结果后的
              后处理）；
            - 调试 / 单元测试场景下复用 task 包装的函数体。

        对于 ``async def`` 函数，在临时 event loop 中同步执行 coroutine
        并返回其结果，而非返回 coroutine 对象。这使用户可以直接
        ``result = task(x)`` 调用 async task，无需手动 ``asyncio.run``。
        """
        if _is_coroutine_function(self._func):
            coro = self._func(*args, **kwargs)
            return _run_coroutine_on_worker_thread(coro)
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

        Note:
            **执行节点失联时为 fail-fast，不自动重跑**：被指定的节点
            死亡后在途任务会以 ``WorkerError`` 终结，句柄不会永久挂起，但任务
            也不会被重派发到别的节点——源节点没有"该任务尚未执行完"的持久凭据，
            自动重跑会静默重复副作用。需要重跑请显式声明重试策略
            （``@task(retries=...)``）或重新提交。
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

        # 单遍解析参数：把上游 AsyncResult 解析为其结果值，同时收集其 task_id
        # 作为编排依赖边（解析与收集合并一次遍历，避免两遍重复递归）。
        resolved_args, resolved_kwargs, deps = _resolve_args_with_deps(args, kwargs)

        # 判定：FlowContext 活跃 → orchestrator 驱动的持久化编排路径；
        # 否则保持独立 @task 直调的任务队列语义不变。
        from actant.flow import current_flow_state

        flow_state = current_flow_state()
        if flow_state is not None:
            return self._flow_submit(
                runtime, flow_state, resolved_args, resolved_kwargs, deps,
            )

        task_id, _workflow_id, _payload, _task_ctx, handle, task_def = self._prepare_task_def(
            resolved_args, resolved_kwargs,
            target_node=target_node, target_endpoint_addr=target_endpoint_addr,
            runtime=runtime,
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

    def _flow_submit(
        self,
        runtime: Any,
        state: Any,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
        deps: list[str],
    ) -> AsyncResult:
        """flow 上下文内的提交路径：生成节点 → 持久化 → orchestrator 派发。

        节点标识 ``{任务名}-{序号}-{workflow_id}`` 确定性生成（跨重放稳定）；依赖边来自参数树引用的上游节点（``_resolve_args_with_deps``，
        上游结果已阻塞解析并内联进 payload）；经 ``add_workflow_node`` 先持久化
        再派发。重放命中历史时（节点已存在且指纹一致）不重新提交：已完成 →
        返回记录结果的句柄；进行中 → 返回绑定句柄。
        """
        seq = state.next_seq()
        task_id = f"{self._name}-{seq}-{state.workflow_id}"

        _tid, _wf, _payload, _ctx, handle, _task_def = self._prepare_task_def(
            resolved_args, resolved_kwargs,
            target_node=None, target_endpoint_addr=None,
            runtime=runtime,
            task_id=task_id,
            flow_mode=True,
        )

        # 工作流外壳惰性创建：首个节点提交前持久化空 workflow（含失败策略与
        # 工作流级 deadline）；空 flow 不创建工作流。
        # 经 flow 侧的唯一入口创建，顺带在真正建槽**之后**广播 submitted/started
        # ——创建只此一处，事件时序才能稳定；各自实现一遍会让时序漂移。
        from actant.flow import _ensure_workflow_created

        _ensure_workflow_created(runtime, state)

        # @task(retries=...) 映射为节点 RetryPolicy（orchestrator 是唯一
        # 重试执行者）；payload 头部 retries 已在 flow_mode 下剥离。
        from actant.actant import _DagNode, _RetryPolicy

        retry = (
            _RetryPolicy(max_retries=self._retries, delay_ms=self._retry_delay_ms)
            if self._retries > 0
            else None
        )
        node = _DagNode(
            task_id=task_id,
            name=self._name,
            payload=_payload,
            retry=retry,
            timeout_ms=self._timeout_ms if self._timeout_ms > 0 else None,
            priority=self._priority,
        )
        outcome = runtime.add_workflow_node(state.workflow_id, node, deps)
        if not outcome.get("created"):
            # 重放命中：按历史状态重建句柄，不重新提交、不重跑。
            from actant.flow import _replay_node_outcome

            _replay_node_outcome(runtime, handle, outcome)
        return handle

    def _prepare_task_def(
        self,
        resolved_args: tuple[Any, ...],
        resolved_kwargs: dict[str, Any],
        *,
        target_node: str | None,
        target_endpoint_addr: str | None,
        runtime: Any,
        task_id: str | None = None,
        flow_mode: bool = False,
    ) -> tuple[str, str, bytes, TaskContext, AsyncResult, Any]:
        """构造单个任务的内部状态（task_id, payload, AsyncResult, _TaskDef）。

        抽出此方法以支持 ``_submit`` / ``_flow_submit`` / ``submit_batch``
        共享序列化、上下文创建、Runtime 注册逻辑，避免多条路径行为漂移。

        Args:
            task_id: 外部指定的任务标识（flow 路径传入确定性节点 id，重放
                身份）；省略时按 uuid 生成。
            flow_mode: flow 编排路径标记。置位时 payload 头部 ``retries`` 置 0
                （重试单层化：flow 任务唯一重试执行者是 orchestrator 节点
                RetryPolicy，worker 层不重试）。

        Returns:
            ``(task_id, workflow_id, payload, task_ctx, handle, task_def)``
            元组。调用方负责调用 ``core.submit_task(task_def)`` / 批量提交，
            或（flow 路径）以 payload 构造 DAG 节点经 orchestrator 派发。
        """
        if task_id is None:
            task_id = f"{self._name}-{uuid.uuid4().hex[:8]}"
        # 从 flow 上下文继承 workflow_id，使 TaskEvent 归属正确。
        from actant.flow import current_workflow_id

        workflow_id = current_workflow_id() or ""

        # 值引用降级（均发生在提交方父进程）：
        # 1. 参数树中的 Ref（上游大结果 / 用户显式传入）→ _RefArg 帧内联字节
        #    （blob 字节原样搬运，无对象级序列化）。
        # 2. 直传大值（> REF_INLINE_THRESHOLD）→ 落 blob + _RefArg。
        from actant.task._ref import _degrade_large_values, _materialize_refs

        def _fetch(rb: bytes) -> bytes:
            from actant.task._ref import _value_fetch

            return _value_fetch(rb, runtime=runtime)

        def _store(data: bytes) -> bytes:
            from actant.task._ref import _value_store

            return _value_store(data, runtime=runtime)

        resolved_args = tuple(_materialize_refs(a, _fetch) for a in resolved_args)
        resolved_kwargs = {
            k: _materialize_refs(v, _fetch) for k, v in resolved_kwargs.items()
        }
        resolved_args = tuple(_degrade_large_values(a, _store) for a in resolved_args)
        resolved_kwargs = {
            k: _degrade_large_values(v, _store) for k, v in resolved_kwargs.items()
        }

        options = {
            "retries": 0 if flow_mode else self._retries,
            "retry_delay_ms": 0 if flow_mode else self._retry_delay_ms,
            "timeout_ms": self._timeout_ms,
            "task_id": task_id,
            "workflow_id": workflow_id,
            "tags": self._tags,
            "priority": self._priority,
            "silent": self._silent,
        }
        payload = _safe_serialize(
            self._func, resolved_args, resolved_kwargs, options, task_id=self._name,
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
        return task_id, workflow_id, payload, task_ctx, handle, task_def

    def submit_batch(
        self,
        items: Iterable[Any],
        *,
        target_node: str | None = None,
        target_endpoint_addr: str | None = None,
        unpack: bool = False,
    ) -> list[AsyncResult]:
        """批量提交多个任务，返回 ``AsyncResult`` 列表。

        与循环调用 ``submit`` 相比，批量提交通过单次 Rust 调用
        ``core.submit_tasks_batch`` 一次性投递所有 TaskDefinition 到
        scheduler 的 ``enqueue_batch``，绕过 N 次 Python→Rust 边界切换和
        channel 单条投递开销。

        适合高吞吐场景（如 ``gather(*handles)`` 前的批量提交）。

        .. note::
            **依赖解析行为**：``items`` 中的 ``AsyncResult`` 会**逐个阻塞解析**
            （与 :meth:`submit` 一致），意味着批量提交的总耗时包含上游任务
            的串行等待时间。若需并行解析上游依赖，应先用 :func:`gather` 等待
            所有上游 handle 完成，再将结果列表传入 ``submit_batch``。

            例：``b.submit_batch([a.submit(x) for x in xs])`` 会按顺序等待
            每个 ``a.submit(x)`` 完成后再提交 ``b``，无法并行化 ``a`` 的执行。
            改写为 ``a_handles = [a.submit(x) for x in xs]; a_results = gather(*a_handles);
            b.submit_batch(a_results)`` 可让 ``a`` 并行执行。

        Args:
            items: 可迭代对象，每个元素作为 ``submit`` 的唯一位置参数。
                与 ``map`` 一致。
            target_node: 路由目标节点 ID（可选）。
            target_endpoint_addr: 路由目标 endpoint 地址（可选）。
            unpack: ``True`` 时每个元素解包为 ``(args, kwargs)`` 或 ``args``
                tuple。与 ``starmap`` 一致。``False``（默认）时元素整体作为
                唯一位置参数。

        Returns:
            ``AsyncResult`` 列表，顺序与输入 ``items`` 一致。

        Raises:
            InvalidStateError: Runtime 未启动，或 flow 的工作流已到达终态
                （如 deadline 到期被强还原后继续提交节点）。

        用法::

            @actant.task
            def square(x): return x * x
            with actant.Runtime.with_defaults():
                handles = square.submit_batch([1, 2, 3, 4, 5])
                results = actant.gather(*handles)  # [1, 4, 9, 16, 25]

            @actant.task
            def add(a, b): return a + b
            with actant.Runtime.with_defaults():
                handles = add.submit_batch([(1, 2), (3, 4)], unpack=True)
                results = actant.gather(*handles)  # [3, 7]
        """
        runtime = get_current_runtime()
        if runtime is None:
            raise InvalidStateError(
                "task.submit_batch: no active Runtime; "
                "wrap your code in `with actant.Runtime() as rt:`"
            )

        # 判定：flow 上下文中逐项走 orchestrator 驱动的编排路径（节点
        # 先持久化再派发），不做批量直调——编排节点的持久化与派发裁决必须
        # 逐节点进行。批量快路径仅保留给独立 @task（任务队列语义）。
        from actant.flow import current_flow_state

        flow_state = current_flow_state()
        if flow_state is not None:
            handles: list[AsyncResult] = []
            for item in items:
                raw_args, raw_kwargs = _unpack_item(item, unpack)
                resolved_args, resolved_kwargs, deps = _resolve_args_with_deps(
                    raw_args, raw_kwargs
                )
                handles.append(
                    self._flow_submit(runtime, flow_state, resolved_args, resolved_kwargs, deps)
                )
            return handles

        core = runtime._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")

        # 准备所有 task_def + handle。任一失败则回滚已注册的 task_id。
        prepared: list[tuple[str, bytes, list[str], AsyncResult, Any]] = []
        registered_ids: list[str] = []
        try:
            for item in items:
                raw_args, raw_kwargs = _unpack_item(item, unpack)
                # 单遍解析本项参数：解析上游 AsyncResult 并收集 task_id 作为依赖边。
                resolved_args, resolved_kwargs, deps = _resolve_args_with_deps(
                    raw_args, raw_kwargs
                )
                task_id, _wf, payload, _ctx, handle, task_def = self._prepare_task_def(
                    resolved_args, resolved_kwargs,
                    target_node=target_node,
                    target_endpoint_addr=target_endpoint_addr,
                    runtime=runtime,
                )
                prepared.append((task_id, payload, deps, handle, task_def))
                registered_ids.append(task_id)
        except Exception:
            # 序列化失败：清理已注册的 task_id。
            for tid in registered_ids:
                runtime.unregister_task(tid)
            raise

        # 一次性提交到 Rust scheduler。
        task_defs = [td for _, _, _, _, td in prepared]
        try:
            core.submit_tasks_batch(task_defs)
        except Exception:
            for tid in registered_ids:
                runtime.unregister_task(tid)
            raise

        return [handle for _, _, _, handle, _ in prepared]

    def delay(self, *args: Any, **kwargs: Any) -> AsyncResult:
        """``submit`` 的别名（Celery 风格）。"""
        return self.submit(*args, **kwargs)

    def map(self, iterable: Iterable[Any]) -> list[AsyncResult]:
        """对 iterable 中每个元素提交一个任务，返回 ``AsyncResult`` 列表。

        等价于 ``[self.submit(x) for x in xs]``，但内部走 ``submit_batch``
        批量路径，单次 Rust 调用投递所有 TaskDefinition，比循环 ``submit``
        快 10-50×。

        Args:
            iterable: 可迭代对象，每个元素作为 ``submit`` 的唯一位置参数。

        Returns:
            ``AsyncResult`` 列表，顺序与输入一致。

        用法::

            @actant.task
            def square(x): return x * x
            with actant.Runtime.with_defaults():
                handles = square.map([1, 2, 3])  # 并行批量提交 3 个任务
                results = actant.gather(*handles)  # [1, 4, 9]
        """
        return self.submit_batch(iterable)

    def starmap(self, iterable: Iterable[Any]) -> list[AsyncResult]:
        """对 iterable 中每个元素解包后提交任务。

        等价于 ``[self.submit(*args) for args in iterable]``，但内部走
        ``submit_batch(unpack=True)`` 批量路径，单次 Rust 调用投递所有
        TaskDefinition，对标 ``itertools.starmap`` / Prefect ``task.map(unpack=True)``。

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
        return self.submit_batch(iterable, unpack=True)

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
    silent: bool = False,
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

        @actant.task(silent=True)  # 不发 TaskLifecycle 事件，高吞吐场景
        def bulk_noop(x):
            return x

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
        silent: ``True`` 时跳过 TaskLifecycle 事件发布。用于高吞吐批量提交
            场景，避免 event_bus 噪声。默认 ``False``。
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
            silent=silent,
        )

    if func is None:
        # 带参数调用：@actant.task(name="foo")
        return _make
    # 无参数调用：@actant.task
    return _make(func)


__all__ = ["Task", "task"]
