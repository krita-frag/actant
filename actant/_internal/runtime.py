"""Python 侧 Runtime：统一注册表 + effect dispatcher。

`Runtime` 是 actant 的统一运行时入口。所有扩展点（数据面 Store/ValueStore、
观测面 TaskLifecycle/WorkflowLifecycle）都注册为 Layer，由 Runtime 按 name 分桶存储。
放置类策略（路由/调度/重试）不在此层注册：路由由 Rust `RoutePolicy` 承担，
可用内置名称或 Python callable（``Runtime(route_policy=fn)``）注入；调度器与
重试策略由 Rust 侧配置或第 3 层外部调度器承担。

# 设计

- Rust `capability::Runtime`：强类型，按 `TypeId` 分桶，服务于 Rust 内置 capability
- Python `Runtime`（本模块）：按 name 分桶，维护 Python handler 列表
- `PyCapabilityRuntime`：Rust Runtime 的 PyO3 包装，本模块持有但不直接暴露给用户

# 模块结构

`Runtime` 由若干 mixin 组合；共享状态与底层原语在 ``runtime_base``，本模块按职责
自上而下排列：

- ``Layer`` / ``_RuntimeEffectsMixin``：ERH 扩展模型的 handler 注册；
- ``_RuntimeTaskMixin``：本地任务句柄、终态回调与生命周期事件；
- ``_RuntimeWorkflowMixin``：持久化工作流读写、上下文值，以及 flow 提交与重放驱动；
- ``_RuntimeNetworkMixin``：节点身份、P2P 网络、指标与并发上限。

# 用法

```python
import actant

rt = actant.Runtime()
rt.layer("MyRouter", "ask").chain(my_router)
rt.layer("TaskLifecycle").chain(lambda e: print(f"task {e.kind}: {e.task_id}"))
rt.start()

# 在 handler 中：
result = actant.ask("MyRouter", ctx)
```
"""

from __future__ import annotations

import logging
import os
import pickle
import shutil
import tempfile
import threading
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any, Literal, cast

from actant._internal.persistence import IdempotencyLedger, TerminalRecordStore
from actant._internal.runtime_base import (
    _CONTINUED_TO_CONTEXT_KEY,
    TASK_STATE_CANCELLED,
    TASK_STATE_COMPLETED,
    TASK_STATE_FAILED,
    TASK_STATE_RUNNING,
    _RuntimeBase,
)
from actant.capabilities import (
    BUILTIN_CAPABILITIES,
    RUST_BACKED_CAPABILITIES,
    TASK_LIFECYCLE,
    VALUE_STORE,
    CapabilityMeta,
    EffectKind,
    ValueStoreReq,
)
from actant.exceptions import (
    InvalidStateError,
    reconstruct_error,
)

if TYPE_CHECKING:
    from actant.actant import _ActantConfig, _StoreStats
    from actant.tasks import AsyncResult, BatchProgress

# 当前线程的 Runtime 上下文。使用 threading.local 保证每个线程有独立的活跃 Runtime。
# `Task.submit` 沿途的 perform/ask 请求经本变量解析活跃 Runtime；任务本身在 worker
# 子进程执行，不共享本局部。父进程侧结果回调（`_on_task_result`）与 TaskLifecycle
# 事件发布显式经 `use_runtime(rt)` 注入自身作为活跃 Runtime。
_runtime_local = threading.local()

_logger = logging.getLogger("actant.runtime")

#: ``Runtime.test()`` 的默认 drain 超时（秒）。测试进程退出应快速，
#: 避免命中生产默认的 30s 等待。
TEST_DRAIN_TIMEOUT_SECS = 5

#: ``stop(timeout=None)`` 传向 Rust ``shutdown`` 的毫秒上限——语义是「不设上限」，
#: 取 i64 最大毫秒数（约 2.9 亿年），避免用 0 被解释成立即强制关闭。
_NO_SHUTDOWN_TIMEOUT_MS = 2**63 - 1


def get_current_runtime() -> Runtime | None:
    """返回当前线程的活跃 Runtime，无则返回 `None`。

    仅查询 ``threading.local``；子线程需通过 ``use_runtime(rt)`` 显式继承。
    任务在 worker 子进程执行，不解析本局部；父进程侧 ``_on_task_result`` 回调
    用 ``use_runtime(self)`` 显式注入运行时后发布 TaskLifecycle 事件。
    """
    return getattr(_runtime_local, "runtime", None)


def _set_current_runtime(rt: Runtime | None) -> None:
    """把当前线程的活跃 Runtime 绑定为 ``rt``，**不恢复旧值**。

    供异步驱动使用：其 event loop 线程在生命周期内固定服务一个 Runtime，
    绑定一次即可覆盖该线程上的全部协程。需要成对恢复的场景请用
    :func:`use_runtime`。
    """
    _runtime_local.runtime = rt


class _DefaultValueStoreHandler:
    """`ValueStore` capability 的默认 handler：桥接本节点 Rust blob 原语。

    ``store`` → ``_RuntimeCore.value_store``（blob_store + BlobRef 编码）；
    ``fetch`` → ``value_fetch``（本地 get_bytes 优先，未命中按 ref.node 跨节点
    流式拉取）。由 ``Runtime.start()`` 在 handler 链为空时链入；用户可在
    start() 前预注册自定义 handler（如 S3 后端）完全替换——perform 取链末位，
    内部 Ref 解析同样经该链分发，覆盖全局生效。
    """

    def __init__(self, runtime: Runtime) -> None:
        self._runtime = runtime

    def __call__(self, req: ValueStoreReq) -> bytes:
        core = self._runtime._rust_core
        if core is None:
            raise InvalidStateError("ValueStore requires a started Runtime")
        if req.op == "store":
            return cast(bytes, core.value_store(req.data))
        return cast(bytes, core.value_fetch(req.ref))


class Layer:
    """构建一个 capability 的 handler 链。

    通过 `Runtime.layer(name)` 创建，支持链式 `chain(handler)` 追加。
    `chain` 直接修改 Runtime 内部 handler 列表（live view），无需显式 register。
    """

    def __init__(self, runtime: Runtime, meta: CapabilityMeta) -> None:
        self._runtime = runtime
        self._meta = meta
        # 不缓存 handlers，直接操作 runtime._layers[name]
        # 确保 runtime 内已有该 name 的列表（layer() 方法已保证）

    @property
    def name(self) -> str:
        return self._meta.name

    @property
    def kind(self) -> EffectKind:
        return self._meta.kind

    def chain(self, handler: Callable[[Any], Any]) -> Layer:
        """追加一个 handler，返回 self 以支持链式调用。

        handler 立即注册到 Runtime，对后续 **Python 端** effect 调用可见。

        .. warning::
            **Python handler 永不进入 Rust 内部 dispatch。** Rust 内部分发路径
            （如 Worker 执行任务时调用 ``task_dispatcher.dispatch()``）不经过
            Python ``_layers``——无论 ``start()`` 前后注册，Python handler 仅
            在用户代码显式调用 ``actant.ask/perform/emit(name, ...)`` 时生效。

            覆盖 Rust 内部行为（如自定义任务执行逻辑）应使用 Rust 侧配置
            （``RuntimeBuilder`` 的 ``with_*`` 注入缝），或 Rust 嵌入场景下显式
            ``register_execute_handler`` 等注册函数。

            放置类策略（路由/调度/重试）不在 Python 层接线：它们由 Rust
            ``RoutePolicy`` / ``Scheduler`` 或第 3 层外部调度器承担。
        """
        if not callable(handler):
            raise TypeError(f"handler must be callable, got {type(handler)}")
        with self._runtime._lock:
            self._runtime._layers[self._meta.name].append(handler)
            # 检测"start 后追加 Rust-backed handler"的常见误用场景并 warn。
            # 不阻止操作——Python 端 actant.ask/perform/emit 仍会读到该 handler，
            # 但用户期望它影响 Rust 内部分发（如 Worker 任务执行）时会失效。
            if self._runtime._started and self._meta.name in RUST_BACKED_CAPABILITIES:
                _logger.warning(
                    "Layer.chain(%r) called after Runtime.start(); "
                    "this Python handler will NOT be consulted by Rust-internal "
                    "dispatch paths (e.g. Worker task execution). It only takes "
                    "effect for Python-initiated actant.ask/perform/emit calls. "
                    "Placement policies (routing/scheduling/retry) are provided "
                    "by Rust RoutePolicy/Scheduler or a layer-3 scheduler. "
                    "To override Rust-internal behavior, configure Rust-side "
                    "handlers via RuntimeBuilder.",
                    self._meta.name,
                )
        return self

    def remove(self, handler: Callable[[Any], Any]) -> bool:
        """移除一个已注册的 handler。

        按 ``==`` 比较。对于多次注册的同一 handler，仅移除最后一个（栈顶）。

        Returns:
            ``True`` 若找到并移除，``False`` 若未注册。
        """
        with self._runtime._lock:
            handlers = self._runtime._layers.get(self._meta.name, [])
            for i in range(len(handlers) - 1, -1, -1):
                if handlers[i] == handler:
                    del handlers[i]
                    return True
            return False

    def clear(self) -> int:
        """清空该 capability 的所有 handler。

        Returns:
            移除的 handler 数量。
        """
        with self._runtime._lock:
            handlers = self._runtime._layers.get(self._meta.name, [])
            count = len(handlers)
            handlers.clear()
            return count

    def handlers(self) -> list[Callable[[Any], Any]]:
        """返回已注册 handler 列表的**副本**。

        副本避免外部代码直接修改内部列表。
        """
        with self._runtime._lock:
            return list(self._runtime._layers.get(self._meta.name, []))

    def __repr__(self) -> str:
        with self._runtime._lock:
            count = len(self._runtime._layers.get(self._meta.name, []))
        return f"Layer(name={self.name!r}, kind={self.kind!r}, handlers={count})"


class _RuntimeEffectsMixin(_RuntimeBase):
    """按 name 分桶注册 capability handler。"""

    def layer(self, name: str, kind: EffectKind | None = None) -> Layer:
        """创建或获取一个 capability 的 Layer，用于追加 handler。

        Args:
            name: capability 名称（内置或自定义）。
            kind: effect 类型。内置 capability 自动推导；自定义 capability 必须指定。

        Returns:
            `Layer` 对象，可链式调用 `.chain(handler)`。
        """
        with self._lock:
            if name in BUILTIN_CAPABILITIES:
                if kind is not None and kind != BUILTIN_CAPABILITIES[name].kind:
                    raise ValueError(
                        f"capability {name!r} kind is {BUILTIN_CAPABILITIES[name].kind!r}, "
                        f"not {kind!r}"
                    )
                meta = BUILTIN_CAPABILITIES[name]
            elif name in self._metas:
                meta = self._metas[name]
            else:
                # 新自定义 capability
                if kind is None:
                    raise ValueError(f"custom capability {name!r} requires explicit kind")
                meta = CapabilityMeta(name, kind)
                self._metas[name] = meta
                self._layers[name] = []
            return Layer(cast("Runtime", self), meta)

    def chain(self, name: str, handler: Callable[[Any], Any]) -> Runtime:
        """向已存在的 capability 追加单个 handler，返回 self 以支持链式调用。

        与 ``Layer.chain`` 行为一致，区别在于此处按 name 直接操作 Runtime。
        同样存在 ``start()`` 后追加 Rust-backed handler 的限制（详见 ``Layer.chain``
        文档）。
        """
        with self._lock:
            if name not in self._layers:
                raise KeyError(f"capability {name!r} not registered")
            self._layers[name].append(handler)
            if self._started and name in RUST_BACKED_CAPABILITIES:
                _logger.warning(
                    "Runtime.chain(%r) called after Runtime.start(); "
                    "this Python handler will NOT be consulted by Rust-internal "
                    "dispatch paths (e.g. Worker task execution). It only takes "
                    "effect for Python-initiated actant.ask/perform/emit calls. "
                    "Placement policies (routing/scheduling/retry) are provided "
                    "by Rust RoutePolicy/Scheduler or a layer-3 scheduler. "
                    "To override Rust-internal behavior, configure Rust-side "
                    "handlers via RuntimeBuilder.",
                    name,
                )
        return cast("Runtime", self)

    def replace_handler(self, name: str, handler: Callable[[Any], Any]) -> Runtime:
        """替换 capability 的**所有** handler 为单个 handler。

        语义明确：``perform`` 取链末位、``ask`` 逆序决策，多 handler 组合语义复杂。
        此方法清空现有 handler 链后追加单个 handler，适合"完全替换"场景。

        Args:
            name: capability 名称。
            handler: 新的单一 handler。

        Returns:
            self（支持链式调用）。

        Raises:
            KeyError: capability 未注册。

        用法::

            rt.replace_handler("Execute", my_custom_executor)
        """
        with self._lock:
            if name not in self._layers:
                raise KeyError(f"capability {name!r} not registered")
            self._layers[name].clear()
            self._layers[name].append(handler)
        return cast("Runtime", self)

    def handlers(self, name: str) -> list[Callable[[Any], Any]]:
        """返回指定 capability 的 handler 列表副本。

        Args:
            name: capability 名称。

        Returns:
            handler 列表副本（修改不影响 Runtime 内部状态）。

        Raises:
            KeyError: capability 未注册。
        """
        with self._lock:
            if name not in self._layers:
                raise KeyError(f"capability {name!r} not registered")
            return list(self._layers[name])


class _RuntimeTaskMixin(_RuntimeBase):
    """本地任务句柄的注册、取消、终态解析与事件发布。"""

    def _publish_task_event(
        self,
        kind: str,
        task_id: str,
        workflow_id: str,
        event_kwargs: dict[str, Any],
    ) -> None:
        """事件批处理器渲染回调：把本 Runtime 绑定到当前线程后直接 emit。

        由 ``_EventBatcher`` 的后台 flush 线程调用（该线程无线程局部 Runtime）。
        ``_bypass_batcher=True`` 防止 flush 中再次路由回 batcher 造成回环。

        Args:
            kind: ``TaskLifecycle`` 事件类型（started/completed/failed）。
            task_id: 事件源任务 id。
            workflow_id: 事件源工作流 id。
            event_kwargs: 事件附加字段（attempt/error 等）。
        """
        from actant._internal.runtime import use_runtime
        from actant._internal.tasks.helpers import _emit_task_event

        with use_runtime(cast("Runtime", self)):
            _emit_task_event(
                kind,
                task_id,
                workflow_id,
                _bypass_batcher=True,
                **event_kwargs,
            )

    def _emit_batch(self, kind: str, task_id: str, workflow_id: str, **kwargs: Any) -> None:
        """发布一条 ``TaskLifecycle`` 事件。

        无任何 ``TaskLifecycle`` 消费者时自动静默：该 capability 是纯可观测事件，
        唯一消费面为 Python handler，任务执行仍由 worker 结果回调（``_on_task_result``）
        驱动，跳过 emit 不影响执行语义。零消费者由 ``_task_lifecycle_consumers`` 判定。
        已启用批处理（start() 后）时把事件累积到 ``_event_batcher`` 批量派发；
        未启用（未 start / 防御性路径）时退化为带运行时上下文的同步 emit。
        """
        if not self._task_lifecycle_consumers():
            return
        batcher = self._event_batcher
        if batcher is not None:
            batcher.add(kind, task_id, workflow_id, **kwargs)
            return
        from actant._internal.runtime import use_runtime
        from actant._internal.tasks.helpers import _emit_task_event

        with use_runtime(cast("Runtime", self)):
            _emit_task_event(kind, task_id, workflow_id, **kwargs)

    def _task_lifecycle_consumers(self) -> bool:
        """批次口径下 ``TaskLifecycle`` 是否仍有 Python 消费者。

        读取 ``_layers[TASK_LIFECYCLE]`` 的非空性。加锁避免与 ``layer()/chain()``
        追加发生竞态；``TaskLifecycle`` 为纯 emit 能力，无 handler 即无可观测消费方。
        """
        with self._lock:
            return bool(self._layers.get(TASK_LIFECYCLE))

    def _report_task_result_to_orchestrator(
        self,
        handle: Any,
        state: str,
        *,
        result: bytes | None = None,
        error: str | None = None,
    ) -> bool:
        """把编排任务终态上报 orchestrator，返回「orchestrator 是否已排定重试」。

        非编排任务（``workflow_id`` 为空）直接返回 ``False``——任务队列语义下
        orchestrator 不持有该任务。

        上报失败（workflow 已终态/被淘汰、actor 不可用）**不向上抛**：句柄侧仍有
        既有事件路径兜底，编排信息缺失不应让提交方永久悬挂。此时按「无裁决」
        （``False``）处理，调用方照常终结句柄。
        """
        workflow_id = getattr(handle, "workflow_id", "") or ""
        if not workflow_id:
            return False
        try:
            verdict = self._report_task_result(
                workflow_id,
                handle.task_id,
                state,
                result=result,
                error=error,
            )
        except Exception:
            _logger.warning(
                "task %s: failed to report %s to orchestrator (workflow %s); "
                "finalizing handle without orchestration verdict",
                handle.task_id,
                state,
                workflow_id,
                exc_info=True,
            )
            return False
        return bool(verdict.get("retry"))

    def _on_task_result(self, completion: Any) -> None:
        """任务结果回调：由 Rust event_bus 触发，解析对应的 ``AsyncResult``。

        ``completion`` 是 ``_TaskCompletion``，含 task_id / state / result / error。
        ``result`` 是 dispatch handler 返回的字节，编码为
        ``cloudpickle.dumps((success, payload_obj))``——payload_obj 是
        未序列化的 result 或 exc 对象（消除双层 dumps 优化）。

        各终态分派到私有方法解析：payload 对 Rust 不透明，本回调是唯一能区分
        「任务成功」与「任务业务失败」的一侧。终态解析后上报 orchestrator；上报
        返回重试裁决时句柄保持等待（不发终态事件、不移出注册表），重试结果到达
        时经同一回调解析——与 worker 层重试的句柄语义一致。
        """
        task_id = completion.task_id
        with self._lock:
            handle = self._tasks.get(task_id)
        if handle is None:
            return
        state = completion.state
        if state == TASK_STATE_RUNNING:
            # Worker 已开始执行任务，更新为 running（非终态，不从注册表移除）。
            handle._set_running()
            self._emit_batch("started", task_id, handle.workflow_id)
            return
        batch_state: str | None = None
        if state == TASK_STATE_COMPLETED:
            keep = self._on_task_state_completed(handle, task_id, completion.result or b"")
            # 业务失败在 Rust 侧同样以 COMPLETED 到达；解析 payload 后句柄
            # 状态才是权威终态（重试裁决时 keep=True，不结算 batch）。
            if not keep:
                batch_state = handle.state
        elif state == TASK_STATE_FAILED:
            # error 字段是 Rust 端生成的字符串，经 reconstruct_error 重建异常
            # 子类，保留错误类型（如 timeout → ActantTimeoutError）。
            keep = self._on_task_state_failed(handle, task_id, completion.error or "unknown error")
            batch_state = "failed"
        elif state == TASK_STATE_CANCELLED:
            # 取消同为编排终态：上报后 orchestrator 才能收尾该节点。
            self._report_task_result_to_orchestrator(handle, "Cancelled")
            if not handle.workflow_id:
                self._persist_task_failure(task_id, "Cancelled", "cancelled")
            handle._set_cancelled()
            keep = False
            batch_state = "cancelled"
        else:
            return
        if not keep:
            if batch_state is not None:
                self._record_batch_terminal(task_id, batch_state)
            # 终态后从注册表移除（本地孤儿回收）；编排裁决为重试时保持等待。
            self.unregister_task(task_id)

    def _on_task_state_completed(self, handle: Any, task_id: str, raw: bytes) -> bool:
        """解析 COMPLETED 结果帧，返回「编排是否裁决重试（句柄保持等待）」。

        进程池后端任务在 worker 内以 silent=True 抑制 TaskLifecycle 事件；
        started/completed/failed 事件改由父进程侧依据 completion 状态发布，
        保持进程隔离前后可观测性契约一致。
        """
        import cloudpickle as _cp

        try:
            success, payload_obj = _cp.loads(raw)
        except (pickle.UnpicklingError, ValueError, TypeError) as e:
            # 无法解码：当作失败处理（同样上报——否则编排节点会永久停留在
            # Running，工作流无法封口收尾）。emit=False 与原有语义一致。
            msg = f"undecodable result for task {task_id!r}: {e}"
            return self._finalize_failed(handle, task_id, msg, msg, emit=False)
        if not success:
            # 失败：payload_obj 是异常对象，先识别取消。
            from actant.exceptions import TaskCancelledError as _TCE

            if isinstance(payload_obj, _TCE):
                # 取消同为编排终态：上报后 orchestrator 才能收尾该节点。
                self._report_task_result_to_orchestrator(handle, "Cancelled")
                if not handle.workflow_id:
                    self._persist_task_failure(task_id, "Cancelled", "cancelled")
                handle._set_cancelled()
                return False
            error_text = f"{type(payload_obj).__name__}: {payload_obj}"
            return self._finalize_failed(handle, task_id, payload_obj, error_text)
        # 编排任务的完成必须回灌 orchestrator（DAG 状态机由 orchestrator 推进，
        # seal 前全部任务终态才允许工作流终态判定）；非编排任务（workflow_id 空）
        # 为 no-op。
        self._report_task_result_to_orchestrator(handle, "Completed", result=raw)
        # 结果侧降级：结果帧超阈值时原样字节落 blob（0 次重序列化），句柄内部
        # 持 Ref。落 blob 失败降级为内联对象并 warning——任务已成功完成，值引用
        # 基建故障不翻转任务语义。
        from actant._internal.tasks.ref import REF_INLINE_THRESHOLD, _value_store

        ref_bytes: bytes | None = None
        if len(raw) > REF_INLINE_THRESHOLD:
            try:
                ref_bytes = _value_store(raw, runtime=self)
            except Exception:
                # 降级原因经 exc_info 日志承载，不静默。
                _logger.warning(
                    "task %s: result (%d bytes) failed to store as blob ref; "
                    "falling back to inline result",
                    task_id,
                    len(raw),
                    exc_info=True,
                )
        if not handle.workflow_id:
            # 独立任务才落终态记录；编排节点结果已在 orchestrator exec 表，
            # 重复写 Store 只会放大体积。先写记录再唤醒等待者，使
            # ``result()`` 返回后 ``get_task_result`` 立即可读。
            self._persist_task_result(task_id, raw, ref_bytes)
        if ref_bytes is not None:
            handle._set_result_ref(ref_bytes)
        else:
            # 内联对象路径：直接存对象避免 dumps/loads 往返；任务返回 bytes
            # （如 echo(b"x")）也按对象存储，不会被误当作序列化结果。
            handle._set_result(payload_obj)
        self._emit_batch("completed", task_id, handle.workflow_id)
        return False

    def _on_task_state_failed(self, handle: Any, task_id: str, error_text: str) -> bool:
        """处理 Rust 侧 FAILED（超时/崩溃耗尽/panic）终态。

        error 是 Rust 端字符串，经 ``reconstruct_error`` 解析 kind 前缀
        （``[actant:KIND] message``）重建对应 Python 异常子类。
        """
        return self._finalize_failed(handle, task_id, reconstruct_error(error_text), error_text)

    def _finalize_failed(
        self,
        handle: Any,
        task_id: str,
        error_payload: Any,
        error_text: str,
        emit: bool = True,
    ) -> bool:
        """失败终态统一出口：先经 orchestrator 裁决，返回「是否保持句柄等待」。

        裁决为重试时句柄保持 pending、不发失败事件、不移出注册表——重试结果
        到达时经同一回调解析，与 worker 层重试语义一致（重试耗尽前提交方见不到
        失败）。
        """
        if self._report_task_result_to_orchestrator(handle, "Failed", error=error_text):
            _logger.info(
                "task %s: orchestrator scheduled retry; handle stays pending",
                task_id,
            )
            return True
        # _set_error 接受 BaseException（含 reconstruct 后的异常对象），内部
        # dumps 存入 _error_payload。先落记录再唤醒等待者（同成功路径）。
        if not handle.workflow_id:
            # 失败文本按跨语言编码原样保存；异常对象可序列化时一并保存，
            # 使 get_task_result 重抛与同进程 result() 相同的异常类型。
            self._persist_task_failure(task_id, "Failed", error_text, exc=error_payload)
        handle._set_error(error_payload)
        if emit:
            self._emit_batch("failed", task_id, handle.workflow_id, error=error_text)
        return False

    def register_task(self, task_id: str, handle: AsyncResult) -> None:
        """注册一个已提交任务的句柄（由 ``Task.submit`` 调用）。"""
        with self._lock:
            self._tasks[task_id] = handle

    def _report_task_result(
        self,
        workflow_id: str,
        task_id: str,
        state: str,
        *,
        result: bytes | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        """上报本地 flow 任务的终态结果给 Rust Orchestrator。

        worker 结果帧正文对 Rust 不透明（业务失败被 worker 编码进正文，dispatcher
        对成功与失败都返回 ``Ok(Ok(body))``），成功/失败只能由本层解析。解析后经
        此上报，由 orchestrator 统一推进 DAG 状态机并做重试裁决。

        Args:
            workflow_id: 工作流标识。空串表示非编排任务（独立 ``@task``），
                Rust 侧直接返回无裁决。
            task_id: 节点标识。
            state: ``"Completed"`` / ``"Failed"`` / ``"Cancelled"``。
            result: ``Completed`` 时的结果字节。
            error: ``Failed`` 时的错误信息。

        Returns:
            ``{"retry": bool, "delay_ms": int}``：``retry`` 为真表示 orchestrator
            已排定重试（Rust 已按 ``delay_ms`` 延迟把重试任务入队调度器），提交方
            句柄应继续等待。

        Raises:
            InvalidStateError: Runtime 未启动。
            ValueError: ``state`` 不是已知终态。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        return cast(
            "dict[str, Any]",
            core.report_task_result(workflow_id, task_id, state, result, error),
        )

    def unregister_task(self, task_id: str) -> bool:
        """从本地任务表中移除任务句柄（任务完成后孤儿回收）。

        同步清理 ``_cancelled_tasks`` 中的条目，防止集合无界增长。
        所有任务终态路径（``_on_task_result``）都会调用此方法，
        确保 ``_cancelled_tasks`` 不会累积已完成任务的 ID。
        """
        self._clear_task_cancelled(task_id)
        with self._lock:
            if task_id in self._tasks:
                del self._tasks[task_id]
                return True
            return False

    def _mark_task_cancelled(self, task_id: str) -> None:
        """标记任务为预取消/取消中（线程安全）。"""
        with self._lock:
            self._cancelled_tasks.add(task_id)

    def _clear_task_cancelled(self, task_id: str) -> None:
        """清除任务的预取消标记（线程安全）。"""
        with self._lock:
            self._cancelled_tasks.discard(task_id)

    def is_cancelled(self, task_id: str) -> bool:
        """查询任务是否处于**取消在途**状态（线程安全）。

        这是"取消已请求、尚未被消费"的标记，**不是任务的终态视图**：任务一旦到达
        终态（含已取消）并被注册表回收（:meth:`unregister_task`），标记即清除。
        因此对**已经取消完成**的任务本方法返回 ``False``。

        三项查询的分工：能否取消看 :meth:`cancel_task` 的返回值；最终结果看
        ``handle.result()``；本方法只用于判断在途取消是否尚未被消费。

        替代直接访问 ``runtime._cancelled_tasks``（无锁读取违反锁定纪律）。
        """
        with self._lock:
            return task_id in self._cancelled_tasks

    def list_tasks(self) -> list[str]:
        """返回所有已提交（本地）任务的 task_id 列表。"""
        with self._lock:
            return list(self._tasks.keys())

    def get_task(self, task_id: str) -> AsyncResult | None:
        """按 task_id 查询任务句柄（``AsyncResult``），不存在返回 ``None``。"""
        with self._lock:
            return self._tasks.get(task_id)

    def cancel_task(self, task_id: str) -> bool:
        """尝试取消指定任务。

        Returns:
            ``True`` 表示任务存在且取消请求已提交；``False`` 表示任务不存在或已完成。
        """
        with self._lock:
            handle = self._tasks.get(task_id)
        if handle is None:
            return False
        state = handle.state
        if state in ("completed", "failed"):
            return False
        if state == "cancelled":
            return True
        # 标记为预取消：dispatch handler 启动时会检查此集合，
        # 若任务尚未被 Worker 拉取，handler 立即返回 TaskCancelledError。
        self._mark_task_cancelled(task_id)
        # 通知 Rust Worker 设置 cancel_flag，使正在运行的任务在下次检查点退出。
        if self._rust_core is not None:
            try:
                self._rust_core.cancel_task(task_id)
            except Exception:
                # Rust 侧取消失败不阻断 Python 侧取消标记的设置（预取消标记
                # 仍能阻止尚未被 Worker 拉取的任务执行），但必须记录告警便于排查。
                _logger.warning(
                    "cancel_task(%s): rust core cancel_task failed",
                    task_id,
                    exc_info=True,
                )
        cancelled = bool(handle.cancel())
        if not cancelled:
            self._clear_task_cancelled(task_id)
            return False
        # 若任务仍在 pending（排队中，尚未被 Worker 拉取），
        # 仅更新 Python 句柄状态；保留 _cancelled_tasks 标记直到 worker
        # 真正消费到该任务并回传终态事件，以免提前清空预取消信号。
        if cancelled and handle.state == "pending":
            handle._set_cancelled()
        return cancelled

    def batch_progress(self, batch_id: str) -> BatchProgress | None:
        """查询 batch/array job 进度；未注册返回 ``None``。

        进度由核心 `BatchTracker` 计数，但终态由 Python 结果泵在解析 payload
        后经 ``_record_batch_terminal`` 上报（Rust EventBus 无法区分协议成功与
        业务失败）：Completed / Failed / Cancelled 各计一档，
        `pending` 为未结算项。
        """
        from actant.tasks import BatchProgress

        if self._rust_core is None:
            return None
        data = self._rust_core.batch_progress(batch_id)
        return BatchProgress.from_dict(data) if data is not None else None

    def remove_batch(self, batch_id: str) -> bool:
        """移除 batch 注册与成员索引；返回该批次此前是否存在。"""
        if self._rust_core is None:
            return False
        return bool(self._rust_core.remove_batch(batch_id))

    def _record_batch_terminal(self, task_id: str, state: str) -> None:
        """把解析后的任务终态计入 batch 进度（非 batch 成员时为空操作）。

        由 ``_on_task_result`` 在唯一能区分业务成功/失败的 Python 侧调用；
        观测失败不翻转任务语义，记录告警后继续。
        """
        core = self._rust_core
        if core is None:
            return
        try:
            core.record_batch_terminal(task_id, state)
        except (RuntimeError, ValueError, TypeError) as e:
            _logger.warning(
                "task %s: failed to record batch terminal state %r: %s",
                task_id,
                state,
                e,
                exc_info=True,
            )


class _RuntimeWorkflowMixin(_RuntimeBase):
    """Orchestrator 工作流的提交、查询与上下文值读写。"""

    def start_flow(
        self,
        name: str,
        *args: Any,
        version: int | None = None,
        timeout_ms: int | None = None,
        failure_strategy: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """提交一个 ``@flow`` 工作流，**不在调用方执行函数体**。

        只落盘工作流外壳与 invocation（flow 名、版本、入参）；函数体由
        :meth:`host_flows` 在任意进程重放。定义未登记时可显式传 ``version``
        （及可选的 ``timeout_ms`` / ``failure_strategy``）；定义已登记时显式
        参数须与登记值一致，否则按定义漂移报错。

        Args:
            name: flow 定义名。
            *args: 传给 flow 函数体的位置参数，随 invocation 持久化。
            version: flow 定义版本；无登记定义时必填。
            timeout_ms: 工作流级 deadline 毫秒。
            failure_strategy: ``"fail_fast"`` / ``"continue"``。
            **kwargs: 传给 flow 函数体的关键字参数。

        Returns:
            :class:`FlowHandle`；跨进程可仅凭 ``workflow_id`` 重建读取。

        Raises:
            InvalidStateError: Runtime 尚未 ``start()``。
            NotFoundError: 当前进程未登记该 flow 名，且未提供 ``version``。
            ValueError: 显式参数与已登记定义不一致。
        """
        if not self._started:
            raise InvalidStateError("Runtime must be started before start_flow()")
        # 延迟导入：`actant._internal.flow` 依赖本模块的 Runtime，模块级导入会成环。
        from actant._internal.flow import start_flow as _start_flow

        return _start_flow(
            name,
            *args,
            runtime=self,
            version=version,
            timeout_ms=timeout_ms,
            failure_strategy=failure_strategy,
            **kwargs,
        )

    def host_flows(
        self, *, once: bool = True, timeout_ms: int = 0
    ) -> list[str]:
        """驱动本 ``data_dir`` 中尚未完成、且带 invocation 的 flow。

        定义来自进程内 ``FLOW_REGISTRY``（由 ``@flow`` 导入登记），flow 名与
        入参来自 journal 的 ``Submitted.invocation``——因此提交进程与执行进程
        可以不同。缺失定义/参数的单个工作流会 warning 并跳过，不阻塞其余。

        Args:
            once: ``True`` 扫描一轮后返回；``False`` 持续轮询（host 服务形态），
                仅在被中断时退出。
            timeout_ms: 重放体的工作流级 deadline（0 表示不设）。

        Returns:
            实际发起重放的 ``workflow_id`` 列表。

        Raises:
            InvalidStateError: Runtime 尚未 ``start()``，或同进程已有 host loop。
        """
        if not self._started:
            raise InvalidStateError("Runtime must be started before host_flows()")
        # 延迟导入：`actant._internal.flow` 依赖本模块的 Runtime，模块级导入会成环。
        from actant._internal.flow import host_flows as _host_flows

        return _host_flows(runtime=self, once=once, timeout_ms=timeout_ms)

    def _spawn_auto_resume(self) -> None:
        """在后台线程执行单轮 ``host_flows``（`start(auto_resume=True)`）。

        重放可能要等到工作流终态（长生命周期 flow），不能阻塞启动路径；
        失败只记录日志——单个工作流异常不应让节点起不来。
        """

        def _run() -> None:
            try:
                resumed = self.host_flows(once=True)
                _logger.info("auto_resume: resumed %d workflow(s)", len(resumed))
            except Exception:
                _logger.exception("auto_resume: failed to resume workflows")

        thread = threading.Thread(
            target=_run, name="actant-auto-resume", daemon=True
        )
        self._auto_resume_thread = thread
        thread.start()

    def _continued_to(self, workflow_id: str) -> bytes | None:
        """返回该代 ``continue_as_new`` 的后继 workflow id；无后继返回 ``None``。"""
        return self._get_workflow_context_value(workflow_id, _CONTINUED_TO_CONTEXT_KEY)

    def submit_workflow(
        self,
        workflow_id: str,
        *,
        failure_strategy: str | None = None,
        timeout_ms: int = 0,
        name: str | None = None,
        version: int = 0,
        args: bytes | None = None,
    ) -> None:
        """创建空持久化工作流外壳（flow 提交路径第一步）。

        flow 函数体首次 ``task.submit()`` 前调用：以空 DAG 持久化工作流，
        后续节点经 :meth:`_add_workflow_node` 增量加入。
        ``timeout_ms > 0`` 时同步设置工作流级 deadline。

        ``name`` / ``version`` / ``args`` 等写入 journal 的 ``Submitted`` 事件，
        使恢复后的任意节点都能重建调用描述；非 flow 提交路径可全部省略。

        Args:
            workflow_id: 工作流唯一标识（``@flow`` 生成）。
            failure_strategy: 失败策略（``"fail_fast"`` / ``"continue"``），
                ``None`` 由 Rust 侧应用默认 FailFast。
            timeout_ms: 工作流级 deadline 毫秒（0 表示不限制）。
            name: flow 定义名；``None`` 表示不记录调用描述。
            version: flow 定义版本；0 表示未声明。
            args: 调用参数的序列化字节（上层序列化，Rust 不解析）。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        core.submit_workflow(
            workflow_id,
            failure_strategy,
            timeout_ms,
            name,
            version,
            args,
        )

    def _add_workflow_node(
        self,
        workflow_id: str,
        node: Any,
        deps: list[str],
    ) -> dict[str, Any]:
        """增量加入单个 DAG 节点（flow 提交路径核心，先持久化再派发）。

        由 ``Task.submit`` 在 flow 上下文中调用。节点在 Orchestrator 内登记、
        建边、同步落盘；重放命中历史时返回已有节点状态（不重新提交），
        提交序列指纹不一致时抛 :class:`FlowReplayError`。

        Args:
            workflow_id: 工作流唯一标识。
            node: ``_DagNode`` 节点定义。
            deps: 参数树引用的上游 ``task_id`` 列表（外部依赖会被 Rust 侧过滤）。

        Returns:
            ``{"created": bool, "state": str | None, "result": bytes | None}``。
            ``created=False`` 表示重放命中：``state``/``result`` 为历史中该
            节点的当前状态与已完成结果字节。

        Raises:
            InvalidStateError: Runtime 未启动。
            FlowReplayError: 提交序列指纹不一致。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        return cast("dict[str, Any]", core.add_workflow_node(workflow_id, node, deps))

    def cancel_workflow(self, workflow_id: str) -> None:
        """取消整个工作流（flow 失败兜底）。

        工作流及运行中任务置为 Cancelled 终态；已终态时幂等 no-op。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        core.cancel_workflow(workflow_id)

    def _seal_workflow(self, workflow_id: str) -> None:
        """封口工作流节点集（flow 函数体返回信号）。

        封口后 orchestrator 才允许工作流终态判定（增量提交期间全部已知
        节点终态不代表提交序列结束）；全部任务已终态时立即执行终态收尾。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        core.seal_workflow(workflow_id)

    def register_wait_point(
        self,
        workflow_id: str,
        wait_key: str,
        *,
        kind: str,
        name: str | None = None,
        deadline_ms: int = 0,
    ) -> None:
        """注册持久化等待点。

        等待点是 orchestrator 的挂起原语：``(workflow_id, wait_key, 条件)``
        随工作流快照落盘，条件满足（signal 递交 / timer 到期 / 挂起恢复）时
        追加唤醒事件进入同一历史。**幂等**：同 ``wait_key`` 重复注册为 no-op，
        这是重放体天然幂等的前提。

        Args:
            workflow_id: 工作流标识。
            wait_key: 等待点注册表键（同一工作流内唯一）。
            kind: ``"signal"``（外部信号）、``"timer"``（定时到期）或
                ``"suspend"``（等待 :meth:`resume_suspended` 恢复）。
            name: ``kind="signal"`` 的信号语义名；``None`` 时退化为 ``wait_key``。
            deadline_ms: ``kind="timer"`` 的**绝对** epoch 毫秒到期时刻，须 > 0。

        Raises:
            InvalidStateError: Runtime 未启动。
            ValueError: ``kind`` 非法或 timer 缺 ``deadline_ms``。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        core.register_wait_point(workflow_id, wait_key, kind, name, deadline_ms)

    def signal_wait_point(
        self,
        workflow_id: str,
        wait_key: str,
        payload: bytes = b"",
    ) -> bytes | None:
        """递交信号唤醒等待点（Signals capability 出口）。

        Args:
            workflow_id: 目标工作流。
            wait_key: 等待点键（flow 内 ``wait_signal(name)`` 对应 ``name``）。
            payload: 随信号携带的任意字节（默认空）。原样进入工作流历史，
                ``wait_signal`` 返回同一份字节；业务序列化由调用方负责。

        Returns:
            ``bytes`` 表示本次递交**唤醒了一个等待点**（返回其记录的 payload；
            首次递交胜出）。若该等待点此前已唤醒（重放体"已收到 → 直接返回"），
            返回的是历史 payload，重复 signal 幂等。
            ``None`` 表示此刻**没有等待点可被唤醒**，信号已入缓冲——将来注册
            同一 ``wait_key`` 的等待点会**立即生成为已唤醒态**。

        Note:
            **信号缓冲**：等待点注册前抵达的信号不会被丢弃——先入缓冲，待同一
            ``wait_key`` 的等待点注册时**立即生成为已唤醒态**。缓冲是**闩锁**：同一
            ``wait_key`` 已有缓冲时重复递交不再追加历史、不覆盖，直接返回
            ``None``，故"按返回值重试"变安全（重试是 no-op）。

            缓冲与等待点同批落盘，故**跨重启存活**（``recover`` = 快照 + 其后
            事件重放）。唯一会丢的窗口是"缓冲写入后、尚未落盘就崩溃"。

            无法区分"``wait_key`` 拼错"与"等待点尚未注册"——递交时无从得知调用
            方将来会注册什么键。这是信息论限制，不是实现缺失。

        Raises:
            InvalidStateError: Runtime 未启动。
            NotFoundError: 工作流不存在（递交方 id 写错）。
            ValueError: ``wait_key`` 为空。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        return cast(
            "bytes | None",
            core.signal_wait_point(workflow_id, wait_key, payload),
        )

    def resume_suspended(self, workflow_id: str) -> int:
        """恢复挂起：唤醒该工作流所有等待中的 ``suspend`` 等待点。

        与 :meth:`signal_wait_point` 的区别是**语义来源**：signal 等待一个具名
        业务事件，suspend 等待操作员的恢复指令。本方法**只唤醒 ``suspend``
        条件**，不触碰 ``signal`` / ``timer`` 等待点——恢复指令不得冒名顶替一个
        业务信号。

        按 ``workflow_id`` 而非按键唤醒，故调用方无需知道 flow 内部给挂起点分配
        了什么键，且"挂起 → 恢复 → 再挂起 → 再恢复"天然成立。

        Args:
            workflow_id: 工作流标识。

        Returns:
            本次唤醒的挂起点数量；``0`` 表示该工作流当前没有处于挂起中的挂起点
            （幂等——重复调用第二次返回 0）。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        return int(core.resume_suspended(workflow_id))

    def resolve_workflow_context_value(
        self,
        workflow_id: str,
        key: str,
        kind: str,
        candidate: bytes,
    ) -> bytes:
        """解析（首次记录 / 重放命中）确定性上下文值。

        flow 体的 wall-clock / 随机数 / UUID 必须跨重放稳定：首次执行时由
        Python 生成候选值，本方法把它写入工作流历史并返回；重放时命中历史，
        返回**记录值**（候选值被丢弃）。

        Args:
            workflow_id: 工作流标识。
            key: 上下文值键（由 flow 体的调用序号决定，如 ``ctx-1``）。
            kind: 值种类（``"now_ms"`` / ``"uuid"`` / ``"random"``）。
            candidate: 首次执行时的候选值（重放时被忽略）。

        Returns:
            记录值：首次执行等于 ``candidate``，重放时等于历史值。

        Raises:
            InvalidStateError: Runtime 未启动。
            FlowReplayError: 同 ``key`` 的历史记录是另一种 ``kind``。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        return bytes(
            core.resolve_context_value(workflow_id, key, kind, candidate)
        )

    def _get_workflow_context_value(
        self,
        workflow_id: str,
        key: str,
    ) -> bytes | None:
        """只读查询工作流已记录的确定性上下文值（未记录返回 ``None``）。

        与 :meth:`resolve_workflow_context_value` 的差异是**不写历史**：
        需要"读一个可能不存在的记录"时用它，避免为缺失键制造一条空记录。
        ``continue_as_new`` 用它读取续跑携带参数。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        return cast("bytes | None", core.get_context_value(workflow_id, key))

    def wait_wait_point(
        self,
        workflow_id: str,
        wait_key: str,
        *,
        timeout_ms: int = 0,
    ) -> bytes | None:
        """阻塞等待等待点条件满足（flow 体 park 原语）。

        **调用方须先** :meth:`register_wait_point`：注册是把等待点写入历史的
        动作，本方法只负责 park。两步之间的竞态由 Rust 侧"先注册句柄、再检查
        是否已 Signaled"关闭——信号早到不会丢失。

        与其它工作流方法不同，本方法**不经 actor 消息循环**（在 actor 内阻塞
        会让全部工作流停摆），而是在 actor 之外阻塞，阻塞期间释放 GIL。

        Args:
            workflow_id: 工作流标识。
            wait_key: 等待点注册表键。
            timeout_ms: 等待上界；``0`` 表示无限等待。

        Returns:
            条件满足时的 payload；``None`` 表示超时，或**等待者被释放**（工作流
            进入终态时 orchestrator 会释放该工作流的全部 park 等待者）。
            这两种含义须由调用方区分——查 :meth:`get_workflow_state` 看工作流是否
            已终态（``actant._internal.flow`` 的 ``_park`` 就是这么做的），否则会把 abort
            误当成"信号返回了空 payload"，让函数体在已死的工作流上继续跑。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        return cast(
            "bytes | None", core.wait_wait_point(workflow_id, wait_key, timeout_ms)
        )

    def wait_wait_point_async(
        self,
        workflow_id: str,
        wait_key: str,
        *,
        timeout_ms: int = 0,
    ) -> Awaitable[bytes | None]:
        """非阻塞等待等待点条件满足（:meth:`wait_wait_point` 的 asyncio 版本）。

        返回的 awaitable 在等待点被 signal / timer 唤醒时完成，等待期间不占用
        线程、不阻塞 event loop；调用方仍须先 :meth:`register_wait_point`。
        语义与阻塞版本一致：``None`` 表示超时或等待者被释放，调用方须自行
        区分（见 :meth:`wait_wait_point`）。

        Args:
            workflow_id: 工作流标识。
            wait_key: 等待点注册表键。
            timeout_ms: 等待上界；``0`` 表示无限等待。

        Returns:
            解析为 payload ``bytes`` 或 ``None`` 的 awaitable。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        return cast(
            "Awaitable[bytes | None]",
            core.wait_wait_point_async(workflow_id, wait_key, timeout_ms),
        )

    def get_workflow_state(self, workflow_id: str) -> dict[str, Any] | None:
        """查询 Orchestrator 中 workflow 的持久化状态。

        Returns:
            状态字典（``workflow_id`` / ``state`` / ``tasks`` / 计数等），
            不存在时返回 ``None``。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        return cast("dict[str, Any] | None", core.get_workflow_state(workflow_id))

    def get_task_result(self, task_id: str) -> Any:
        """读取独立任务被持久化的终态结果（按 ``task_id`` 跨重启查询）。

        独立任务（不经 workflow 提交）的终态记录写入 ``data_dir`` 的 Store：
        成功任务保存结果帧（大结果只存 ``Ref``），失败/取消保存错误文本。
        **大结果**经 ``ValueStore`` 拉取，与 ``AsyncResult.result()`` 语义一致。

        Args:
            task_id: 任务标识（``AsyncResult.task_id``）。

        Returns:
            任务返回值。

        Raises:
            InvalidStateError: Runtime 未启动。
            NotFoundError: 没有该 ``task_id`` 的记录——未知任务，或它由
                workflow 提交（节点结果请查 :meth:`get_workflow_state`）。
            TaskCancelledError: 任务以取消终态结束。
            ActantError: 任务以失败终态结束；重抛记录的异常（与
                ``AsyncResult.result()`` 同类型），不可反序列化时按文本重建。
        """
        # 不把 core 绑定到局部变量：本方法可能抛异常，异常 traceback 会持有
        # 当前帧的局部引用；局部 core 会延长 LMDB env 的生命周期直到 GC
        # （同进程立即重开同一 data_dir 会撞上 "environment already open"）。
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        store = self._terminal
        if store is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        return store.task_result(task_id)

    def forget(
        self,
        kind: Literal["task", "workflow_result", "idempotency_key"],
        key: str,
    ) -> None:
        """删除指定类型的持久化登记（幂等）。

        内部记录**不会自动过期**：它们随 ``data_dir`` 长期保留，由部署方按
        自身保留策略显式清理。已执行的副作用不会撤销。

        Args:
            kind: 记录类型。

                - ``"task"``：独立任务的终态记录（``task_id``）。删除后
                  :meth:`get_task_result` 抛 ``NotFoundError``。
                - ``"workflow_result"``：workflow 返回值记录（``workflow_id``）。
                  只删除传入 id 这一代；``continue_as_new`` 链上的其它代需逐个删除。
                - ``"idempotency_key"``：幂等键登记（内存与持久层）。删除后同键
                  提交会重新入队一次；正在执行的首次提交不受影响。
            key: 对应记录的主键。

        Raises:
            InvalidStateError: Runtime 未启动（``"task"`` / ``"workflow_result"``）。
            ValueError: 未知的 ``kind``。
        """
        if kind == "idempotency_key":
            self._idempotency.forget(key)
            return
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        store = self._terminal
        if store is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        if kind == "task":
            store.forget_task(key)
        elif kind == "workflow_result":
            store.forget_workflow_result(key)
        else:
            raise ValueError(
                f"unknown forget kind {kind!r}: expected 'task', "
                "'workflow_result' or 'idempotency_key'"
            )

    def get_workflow_result(self, workflow_id: str) -> Any:
        """读取 workflow 函数体被持久化的返回值。

        返回值在 flow 正常结束时写入 ``data_dir`` 的 Store（大结果只存
        ``Ref``）。失败/取消的 workflow 没有返回值，本方法抛 ``NotFoundError``；
        要判断失败原因请用 :meth:`get_workflow_state`。

        用 ``continue_as_new`` 续跑过的逻辑工作流，返回值只记录在最终一代上；
        本方法沿 ``@continued_to`` 链找到最终代，故用**任意一代**的 id 查询
        结果一致。链上尚未产生返回值时抛 ``NotFoundError``。

        Args:
            workflow_id: 工作流标识（任意一代）。

        Returns:
            函数体返回值（可能是 ``None``——它同样是"已记录"的终态值）。

        Raises:
            InvalidStateError: Runtime 未启动。
            NotFoundError: 没有该 workflow 的返回值记录（未完成、失败/取消，
                或从未在此 ``data_dir`` 运行）。
        """
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        store = self._terminal
        if store is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        return store.workflow_result(workflow_id, self._continued_to)

    def get_dag(self, workflow_id: str) -> dict[str, Any] | None:
        """查询工作流的 DAG **结构**（Python 暴露面）。

        与 :meth:`get_workflow_state` 的分工：后者是**执行**状态（每个任务的
        ``state`` / ``result`` / ``error`` / ``retry_count`` / ``attempt``），
        本方法是**结构**（节点、依赖边、重试策略、超时、优先级、元数据）。

        Returns:
            ``None``（工作流不存在）或 dict：

            - ``workflow_id`` / ``failure_strategy``
            - ``default_retry_policy``：DAG 级默认重试策略。节点自身 ``retry_policy`` 为 ``None`` 时生效的是它（对应 Rust 侧 ``Dag::effective_retry_policy``），故一并暴露。
            - ``nodes``：每项 ``{task_id, name, deps, timeout_ms, priority, metadata, retry_policy}``，``deps`` 为前驱 ``task_id`` 列表。
            - ``edges``：每项 ``{from, to, condition}``。

            **不含任务 payload**：它是签名的 cloudpickle 字节，对 Rust 不透明
            且可能很大。需要任务结果请查 :meth:`get_workflow_state` 的
            ``tasks[*]["result"]``。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        return cast("dict[str, Any] | None", core.get_dag(workflow_id))

    def get_workflow_history(
        self,
        workflow_id: str,
        *,
        after: tuple[int, int] | None = None,
    ) -> list[dict[str, Any]]:
        """读取工作流的事件历史（审计出口）。

        历史是**事实源**：``recover`` = 快照 + 其后事件重放，等待点与信号也以
        事件形式进入同一历史。本方法是它的对外读取出口。

        Args:
            workflow_id: 工作流标识。
            after: 游标 ``(sequence, timestamp_ms)``（取上一项的对应字段），
                ``None`` 表示从头读取。

        Returns:
            每项 ``{sequence, timestamp_ms, kind, task_id, error, payload}``：
            ``kind``（如 ``"TaskCompleted"``）/ ``task_id`` / ``error`` 供筛选；
            ``payload`` 是原始编码字节，**不透明**——Python 不解释其布局。
            无 event_log 或工作流不存在时返回空列表（历史是可选观测面，
            缺失不构成错误）。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        return cast(
            "list[dict[str, Any]]",
            core.get_workflow_history(workflow_id, after=after),
        )

    def get_workflow_journal(self, workflow_id: str) -> list[dict[str, Any]]:
        """读取工作流的稳定 journal（跨语言消费面）。

        与 :meth:`get_workflow_history` 读同一份事实源，但导出**稳定、自描述**
        的字段：``{seq, timestamp_ms, kind, workflow_id, task_id, wait_key,
        error, payload, condition, invocation}``。

        - ``kind`` 为小写稳定事件名（``"submitted"`` / ``"task_completed"`` …）；
        - ``payload`` 是该事件的不透明业务字节（节点入参、任务结果、信号载荷、
          上下文值），可 base64 后直接导出为 JSON；
        - ``condition`` 是等待点条件的字符串标签；``invocation`` 是提交描述
          dict（无则为 ``None``）。

        Args:
            workflow_id: 工作流标识。

        Returns:
            按 ``seq`` 升序的 journal 记录列表；工作流不存在时为空。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        return cast("list[dict[str, Any]]", core.get_workflow_journal(workflow_id))

    def get_workflow_invocation(self, workflow_id: str) -> dict[str, Any] | None:
        """读取工作流的提交描述（``Submitted`` 事件中的 invocation）。

        Returns:
            ``{workflow_id, name, version, args}``；
            旧记录或非 flow 提交路径返回 ``None``。``args`` 是上层序列化字节，
            Python 侧可用 ``cloudpickle.loads`` 还原。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        return cast("dict[str, Any] | None", core.get_workflow_invocation(workflow_id))

    def list_workflows(self, *, include_archived: bool = False) -> list[str]:
        """列出 workflow id。

        Args:
            include_archived: ``False``（默认）仅返回内存中活跃的非终态工作流；
                ``True`` 额外遍历 Store（``STORE_KEY_DAG`` 前缀扫描），含已完成
                并淘汰的历史工作流，适合面板展示。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        return list(
            core.list_all_workflows() if include_archived else core.list_workflows()
        )

    def delete_workflow(self, workflow_id: str) -> None:
        """删除工作流（运维清理）。

        从内存与持久化存储中移除该工作流的全部状态。与
        ``completed_retention_count`` 的自动淘汰互补，供手动清理已完成/
        孤儿工作流。幂等：工作流不存在时为 no-op。

        Warning:
            运行中的工作流删除后其任务结果将不可恢复。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        core.delete_workflow(workflow_id)


class _RuntimeNetworkMixin(_RuntimeBase):
    """节点身份、peer 发现、指标与并发上限。"""

    def _write_node_meta(self) -> None:
        """将节点元数据写入 ``data_dir/node_meta.json``，供 CLI 离线查询。"""
        if not self._data_dir:
            return
        import json
        import os
        from datetime import datetime, timezone

        try:
            os.makedirs(self._data_dir, exist_ok=True)
            meta = {
                "node_id": self.node_id,
                "name": self._name,
                "last_started": datetime.now(timezone.utc).isoformat(),
                "version": __import__("actant").__version__,
            }
            meta_path = os.path.join(self._data_dir, "node_meta.json")
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2)
        except OSError:
            _logger.debug("failed to write node_meta.json", exc_info=True)

    @property
    def node_id(self) -> str:
        """本节点的 Actant 内部 ID（可能是用户提供的 ``name``）。"""
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started")
        return str(self._rust_core.node_id())

    @property
    def role(self) -> str:
        """本节点拓扑角色：``"both"``（默认）/ ``"orchestrator"`` / ``"worker"``。

        配置决定，启动前后均可读。``"worker"`` 节点不驱动 flow host。
        """
        config = self._config
        return str(config.role) if config is not None else "both"

    @property
    def peer_id(self) -> str:
        """本节点的 iroh P2P peer ID（公钥 hex），用于 ``add_gossip_peer``。"""
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started")
        return str(self._rust_core.peer_id())

    def listen_addresses(self) -> dict[str, Any]:
        """返回本节点的监听地址信息。

        Returns:
            包含以下字段的 dict：

            - ``endpoint_id``: iroh endpoint ID（节点公钥）
            - ``relay_url``: relay 服务器 URL（若启用），无则为 ``None``
            - ``direct_addrs``: 直连 IP 地址列表
            - ``endpoint_addr``: 完整 NodeAddr 编码（hex postcard），传给对端 ``dial()``
        """
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started")
        addrs = self._rust_core.listen_addresses()
        return {
            "endpoint_id": addrs.endpoint_id,
            "relay_url": addrs.relay_url,
            "direct_addrs": list(addrs.direct_addrs),
            "endpoint_addr": addrs.endpoint_addr,
        }

    def dial(self, addr: str) -> None:
        """拨号远端节点建立 P2P 直连，并自动加入其 gossip 网络。

        Args:
            addr: 对端 ``listen_addresses()["endpoint_addr"]`` 返回的 hex 字符串。
        """
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started")
        self._rust_core.dial(addr)

    def add_gossip_peer(self, peer_id: str) -> None:
        """将远端节点加入 gossip 网络（不建立直连）。

        Args:
            peer_id: 对端 ``peer_id`` 返回的 iroh 公钥 hex 字符串
                （注意不是 ``node_id``）。
        """
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started")
        self._rust_core.add_gossip_peer(peer_id)

    def discover_peers(self) -> list[str]:
        """返回当前已知对等节点的 peer_id 列表。"""
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started")
        return list(self._rust_core.discover_peers())

    @property
    def capabilities(self) -> list[str]:
        """返回所有已注册 capability 的名称。"""
        with self._lock:
            return list(self._metas.keys())

    def capability_meta(self, name: str) -> CapabilityMeta:
        """返回指定 capability 的元数据（含自定义 capability）。"""
        with self._lock:
            if name not in self._metas:
                raise KeyError(f"capability {name!r} not registered")
            return self._metas[name]

    def on_task_log(
        self,
        callback: Callable[[dict[str, Any]], None],
        *,
        task_id: str | None = None,
    ) -> None:
        """订阅任务日志流。

        任务在 worker 子进程执行期间，``logging``（WARNING 及以上）与
        ``print`` 输出经 stderr 边带回传，每个事件调用
        ``callback({task_id, level, message})``。

        指定 ``task_id`` 时只接收该任务的日志（逐任务通道），省略则接收
        所有任务日志。通道为 tap 语义（best-effort 可丢），不承载正确性语义。

        Args:
            callback: 收到 ``dict(task_id=..., level=..., message=...)`` 时调用，
                在后台线程执行（勿在其中阻塞）。
            task_id: 可选，仅订阅该任务的日志。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        core.register_task_log_callback(callback, task_id)

    def on_worker_state(self, callback: Callable[[dict[str, Any]], None]) -> None:
        """订阅 Worker 生命周期事件。

        节点进入排空/完成排空/停止时调用
        ``callback({state, node_id})``（state ∈ ``draining``/``drained``/``stopped``）。

        Args:
            callback: 在后台线程执行（勿在其中阻塞）。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        core.register_worker_state_callback(callback)

    def peers(self) -> list[dict[str, Any]]:
        """枚举当前在线的 peer 节点（节点可见性）。

        每项为 ``node_id`` / ``endpoint`` / ``available_slots`` / ``max_slots`` /
        ``active_workflows`` / ``labels`` / ``platform`` / ``last_heartbeat_ms``。
        在线判定复用心跳新鲜度语义（超过 failure_timeout 未更新即不在线）。

        用于节点可见性与资源核算；基于此数据的路由/调度策略属第 3 层。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        return [dict(item) for item in core.peers()]

    def metrics_text(self) -> str:
        """返回所有已注册指标的 Prometheus exposition format 文本。

        用于自定义 HTTP 服务器暴露 ``/metrics`` 端点，或直接抓取/打印。
        若 Runtime 未启动（``metrics::init()`` 未调用），返回空字符串。

        HTTP 托管不在核心层（外置默认）：需要 HTTP 端点时用 ``http.server``
        自行包裹本方法，参见 ``examples/metrics_server.py`` 与
        ``actant worker --metrics-port``。

        Returns:
            Prometheus 文本格式字符串。
        """
        from actant.actant import metrics_text

        return metrics_text()

    def update_node_state(
        self,
        *,
        labels: Mapping[str, str] | None = None,
        quotas: Mapping[str, int] | None = None,
        draining: bool | None = None,
    ) -> None:
        """运行时更新本节点状态：标签 / 配额容量 / draining。

        任一参数为 ``None`` 表示保持当前值。标签与 draining 随下一次心跳
        广播给集群；``draining=True`` 停止接纳新任务并让远端路由跳过本节点，
        在途任务继续完成，``draining=False`` 恢复。配额容量整表替换，已用
        单元按差值保留。

        Args:
            labels: 新的标签集（整表覆盖）。总字节量超限抛 ``ConfigError``。
            quotas: 新的命名配额容量（整表覆盖）。
            draining: ``True`` 开始软停，``False`` 恢复。

        Raises:
            InvalidStateError: Runtime 未启动。
            ConfigError: 标签超出允许的字节量。
        """
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started")
        self._rust_core.update_node_state(
            labels=dict(labels) if labels is not None else None,
            quotas=dict(quotas) if quotas is not None else None,
            draining=draining,
        )

    def set_max_concurrent_tasks(self, new_max: int) -> None:
        """运行时调整 Worker 最大并发任务数（仅支持扩容）。

        Tokio Semaphore 不支持减少 permits，缩容请求会被忽略并记录警告日志。
        若需缩容，建议重启 Worker。

        Args:
            new_max: 新的最大并发任务数。必须大于当前值才会生效。
        """
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started")
        self._rust_core.set_max_concurrent_tasks(new_max)

    @property
    def max_concurrent_tasks(self) -> int:
        """Worker 当前最大并发任务数。"""
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started")
        return int(self._rust_core.max_concurrent_tasks())

    def storage_stats(self) -> _StoreStats:
        """返回本地存储容量与使用情况快照。

        供运维周期性采样；启动时已同步上报一次 ``actant.store.*`` 指标。
        ``file_bytes / map_size`` 是写满判断口径（LMDB 高水位），``used_bytes``
        是不含空闲页的实际占用。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started")
        return cast("_StoreStats", self._rust_core.storage_stats())

    def backup_to(self, path: str | os.PathLike[str]) -> int:
        """在线热备份本地存储到 ``path``，返回备份文件字节数。

        备份启用 compaction，会顺带回收空闲页；可在运行中调用，不影响读写。

        Raises:
            InvalidStateError: Runtime 未启动。
            StorageError: 备份目录不可写或复制失败。
        """
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started")
        return int(self._rust_core.backup_to(os.fspath(path)))


class Runtime(
    _RuntimeEffectsMixin,
    _RuntimeTaskMixin,
    _RuntimeWorkflowMixin,
    _RuntimeNetworkMixin,
):
    """Capability 注册表 + effect dispatcher。

    所有扩展点都注册为 `Layer`，由 `Runtime` 按 name 分桶存储。
    `Runtime` 提供 `ask` / `perform` / `emit` 三种执行模型。

    # 构造入口

    ``Runtime()`` 以默认配置构造 ``_RuntimeCore``：``Store`` 回退到 Rust
    ``StoreHandler``，``ValueStore`` 默认走 Python→Rust blob 桥，两者开箱即用；
    ``TaskLifecycle`` / ``WorkflowLifecycle`` 由引擎发射、用户以 handler 订阅。
    覆盖数据面请用 ``rt.layer("Store").chain(handler)``：该 handler 只接管
    **Python 侧发起**的 Store 调用，不改变 Rust 内部直接使用 LMDB 的路径。
    放置类策略由 Rust ``RoutePolicy`` / ``Scheduler`` 或第 3 层外部调度器承担；
    构造时不注册任何 Python 策略 handler。

    # 线程安全

    `Runtime` 内部使用 `threading.RLock` 保护注册表，支持并发注册与查询。
    ``Task.submit`` 通过 ``use_runtime(rt)`` 在独立线程池中传播上下文。

    # 生命周期

    `Runtime` 可作为 context manager 使用：

    ```python
    with actant.Runtime() as rt:
        rt.layer("MyRouter", "ask").chain(my_router)
        result = actant.ask("MyRouter", ctx)
    ```
    """

    def __init__(
        self,
        *,
        name: str | None = None,
        data_dir: str | None = None,
        config: _ActantConfig | None = None,
        max_concurrent_tasks: int | None = None,
        default_task_timeout_ms: int | None = None,
        drain_timeout_secs: int | None = None,
        remote_fallback_delay_ms: int | None = None,
        scheduler: str | None = None,
        route_policy: (
            str | Callable[[dict[str, Any], list[dict[str, Any]]], Any] | None
        ) = None,
    ) -> None:
        self._layers: dict[str, list[Callable[[Any], Any]]] = {}
        self._metas: dict[str, CapabilityMeta] = {}
        self._lock = threading.RLock()
        self._started = False
        # `start(auto_resume=True)` 拉起的后台重放线程（daemon；默认不创建）。
        self._auto_resume_thread: threading.Thread | None = None
        self._rust_runtime: Any = None  # PyCapabilityRuntime，延迟绑定
        self._rust_core: Any = None  # _RuntimeCore，保活统一运行时
        self._name = name  # 节点名称，传给 _RuntimeCore
        # 远端放置策略：内置名称（"default" / "label_affinity"）或 Python
        # callable ``(ctx, candidates) -> 决策``，原样传给 _RuntimeCore 解析。
        self._route_policy = route_policy
        # data_dir 解析优先级：显式参数 > config.store.data_dir > 自动临时目录。
        # 未指定时自动生成唯一临时目录，避免多实例/多测试共享 LMDB 数据
        # 导致状态污染或测试不稳定。Runtime 拥有该目录时 stop() 负责清理。
        # auto_data_dir 标志随构造参数传给 _build_config 决定网络 preset，
        # 不做路径嗅探——用户显式传入的路径（哪怕位于系统临时目录下）一律尊重。
        if data_dir is None and config is not None:
            data_dir = config.store.data_dir
        if data_dir is None:
            data_dir = tempfile.mkdtemp(prefix="actant-")
            self._owns_data_dir = True
        else:
            self._owns_data_dir = False
        self._data_dir = data_dir  # 持久化目录，传给 _RuntimeCore
        self._config: _ActantConfig | None = self._build_config(
            config=config,
            auto_data_dir=self._owns_data_dir,
            max_concurrent_tasks=max_concurrent_tasks,
            default_task_timeout_ms=default_task_timeout_ms,
            drain_timeout_secs=drain_timeout_secs,
            remote_fallback_delay_ms=remote_fallback_delay_ms,
            scheduler=scheduler,
        )
        # 已提交任务的注册表（本地任务追踪），供 list_tasks/get_task/cancel_task 查询。
        # task_id -> AsyncResult；任务查询/取消 API 的数据来源。
        self._tasks: dict[str, AsyncResult] = {}
        # 已取消任务集合（Python 端预取消标记），dispatch handler 启动时检查。
        # 用于在 Rust Worker 尚未拉取任务时实现本地取消。
        self._cancelled_tasks: set[str] = set()
        # 提交幂等：进程内句柄缓存 + 持久化映射 + 并发占位协议，见
        # ``IdempotencyLedger``。键基数由调用方控制，未做 TTL/上限裁剪。
        self._idempotency: IdempotencyLedger[AsyncResult] = IdempotencyLedger(self)
        # TaskLifecycle 事件批处理器：start() 时创建、stop() 时关闭。
        # 父进程侧每任务 2 次 emit（started/completed/failed）在后台线程批量派发，
        # 降低高吞吐下 event_bus 的 publish 次数。未创建（未 start）时为 None。
        self._event_batcher: Any = None
        # 终态记录与 workflow 返回值的持久化：start() 时创建、stop() 时关闭。
        # 未 start（或已 stop）时为 None，此时持久化调用被跳过。
        self._terminal: TerminalRecordStore | None = None
        # 注册所有内置 capability 为空 layer
        for cap_name, meta in BUILTIN_CAPABILITIES.items():
            self._metas[cap_name] = meta
            self._layers[cap_name] = []

    @staticmethod
    def _build_config(
        *,
        config: _ActantConfig | None,
        auto_data_dir: bool,
        max_concurrent_tasks: int | None,
        default_task_timeout_ms: int | None,
        drain_timeout_secs: int | None,
        remote_fallback_delay_ms: int | None,
        scheduler: str | None,
    ) -> _ActantConfig | None:
        """从 ``config`` 或便捷参数构造 ``_ActantConfig``。

        若提供了 ``config``，直接使用；否则构造一个 ``_ActantConfig``。
        ``auto_data_dir`` 为 ``True``（data_dir 由 Runtime 内部自动生成的
        临时目录）时使用 ``preset="none"`` 禁用 P2P 发现，避免测试/短生命
        周期实例受 iroh 网络初始化抖动影响；用户显式传入 data_dir 时一律
        使用默认 "local" preset，即使路径位于系统临时目录下。
        """
        if config is not None:
            return config
        from actant.actant import _ActantConfig, _NetworkConfig

        preset = "none" if auto_data_dir else "local"

        return _ActantConfig(
            payload_signing_key="",
            network=_NetworkConfig(preset=preset),
            max_concurrent_tasks=max_concurrent_tasks,
            default_task_timeout_ms=default_task_timeout_ms,
            drain_timeout_secs=drain_timeout_secs,
            remote_fallback_delay_ms=remote_fallback_delay_ms,
            scheduler=scheduler,
        )

    @classmethod
    def test(
        cls,
        *,
        name: str | None = None,
        max_concurrent_tasks: int | None = None,
        default_task_timeout_ms: int | None = None,
        drain_timeout_secs: int | None = None,
        scheduler: str | None = None,
    ) -> Runtime:
        """创建适用于测试的内存模式 Runtime。

        与 ``Runtime()`` 的区别：
        - **禁用 P2P 发现**：``network.preset="none"``，不启动 iroh 网络栈，
          避免测试环境中 DNS 解析阻塞和端口竞争。
        - **临时 data_dir**：使用 ``tempfile.mkdtemp()`` 创建临时目录，
          ``stop()`` 自动清理，无状态残留。
        - **空 payload 签名密钥**：测试场景无需跨节点身份认证。
        - **较短 drain 超时**：默认 5s（而非 30s），加速测试 teardown。

        适合单元测试、集成测试、CI 环境。不适合生产部署——生产环境
        请使用 ``Runtime.production()``。

        Args:
            name: 节点名称（默认 ``"test-{pid}"``）。
            max_concurrent_tasks: 单节点最大并发任务数（默认 ``num_cpus``）。
            default_task_timeout_ms: 任务默认超时毫秒（默认 30000）。
            drain_timeout_secs: 退出时等待在途任务的最长秒数（默认 5）。
            scheduler: 调度器类型（``"priority"`` / ``"fifo"``，默认 ``"priority"``）。
        """
        rt = cls(
            name=name or f"test-{os.getpid()}",
            data_dir=None,  # 自动创建临时目录，preset 自动为 "none"
            max_concurrent_tasks=max_concurrent_tasks,
            default_task_timeout_ms=default_task_timeout_ms,
            drain_timeout_secs=(
                drain_timeout_secs
                if drain_timeout_secs is not None
                else TEST_DRAIN_TIMEOUT_SECS
            ),
            scheduler=scheduler,
        )
        return rt

    @classmethod
    def production(
        cls,
        *,
        payload_signing_key: str,
        data_dir: str,
        name: str | None = None,
        max_concurrent_tasks: int | None = None,
        default_task_timeout_ms: int | None = None,
        drain_timeout_secs: int | None = None,
        remote_fallback_delay_ms: int | None = None,
        scheduler: str | None = None,
        network: Any = None,
        failover: Any = None,
        gossip: Any = None,
    ) -> Runtime:
        """创建生产级 Runtime，强制启用 payload 完整性签名。

        与 ``Runtime()`` 的区别：
        - **强制要求** ``payload_signing_key`` 非空（否则 ``ValueError``），
          防止生产部署因默认空密钥导致 payload 完整性保护被静默禁用。
        - 显式设置 ``require_payload_signing=True``：任一节点密钥不匹配
          时跨节点消息会被对端拒绝，提供集群身份认证。
        - **强制心跳节点记录签名**（``require_signed_records=True``）：
          节点记录必须由其 iroh endpoint 私钥签名，未持密钥的对端无法伪造
          身份加入集群视图。
        - **空 allowlist 拒启**：``network.allowed_peer_ids`` 为空时直接
          ``ValueError``——开放成员身份不属于生产语义。节点 endpoint id 可在
          启动前经 ``Runtime`` 的 ``peer_id`` 查询，逐一登记进各节点的白名单。
        - ``data_dir`` 必填：生产部署必须显式指定持久化目录，
          避免使用临时目录导致重启后状态丢失。节点身份密钥持久化于
          ``data_dir/identity.key``，endpoint id 因此跨重启稳定，
          白名单登记一次即长期有效。

        Args:
            payload_signing_key: 集群共享密钥，所有节点必须一致。空字符串
                会抛 ``ValueError``。
            data_dir: 持久化数据目录，必填。
            name: 节点名称（默认自动生成）。
            max_concurrent_tasks: 单节点最大并发任务数（默认 ``num_cpus``）。
            default_task_timeout_ms: 任务默认超时毫秒（默认 30000）。
            drain_timeout_secs: 退出时等待在途任务的最长秒数（默认 30）。
            remote_fallback_delay_ms: 远程回退延迟毫秒（默认 500）。
            scheduler: 调度器类型（``"priority"`` / ``"fifo"``，默认 ``"priority"``）。
            network: ``_NetworkConfig``，``None`` 用默认 ``local`` preset。
            failover: ``_FailoverConfig``，``None`` 用默认。
            gossip: ``_GossipConfig``，``None`` 用默认。

        Raises:
            ValueError: ``payload_signing_key`` 为空、``data_dir`` 为空，
                或 ``network.allowed_peer_ids`` 为空。
        """
        if not payload_signing_key:
            raise ValueError(
                "production() requires a non-empty payload_signing_key: "
                "payload integrity protection must be enabled in production"
            )
        if not data_dir:
            raise ValueError(
                "production() requires an explicit data_dir: "
                "temporary directories are not safe for production persistence"
            )
        from actant.actant import _ActantConfig, _NetworkConfig

        # 生产安全硬约束：空 allowlist = 任意 iroh 节点都能加入集群 gossip 视图，
        # 存在被恶意节点投递伪造消息的风险。生产语义下要求显式登记集群成员。
        effective_network = network if network is not None else _NetworkConfig(preset="local")
        if not effective_network.allowed_peer_ids:
            raise ValueError(
                "Runtime.production(): network.allowed_peer_ids is empty — "
                "open membership is not safe for production. Pass network="
                "_NetworkConfig(allowed_peer_ids=['node-a', ...], "
                "require_signed_records=True) to restrict membership to known "
                "peers; query each node's endpoint id via its peer_id."
            )
        # 补齐心跳签名强制：直接置位，不逐字段重建（避免字段漂移陷阱）。
        effective_network.require_signed_records = True

        config = _ActantConfig(
            payload_signing_key=payload_signing_key,
            network=effective_network,
            failover=failover,
            gossip=gossip,
            max_concurrent_tasks=max_concurrent_tasks,
            default_task_timeout_ms=default_task_timeout_ms,
            drain_timeout_secs=drain_timeout_secs,
            remote_fallback_delay_ms=remote_fallback_delay_ms,
            scheduler=scheduler,
            require_payload_signing=True,
        )
        rt = cls(name=name, data_dir=data_dir, config=config)
        return rt

    def start(self, *, auto_resume: bool = False) -> Runtime:
        """启动 Runtime，设置为当前线程的活跃 Runtime。

        启动时创建统一的 Rust `_RuntimeCore`，并从中获取 `_CapabilityRuntime`
        视图作为 Python handler 缺失/弃权时的内置回退路径。两者共享同一套
        tokio runtime 与 capability 句柄。

        分布式任务执行：启动时创建 Rust `_RuntimeCore` 并以进程池后端执行任务。
        提交的任务经 ``submit_task`` 投递到后台 channel，由 **scheduler
        ``enqueue`` → ``ProcessTaskDispatcher``** 分发到 worker 子进程执行
        （**不经过** ``Execute`` capability —— 默认的 ``register_defaults``
        不注册 ``ExecuteHandler``，那是给 Rust 嵌入场景按需调用的公开 API）；
        注册结果回调，使 ``AsyncResult`` 能在任务完成时被解析。

        Args:
            auto_resume: 启动后在**后台线程**重放本节点未终态工作流（见
                :meth:`host_flows`）。默认 ``False``：重放会执行用户 flow
                代码并重新派发任务，是否在启动时自动执行应由调用方显式决定。
                后台执行不阻塞 ``start()``；单个工作流重放失败只记录日志，
                不影响 Runtime 启动与其它工作流。
        """
        with self._lock:
            if self._started:
                return self
            if self._rust_runtime is None:
                from actant.actant import _RuntimeCore

                core = _RuntimeCore(
                    name=self._name,
                    data_dir=self._data_dir,
                    config=self._config,
                    route_policy=self._route_policy,
                )
                self._rust_runtime = core.capability_runtime()
                # 保留 core 引用，避免其被提前释放导致 tokio runtime 关闭。
                # 后续如需访问 workflow / network 句柄，也通过此 core。
                self._rust_core = core
            core = self._rust_core
            if core is not None:
                core.register_task_result_callback(self._on_task_result)
                # ValueStore 默认 handler（Python→Rust blob 桥）。
                # 用户预注册的 handler 优先（perform 取链末位），仅链为空时链入。
                if not self._layers.get(VALUE_STORE):
                    self._layers[VALUE_STORE].append(_DefaultValueStoreHandler(self))
            self._started = True
            # 创建 TaskLifecycle 事件批处理器。flush 发生在后台线程，经
            # _publish_task_event 把本 Runtime 绑定到该线程上下文后再 emit，
            # 避免进程池后端下每任务 2 次同步 emit 成为高吞吐瓶颈。
            from actant._internal.tasks.helpers import _EventBatcher

            self._event_batcher = _EventBatcher(render=self._publish_task_event)
            self._terminal = TerminalRecordStore(self)
        _runtime_local.runtime = self
        # 启动 Worker 守护循环（订阅 P2P topic + 任务执行循环）。
        # 非阻塞：worker.run() 在 tokio 后台 spawn，使任务能被调度执行。
        # serve() 内部会事件驱动等待 Worker 进入任务执行循环（基于 watch channel），
        # 因此此处无需 time.sleep(0.1) 等硬编码等待。
        self.serve()
        # 写入 node_meta.json 供 `actant status --data-dir` 离线读取。
        self._write_node_meta()
        if auto_resume:
            self._spawn_auto_resume()
        return self

    def stop(self, timeout: float | None = None) -> None:
        """停止 Runtime，关闭 Rust tokio runtime 并清除线程上下文。

        shutdown 失败时仍会清理线程上下文，避免后续 effect 调用卡在已损坏的
        Runtime 上；同时向上抛出首个错误，让调用方感知 shutdown 异常。

        Args:
            timeout: 等待在途任务的最长秒数（分布式任务由 Worker drain）。
                ``None`` 表示不设上限（由 Rust 侧优雅关闭自身完成），``0`` 表示
                立即强制关闭。单位是**秒**，Rust 侧 ``shutdown`` 用毫秒。
        """
        shutdown_err: Exception | None = None
        # timeout 以秒传入，Rust shutdown 以毫秒收口。None ⇒ 不设上限。
        shutdown_timeout_ms = (
            _NO_SHUTDOWN_TIMEOUT_MS if timeout is None else max(0, int(timeout * 1000))
        )

        # 关闭事件批处理器：停止后台 flush 线程并派发剩余缓冲事件。须在
        # rust_runtime 释放之前执行，使 flush 能经 _publish_task_event 正常 emit。
        if self._event_batcher is not None:
            try:
                self._event_batcher.close()
            except Exception as e:
                _logger.warning("failed to close event batcher", exc_info=True)
                if shutdown_err is None:
                    shutdown_err = e
            self._event_batcher = None
        # 刷出待写的终态记录并停止合并提交线程。须在 rust_runtime 释放之前
        # 完成——写入路径经 Store capability 访问 LMDB。
        if self._terminal is not None:
            try:
                self._terminal.close()
            except Exception as e:
                _logger.warning("failed to flush terminal record writer", exc_info=True)
                if shutdown_err is None:
                    shutdown_err = e
            self._terminal = None
        with self._lock:
            self._started = False
            core = self._rust_core
            # 清理引用：让 PyRuntimeCore 在 stop() 内显式释放（shutdown 已释放 GIL），
            # 而非延迟到 GC 时（GIL 持有状态下 Drop iroh/actor 资源会死锁）。
            self._rust_core = None
            self._rust_runtime = None
        if core is not None:
            try:
                # shutdown() 内部用 py.detach 释放 GIL，Drop iroh router 等重资源时
                # tokio worker 的 pyo3_log 回调能获取 GIL，避免死锁。
                core.shutdown(shutdown_timeout_ms)
                # shutdown 成功后才显式 del core，避免 shutdown 失败时 Drop
                # 触发半释放资源的二次清理（iroh router / actor system 等）。
                del core
            except Exception as e:
                shutdown_err = e
                _logger.warning("runtime shutdown failed", exc_info=True)
                # 不 del core：让 GC 在后续安全时机处理 Drop，
                # 避免在异常状态下重复释放已半关闭的资源。
        # Runtime 已进入终止流程，清空本地任务注册表与取消标记，避免
        # pending/cancelled 句柄在长期运行进程中滞留。
        #
        # 仍在册的句柄必须先结算为 cancelled：shutdown 之后不会再有结果回灌，
        # 留着 pending 会让 ``h.result()`` 一直阻塞到超时、``h.state`` 永远停在
        # running。已在 shutdown 期间拿到真实结果的句柄已由 ``_on_task_result``
        # 注销，这里只会碰到真正未结算的那批。
        with self._lock:
            unsettled = list(self._tasks.values())
            self._tasks.clear()
            self._cancelled_tasks.clear()
        for handle in unsettled:
            try:
                handle._set_cancelled()
            except Exception:  # 单个句柄结算失败不得阻断 stop
                _logger.warning("failed to settle pending task on stop", exc_info=True)
        # 无论 shutdown 是否成功都清理线程上下文，避免后续 effect 调用
        # 持有已失效的 Runtime 引用。
        if getattr(_runtime_local, "runtime", None) is self:
            _runtime_local.runtime = None
        if shutdown_err is not None:
            raise shutdown_err
        # 清理由 Runtime 自动创建的临时 data_dir，避免测试/短生命周期实例留下
        # 大量空目录；用户显式传入的目录不清理，符合预期。
        # 使用 os.path.exists 前置检查：重复 stop() 时该目录可能已被第一次清理。
        if self._owns_data_dir and self._data_dir and os.path.exists(self._data_dir):
            shutil.rmtree(self._data_dir, ignore_errors=True)

    def __enter__(self) -> Runtime:
        return self.start()

    def __exit__(self, *exc: Any) -> None:
        """退出 with 块时停止 Runtime。

        **异常处理**：若 with 块内有业务异常（``exc[1] is not None``），
        stop 的清理异常被 log 后吞掉，**不掩盖**业务异常（遵循 PEP 343 惯例）。
        若 with 块无异常，stop 的 shutdown 异常正常向上传播。
        """
        business_exc = exc[1] is not None
        try:
            self.stop()
        except Exception:
            if business_exc:
                _logger.warning("Runtime.stop() failed during __exit__; suppressed", exc_info=True)
            else:
                raise

    def serve(self) -> None:
        """启动 Worker 守护循环（订阅 P2P topic + 任务执行循环）。

        非阻塞：`worker.run()` 在 Rust tokio runtime 后台 spawn，直到 `stop()` 取消。
        用于 CLI `actant worker` 命令——使本节点作为后台任务执行器常驻。

        与 ray/prefect/celery 不同：无需连接中心服务器，P2P 自动发现对端节点。
        调用前必须先 `start()`。
        """
        with self._lock:
            if not self._started:
                raise InvalidStateError("Runtime must be started before serve()")
            if self._rust_core is not None:
                self._rust_core.serve()

    def __repr__(self) -> str:
        node_id = ""
        if self._started and self._rust_core is not None:
            with suppress(Exception):
                node_id = f", node_id={self._rust_core.node_id()!r}"
        return f"Runtime(capabilities={len(self._metas)}, started={self._started}{node_id})"


@contextmanager
def use_runtime(rt: Runtime) -> Iterator[None]:
    """临时切换当前线程的活跃 Runtime。"""
    prev = getattr(_runtime_local, "runtime", None)
    _runtime_local.runtime = rt
    try:
        yield
    finally:
        _runtime_local.runtime = prev


# 按 AGENTS.md 分层原则：
# - 放置类策略（路由/调度/重试）不在 Python 层注册 handler。它们由 Rust
#   `RoutePolicy` / `Scheduler` 或第 3 层外部调度器承担，避免进程内闭包在
#   workflow failover 后失效。
# - 数据面 capability（Store/ValueStore）可经 `rt.layer(name).chain(custom)`
#   覆盖；只影响 Python 侧发起的 effect，不进入 Rust 内部 dispatch。
