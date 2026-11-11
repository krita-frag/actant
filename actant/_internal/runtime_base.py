"""Runtime 的共享状态与底层原语。

`Runtime` 由多个 mixin 组合而成：本模块声明全部实例属性，并提供不依赖
具体业务分组的底层能力——capability 分发（同步/异步）与持久化 Store/blob
读写。各 mixin 通过这些原语协作，不互相直接依赖。"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal

from actant.capabilities import RUST_BACKED_CAPABILITIES, CapabilityMeta, StoreReq
from actant.exceptions import ActantError, InvalidStateError, NotFoundError

if TYPE_CHECKING:
    from actant._internal.persistence import IdempotencyLedger, TerminalRecordStore
    from actant.actant import _ActantConfig
    from actant.tasks import AsyncResult

_logger = logging.getLogger("actant.runtime")

# Rust `_TaskCompletion.state` 枚举的字符串镜像（src/py/types.rs）。
TASK_STATE_RUNNING = "Running"
TASK_STATE_COMPLETED = "Completed"
TASK_STATE_FAILED = "Failed"
TASK_STATE_CANCELLED = "Cancelled"
# Rust Orchestrator workflow 状态字符串镜像（get_workflow_state 返回的
# "state" 字段，src/runtime/workflow/ 的持久化状态机）。
WORKFLOW_STATE_COMPLETED = "Completed"
WORKFLOW_STATE_FAILED = "Failed"
WORKFLOW_STATE_CANCELLED = "Cancelled"

# continue_as_new 写入"下一代 workflow id"的保留上下文键。结果查询沿该链
# 找到最终代，使调用方用任意一代的 id 都能取到逻辑工作流的返回值。
_CONTINUED_TO_CONTEXT_KEY = "@continued_to"


class _RuntimeBase:
    """Runtime 组合根共享的实例状态与底层原语。"""

    # 全部实例状态由 Runtime.__init__ 赋值；在此声明使各 mixin 能在不互相
    # 导入的前提下共享类型已知的状态。
    _layers: dict[str, list[Callable[[Any], Any]]]
    _metas: dict[str, CapabilityMeta]
    _lock: Any
    _started: bool
    _auto_resume_thread: Any
    _rust_runtime: Any
    _rust_core: Any
    _name: str | None
    # 内置策略名（"default" / "label_affinity"）或用户 callable；
    # 由 `_RuntimeCore` 在构造时区分二者。
    _route_policy: (
        str | Callable[[dict[str, Any], list[dict[str, Any]]], Any] | None
    )
    _owns_data_dir: bool
    _data_dir: str | None
    _config: _ActantConfig | None
    _tasks: dict[str, AsyncResult]
    _cancelled_tasks: set[str]
    _idempotency: IdempotencyLedger[AsyncResult]
    _event_batcher: Any
    _terminal: TerminalRecordStore | None

    def _record_effect(self, name: str, effect: str) -> None:
        """记录一次 capability 调用计数（Rust 指标面）。

        Runtime 未启动时没有指标管道，直接跳过。只由公共入口
        ``actant.ask/perform/emit`` 与 ``actant.aio.*`` 调用；框架内部经
        ``_dispatch_perform`` 的 Store / ValueStore 读写不计入，避免内部持久化
        与 payload offload 淹没业务 capability 的计数。
        """
        core = self._rust_core
        if core is not None:
            core._record_capability_invocation(name, effect)

    def _record_effects(self, entries: list[tuple[str, str]]) -> None:
        """批量记录 capability 调用计数（整批一次 Python→Rust 穿越）。

        与 ``_record_effect`` 同口径；供 ``aio.perform_batch`` 使用，避免逐条
        记账抵消批量快路径的单次边界穿越。
        """
        core = self._rust_core
        if core is not None and entries:
            core._record_capability_invocations(entries)

    def _dispatch_ask(self, name: str, request: Any) -> Any | None:
        """决策型：逆序调用 handler，第一个返回非 None 的决定结果。

        Python handler 全部弃权时，仅对 `RUST_BACKED_CAPABILITIES` 中的 capability
        转发到 Rust capability runtime；是否有内置 handler 取决于该 capability。
        自定义 capability 无 Rust 路径，全部弃权时返回 None。
        """
        with self._lock:
            handlers = list(self._layers.get(name, []))
        # 逆序：后注册的 handler 优先决策（用户自定义覆盖默认）
        for handler in reversed(handlers):
            result = handler(request)
            if result is not None:
                return result
        if self._rust_runtime is not None and name in RUST_BACKED_CAPABILITIES:
            return self._rust_runtime.ask(name, request)
        _logger.debug("ask %r: all %d handler(s) abstained", name, len(handlers))
        return None

    def _dispatch_perform(self, name: str, request: Any) -> Any:
        """副作用型：调用最后注册的 handler，结果直接返回。

        无 Python handler 时，仅对 `RUST_BACKED_CAPABILITIES` 转发到 Rust
        capability runtime。`Store` 有 Rust `StoreHandler`；两个 lifecycle
        capability 在 Rust 侧只有 codec 与空 layer，emit 时为空操作。
        """
        with self._lock:
            handlers = list(self._layers.get(name, []))
        if handlers:
            return handlers[-1](request)
        if self._rust_runtime is not None and name in RUST_BACKED_CAPABILITIES:
            return self._rust_runtime.perform(name, request)

        raise NotFoundError(f"perform: capability {name!r} has no handlers")

    def _dispatch_emit(
        self,
        name: str,
        request: Any,
        *,
        on_error: Literal["log", "raise", "collect"] = "log",
    ) -> None:
        """反应型：所有 handler 顺序执行，无返回值。

        对 `RUST_BACKED_CAPABILITIES` 中的 lifecycle capability，先触发 Rust 事件总线，
        再执行 Python handlers。这确保 Rust 侧的状态变更（如 WorkerLifecycle
        事件）先于 Python handler 生效，避免 Python handler 观察到不一致的状态。

        Args:
            name: capability 名称。
            request: 事件 payload。
            on_error: 错误处理策略。
                - ``"log"``（默认）：记录 warning 并继续执行后续 handler。
                - ``"raise"``：第一个失败的 handler 抛出异常时立即向上传播。
                - ``"collect"``：收集所有错误，全部执行完后聚合抛出 ``RuntimeError``。
        """
        # Rust emit 先执行：确保 Rust 侧状态变更先于 Python handler。
        # Rust emit 错误与 Python handler 错误统一按 on_error 策略处理。
        with self._lock:
            handlers = list(self._layers.get(name, []))
        errors: list[Exception] = []
        if self._rust_runtime is not None and name in RUST_BACKED_CAPABILITIES:
            try:
                self._rust_runtime.emit(name, request)
            except Exception as e:
                if on_error == "raise":
                    raise
                errors.append(e)
                _logger.warning("emit rust handler for %r failed", name, exc_info=True)
        for handler in handlers:
            try:
                handler(request)
            except Exception as e:
                if on_error == "raise":
                    raise
                errors.append(e)
                _logger.warning("emit handler for %r failed", name, exc_info=True)
        if errors and on_error == "collect":
            # ``from errors[0]`` 保留首个失败的异常链，便于 traceback 排因。
            raise ActantError(
                f"emit {name!r}: {len(errors)} handler(s) failed; first: {errors[0]!r}"
            ) from errors[0]

    @staticmethod
    def _get_asyncio_loop() -> asyncio.AbstractEventLoop:
        """获取当前线程的 asyncio 事件循环。

        若无运行中的 loop（如从同步代码调用），抛 ``InvalidStateError``。
        该场景与 Runtime 生命周期无关，故覆写 hint，避免提示用户去
        ``Runtime.start()``。
        """
        try:
            return asyncio.get_running_loop()
        except RuntimeError as e:
            raise InvalidStateError(
                f"async dispatch requires a running asyncio event loop: {e}",
                hint=(
                    " Call this from async code (`async def` + `await`) "
                    "or run it via `asyncio.run(...)`."
                ),
            ) from e

    def _dispatch_ask_async(self, name: str, request: Any) -> Any:
        """异步决策型 effect：返回 ``asyncio.Future``。

        与 ``_dispatch_ask`` 的区别：
        - 若 capability 是 Rust-backed 且无 Python handler 弃权，
          转发到 ``_rust_runtime.ask_async``，结果通过 ``asyncio.Future`` 异步返回。
        - 若 Python handler 命中，结果立即 resolve（无阻塞）。
        - 若 Python handler 全部弃权且为 Python-only capability，
          立即 resolve 为 ``None``。

        Returns:
            ``asyncio.Future``，await 后得到 handler 结果或 ``None``。
        """
        loop = self._get_asyncio_loop()
        with self._lock:
            handlers = list(self._layers.get(name, []))
        if handlers:
            # Python handler 链：逆序调用，可能命中。
            # 由于 handler 是同步函数，用 run_in_executor 在默认线程池中执行，
            # 避免 GIL 长时间阻塞 event loop（如 CPU-bound handler）。
            def _run_python_chain() -> Any | None:
                for handler in reversed(handlers):
                    result = handler(request)
                    if result is not None:
                        return result
                # 全部弃权 → 若 Rust-backed 回退到 Rust，否则 None
                if self._rust_runtime is not None and name in RUST_BACKED_CAPABILITIES:
                    # 同步回退路径：在 executor 中调用同步 ask。
                    return self._rust_runtime.ask(name, request)
                _logger.debug("ask_async %r: all %d handler(s) abstained", name, len(handlers))
                return None

            return loop.run_in_executor(None, _run_python_chain)
        # 无 Python handler：Rust-backed 走 ask_async，否则立即 None
        if self._rust_runtime is not None and name in RUST_BACKED_CAPABILITIES:
            return self._rust_runtime.ask_async(name, request)
        fut = loop.create_future()
        fut.set_result(None)
        return fut

    def _dispatch_perform_async(self, name: str, request: Any) -> Any:
        """异步副作用型 effect：返回 ``asyncio.Future``。

        - 若 Python handler 存在，在 executor 中执行最后一个 handler。
        - 若无 Python handler 但为 Rust-backed，转发到 ``perform_async``。
        - 若无 Python handler 且非 Rust-backed，抛 ``NotFoundError``。
        """
        loop = self._get_asyncio_loop()
        with self._lock:
            handlers = list(self._layers.get(name, []))
        if handlers:
            last = handlers[-1]
            return loop.run_in_executor(None, lambda: last(request))
        if self._rust_runtime is not None and name in RUST_BACKED_CAPABILITIES:
            return self._rust_runtime.perform_async(name, request)
        # 异步上下文中无法同步抛异常，包装到 future 中。
        fut = loop.create_future()
        fut.set_exception(NotFoundError(f"perform_async: capability {name!r} has no handlers"))
        return fut

    def _dispatch_perform_batch_async(
        self,
        items: list[tuple[str, Any]],
    ) -> Any:
        """异步批量副作用型 effect：返回 ``asyncio.Future``。

        当全部条目都由 Rust 内置 handler 处理（无 Python handler）时，委托
        ``_rust_runtime.perform_batch_async``：单次 Python→Rust 边界穿越 +
        Rust 侧 ``join_all`` 并发执行。否则逐条分发（Python handler 在默认
        executor 中执行，Rust-backed 走 ``perform_async``），用
        ``asyncio.gather`` 并发等待。

        单个 perform 失败不中断批量——失败项在结果列表对应位置为 ``Exception`` 实例
        （``return_exceptions=True``）。

        Args:
            items: ``(capability_name, request)`` 元组列表。

        Returns:
            ``asyncio.Future``，await 后得到结果列表，顺序与输入一致。
        """
        loop = self._get_asyncio_loop()
        if items and self._rust_runtime is not None and all(
            self._is_rust_only(name) for name, _ in items
        ):
            return self._rust_runtime.perform_batch_async(items)

        async def _run_batch() -> list[Any]:
            coros = [self._perform_one_async(name, req) for name, req in items]
            return await asyncio.gather(*coros, return_exceptions=True)

        return asyncio.ensure_future(_run_batch(), loop=loop)

    def _is_rust_only(self, name: str) -> bool:
        """capability 是否完全由 Rust 内置 handler 处理（无 Python handler）。"""
        with self._lock:
            if self._layers.get(name):
                return False
        return name in RUST_BACKED_CAPABILITIES

    async def _perform_one_async(self, name: str, request: Any) -> Any:
        """单个 perform 的 async 包装，供 batch 使用。

        区分 Python handler / Rust-backed / 未注册三种路径。
        """
        with self._lock:
            handlers = list(self._layers.get(name, []))
        if handlers:
            last = handlers[-1]
            # 同步 handler 在默认 executor 中执行
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, lambda: last(request))
        if self._rust_runtime is not None and name in RUST_BACKED_CAPABILITIES:
            return await self._rust_runtime.perform_async(name, request)
        raise NotFoundError(f"perform_async: capability {name!r} has no handlers")

    def _store_get(self, key: str) -> bytes | None:
        """读取持久化 Store 中的原始字节；未启动时返回 ``None``。

        存储错误原样传播：调用方需要区分"未记录"与"读失败"时，不能把读失败
        静默当作未记录。
        """
        if self._rust_runtime is None:
            return None
        raw = self._dispatch_perform("Store", StoreReq(op="get", key=key.encode()))
        return None if raw is None else bytes(raw)

    def _store_put(self, key: str, value: bytes) -> None:
        """写入持久化 Store 原始字节；未启动时抛 ``InvalidStateError``。"""
        if self._rust_runtime is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        self._dispatch_perform("Store", StoreReq(op="put", key=key.encode(), value=value))

    def _store_put_batch(self, entries: list[tuple[str, bytes]]) -> None:
        """在单个 Store 事务内写入多条记录；未启动时抛 ``InvalidStateError``。"""
        if self._rust_runtime is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        self._dispatch_perform(
            "Store",
            StoreReq(
                op="put_batch",
                entries=[(key.encode(), value) for key, value in entries],
            ),
        )

    def _store_delete(self, key: str) -> None:
        """删除持久化 Store 中的记录；未启动时抛 ``InvalidStateError``。"""
        if self._rust_runtime is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        self._dispatch_perform("Store", StoreReq(op="delete", key=key.encode()))

    def _idempotent_submit(
        self,
        key: str,
        submit: Callable[[], AsyncResult],
    ) -> AsyncResult:
        """按 ``key`` 去重提交，语义见 ``IdempotencyLedger.submit_once``。"""
        return self._idempotency.submit_once(key, submit)

    def _blob_store(self, payload: bytes) -> bytes:
        """把 payload 写入本节点内容寻址 blob store，返回 ``Ref`` 编码。"""
        from actant._internal.tasks.ref import _value_store

        return _value_store(payload, runtime=self)

    def _blob_fetch(self, ref: Any) -> bytes:
        """按 ``Ref`` 编码拉取 blob 原始字节。"""
        from actant._internal.tasks.ref import _value_fetch

        return _value_fetch(ref, runtime=self)

    def _persist_task_result(self, task_id: str, frame: bytes, ref_bytes: bytes | None) -> None:
        """持久化成功任务的终态；未 start 时跳过（无 Store 可写）。"""
        store = self._terminal
        if store is not None:
            store.put_task_result(task_id, frame, ref_bytes)

    def _persist_task_failure(
        self,
        task_id: str,
        state: str,
        error_text: str,
        exc: BaseException | None = None,
    ) -> None:
        """持久化失败/取消/跳过终态；未 start 时跳过（无 Store 可写）。"""
        store = self._terminal
        if store is not None:
            store.put_task_failure(task_id, state, error_text, exc=exc)

    def _persist_flow_result(self, workflow_id: str, result: Any) -> None:
        """持久化 workflow 返回值；未 start 时跳过（无 Store 可写）。"""
        store = self._terminal
        if store is not None:
            store.put_flow_result(workflow_id, result)
