"""Python 侧 Runtime：统一注册表 + effect dispatcher。

`Runtime` 是 actant 0.2.0 的统一运行时入口。所有扩展点（Routing、Scheduling、
Transport、Store、Actor、Lifecycle 等）都注册为 Layer，由 Runtime 按 name 分桶存储。

# 设计

- Rust `capability::Runtime`：强类型，按 `TypeId` 分桶，服务于 Rust 内置 capability
- Python `Runtime`（本模块）：按 name 分桶，维护 Python handler 列表
- `PyCapabilityRuntime`：Rust Runtime 的 PyO3 包装，本模块持有但不直接暴露给用户

# 用法

```python
import actant

rt = actant.Runtime()
rt.layer("Routing").chain(my_router)
rt.layer("TaskLifecycle").chain(lambda e: print(f"task {e.kind}: {e.task_id}"))
rt.start()

# 在 handler 中：
result = actant.ask("Routing", ctx)
```
"""

from __future__ import annotations

import logging
import threading
import zlib
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from actant.capabilities import (
    BUILTIN_CAPABILITIES,
    RUST_BACKED_CAPABILITIES,
    CapabilityMeta,
    EffectKind,
    RetryCtx,
    RouteCtx,
    ScheduleCtx,
)
from actant.exceptions import NotFoundError

# 当前线程的 Runtime 上下文（用 contextvar 替代 threadlocal 更优雅，但需 Python 3.10+，
# 这里用 threading.local 保证与现有代码一致）。
_runtime_local = threading.local()

_logger = logging.getLogger("actant.runtime")

def get_current_runtime() -> Runtime | None:
    """返回当前线程的活跃 Runtime，无则返回 `None`。"""
    return getattr(_runtime_local, "runtime", None)


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

        handler 立即注册到 Runtime，对后续 effect 调用可见。
        """
        if not callable(handler):
            raise TypeError(f"handler must be callable, got {type(handler)}")
        with self._runtime._lock:
            self._runtime._layers[self._meta.name].append(handler)
        return self

    def __repr__(self) -> str:
        with self._runtime._lock:
            count = len(self._runtime._layers.get(self._meta.name, []))
        return f"Layer(name={self.name!r}, kind={self.kind!r}, handlers={count})"


class Runtime:
    """Capability 注册表 + effect dispatcher。

    所有扩展点都注册为 `Layer`，由 `Runtime` 按 name 分桶存储。
    `Runtime` 提供 `ask` / `perform` / `emit` 三种执行模型。

    # 线程安全

    `Runtime` 内部使用 `threading.RLock` 保护注册表，支持并发注册与查询。

    # 生命周期

    `Runtime` 可作为 context manager 使用：

    ```python
    with actant.Runtime() as rt:
        rt.layer("Routing").chain(my_router)
        result = actant.ask("Routing", ctx)
    ```
    """

    def __init__(self) -> None:
        self._layers: dict[str, list[Callable[[Any], Any]]] = {}
        self._metas: dict[str, CapabilityMeta] = {}
        self._lock = threading.RLock()
        self._started = False
        self._rust_runtime: Any = None  # PyCapabilityRuntime，延迟绑定
        self._rust_core: Any = None  # _RuntimeCore，保活统一运行时
        # 注册所有内置 capability 为空 layer
        for name, meta in BUILTIN_CAPABILITIES.items():
            self._metas[name] = meta
            self._layers[name] = []

    @classmethod
    def with_defaults(cls) -> Runtime:
        """创建 Runtime 并注册所有内置默认 handler。

        等价于 `rt = Runtime(); _register_default_handlers(rt)`。
        用户随后可通过 `rt.layer("Routing").chain(custom)` 追加自定义 handler，
        自定义 handler 优先级更高（chain 顺序决定 ask 的决策顺序）。
        """
        rt = cls()
        _register_default_handlers(rt)
        return rt

    # ------------------------------------------------------------------
    # Layer 注册
    # ------------------------------------------------------------------

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
                    raise ValueError(
                        f"custom capability {name!r} requires explicit kind"
                    )
                meta = CapabilityMeta(name, kind)
                self._metas[name] = meta
                self._layers[name] = []
            return Layer(self, meta)

    def register(self, layer: Layer) -> None:
        """注册一个已构建的 Layer，替换该 capability 的所有 handler。

        注意：通常无需调用此方法，`layer(name).chain(handler)` 已自动注册。
        此方法用于需要整体替换 handler 链的场景。
        """
        with self._lock:
            # Layer 是 live view，handler 已在 chain 时注册。
            # 这里仅更新 meta。
            self._metas[layer.name] = layer._meta

    def chain(self, name: str, handler: Callable[[Any], Any]) -> None:
        """向已存在的 capability 追加单个 handler。"""
        with self._lock:
            if name not in self._layers:
                raise KeyError(f"capability {name!r} not registered")
            self._layers[name].append(handler)

    # ------------------------------------------------------------------
    # Effect 分发
    # ------------------------------------------------------------------

    def _dispatch_ask(self, name: str, request: Any) -> Any | None:
        """决策型：逆序调用 handler，第一个返回非 None 的决定结果。

        Python handler 全部弃权时，仅对 `RUST_BACKED_CAPABILITIES` 中的 capability
        回退到 Rust 内置 handler。`Routing`/`Scheduling`/`RetryPolicy` 是纯 Python
        策略，无 Rust fallback，全部弃权时返回 None。
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
        return None

    def _dispatch_perform(self, name: str, request: Any) -> Any:
        """副作用型：调用最后注册的 handler，结果直接返回。

        无 Python handler 时，仅对 `RUST_BACKED_CAPABILITIES` 回退到 Rust 内置 handler。
        """
        with self._lock:
            handlers = list(self._layers.get(name, []))
        if handlers:
            return handlers[-1](request)
        if self._rust_runtime is not None and name in RUST_BACKED_CAPABILITIES:
            return self._rust_runtime.perform(name, request)
        # M3 改进：使用 NotFoundError 而非泛型 RuntimeError，与 Rust ActantError 层级对齐。
        raise NotFoundError(f"perform: capability {name!r} has no handlers")

    def _dispatch_emit(
        self,
        name: str,
        request: Any,
        *,
        on_error: str = "log",
    ) -> None:
        """反应型：所有 handler 顺序执行，无返回值。

        对 `RUST_BACKED_CAPABILITIES` 中的 lifecycle capability，同步触发 Rust 事件总线。

        Args:
            name: capability 名称。
            request: 事件 payload。
            on_error: 错误处理策略。
                - ``"log"``（默认）：记录 warning 并继续执行后续 handler。
                - ``"raise"``：第一个失败的 handler 抛出异常时立即向上传播。
                - ``"collect"``：收集所有错误，全部执行完后聚合抛出 ``RuntimeError``。
        """
        with self._lock:
            handlers = list(self._layers.get(name, []))
        errors: list[Exception] = []
        for handler in handlers:
            try:
                handler(request)
            except Exception as e:
                if on_error == "raise":
                    raise
                errors.append(e)
                _logger.warning("emit handler for %r failed", name, exc_info=True)
        if errors and on_error == "collect":
            raise RuntimeError(
                f"emit {name!r}: {len(errors)} handler(s) failed; first: {errors[0]!r}"
            )
        if self._rust_runtime is not None and name in RUST_BACKED_CAPABILITIES:
            self._rust_runtime.emit(name, request)

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    def start(self) -> Runtime:
        """启动 Runtime，设置为当前线程的活跃 Runtime。

        启动时创建统一的 Rust `_RuntimeCore`，并从中获取 `_CapabilityRuntime`
        视图作为 Python handler 缺失/弃权时的内置回退路径。两者共享同一套
        tokio runtime 与 capability 句柄。
        """
        with self._lock:
            if self._started:
                return self
            if self._rust_runtime is None:
                from actant.actant import _RuntimeCore

                core = _RuntimeCore()
                self._rust_runtime = core.capability_runtime()
                # 保留 core 引用，避免其被提前释放导致 tokio runtime 关闭。
                # 后续如需访问 actor / workflow / network 句柄，也通过此 core。
                self._rust_core = core
            self._started = True
        _runtime_local.runtime = self
        return self

    def stop(self) -> None:
        """停止 Runtime，关闭 Rust tokio runtime 并清除线程上下文。

        shutdown 失败时仍会清理线程上下文，避免后续 effect 调用卡在已损坏的
        Runtime 上；同时向上抛出首个错误，让调用方感知 shutdown 异常（H2 改进）。
        """
        with self._lock:
            self._started = False
            core = self._rust_core
            # 清理引用：让 PyRuntimeCore 在 stop() 内显式释放（shutdown 已释放 GIL），
            # 而非延迟到 GC 时（GIL 持有状态下 Drop iroh/actor 资源会死锁）。
            self._rust_core = None
            self._rust_runtime = None
        shutdown_err: Exception | None = None
        if core is not None:
            try:
                # shutdown() 内部用 py.detach 释放 GIL，Drop iroh router 等重资源时
                # tokio worker 的 pyo3_log 回调能获取 GIL，避免死锁。
                core.shutdown()
            except Exception as e:
                shutdown_err = e
                _logger.warning("runtime shutdown failed", exc_info=True)
            del core
        # 无论 shutdown 是否成功都清理线程上下文，避免后续 effect 调用
        # 持有已失效的 Runtime 引用。
        if getattr(_runtime_local, "runtime", None) is self:
            _runtime_local.runtime = None
        if shutdown_err is not None:
            raise shutdown_err

    def __enter__(self) -> Runtime:
        return self.start()

    def __exit__(self, *exc: Any) -> None:
        self.stop()

    def serve(self) -> None:
        """启动 Worker 守护循环（订阅 P2P topic + 任务执行循环）。

        非阻塞：`worker.run()` 在 Rust tokio runtime 后台 spawn，直到 `stop()` 取消。
        用于 CLI `actant worker` 命令——使本节点作为后台任务执行器常驻。

        与 ray/prefect/celery 不同：无需连接中心服务器，P2P 自动发现对端节点。
        调用前必须先 `start()`。
        """
        with self._lock:
            if not self._started:
                raise RuntimeError("Runtime must be started before serve()")
            if self._rust_core is not None:
                self._rust_core.serve()

    @property
    def node_id(self) -> str:
        """本节点的唯一 ID（由 Rust 核心分配）。"""
        if self._rust_core is None:
            raise RuntimeError("Runtime not started")
        return str(self._rust_core.node_id())

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    @property
    def capabilities(self) -> list[str]:
        """返回所有已注册 capability 的名称。"""
        with self._lock:
            return list(self._metas.keys())

    def capability_meta(self, name: str) -> CapabilityMeta:
        """返回指定 capability 的元数据。"""
        with self._lock:
            if name not in self._metas:
                raise KeyError(f"capability {name!r} not registered")
            return self._metas[name]

    def handler_count(self, name: str) -> int:
        """返回指定 capability 的 handler 数量。"""
        with self._lock:
            return len(self._layers.get(name, []))

    def __repr__(self) -> str:
        return (
            f"Runtime(capabilities={len(self._metas)}, "
            f"started={self._started})"
        )


@contextmanager
def use_runtime(rt: Runtime) -> Iterator[None]:
    """临时切换当前线程的活跃 Runtime。"""
    prev = getattr(_runtime_local, "runtime", None)
    _runtime_local.runtime = rt
    try:
        yield
    finally:
        _runtime_local.runtime = prev


# ============================================================================
# 内置默认 handler（Python 策略层）
# ============================================================================
#
# 按 AGENTS.md 分层原则：
# - `Routing` / `Scheduling` / `RetryPolicy` 是策略型 capability，**仅 Python 层**实现。
#   Rust 核心不提供默认 handler（见 `src/runtime/capability.rs` 顶部注释）。
# - `Runtime.with_defaults()` 注册以下 Python 默认 handler，用户可通过
#   `rt.layer(name).chain(custom)` 覆盖（后注册的 handler 在 ask 中优先决策）。
#
# 这些 handler 是**生产可用**的实现，非桩代码：
# - `LocalRouter`：无 peer 时路由到本地节点；有 peer 时轮询（round-robin）。
# - `FifoScheduler`：返回 pending 列表的第一个任务（FIFO 语义）。
# - `NoRetryPolicy`：attempt < max_retries 时重试，否则放弃。


class LocalRouter:
    """`Routing` capability 的默认 Python handler。

    策略：
    - `ctx.peers` 为空 → 返回 `ctx.local_node`（本地执行）。
    - `ctx.peers` 非空 → round-robin 轮询 peer 列表（基于 task_name 哈希）。

    用户可通过 `rt.layer("Routing").chain(custom_router)` 覆盖。
    """

    def __call__(self, ctx: RouteCtx) -> str | None:
        if not ctx.peers:
            return ctx.local_node or None
        # 稳定哈希路由：同一 task_name 总是路由到同一 peer（除非 peer 列表变化）。
        # 使用 zlib.crc32 而非内置 hash()，因为 hash() 对字符串有 PYTHONHASHSEED
        # 随机化，不同进程会路由到不同 peer，破坏稳定哈希语义。
        idx = zlib.crc32(ctx.task_name.encode()) % len(ctx.peers)
        return ctx.peers[idx]


class FifoScheduler:
    """`Scheduling` capability 的默认 Python handler。

    返回 `ctx.pending` 的第一个任务 ID（FIFO 语义）。`pending` 为空时返回 None。
    """

    def __call__(self, ctx: ScheduleCtx) -> str | None:
        if not ctx.pending:
            return None
        return ctx.pending[0]


class NoRetryPolicy:
    """`RetryPolicy` capability 的默认 Python handler。

    `attempt < max_retries` 时返回 True（重试），否则返回 None（放弃，交由上层处理）。
    `max_retries` 来自 `RetryCtx.max_retries`，由调用方（编排循环）填充。
    """

    def __call__(self, ctx: RetryCtx) -> bool | None:
        if ctx.attempt < ctx.max_retries:
            return True
        return None


def _register_default_handlers(rt: Runtime) -> None:
    """向 Runtime 注册 Python 策略层默认 handler。

    仅注册 `Routing` / `Scheduling` / `RetryPolicy` 三个纯 Python capability。
    其余 Rust-backed capability 的默认 handler 由 `RuntimeBuilder` 在 Rust 侧注入
    （如 StoreHandler / ExecuteHandler），Python handler 缺失时自动回退。
    """
    rt.layer("Routing").chain(LocalRouter())
    rt.layer("Scheduling").chain(FifoScheduler())
    rt.layer("RetryPolicy").chain(NoRetryPolicy())


__all__ = [
    "FifoScheduler",
    "Layer",
    "LocalRouter",
    "NoRetryPolicy",
    "Runtime",
    "get_current_runtime",
    "use_runtime",
]
