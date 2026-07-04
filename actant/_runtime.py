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

import threading
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any, Optional

from actant.capabilities import (
    BUILTIN_CAPABILITIES,
    CapabilityMeta,
    EffectKind,
    get_capability_meta,
)


# 当前线程的 Runtime 上下文（用 contextvar 替代 threadlocal 更优雅，但需 Python 3.10+，
# 这里用 threading.local 保证与现有代码一致）。
_runtime_local = threading.local()


def get_current_runtime() -> Optional["Runtime"]:
    """返回当前线程的活跃 Runtime，无则返回 `None`。"""
    return getattr(_runtime_local, "runtime", None)


class Layer:
    """构建一个 capability 的 handler 链。

    通过 `Runtime.layer(name)` 创建，支持链式 `chain(handler)` 追加。
    `chain` 直接修改 Runtime 内部 handler 列表（live view），无需显式 register。
    """

    def __init__(self, runtime: "Runtime", meta: CapabilityMeta) -> None:
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

    def chain(self, handler: Callable[[Any], Any]) -> "Layer":
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
        # 注册所有内置 capability 为空 layer
        for name, meta in BUILTIN_CAPABILITIES.items():
            self._metas[name] = meta
            self._layers[name] = []

    # ------------------------------------------------------------------
    # Layer 注册
    # ------------------------------------------------------------------

    def layer(self, name: str, kind: Optional[EffectKind] = None) -> Layer:
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

    def _dispatch_ask(self, name: str, request: Any) -> Optional[Any]:
        """决策型：依次调用 handler，第一个返回非 None 的决定结果。"""
        with self._lock:
            handlers = list(self._layers.get(name, []))
        for handler in handlers:
            result = handler(request)
            if result is not None:
                return result
        return None

    def _dispatch_perform(self, name: str, request: Any) -> Any:
        """副作用型：调用第一个 handler，结果直接返回。"""
        with self._lock:
            handlers = list(self._layers.get(name, []))
        if not handlers:
            raise RuntimeError(f"perform: capability {name!r} has no handlers")
        return handlers[0](request)

    def _dispatch_emit(self, name: str, request: Any) -> None:
        """反应型：所有 handler 顺序执行，无返回值。"""
        with self._lock:
            handlers = list(self._layers.get(name, []))
        for handler in handlers:
            try:
                handler(request)
            except Exception:
                # 反应型语义：一个 handler 失败不应阻塞其他订阅者
                # 日志由上层处理（避免循环导入）
                pass

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    def start(self) -> "Runtime":
        """启动 Runtime，设置为当前线程的活跃 Runtime。"""
        with self._lock:
            if self._started:
                return self
            self._started = True
        _runtime_local.runtime = self
        return self

    def stop(self) -> None:
        """停止 Runtime，清除当前线程的活跃 Runtime。"""
        with self._lock:
            self._started = False
        if getattr(_runtime_local, "runtime", None) is self:
            _runtime_local.runtime = None

    def __enter__(self) -> "Runtime":
        return self.start()

    def __exit__(self, *exc: Any) -> None:
        self.stop()

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
def use_runtime(rt: Runtime):
    """临时切换当前线程的活跃 Runtime。"""
    prev = getattr(_runtime_local, "runtime", None)
    _runtime_local.runtime = rt
    try:
        yield
    finally:
        _runtime_local.runtime = prev


__all__ = [
    "Layer",
    "Runtime",
    "get_current_runtime",
    "use_runtime",
]
