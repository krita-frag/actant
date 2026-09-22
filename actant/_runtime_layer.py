"""Layer：capability handler 链的可组合视图（C4 移动性拆分自 `_runtime.py`）。

独立于 Runtime 内部实现：经构造器注入 runtime 与 capability 元数据，
对 handler 列表的操作直接作用于 ``runtime._layers[name]``。
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from actant.capabilities import RUST_BACKED_CAPABILITIES, CapabilityMeta, EffectKind

if TYPE_CHECKING:
    from actant._runtime import Runtime

_logger = logging.getLogger("actant.runtime")


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
            ``start()`` 后追加 Rust-backed capability（如 ``Execute`` / ``Store`` /
            ``Transport``）的 handler 存在**预期外限制**：
            Rust 内部分发路径（如 Worker 执行任务时调用 ``task_dispatcher.dispatch()``）
            **不经过** Python ``_layers``，因此新追加的 Python handler 不会被
            Rust 内部调用所咨询。Python handler 仅在用户代码显式调用
            ``actant.ask/perform/emit(name, ...)`` 时生效。

            如需覆盖 Rust 内部 dispatch 行为（如自定义任务执行逻辑），
            ``Runtime.layer(name).chain(handler)`` **做不到**——它只登记在
            Python 侧，不会桥接到 Rust（桥接能力 ``PyCapabilityRuntime``
            的 ``chain_python_handler`` 存在，但 Python 层默认不调用它）。
            要走通请用 Rust 侧配置（``RuntimeBuilder``），或在 Rust 嵌入场景
            下显式 ``register_execute_handler`` 等注册函数。

            纯 Python capability（``Routing`` / ``Scheduling`` / ``RetryPolicy``）
            无此限制——它们始终走 Python 分发路径，``start()`` 后追加立即生效。
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
                    "For Routing/Scheduling/RetryPolicy this is fine (Python-only "
                    "capabilities). For Execute/Store/Transport, "
                    "register before start() or configure Rust-side handlers via "
                    "RuntimeBuilder to override Rust-internal behavior.",
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
