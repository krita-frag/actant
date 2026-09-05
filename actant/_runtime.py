"""Python 侧 Runtime：统一注册表 + effect dispatcher。

`Runtime` 是 actant 的统一运行时入口。所有扩展点（Routing、Scheduling、
Transport、Store、Lifecycle 等）都注册为 Layer，由 Runtime 按 name 分桶存储。

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

import asyncio
import logging
import os
import pickle
import shutil
import tempfile
import threading
import zlib
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any, Literal, cast

from actant.capabilities import (
    BUILTIN_CAPABILITIES,
    RUST_BACKED_CAPABILITIES,
    TASK_LIFECYCLE,
    CapabilityMeta,
    EffectKind,
    RetryCtx,
    RouteCtx,
    ScheduleCtx,
)
from actant.exceptions import ActantError, InvalidStateError, NotFoundError, reconstruct_error

if TYPE_CHECKING:
    from actant.actant import _ActantConfig
    from actant.task import AsyncResult

# 当前线程的 Runtime 上下文。使用 threading.local 保证每个线程有独立的活跃 Runtime。
# `Task.submit` 沿途的 perform/ask 请求经本变量解析活跃 Runtime；任务本身在 worker
# 子进程中执行，不共享本局部。父进程侧结果回调（`_on_task_result`）与 TaskLifecycle
# 事件发布显式经 `use_runtime(rt)` 注入自身作为活跃 Runtime。
_runtime_local = threading.local()

_logger = logging.getLogger("actant.runtime")

# Rust `_TaskCompletion.state` 枚举的字符串镜像（src/py/types.rs）。
# 集中定义避免与 Rust 枚举的裸字符串契约散落在比较逻辑中。
TASK_STATE_RUNNING = "Running"
TASK_STATE_COMPLETED = "Completed"
TASK_STATE_FAILED = "Failed"
TASK_STATE_CANCELLED = "Cancelled"
TASK_STATE_SKIPPED = "Skipped"
# Rust Orchestrator workflow 状态字符串镜像（get_workflow_state 返回的
# "state" 字段，src/runtime/workflow/ 的持久化状态机）。
WORKFLOW_STATE_COMPLETED = "Completed"
WORKFLOW_STATE_FAILED = "Failed"


def get_current_runtime() -> Runtime | None:
    """返回当前线程的活跃 Runtime，无则返回 `None`。

    仅查询 ``threading.local``；子线程需通过 ``use_runtime(rt)`` 显式继承。
    任务在 worker 子进程执行，不解析本局部；父进程侧 ``_on_task_result`` 回调
    用 ``use_runtime(self)`` 显式注入运行时后发布 TaskLifecycle 事件。
    """
    return getattr(_runtime_local, "runtime", None)


def require_runtime() -> Runtime:
    """返回当前线程的活跃 Runtime，无则抛 ``InvalidStateError``。

    与 ``get_current_runtime`` 的区别：此函数在无 Runtime 时直接抛异常，
    减少调用方手写 ``if rt is None: raise`` 的样板代码。

    Raises:
        InvalidStateError: 无活跃 Runtime。
    """
    rt = getattr(_runtime_local, "runtime", None)
    if rt is None:
        raise InvalidStateError(
            "no active Runtime; wrap your code in `with actant.Runtime() as rt:`"
        )
    return cast(Runtime, rt)


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
            请在 ``start()`` **之前** 通过 ``Runtime.layer(name).chain(handler)``
            注册，或直接配置 Rust 侧 handler（参见 ``RuntimeBuilder``）。

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


class Runtime:
    """Capability 注册表 + effect dispatcher。

    所有扩展点都注册为 `Layer`，由 `Runtime` 按 name 分桶存储。
    `Runtime` 提供 `ask` / `perform` / `emit` 三种执行模型。

    # 选择哪个构造入口？

    - **绝大多数用户用 ``Runtime.with_defaults()``**：预装 Routing/Scheduling/RetryPolicy
      默认 handler + generic execute handler，开箱即用。
    - ``Runtime()`` 仅注册内置 capability 的空 layer（无默认 handler），适合需要完全
      自定义策略层的高级用户。注意：``start()`` 仍会注册 generic execute handler，
      使 ``@task`` 的 ``submit()`` 可用。

    # 线程安全

    `Runtime` 内部使用 `threading.RLock` 保护注册表，支持并发注册与查询。
    ``Task.submit`` 通过 ``use_runtime(rt)`` 在独立线程池中传播上下文。

    # 生命周期

    `Runtime` 可作为 context manager 使用：

    ```python
    with actant.Runtime.with_defaults() as rt:
        rt.layer("Routing").chain(my_router)
        result = actant.ask("Routing", ctx)
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
    ) -> None:
        self._layers: dict[str, list[Callable[[Any], Any]]] = {}
        self._metas: dict[str, CapabilityMeta] = {}
        self._lock = threading.RLock()
        self._started = False
        self._rust_runtime: Any = None  # PyCapabilityRuntime，延迟绑定
        self._rust_core: Any = None  # _RuntimeCore，保活统一运行时
        self._name = name  # 节点名称，传给 _RuntimeCore
        # 未指定 data_dir 时自动生成唯一临时目录，避免多实例/多测试共享 LMDB 数据
        # 导致状态污染或测试不稳定。Runtime 拥有该目录时 stop() 负责清理。
        # auto_data_dir 标志随构造参数传给 _build_config 决定网络 preset，
        # 不做路径嗅探——用户显式传入的路径（哪怕位于系统临时目录下）一律尊重。
        if data_dir is None:
            data_dir = tempfile.mkdtemp(prefix="actant-")
            self._owns_data_dir = True
        else:
            self._owns_data_dir = False
        self._data_dir = data_dir  # 持久化目录，传给 _RuntimeCore
        self._config: _ActantConfig | None = self._build_config(
            config=config,
            data_dir=data_dir,
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
        # 活跃 flow 线程注册表：Runtime.stop() 时 join 它们，
        # 避免子线程在 Runtime 关闭后继续访问已释放的资源。
        self._flow_threads: list[threading.Thread] = []
        # TaskLifecycle 事件批处理器：start() 时创建、stop() 时关闭。
        # 父进程侧每任务 2 次 emit（started/completed/failed）在后台线程批量派发，
        # 降低高吞吐下 event_bus 的 publish 次数。未创建（未 start）时为 None。
        self._event_batcher: Any = None
        # 注册所有内置 capability 为空 layer
        for cap_name, meta in BUILTIN_CAPABILITIES.items():
            self._metas[cap_name] = meta
            self._layers[cap_name] = []

    @staticmethod
    def _build_config(
        *,
        config: _ActantConfig | None,
        data_dir: str | None,
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
            data_dir=data_dir,
            drain_timeout_secs=drain_timeout_secs,
            remote_fallback_delay_ms=remote_fallback_delay_ms,
            scheduler=scheduler,
        )

    @classmethod
    def with_defaults(
        cls,
        *,
        name: str | None = None,
        data_dir: str | None = None,
        config: _ActantConfig | None = None,
        max_concurrent_tasks: int | None = None,
        default_task_timeout_ms: int | None = None,
        drain_timeout_secs: int | None = None,
        remote_fallback_delay_ms: int | None = None,
        scheduler: str | None = None,
    ) -> Runtime:
        """创建 Runtime 并注册所有内置默认 handler。

        等价于::

            rt = Runtime(name=name, data_dir=data_dir, config=config)
            _register_default_handlers(rt)

        用户随后可通过 `rt.layer("Routing").chain(custom)` 追加自定义 handler，
        自定义 handler 优先级更高（chain 顺序决定 ask 的决策顺序）。

        Args:
            name: 节点名称（默认自动生成）。
            data_dir: 持久化数据目录（默认系统临时目录）。
            config: ``actant.actant._ActantConfig``，控制 Worker 行为
                （并发槽位、超时、drain 等）。``None`` 用默认配置。
                与 ``max_concurrent_tasks`` 等便捷参数互斥（提供 ``config`` 时便捷参数被忽略）。
            max_concurrent_tasks: 单节点最大并发任务数（便捷参数）。不指定时由
                ``_ActantConfig`` 默认取 ``num_cpus::get()``（CPU 核数）；如需更低
                并发，建议在 ``config`` 中显式指定或启动多个 Worker 进程分担负载。
            default_task_timeout_ms: 任务默认超时毫秒（便捷参数，默认 30000）。
            drain_timeout_secs: 退出时等待在途任务的最长秒数（便捷参数，默认 30）。
            remote_fallback_delay_ms: 远程回退延迟毫秒（便捷参数，默认 500）。
            scheduler: 调度器类型（便捷参数，``"priority"`` 或 ``"fifo"``，默认 ``"priority"``）。
        """
        rt = cls(
            name=name,
            data_dir=data_dir,
            config=config,
            max_concurrent_tasks=max_concurrent_tasks,
            default_task_timeout_ms=default_task_timeout_ms,
            drain_timeout_secs=drain_timeout_secs,
            remote_fallback_delay_ms=remote_fallback_delay_ms,
            scheduler=scheduler,
        )
        _register_default_handlers(rt)
        return rt

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

        与 ``with_defaults`` 的区别：
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
            drain_timeout_secs=drain_timeout_secs if drain_timeout_secs is not None else 5,
            scheduler=scheduler,
        )
        _register_default_handlers(rt)
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

        与 ``with_defaults`` 的区别：
        - **强制要求** ``payload_signing_key`` 非空（否则 ``ValueError``），
          防止生产部署因默认空密钥导致 payload 完整性保护被静默禁用。
        - 显式设置 ``require_payload_signing=True``：任一节点密钥不匹配
          时跨节点消息会被对端拒绝，提供集群身份认证。
        - ``data_dir`` 必填：生产部署必须显式指定持久化目录，
          避免使用临时目录导致重启后状态丢失。

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
            ValueError: ``payload_signing_key`` 为空，或 ``data_dir`` 为空。
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

        # 生产安全告警：allowed_peer_ids 为空时 peer_allowed 返回 true（allow-all），
        # 任意 iroh 节点都可加入集群 gossip，存在被恶意节点投递伪造消息的风险。
        # 此处仅 warn 而非强制报错——某些受信内网环境（如 K8s pod 间通信）
        # 确实依赖默认的 "接受所有 peer" 行为；让用户根据告警决定是否收紧。
        effective_network = network if network is not None else _NetworkConfig(preset="local")
        if not effective_network.allowed_peer_ids:
            _logger.warning(
                "Runtime.production(): network.allowed_peer_ids is empty — "
                "the node will accept gossip/wire messages from ANY iroh peer. "
                "For production clusters, pass network=_NetworkConfig("
                "allowed_peer_ids=['node-a', 'node-b', ...]) to restrict "
                "membership to known peers."
            )

        config = _ActantConfig(
            payload_signing_key=payload_signing_key,
            network=effective_network,
            failover=failover,
            gossip=gossip,
            max_concurrent_tasks=max_concurrent_tasks,
            default_task_timeout_ms=default_task_timeout_ms,
            data_dir=data_dir,
            drain_timeout_secs=drain_timeout_secs,
            remote_fallback_delay_ms=remote_fallback_delay_ms,
            scheduler=scheduler,
            require_payload_signing=True,
        )
        rt = cls(name=name, data_dir=data_dir, config=config)
        _register_default_handlers(rt)
        return rt

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
            return Layer(self, meta)

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
                    "For Routing/Scheduling/RetryPolicy this is fine (Python-only "
                    "capabilities). For Execute/Store/Transport, "
                    "register before start() or configure Rust-side handlers via "
                    "RuntimeBuilder to override Rust-internal behavior.",
                    name,
                )
        return self

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
        return self

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
        _logger.debug("ask %r: all %d handler(s) abstained", name, len(handlers))
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
        """
        try:
            return asyncio.get_running_loop()
        except RuntimeError as e:
            raise InvalidStateError(
                f"async dispatch requires a running asyncio event loop: {e}"
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

        对每个 ``(name, request)`` 元组调用 ``_dispatch_perform_async``，
        用 ``asyncio.gather`` 并发等待所有结果。

        单个 perform 失败不中断批量——失败项在结果列表对应位置为 ``Exception`` 实例
        （``return_exceptions=True``）。

        Args:
            items: ``(capability_name, request)`` 元组列表。

        Returns:
            ``asyncio.Future``，await 后得到结果列表，顺序与输入一致。
        """
        loop = self._get_asyncio_loop()

        async def _run_batch() -> list[Any]:
            coros = [self._perform_one_async(name, req) for name, req in items]
            return await asyncio.gather(*coros, return_exceptions=True)

        return asyncio.ensure_future(_run_batch(), loop=loop)

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

    def start(self) -> Runtime:
        """启动 Runtime，设置为当前线程的活跃 Runtime。

        启动时创建统一的 Rust `_RuntimeCore`，并从中获取 `_CapabilityRuntime`
        视图作为 Python handler 缺失/弃权时的内置回退路径。两者共享同一套
        tokio runtime 与 capability 句柄。

        分布式任务执行：启动时创建 Rust `_RuntimeCore` 并以进程池后端执行任务。
        提交的任务由 Rust `ExecuteHandler` → `ProcessTaskDispatcher` 分发到
        worker 子进程执行；注册结果回调，使 ``AsyncResult`` 能在任务完成时被解析。
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
                )
                self._rust_runtime = core.capability_runtime()
                # 保留 core 引用，避免其被提前释放导致 tokio runtime 关闭。
                # 后续如需访问 workflow / network 句柄，也通过此 core。
                self._rust_core = core
            core = self._rust_core
            if core is not None:
                core.register_task_result_callback(self._on_task_result)
            self._started = True
            # 创建 TaskLifecycle 事件批处理器。flush 发生在后台线程，经
            # _publish_task_event 把本 Runtime 绑定到该线程上下文后再 emit，
            # 避免进程池后端下每任务 2 次同步 emit 成为高吞吐瓶颈。
            from actant.task._helpers import _EventBatcher

            self._event_batcher = _EventBatcher(render=self._publish_task_event)
        _runtime_local.runtime = self
        # 启动 Worker 守护循环（订阅 P2P topic + 任务执行循环）。
        # 非阻塞：worker.run() 在 tokio 后台 spawn，使任务能被调度执行。
        # serve() 内部会事件驱动等待 Worker 进入任务执行循环（基于 watch channel），
        # 因此此处无需 time.sleep(0.1) 等硬编码等待。
        self.serve()
        # 写入 node_meta.json 供 `actant status --data-dir` 离线读取。
        self._write_node_meta()
        return self

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
        from actant.task._helpers import _emit_task_event

        with use_runtime(self):
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
        from actant.task._helpers import _emit_task_event

        with use_runtime(self):
            _emit_task_event(kind, task_id, workflow_id, **kwargs)

    def _task_lifecycle_consumers(self) -> bool:
        """批次口径下 ``TaskLifecycle`` 是否仍有 Python 消费者。

        读取 ``_layers[TASK_LIFECYCLE]`` 的非空性。加锁避免与 ``layer()/chain()``
        追加发生竞态；``TaskLifecycle`` 为纯 emit 能力，无 handler 即无可观测消费方。
        """
        with self._lock:
            return bool(self._layers.get(TASK_LIFECYCLE))

    def _on_task_result(self, completion: Any) -> None:
        """任务结果回调：由 Rust event_bus 触发，解析对应的 ``AsyncResult``。

        ``completion`` 是 ``_TaskCompletion``，含 task_id / state / result / error。
        ``result`` 是 dispatch handler 返回的字节，编码为
        ``cloudpickle.dumps((success, payload_obj))``——payload_obj 是
        未序列化的 result 或 exc 对象（消除双层 dumps 优化）。
        """
        import cloudpickle as _cp

        task_id = completion.task_id
        with self._lock:
            handle = self._tasks.get(task_id)
        if handle is None:
            return
        # 进程池后端：任务在 worker 子进程内执行，子进程以 silent=True 抑制
        # TaskLifecycle 事件。started/completed/failed 事件改由本回调在父进程侧
        # 依据 completion 状态发布，保持进程隔离前后可观测性契约一致。
        state = completion.state
        if state == TASK_STATE_RUNNING:
            # Worker 已开始执行任务，更新状态为 running（非终态，不从注册表移除）。
            handle._set_running()
            self._emit_batch("started", task_id, handle.workflow_id)
            return
        if state == TASK_STATE_COMPLETED:
            # dispatch handler 返回 cloudpickle.dumps((success, payload_obj))
            # payload_obj 是未序列化的 result/exc 对象。
            raw = completion.result or b""
            try:
                success, payload_obj = _cp.loads(raw)
            except (pickle.UnpicklingError, ValueError, TypeError) as e:
                # 无法解码：当作失败处理
                handle._set_error(f"undecodable result for task {task_id!r}: {e}")
                self.unregister_task(task_id)
                return
            if success:
                # 直接传对象给 _set_result_obj，避免再 dumps/loads 往返。
                # 使用 _set_result_obj 而非 _set_result：标记 _result_is_obj=True，
                # 避免任务返回 bytes（如 echo(b"x")）被 result() 误当作序列化结果。
                handle._set_result_obj(payload_obj)
                self._emit_batch("completed", task_id, handle.workflow_id)
            else:
                # 失败：payload_obj 是异常对象。
                # 检查是否为 TaskCancelledError，是则设置 cancelled 状态。
                from actant.exceptions import TaskCancelledError as _TCE

                if isinstance(payload_obj, _TCE):
                    handle._set_cancelled()
                else:
                    # _set_error 接受 BaseException，内部 dumps 存入 _error_payload。
                    handle._set_error(payload_obj)
                    self._emit_batch(
                        "failed",
                        task_id,
                        handle.workflow_id,
                        error=f"{type(payload_obj).__name__}: {payload_obj}",
                    )
        elif state == TASK_STATE_FAILED:
            # error 字段是 Rust 端生成的字符串。通过 reconstruct_error 解析
            # kind 前缀（``[actant:KIND] message``）重建对应 Python 异常子类，
            # 保留错误类型（如 timeout → ActantTimeoutError）。

            handle._set_error(reconstruct_error(completion.error or "unknown error"))
            self._emit_batch(
                "failed",
                task_id,
                handle.workflow_id,
                error=completion.error or "unknown error",
            )
        elif state == TASK_STATE_CANCELLED:
            handle._set_cancelled()
        elif state == TASK_STATE_SKIPPED:
            handle._set_error("task skipped")
        # 任务终态后从注册表移除（本地孤儿回收）。
        self.unregister_task(task_id)

    def stop(self, timeout: float | None = None) -> None:
        """停止 Runtime，关闭 Rust tokio runtime 并清除线程上下文。

        shutdown 失败时仍会清理线程上下文，避免后续 effect 调用卡在已损坏的
        Runtime 上；同时向上抛出首个错误，让调用方感知 shutdown 异常。

        shutdown 前先 join 活跃 flow 线程，避免子线程在 Runtime 关闭后
        继续访问已释放的 Rust 资源（如通过 ``Task.submit`` 调用已 shutdown
        的 Worker）。

        Args:
            timeout: 等待在途任务的最长秒数（分布式任务由 Worker drain）。
                ``None`` 表示无限等待。``0`` 表示立即关闭。
        """
        shutdown_err: Exception | None = None

        # 先 join 活跃 flow 线程，避免子线程在 Rust runtime 关闭后访问它。
        # timeout 语义：
        #   None → 使用 config.drain_timeout_secs（默认 30），与 Rust Worker drain 对齐
        #   0    → 跳过 join（立即关闭），log warning
        #   >0   → 使用指定秒数
        with self._lock:
            flow_threads = list(self._flow_threads)
            self._flow_threads.clear()
        if timeout == 0:
            if flow_threads:
                _logger.warning(
                    "stop(timeout=0): skipping join of %d active flow thread(s)",
                    len(flow_threads),
                )
        else:
            join_timeout = timeout
            if join_timeout is None:
                join_timeout = float(
                    self._config.drain_timeout_secs
                    if self._config is not None
                    and getattr(self._config, "drain_timeout_secs", None)
                    else 30
                )
            for t in flow_threads:
                if t.is_alive() and t is not threading.current_thread():
                    try:
                        t.join(timeout=join_timeout)
                    except Exception as e:
                        _logger.warning("failed to join flow thread %s", t.name, exc_info=True)
                        if shutdown_err is None:
                            shutdown_err = e
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
                core.shutdown()
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
        with self._lock:
            self._tasks.clear()
            self._cancelled_tasks.clear()
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

    @property
    def node_id(self) -> str:
        """本节点的 Actant 内部 ID（可能是用户提供的 ``name``）。"""
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started")
        return str(self._rust_core.node_id())

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

    def dial_async(self, addr: str) -> Any:
        """``dial`` 的异步版本，返回 ``asyncio.Future``。

        在 ``async`` 函数中 ``await`` 此返回值，避免阻塞 Python 事件循环。
        """
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started")
        return self._rust_core.dial_async(addr)

    def add_gossip_peer(self, peer_id: str) -> None:
        """将远端节点加入 gossip 网络（不建立直连）。

        Args:
            peer_id: 对端 ``peer_id`` 返回的 iroh 公钥 hex 字符串
                （注意不是 ``node_id``）。
        """
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started")
        self._rust_core.add_gossip_peer(peer_id)

    def add_gossip_peer_async(self, peer_id: str) -> Any:
        """``add_gossip_peer`` 的异步版本，返回 ``asyncio.Future``。"""
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started")
        return self._rust_core.add_gossip_peer_async(peer_id)

    def discover_peers(self) -> list[str]:
        """返回当前已知对等节点的 peer_id 列表。"""
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started")
        return list(self._rust_core.discover_peers())

    def discover_peers_async(self) -> Any:
        """``discover_peers`` 的异步版本，返回 ``asyncio.Future``。

        ``await`` 后得到 ``list[str]``。
        """
        if self._rust_core is None:
            raise InvalidStateError("Runtime not started")
        return self._rust_core.discover_peers_async()

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

    def handler_count(self, name: str) -> int:
        """返回指定 capability 的 handler 数量。"""
        with self._lock:
            return len(self._layers.get(name, []))

    def register_task(self, task_id: str, handle: AsyncResult) -> None:
        """注册一个已提交任务的句柄（由 ``Task.submit`` 调用）。"""
        with self._lock:
            self._tasks[task_id] = handle

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
        """线程安全地查询任务是否已被标记为预取消。

        替代直接访问 ``runtime._cancelled_tasks``（无锁读取违反锁定纪律）。
        """
        with self._lock:
            return task_id in self._cancelled_tasks

    def register_flow_thread(self, thread: threading.Thread) -> None:
        """注册活跃 flow 线程，供 ``stop()`` 时 join。"""
        with self._lock:
            self._flow_threads.append(thread)

    def unregister_flow_thread(self, thread: threading.Thread) -> None:
        """移除已完成的 flow 线程。"""
        with self._lock:
            try:
                self._flow_threads.remove(thread)
            except ValueError:
                _logger.debug("flow thread %s was already unregistered", thread.name)

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

    def submit_dag(
        self,
        workflow_id: str,
        nodes: list[Any],
        edges: list[tuple[str, str]],
        *,
        failure_strategy: str | None = None,
        default_retry_policy: Any = None,
    ) -> None:
        """将 ``@flow`` 执行期记录的 DAG 提交到 Rust Orchestrator 持久化。

        由 ``@flow`` 在函数体成功返回后调用。此后该 workflow 的状态由
        Orchestrator 状态机驱动，可通过 :meth:`get_workflow_state` 查询。

        Args:
            workflow_id: 工作流唯一标识（``@flow`` 生成）。
            nodes: ``_DagNode`` 列表（由 ``FlowDAG.to_nodes`` 构造）。
            edges: ``(upstream_task_id, downstream_task_id)`` 依赖边列表。
            failure_strategy: 失败策略（``"fail_fast"`` / ``"continue"``）。
            default_retry_policy: DAG 级默认重试策略（``_RetryPolicy``）。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        core.submit_dag(
            workflow_id,
            nodes,
            edges,
            failure_strategy,
            default_retry_policy,
        )

    def complete_workflow(
        self,
        workflow_id: str,
        outcomes: list[tuple[str, bool, bytes]],
    ) -> None:
        """把 flow 已执行完成的任务结果回灌 Orchestrator，驱动状态机推进到终态。

        由 ``@flow`` 在 DAG 提交后调用：成功项调用 ``COMPLETE_TASK``，失败项
        调用 ``FAIL_TASK``（WorkflowLevel 作用域），使持久化工作流从 Pending
        推进到 Completed / Failed，从而让生命周期事件与 Orchestrator 状态一致。

        Args:
            workflow_id: 工作流唯一标识（``@flow`` 生成）。
            outcomes: ``[(task_id, success, result_bytes)]``，需按拓扑序传入
                （依赖先于下游）。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        core.complete_workflow(workflow_id, outcomes)

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

    def list_workflows(self) -> list[str]:
        """列出 Orchestrator 中活跃 workflow 的 id 列表。

        Raises:
            InvalidStateError: Runtime 未启动。
        """
        core = self._rust_core
        if core is None:
            raise InvalidStateError("Runtime not started: rust_core is None")
        return list(core.list_workflows())

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
        from actant.actant import prometheus_text

        return prometheus_text()

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
# - `Routing` / `Scheduling` / `RetryPolicy` 是策略型 capability，**仅 Python 层**实现。
#   Rust 核心不提供默认 handler（见 `src/runtime/capability.rs` 顶部注释）。
# - `Runtime.with_defaults()` 注册以下 Python 默认 handler，用户可通过
#   `rt.layer(name).chain(custom)` 覆盖（后注册的 handler 在 ask 中优先决策）。


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


class DefaultRetryPolicy:
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
    rt.layer("RetryPolicy").chain(DefaultRetryPolicy())


__all__ = [
    "DefaultRetryPolicy",
    "FifoScheduler",
    "Layer",
    "LocalRouter",
    "Runtime",
    "get_current_runtime",
    "use_runtime",
]
