"""内置默认 handler。

LocalRouter / FifoScheduler / DefaultRetryPolicy 是 ERH 的策略默认值；
`_DefaultValueStoreHandler` 是 ValueStore 的 Python→Rust blob 桥。
`_register_default_handlers` 在 Runtime 工厂（with_defaults/test/production）
中调用。守则 1 允许它们住主仓库——删除会破坏开箱即用的单节点体验。
"""

from __future__ import annotations

import zlib
from typing import TYPE_CHECKING, cast

from actant.capabilities import (
    RetryCtx,
    RouteCtx,
    ScheduleCtx,
    ValueStoreReq,
)
from actant.exceptions import InvalidStateError

if TYPE_CHECKING:
    from actant._runtime import Runtime

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


def _register_default_handlers(rt: Runtime) -> None:
    """向 Runtime 注册 Python 策略层默认 handler。

    仅注册 `Routing` / `Scheduling` / `RetryPolicy` 三个纯 Python capability。
    其余 Rust-backed capability 的默认 handler 由 `RuntimeBuilder` 在 Rust 侧注入
    （如 StoreHandler / ExecuteHandler），Python handler 缺失时自动回退。
    """
    rt.layer("Routing").chain(LocalRouter())
    rt.layer("Scheduling").chain(FifoScheduler())
    rt.layer("RetryPolicy").chain(DefaultRetryPolicy())
