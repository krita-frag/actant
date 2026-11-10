"""测试台专用入口：故障注入与节点模拟。

这些操作不属于生产 ``Runtime`` 公开面，仅供 testkit 与端到端测试模拟
网络分区、心跳延迟、时钟漂移和进程崩溃。调用不改变持久化格式，也不影响
正常编排语义。

用法::

    from actant import Runtime, testkit

    rt = Runtime(...)
    rt.start()
    testkit.close_peer(rt, other.node_id)   # 模拟单向分区
    testkit.delay_heartbeat(rt, 60_000)     # 暂停对外心跳
    testkit.kill_node(rt)                   # 无 drain 关闭，模拟崩溃
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from actant.exceptions import InvalidStateError

if TYPE_CHECKING:
    from actant._internal.runtime import Runtime

__all__ = [
    "close_peer",
    "delay_heartbeat",
    "inject_hlc_drift",
    "kill_node",
    "reopen_peer",
]


def _core(runtime: Runtime) -> Any:
    core = runtime._rust_core
    if core is None:
        raise InvalidStateError("Runtime not started")
    return core


def close_peer(runtime: Runtime, target: str) -> None:
    """模拟本节点到 ``target`` 的单向网络分区。

    立即将 ``target`` 移出本节点 peer 视图，并忽略其后续心跳，直到
    :func:`reopen_peer`。用于测试失联判定与副本接管。
    """
    _core(runtime).close_peer(target)


def reopen_peer(runtime: Runtime, target: str) -> None:
    """解除 :func:`close_peer` 施加的分区模拟。"""
    _core(runtime).reopen_peer(target)


def delay_heartbeat(runtime: Runtime, ms: int) -> None:
    """在接下来 ``ms`` 毫秒内不广播心跳（``0`` 立即恢复）。

    模拟网络抖动/心跳延迟；对端会按 ``failure_timeout`` 判定失联。
    """
    _core(runtime).delay_heartbeat(ms)


def inject_hlc_drift(runtime: Runtime, ms: int) -> None:
    """给本节点 HLC 施加时钟偏移（毫秒，可正可负），``0`` 恢复。"""
    _core(runtime).inject_hlc_drift(ms)


def kill_node(runtime: Runtime) -> None:
    """立即关闭本节点（不等待在途任务 drain），模拟进程崩溃。

    与 :meth:`Runtime.stop` 的差异：``timeout=0`` 不做优雅 drain。
    """
    runtime.stop(timeout=0)
