"""网络与节点辅助：启动 _Node、连接 peer、等待发现。

用于 integration 和 e2e 层测试，避免每个测试重复写节点生命周期管理。
"""

from __future__ import annotations

import contextlib
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from actant._node import _Node


def run_node_in_thread(
    app: _Node,
    ready_event: threading.Event | None = None,
    timeout_s: float = 30.0,
) -> threading.Thread:
    """在后台线程启动 _Node 并等待就绪。

    Args:
        app: 未启动的 _Node 实例。
        ready_event: 可选的就绪信号 Event；None 时内部创建。
        timeout_s: 等待 _runtime 初始化的超时秒数。

    Returns:
        运行 app.run() 的 daemon 线程。

    Raises:
        TimeoutError: _runtime 在 timeout_s 内未就绪。
        RuntimeError: app.run() 启动即失败。
    """
    if ready_event is None:
        ready_event = threading.Event()

    error: list[BaseException] = []

    def runner() -> None:
        try:
            app.run()
        except BaseException as exc:  # pragma: no cover - defensive
            error.append(exc)
        finally:
            ready_event.set()

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if app._runtime is not None:
            ready_event.set()
            return t
        if error:
            raise RuntimeError(f"app.run() failed: {error[0]!r}") from error[0]
        if not t.is_alive():
            break
        time.sleep(0.05)
    if error:
        raise RuntimeError(f"app.run() failed: {error[0]!r}") from error[0]
    raise TimeoutError(f"_Node lifecycle did not initialize within {timeout_s}s")


async def connect_peers(app_a: _Node, app_b: _Node) -> None:
    """双向连接两个节点：B dial A，A add_gossip_peer(B)。

    dial() 单向建立 B→A 连接并自动将 A 加入 B 的 gossip topics。
    A 不会自动感知 B，需显式 add_gossip_peer。
    """
    addrs_a = await app_a._runtime.listen_addresses()
    endpoint_addr = addrs_a["endpoint_addr"]
    with contextlib.suppress(Exception):
        await app_b._runtime.dial(endpoint_addr)
    peer_id_b = app_b._runtime.peer_id()
    await app_a._runtime._add_gossip_peer(peer_id_b)


async def wait_for_peers(
    app: _Node,
    min_peers: int = 1,
    timeout_s: float = 10.0,
    interval_s: float = 0.2,
) -> bool:
    """等待 failover manager 发现至少 min_peers 个对等节点。

    Returns:
        True 若在 timeout_s 内发现足够 peer；False 超时。
    """
    import asyncio

    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        peers = app._runtime.get_peer_infos()  # type: ignore[union-attr]
        if len(peers) >= min_peers:
            return True
        await asyncio.sleep(interval_s)
    return False
