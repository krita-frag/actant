"""跨节点 e2e 测试辅助：启动 Runtime、连接 peer、等待发现。

提供同步辅助函数，避免每个 e2e 测试重复写节点生命周期管理。
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from actant._runtime import Runtime


def wait_until(predicate, timeout_s: float = 10.0, interval_s: float = 0.1) -> bool:
    """轮询 predicate 直到返回真或超时。

    Returns:
        ``True`` 若 predicate 在超时前返回真；``False`` 超时。
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return False


def connect_peers(rt_a: Runtime, rt_b: Runtime, timeout_s: float = 10.0) -> bool:
    """双向连接两个 Runtime 节点：B dial A，A add_gossip_peer(B)。

    dial() 单向建立 B→A 连接并自动将 A 加入 B 的 gossip topics。
    A 不会自动感知 B，需显式 add_gossip_peer，传入 B 的 iroh peer_id。

    Args:
        rt_a: 节点 A（被 dial 方）。
        rt_b: 节点 B（dial 方）。
        timeout_s: 等待双向发现的超时秒数。

    Returns:
        ``True`` 若双向发现成功；``False`` 超时。
    """
    addrs_a = rt_a.listen_addresses()
    endpoint_addr = addrs_a["endpoint_addr"]
    rt_b.dial(endpoint_addr)
    # add_gossip_peer 需要 iroh peer_id（公钥），不是 Actant node_id
    peer_id_b = rt_b.peer_id
    rt_a.add_gossip_peer(peer_id_b)
    # 等待双向发现：discover_peers 返回的是 iroh peer_id 列表
    return wait_until(
        lambda: peer_id_b in rt_a.discover_peers()
        and rt_a.peer_id in rt_b.discover_peers(),
        timeout_s=timeout_s,
    )


def wait_for_peers(
    rt: Runtime,
    min_peers: int = 1,
    timeout_s: float = 10.0,
    interval_s: float = 0.1,
) -> bool:
    """等待 Runtime 发现至少 min_peers 个对等节点。

    Returns:
        True 若在 timeout_s 内发现足够 peer；False 超时。
    """
    return wait_until(
        lambda: len(rt.discover_peers()) >= min_peers,
        timeout_s=timeout_s,
        interval_s=interval_s,
    )
