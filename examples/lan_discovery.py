"""LAN 内网节点发现：``PeerBeacon`` + ``Dialer`` 组网演示。

单机起两个节点，组播信标互发现后经 ``rt.dial(addr)`` 建立直连并加入
对端 gossip，最终两个节点的 ``discover_peers()`` 都能看到对方。
Rust 核心零改动——依赖的全部是已接线的公开 API。

跨机部署：所有节点把 ``interface`` 指向 LAN 网卡 IP 即可；
默认发现受阻的环境可配 ``ACTANT_DISCOVERY=none`` 强制离线后完全
依赖本信标组网。

运行::

    uv run python examples/lan_discovery.py
"""

from __future__ import annotations

import tempfile
import time

from actant import Runtime
from actant.ext import PeerBeacon


def main() -> None:
    # 显式 data_dir 启用网络（自动临时目录会禁用 P2P）；两个节点共置一台
    # 机器，信标以 127.0.0.1 为组播出口。
    dir_a = tempfile.mkdtemp(prefix="actant-lan-a-")
    dir_b = tempfile.mkdtemp(prefix="actant-lan-b-")
    rt_a = Runtime(name="node-a", data_dir=dir_a)
    rt_b = Runtime(name="node-b", data_dir=dir_b)
    rt_a.start()
    rt_b.start()

    beacon_a = PeerBeacon(rt_a, interval_s=0.5, interface="127.0.0.1")
    beacon_b = PeerBeacon(rt_b, interval_s=0.5, interface="127.0.0.1")
    beacon_a.start()
    beacon_b.start()
    try:
        # 等待双向互发现（信标周期 0.5s，通常 1-2 个周期内完成）。
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            a_sees_b = rt_a.discover_peers()
            b_sees_a = rt_b.discover_peers()
            if a_sees_b and b_sees_a:
                break
            time.sleep(0.3)

        print("=== 互发现结果 ===")
        print(f"node-a peers : {rt_a.discover_peers()}")
        print(f"node-b peers : {rt_b.discover_peers()}")
        print(f"node-a 地址簿: {beacon_a.dialer.known_peers()}")
        print(f"node-b 地址簿: {beacon_b.dialer.known_peers()}")
        if not (rt_a.discover_peers() and rt_b.discover_peers()):
            print("!! 15s 内未完成互发现（检查本机组播回环可用性）")
    finally:
        beacon_a.stop()
        beacon_b.stop()
        rt_a.stop()
        rt_b.stop()


if __name__ == "__main__":
    main()
