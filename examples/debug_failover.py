"""Quick diagnostic for failover peer discovery.

连接两个 worker 节点并检查 peer 发现是否正常工作。
"""

import actant
import asyncio
import threading
import time

from actant.config import NetworkConfig

# 启动两个节点，其中 A 作为 bootstrap
network_a = NetworkConfig(preset="none")
network_b = NetworkConfig(preset="none")

app_a = actant.start(
    "debug_a",
    signing_key="debug-key",
    port=0,
    network=network_a,
)
app_b = actant.start(
    "debug_b",
    signing_key="debug-key",
    port=0,
    network=network_b,
)


async def main():
    assert app_a._runtime is not None
    assert app_b._runtime is not None

    addrs_a = await app_a._runtime.listen_addresses()
    print(f"A addrs: {addrs_a}")

    endpoint_addr = addrs_a.get("endpoint_addr")
    if endpoint_addr:
        result = await app_b._runtime.dial(endpoint_addr)
        print(f"dial B->A result: {result}")

    await asyncio.sleep(3.0)

    peers_a = app_a._runtime.get_peer_infos()
    peers_b = app_b._runtime.get_peer_infos()
    print(f"A peers: {peers_a}")
    print(f"B peers: {peers_b}")

    actant.stop()


asyncio.run(main())
