"""可靠性矩阵测试的子进程节点入口。

以 ``python -m tests.python.e2e._reliability_child`` 启动一个真实 iroh 节点：
dial 父节点后把 ``node_id``/``peer_id``/``endpoint_addr`` 写入 JSON 信息文件
（父进程轮询该文件获取连接信息），随后 ``serve()`` 常驻直至被外部 SIGKILL。

节点强杀只能发生在独立进程上，这是 ``test_node_kill_*`` 系列用例的
故障注入手段；进程内 Runtime 无法表达"强杀节点"语义。
"""

from __future__ import annotations

import json
import sys
import time

import actant


def main() -> int:
    parent_addr = sys.argv[1]
    info_path = sys.argv[2]
    data_dir = sys.argv[3]
    name = sys.argv[4] if len(sys.argv) > 4 else "child-node"

    rt = actant.Runtime.with_defaults(name=name, data_dir=data_dir)
    rt.start()
    rt.dial(parent_addr)
    info = {
        "node_id": rt.node_id,
        "peer_id": rt.peer_id,
        "endpoint_addr": rt.listen_addresses()["endpoint_addr"],
    }
    with open(info_path, "w", encoding="utf-8") as f:
        json.dump(info, f)
    rt.serve()
    while True:
        time.sleep(1.0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
