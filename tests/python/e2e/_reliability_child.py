"""可靠性矩阵测试的子进程节点入口。

以 ``python -m tests.python.e2e._reliability_child`` 启动一个真实 iroh 节点：
dial 父节点后把 ``node_id``/``peer_id``/``endpoint_addr`` 写入 JSON 信息文件
（父进程轮询该文件获取连接信息），随后 ``serve()`` 常驻直至被外部 SIGKILL。

节点强杀只能发生在独立进程上，这是 ``test_node_kill_*`` 系列用例的
故障注入手段；进程内 Runtime 无法表达"强杀节点"语义。

传入第 5 个参数（marker 文件路径）时进入**编排节点模式**：先提交一个
``@flow``（函数体内提交一个长任务），把工作流 ID 一并写进信息文件。
``TestOrchestratorTakeover`` 用它构造"编排节点被强杀"的场景——只有让
子进程自己持有工作流，接管语义才被真正驱动。
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time

import actant
from actant.tasks import task


def tuned_config():
    """缩短心跳 / 失效检测 / heads 广播周期。

    默认值（心跳 2s、失效 8s、heads 30s）下"检测到失联 + 状态副本扩散"要
    40s 量级，超出用例预算。约束仍是 ``heartbeat < failure < lease``。
    """
    return actant.actant._ActantConfig(
        payload_signing_key="",
        failover=actant.actant._FailoverConfig(
            heartbeat_interval_ms=500,
            failure_timeout_ms=2000,
            lease_duration_ms=4000,
            lease_expiry_check_interval_secs=1,
        ),
        gossip=actant.actant._GossipConfig(heads_broadcast_interval_ms=1000),
    )


def main() -> int:
    parent_addr = sys.argv[1]
    info_path = sys.argv[2]
    data_dir = sys.argv[3]
    name = sys.argv[4] if len(sys.argv) > 4 else "child-node"
    flow_marker = sys.argv[5] if len(sys.argv) > 5 else None

    wf_ids: list[str] = []
    # 子进程日志默认静默（stderr 已被父进程重定向到 workdir/<name>.log）。
    # 跨节点问题只能从"远端视角"定位，排查时用 ACTANT_CHILD_LOG_LEVEL=INFO
    # 打开即可看到收到的消息、状态请求与拒绝原因。
    logging.basicConfig(
        level=os.environ.get("ACTANT_CHILD_LOG_LEVEL", "WARNING").upper(),
        format="%(levelname)s %(name)s: %(message)s",
    )
    rt = actant.Runtime(
        name=name,
        data_dir=data_dir,
        config=tuned_config() if flow_marker else None,
    )
    if flow_marker:
        # WorkflowLifecycle 是 Python emit 能力，必须在 start() 前链入。
        rt.layer("WorkflowLifecycle", "emit").chain(
            lambda e: wf_ids.append(e.workflow_id)
        )
    rt.start()
    try:
        rt.dial(parent_addr)
        if flow_marker:
            _submit_flow(rt, flow_marker, wf_ids)
        info = {
            "node_id": rt.node_id,
            "peer_id": rt.peer_id,
            "endpoint_addr": rt.listen_addresses()["endpoint_addr"],
            "workflow_id": wf_ids[0] if wf_ids else "",
        }
        # 父进程按「文件存在」判定就绪，因此必须原子发布：直接写 info_path 会让
        # 轮询方读到刚创建、尚未写入内容的 0 字节文件（json.load 报 Expecting value）。
        staging = f"{info_path}.tmp"
        with open(staging, "w", encoding="utf-8") as f:
            json.dump(info, f)
        os.replace(staging, info_path)
        rt.serve()
        while True:
            time.sleep(1.0)
    finally:
        rt.stop()


def _submit_flow(rt: actant.Runtime, marker: str, wf_ids: list[str]) -> None:
    """提交一个只含单个长任务的工作流，并等它拿到工作流 ID。

    任务体 sleep 8s：足够父进程在它跑完前强杀节点，又不至于让接管后的重跑
    拖爆用例预算。
    """
    from tests.python._helpers import reliability_tasks as tasks_mod

    with actant.use_runtime(rt):
        slow = task(tasks_mod.touch_and_sleep)

        @actant.flow(name="orchestrator-takeover")
        def pipeline() -> None:
            slow.submit(marker, 8.0)

        pipeline()

    deadline = time.monotonic() + 10.0
    while not wf_ids and time.monotonic() < deadline:
        time.sleep(0.05)
    if not wf_ids:
        raise RuntimeError("flow did not emit a workflow id within 10s")


if __name__ == "__main__":
    sys.exit(main())
