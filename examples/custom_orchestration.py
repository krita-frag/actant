"""自定义编排：展示 Python 层编排循环的三个扩展点。

Actant 的编排逻辑完全在 Python 层（`actant._orchestration.OrchestrationLoop`），
Rust 核心仅提供状态机原语。本示例演示三种自定义方式：

1. 自定义 TaskRouter：替换默认的 LeastLoadedRouter，实现基于标签的路由。
2. 自定义条件分支：通过 branch() 注入业务条件函数。
3. 任务监控钩子：通过节点事件回调观察任务生命周期。

运行方式:
    python examples/custom_orchestration.py
"""

import actant
from actant import TaskRouter, NodeCapacity, flow, branch, task
from typing import Any


# ---------------------------------------------------------------------------
# 扩展点 1：自定义 TaskRouter
# ---------------------------------------------------------------------------

class TagPreferredRouter(TaskRouter):
    """优先将任务路由到带有特定标签的节点，回退到 LeastLoaded 策略。

    自定义路由器只需实现 `route(local_node, task_id, task_meta, peer_capacities)` 方法：
      - local_node: 本地节点 ID
      - task_id: 任务实例 ID
      - task_meta: 含 name/tags/priority 的元数据字典
      - peer_capacities: dict[node_id, NodeCapacity]，peer 容量快照
    返回选中的 node_id，或 None 表示本地执行。
    """

    def __init__(self, preferred_tag: str) -> None:
        self._preferred_tag = preferred_tag

    def route(
        self,
        local_node: str,
        task_id: str,
        task_meta: dict[str, Any],
        peer_capacities: dict[str, NodeCapacity],
    ) -> str | None:
        tags = task_meta.get("tags") or []
        # 带有 preferred_tag 的任务优先路由到有可用容量的 peer
        if self._preferred_tag in tags:
            for node_id, cap in peer_capacities.items():
                if cap.available > 0:
                    return node_id
        # 回退：选择可用容量最大的节点
        best: str | None = None
        best_avail = 0
        for node_id, cap in peer_capacities.items():
            if cap.available > best_avail:
                best = node_id
                best_avail = cap.available
        return best


# ---------------------------------------------------------------------------
# 定义任务
# ---------------------------------------------------------------------------

@task
def check_threshold(x: int) -> int:
    """返回输入值，用于条件判断。"""
    return x


@task(tags=["gpu"])
def heavy_compute(x: int) -> int:
    """模拟 GPU 密集型任务，通过 tags 标记便于路由器识别。"""
    return x * x


@task
def light_compute(x: int) -> int:
    """轻量级任务，无特殊标签。"""
    return x + 1


# ---------------------------------------------------------------------------
# 扩展点 2：自定义条件分支
# ---------------------------------------------------------------------------

@flow
def conditional_workflow(x: int):
    """根据 check_threshold 结果选择 heavy_compute 或 light_compute。

    branch() 接收一个条件函数，运行时由编排循环调用该函数评估
    上游任务结果，决定激活哪个分支。条件函数签名：(result) -> bool。
    """
    result = check_threshold(x)
    # result > 10 时走 heavy_compute，否则走 light_compute
    br = branch(result, lambda r: r > 10, heavy_compute(result), light_compute(result))
    # BranchRef 作为返回值，编排循环会等待激活的分支完成
    return br


# ---------------------------------------------------------------------------
# 扩展点 3：任务监控钩子
# ---------------------------------------------------------------------------

def on_task_start(workflow_id: str, task_name: str) -> None:
    print(f"  [START] workflow={workflow_id[:8]} task={task_name}")


def on_task_complete(workflow_id: str, task_name: str, success: bool) -> None:
    status = "OK" if success else "FAIL"
    print(f"  [DONE ] workflow={workflow_id[:8]} task={task_name} {status}")


def main() -> None:
    # 启动节点，注入自定义路由器
    router = TagPreferredRouter(preferred_tag="gpu")
    node = actant.start("custom-orch-node", signing_key="example-key", router=router, max_concurrent_tasks=2)

    # 注册任务生命周期钩子（内部 API，用于观测）
    node._register_event("task_start", on_task_start)
    node._register_event("task_complete", on_task_complete)

    try:
        # 提交工作流：x=20 触发 heavy_compute 分支
        print("提交 conditional_workflow(20) — 应走 heavy_compute 分支")
        result1 = actant.submit(conditional_workflow, 20, signing_key="example-key").get_sync(timeout=10.0)
        print(f"结果: {result1.value}\n")  # heavy(20)=400

        # 提交工作流：x=5 触发 light_compute 分支
        print("提交 conditional_workflow(5) — 应走 light_compute 分支")
        result2 = actant.submit(conditional_workflow, 5, signing_key="example-key").get_sync(timeout=10.0)
        print(f"结果: {result2.value}\n")  # light(5)=6

        print(f"workflow 1 状态: {result1.state}")
        print(f"workflow 2 状态: {result2.state}")
    finally:
        actant.stop()


if __name__ == "__main__":
    main()
