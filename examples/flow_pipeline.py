"""DAG 编排流水线：以 ``@flow`` + ``task.submit`` 构建真实可查询的工作流。

本示例演示动态 DAG 语义：flow 函数体以命令式方式调用 ``task.submit()``，
依赖通过 ``AsyncResult`` 自动解析（下游 submit 阻塞等待上游结果）隐式表达。
执行期由 ``FlowDAG`` 记录器捕获节点与依赖边，flow 返回后提交到 Rust
Orchestrator 持久化，并把任务实际结果回灌驱动状态机推进到终态。

数据流::

    fetch(取数)
      ├── transform_raw(清洗) ──┐
      └── analyze_stats(统计) ──┼── partition(分流：走向/失败) ── store(入库落盘)
                   └────────────┘

  - 分支：``fetch`` 完成后并行执行 ``transform_raw`` 与 ``analyze_stats``
  - 汇合：``partition`` 同时等待两个上游结果
  - 失败路径：某批次命中 ``fail`` 标记时 task 抛异常，经重试后仍失败，
    工作流整体进入 ``Failed``，后续任务不执行

演示能力：
  - 工作流生命周期事件订阅（``WorkflowLifecycle``：submitted/started/completed/failed）
  - 提交后按 ID 查询持久化状态（``get_workflow_state``）
  - 失败路径：任务重试耗尽后状态机标记 Failed，事件与状态一致

运行::

    uv run python examples/flow_pipeline.py
    uv run python examples/flow_pipeline.py --fail   # 演示失败路径
"""

from __future__ import annotations

import argparse
from typing import Any

from actant import Runtime, flow, task

# 生命周期事件订阅的处理器：记录 workflow_id，供提交后按 ID 查询终态状态。
_event_wf_ids: list[str] = []


def on_lifecycle(event: Any) -> None:
    _event_wf_ids.append(event.workflow_id)
    print(f"  [lifecycle] {event.kind:>9s}  {event.workflow_id}")


@task(name="fetch")
def fetch(src: int) -> int:
    """取数：返回输入，模拟 I/O。"""
    return src


@task(name="transform_raw")
def transform_raw(x: int) -> int:
    """清洗：对原始数做线性变换。"""
    return x * 2 + 1


@task(name="analyze_stats")
def analyze_stats(x: int) -> int:
    """统计：返回计数标记，模拟聚合。"""
    return x + 10


@task(name="partition", retries=2, retry_delay_ms=50)
def partition(a: int, b: int, *, fail: bool = False) -> str:
    """汇合分流：同时等待上游结果后，按 ``fail`` 标记决定走成功/失败路径。"""
    if fail:
        raise ValueError("simulated downstream failure")
    return f"ok:{a}:{b}"


@task(name="store")
def store(payload: str) -> str:
    """入库落盘：消费 partition 的成功输出。"""
    return f"stored[{payload}]"


@flow(name="flow-pipeline")
def pipeline(src: int, *, fail: bool = False) -> str:
    """编排函数体：命令式组合任务，依赖经 AsyncResult 自动解析。

    返回值即整个 flow 的返回值（下游 ``store`` 的结果）。
    """
    # 根任务：无依赖。
    raw = fetch.submit(src)

    # 分支：并行从 raw 派生两个任务，互不依赖、可并行执行。
    transformed = transform_raw.submit(raw)
    analyzed = analyze_stats.submit(raw)

    # 汇合：partition 同时等待两个上游 AsyncResult，自动阻塞解析。
    routed = partition.submit(transformed, analyzed, fail=fail)

    # 失败路径的末尾任务：partition 成功时才执行。
    return store.submit(routed).result()


def main() -> None:
    parser = argparse.ArgumentParser(description="Actant DAG 编排流水线示例")
    parser.add_argument(
        "--fail",
        action="store_true",
        help="演示失败路径：partition 任务抛异常，重试耗尽后工作流进入 Failed",
    )
    args = parser.parse_args()

    with Runtime.with_defaults() as rt:
        # 订阅工作流生命周期事件（Rust 内部路径调用，需在 start 前注册）。
        rt.layer("WorkflowLifecycle", "emit").chain(on_lifecycle)

        print("=== 执行 @flow pipeline ===")
        try:
            result = pipeline(5, fail=args.fail)
        except Exception as exc:
            print(f"flow 抛出: {type(exc).__name__}: {exc}")
            terminated = True
        else:
            print(f"flow 返回: {result}")
            terminated = False

        # 从生命周期事件捕获实际 workflow_id，按 ID 查询持久化终态状态。
        # flow 采用 eager 执行，DAG 提交后经 complete_workflow 立即可达终态，
        # 工作流已离开"活跃"列表（list_workflows 返回空属预期），但可按 ID 查询。
        wf_id = _event_wf_ids[-1] if _event_wf_ids else None
        if wf_id is None:
            print("\n未捕获到工作流生命周期事件")
            return

        state = rt.get_workflow_state(wf_id)
        print(f"\n工作流状态 [{wf_id}]:")
        print(f"  state            = {state['state']}")
        print(f"  succeeded/total  = {state['succeeded_count']}/{state['total_count']}")
        print(f"  failure_strategy = {state['failure_strategy']}")
        for tid, ts in state["tasks"].items():
            err = f", error={ts['error']!r}" if ts["error"] else ""
            print(f"  task {tid:<22s} state={ts['state']}{err}")

    if not terminated:
        print("\n示例完成：DAG 已提交、任务执行、状态回灌并持久化。")


if __name__ == "__main__":
    main()
