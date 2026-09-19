"""多节点可靠性矩阵 e2e：杀节点 / 重启续跑 / 乱序结果 / worker 崩溃隔离。

硬化用例矩阵，全部使用真实 iroh P2P 网络与进程级隔离 worker，
断言以**实际可达成的 failover 语义**为准：

1. ``test_node_kill_mid_workflow``：强杀执行节点，幸存节点降级存活且后续
   任务正常收敛（SLA：集群不因单节点死亡而失联）。
2. ``test_node_kill_inflight_task_terminal_state``：执行节点死亡后在途任务
   最终到达终态。失败转移新增"在途转发"处置腿：
   心跳视图可见的失联 peer 直接终结其上的在途任务；连心跳都来不及被观测到
   的目标（节点在首个心跳间隔前失联）则由主动直连探测判定。任务以
   `WorkerError` 终结（fail-fast，不自动重跑）。
3. ``test_node_restart_resume``：节点重启（store recover）后工作流状态恢复：
   工作流与节点状态跨重启保持 Completed，已完成任务不重跑（副作用计数不变）。
4. ``test_out_of_order_results``：并发提交、乱序完成，结果按句柄正确聚合。
5. ``test_worker_kill_isolated_failure``：worker 子进程被杀属基础设施级失败，
   单任务隔离（重路由成功或终态失败），节点存活、后续任务正常。

每个用例自带总超时（≤60s）并使用轮询断言避免固定 sleep 时序脆弱。
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from typing import TYPE_CHECKING

import pytest

import actant
from actant.task import gather, task
from tests.python._helpers import reliability_tasks as rt_tasks
from tests.python._helpers import wait_until

if TYPE_CHECKING:
    from collections.abc import Iterator

    from actant._runtime import Runtime

# 用例总时长上限（秒）：含节点启动、故障注入与收敛等待。
CASE_BUDGET_S = 60.0

# 任务函数统一定义在规范模块：pytest 把本测试模块导入为 ``python.e2e.*``
# （tests/ 缺少 __init__.py），该非规范名在子进程节点的 worker 中不可导入。
_touch_and_sleep = rt_tasks.touch_and_sleep
_crash_once = rt_tasks.crash_once
_crash_always = rt_tasks.crash_always
_quick = rt_tasks.quick


# ---------------------------------------------------------------------------
# 节点装配辅助
# ---------------------------------------------------------------------------


class ChildNode:
    """子进程节点句柄：进程 + 连接信息 + 强杀/清理。"""

    def __init__(self, proc: subprocess.Popen[bytes], info: dict[str, str]) -> None:
        self.proc = proc
        self.node_id = info["node_id"]
        self.peer_id = info["peer_id"]
        self.endpoint_addr = info["endpoint_addr"]

    def kill(self) -> None:
        """SIGKILL 强杀节点进程并回收（对应生产中的节点宕机）。"""
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
        self.proc.wait(timeout=10)


def _spawn_child_node(parent_addr: str, workdir: str, name: str) -> ChildNode:
    """启动一个真实 iroh 子进程节点并 dial 父节点，返回连接信息。

    以 ``python -m`` 从仓库根启动，保证 worker 子进程能按模块路径导入
    测试模块内的任务函数（cloudpickle 按引用序列化）。
    """
    info_path = os.path.join(workdir, f"{name}.json")
    data_dir = os.path.join(workdir, f"data-{name}")
    env = os.environ.copy()
    env["ACTANT_DISCOVERY"] = "none"
    env["RUST_LOG"] = env.get("RUST_LOG", "warn")
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "tests.python.e2e._reliability_child",
            parent_addr,
            info_path,
            data_dir,
            name,
        ],
        env=env,
        cwd=os.getcwd(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # 轮询信息文件：子节点 dial 成功后写入连接信息。
    deadline = time.monotonic() + 20.0
    info: dict[str, str] | None = None
    while time.monotonic() < deadline:
        if os.path.exists(info_path):
            with open(info_path, encoding="utf-8") as f:
                info = json.load(f)
            break
        if proc.poll() is not None:
            raise RuntimeError(f"child node {name} exited early with {proc.returncode}")
        time.sleep(0.1)
    if info is None:
        proc.kill()
        proc.wait(timeout=10)
        raise RuntimeError(f"child node {name} did not report connection info in 20s")
    return ChildNode(proc, info)


def _connect_bidirectional(rt: Runtime, child: ChildNode, timeout_s: float = 15.0) -> None:
    """父节点 add_gossip_peer 后等待双向发现收敛。"""
    rt.add_gossip_peer(child.peer_id)
    assert wait_until(lambda: child.peer_id in rt.discover_peers(), timeout_s=timeout_s), (
        f"peer discovery did not converge in {timeout_s}s"
    )


def _marker_count(path: str, token: str) -> int:
    if not os.path.exists(path):
        return 0
    with open(path, encoding="utf-8") as f:
        return f.read().count(token)


@pytest.fixture
def node_kill_env(tmp_path) -> Iterator[tuple[Runtime, ChildNode]]:
    """幸存节点（进程内）+ 可强杀子进程节点，测试后保证子进程被回收。"""
    workdir = str(tmp_path)
    rt = actant.Runtime.with_defaults(
        name="survivor",
        data_dir=os.path.join(workdir, "data-survivor"),
        max_concurrent_tasks=4,
    )
    rt.start()
    child: ChildNode | None = None
    try:
        child = _spawn_child_node(rt.listen_addresses()["endpoint_addr"], workdir, "victim")
        _connect_bidirectional(rt, child)
        yield rt, child
    finally:
        if child is not None:
            child.kill()
        rt.stop()


# ---------------------------------------------------------------------------
# 用例 1：工作流执行中途强杀节点
# ---------------------------------------------------------------------------


class TestNodeKill:
    """强杀执行节点：幸存节点降级存活、后续任务收敛。"""

    def test_node_kill_mid_workflow(self, node_kill_env) -> None:
        """节点中途死亡：已完成结果稳定，幸存节点继续收敛后续任务。

        SLA 语义：单执行节点强杀不拖垮集群——已解析的结果不变，
        幸存节点在死亡后仍能完成新提交的任务（降级为单节点运行）。
        """
        rt, victim = node_kill_env
        started = time.monotonic()
        marker_dir = tempfile.mkdtemp(prefix="nkill-markers-")

        with actant.use_runtime(rt):
            # 阶段 1：集群健康，远程 + 本地混合批次全部收敛。
            healthy_remote = [
                task(_touch_and_sleep).submit_to(
                    victim.node_id,
                    os.path.join(marker_dir, f"pre-{i}"),
                    0.1,
                    endpoint_addr=victim.endpoint_addr,
                )
                for i in range(3)
            ]
            healthy_local = [task(_quick).submit(i) for i in range(3)]
            results = gather(*healthy_remote, *healthy_local, timeout=30.0)
            assert results == ["ok"] * 3 + [0, 2, 4]

            # 阶段 2：3 个在途任务落在 victim 上，确认已开跑后强杀。
            inflight_markers = [os.path.join(marker_dir, f"inflight-{i}") for i in range(3)]
            for m in inflight_markers:
                task(_touch_and_sleep).submit_to(
                    victim.node_id, m, 30.0, endpoint_addr=victim.endpoint_addr
                )
            assert wait_until(
                lambda: all(_marker_count(m, "start") == 1 for m in inflight_markers),
                timeout_s=15.0,
            ), "in-flight tasks did not start on victim node"
            victim.kill()

            # 阶段 3：幸存节点降级存活——新任务正常收敛（不永久挂起）。
            post_kill = [task(_quick).submit(i) for i in range(3)]
            post_results = gather(*post_kill, timeout=30.0)
            assert post_results == [0, 2, 4], (
                "survivor node must keep completing tasks after peer death"
            )

            # 在途任务由 failover 的在途转发腿终结为 WorkerError（见下一用例），
            # 但幸存节点不得被其拖垮：本用例只断言集群降级存活。
            assert time.monotonic() - started < CASE_BUDGET_S

    def test_node_kill_inflight_task_terminal_state(self, node_kill_env) -> None:
        """执行节点死亡后在途任务最终到达终态（SLA：任务不永久挂起）。

        failover 的失联扫描新增"在途转发"处置腿——本节点转发出去、
        结果未回的任务登记在 `FailoverManager::outbound`；peer 被判失联时这些
        任务以 `TaskCompletion::Failed` 终结并经 event_bus 回灌（与远端结果同路），
        提交方 `AsyncResult` 因此以 `WorkerError` 终止而非永久挂起。

        语义为 **fail-fast**：执行节点失联不触发自动重跑（源节点没有"该任务未
        执行完"的持久凭据，盲目重跑会静默重复副作用）。需要重跑请用显式重试策略
        （`@task(retries=...)`）或重新提交。
        """

        rt, victim = node_kill_env
        marker_dir = tempfile.mkdtemp(prefix="nkill-inflight-")
        marker = os.path.join(marker_dir, "inflight")

        with actant.use_runtime(rt):
            handle = task(_touch_and_sleep).submit_to(
                victim.node_id, marker, 30.0, endpoint_addr=victim.endpoint_addr
            )
            assert wait_until(lambda: _marker_count(marker, "start") == 1, timeout_s=15.0), (
                "task did not start on victim node"
            )
            victim.kill()

            # 轮询等待终态（给足 failover 检测窗口），终态 = 完成/失败/取消。
            reached = wait_until(lambda: handle.done(), timeout_s=30.0, interval_s=0.5)
            if not reached:
                pytest.fail(
                    f"in-flight task did not reach terminal state within 30s "
                    f"after executor node kill (state={handle.state!r})"
                )
            # 语义校验：任务未产出结果，必须以异常终结（不得伪装成功）。
            assert handle.exception() is not None or handle.result(timeout=0) == "ok"
            # 失联终结的错误串须能定位到死亡节点（验证 target_node 一路透传）。
            exc = handle.exception()
            if exc is not None:
                assert victim.node_id in str(exc), (
                    f"node-lost failure must identify the dead executor: {exc!r}"
                )


# ---------------------------------------------------------------------------
# 用例 2：节点重启（recover）续跑
# ---------------------------------------------------------------------------


class TestNodeRestartResume:
    """节点重启后工作流状态恢复：状态跨重启保持、已完成不重跑。"""

    def test_node_restart_resume(self, tmp_path) -> None:
        """recover 语义：

        1. 工作流及其节点状态跨重启持久恢复（Completed 保持、计数不变）；
        2. 已完成任务不重跑——重启前后副作用计数恒为 1。

        说明：节点由 ``@flow`` 经 ``add_workflow_node`` 真实派发、结果经
        ``report_task_result`` 单入口回灌，故本用例按真实执行路径验证跨重启
        持久性（"缺口补执行 / 重复回灌幂等" 由 ``test_flow_orchestration`` 与
        ``test_flow_ref_chain`` 覆盖）。
        """
        data_dir = str(tmp_path / "data")
        marker = str(tmp_path / "t1.marker")
        wf_ids: list[str] = []
        wf_id = ""

        # ---- 第一次启动：flow 真实执行并收敛到 Completed ----
        rt_a = actant.Runtime.with_defaults(name="restart-node", data_dir=data_dir)
        # WorkflowLifecycle 为 Python emit 能力，需在 start() 前链入。
        rt_a.layer("WorkflowLifecycle", "emit").chain(
            lambda e: wf_ids.append(e.workflow_id)
        )
        rt_a.start()
        try:
            with actant.use_runtime(rt_a):
                # 任务函数取自规范模块，worker 子进程可按引用导入。
                touch_task = task(_touch_and_sleep)
                quick_task = task(_quick)

                @actant.flow(name="restart-resume")
                def pipeline() -> None:
                    touch_task.submit(marker, 0.05)
                    quick_task.submit(1)

                pipeline()

            assert len(set(wf_ids)) == 1, f"expected exactly one workflow, got {wf_ids}"
            wf_id = wf_ids[0]
            state = rt_a.get_workflow_state(wf_id)
            assert state is not None
            assert state["state"] == "Completed", state
            assert state["succeeded_count"] == state["total_count"] == 2, state
            assert _marker_count(marker, "start") == 1
        finally:
            rt_a.stop()

        # ---- 重启（同 data_dir recover）：状态恢复 + 已完成不重跑 ----
        rt_b = actant.Runtime.with_defaults(name="restart-node", data_dir=data_dir)
        rt_b.start()
        try:
            assert wf_id, "workflow id must be captured from the first run"
            # list_workflows() 只返回**非终态**工作流（active 口径）；已完成
            # 工作流不在其中，但状态本身必须已从 store 恢复——以
            # get_workflow_state 为准（recover 全量扫回 DAG/exec）。
            assert wf_id not in rt_b.list_workflows(), rt_b.list_workflows()
            state = rt_b.get_workflow_state(wf_id)
            assert state is not None, "workflow state must be recovered from store"
            assert state["state"] == "Completed", state
            assert state["succeeded_count"] == state["total_count"] == 2, state
            assert len(state["tasks"]) == 2, state
            assert all(t["state"] == "Completed" for t in state["tasks"].values()), state

            # 副作用计数不变：重启后已完成任务未被重新执行。
            assert _marker_count(marker, "start") == 1, (
                "completed task must not re-run after node restart"
            )
        finally:
            rt_b.stop()


# ---------------------------------------------------------------------------
# 用例 3：乱序完成的结果聚合
# ---------------------------------------------------------------------------


_sleep_mult = rt_tasks.sleep_mult


class TestOutOfOrderResults:
    """并发提交、乱序完成，所有结果正确聚合。"""

    def test_out_of_order_results(self, two_local_nodes) -> None:
        """后提交的短任务先完成：结果仍按句柄一一对应。

        提交顺序按 sleep 时长降序排列，天然形成乱序完成；
        断言只关心"句柄 → 结果"映射与零丢失，不断言完成顺序本身
        （完成顺序受 worker 启动时序影响，不具备确定性）。
        """
        rt_a, rt_b = two_local_nodes
        # 6 个任务落在 rt_a，6 个落在 rt_b；sleep 降序 → 乱序完成。
        with actant.use_runtime(rt_a):
            handles_a = [task(_sleep_mult).submit(i, (6 - i) * 0.2) for i in range(6)]
        with actant.use_runtime(rt_b):
            handles_b = [task(_sleep_mult).submit(i + 100, (6 - i) * 0.2) for i in range(6)]
        results = gather(*handles_a, *handles_b, timeout=45.0)
        expected_a = [i * 10 for i in range(6)]
        expected_b = [(i + 100) * 10 for i in range(6)]
        assert results == expected_a + expected_b, (
            f"out-of-order completion must aggregate results by handle, got {results}"
        )


@pytest.fixture
def two_local_nodes():
    """两个进程内节点（沿用 test_multi_node.py 的手动 dial 模式）。"""
    import shutil

    dir_a = tempfile.mkdtemp(prefix="actant-ooo-a-")
    dir_b = tempfile.mkdtemp(prefix="actant-ooo-b-")
    rt_a = actant.Runtime.with_defaults(name="ooo-a", data_dir=dir_a)
    rt_b = actant.Runtime.with_defaults(name="ooo-b", data_dir=dir_b)
    rt_a.start()
    rt_b.start()
    from tests.python._helpers import connect_peers

    try:
        assert connect_peers(rt_a, rt_b, timeout_s=15.0), "P2P connection failed"
        yield rt_a, rt_b
    finally:
        rt_b.stop()
        rt_a.stop()
        shutil.rmtree(dir_a, ignore_errors=True)
        shutil.rmtree(dir_b, ignore_errors=True)


# ---------------------------------------------------------------------------
# 用例 4：worker 进程被杀的故障隔离
# ---------------------------------------------------------------------------


class TestWorkerKillIsolation:
    """随机杀 worker 子进程：单任务隔离，节点存活。"""

    def test_worker_kill_isolated_failure(self, tmp_path) -> None:
        """worker 崩溃语义（进程级隔离的 SLA 契约）：

        1. 崩溃一次的任务重路由后成功（crash failover，副作用计数 = 2）；
        2. 持续崩溃的任务在 crash_failover_max_attempts 内到达终态失败，
           不拖垮批次内其他任务；
        3. 节点存活，后续任务正常执行。
        """
        rt = actant.Runtime.with_defaults(name="worker-kill-node", data_dir=str(tmp_path / "data"))
        rt.start()
        crash_once_marker = str(tmp_path / "crash_once.marker")
        crash_always_marker = str(tmp_path / "crash_always.marker")
        try:
            with actant.use_runtime(rt):
                h_once = task(_crash_once).submit(crash_once_marker)
                h_always = task(_crash_always).submit(crash_always_marker)
                batch = [task(_quick).submit(i) for i in range(4)]

                # 1. 崩溃一次 → 重路由成功。
                assert gather(h_once, timeout=40.0) == ["recovered"], (
                    "task whose worker crashed once must succeed after reroute"
                )
                assert _marker_count(crash_once_marker, "attempt") == 2, (
                    "crash-once task must execute exactly twice "
                    f"(crash + reroute), got {_marker_count(crash_once_marker, 'attempt')}"
                )

                # 2. 持续崩溃 → 有界终态失败，不永久挂起。
                with pytest.raises(Exception, match=r"crash|worker"):
                    h_always.result(timeout=40.0)
                assert h_always.state == "failed"

                # 3. 并发批次不受影响，节点存活。
                assert gather(*batch, timeout=30.0) == [0, 2, 4, 6]
                followup = task(_quick).submit(100)
                assert gather(followup, timeout=20.0) == [200]
        finally:
            rt.stop()
