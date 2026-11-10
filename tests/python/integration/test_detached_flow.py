"""分离式 flow：提交、驱动、读取分属不同 Runtime 实例。

这是通用化的关键契约——工作流生命周期不依赖提交进程：

- **提交**：``start_flow`` 只落盘工作流外壳与 invocation（flow 名、版本、入参），
  不执行函数体；
- **驱动**：另一个持有相同 ``data_dir`` 与 flow 定义的 Runtime 调用 ``host_flows``，
  仅凭 journal 里的 flow 名与 ``cloudpickle`` 入参即可重放；
- **读取**：第三个 Runtime 用 ``FlowHandle`` 等终态、读持久化结果与 journal，
  不参与执行。

本文件用同一进程内的独立 ``Runtime`` 实例模拟这三个进程——状态只经 LMDB
交换，不共享任何 Python 内存。
"""

from __future__ import annotations

import cloudpickle
import pytest

from actant import FlowHandle, Runtime, flow, start_flow, task
from actant.exceptions import NotFoundError
from actant.workflow import FLOW_REGISTRY, FlowDefinition


@task(name="detach_double")
def _double(v: int) -> int:
    return v * 2


@flow(name="detach-flow", version=2, timeout_ms=60_000)
def _detach_pipeline(base: int, *, step: int = 1) -> int:
    return _double.submit(base + step).result()


def _runtime(data_dir: str) -> Runtime:
    return Runtime(name="detach-node", data_dir=data_dir)


class TestDetachedFlow:
    def test_submit_host_read_across_runtime_instances(self, tmp_path) -> None:
        data_dir = str(tmp_path)

        # ---- 提交方：不执行函数体 ----
        submitter = _runtime(data_dir)
        submitter.start()
        try:
            handles = [
                submitter.start_flow("detach-flow", i, step=10) for i in range(3)
            ]
            workflow_ids = [h.workflow_id for h in handles]
            for i, workflow_id in enumerate(workflow_ids):
                journal = submitter.get_workflow_journal(workflow_id)
                assert journal[0]["kind"] == "submitted", journal
                # 函数体未执行：只有提交与定义元数据，没有节点/任务事件。
                assert {e["kind"] for e in journal} <= {
                    "submitted",
                    "context_value",
                }, journal
                invocation = submitter.get_workflow_invocation(workflow_id)
                assert invocation is not None
                assert invocation["name"] == "detach-flow"
                assert invocation["version"] == 2
                args, kwargs = cloudpickle.loads(invocation["args"])
                assert (args, kwargs) == ((i,), {"step": 10})
        finally:
            submitter.stop()

        # ---- 执行方：仅凭 data_dir + 导入的定义驱动 ----
        host = _runtime(data_dir)
        host.start()
        try:
            driven = host.host_flows()
            assert set(workflow_ids) <= set(driven), (driven, workflow_ids)
        finally:
            host.stop()

        # ---- 读取方：不执行，只等终态并读结果 ----
        reader = _runtime(data_dir)
        reader.start()
        try:
            for i, workflow_id in enumerate(workflow_ids):
                handle = FlowHandle(reader, workflow_id, "detach-flow")
                assert handle.result(timeout_ms=10_000) == (i + 10) * 2
                kinds = [e["kind"] for e in handle.journal()]
                assert kinds[0] == "submitted"
                # 读取方重启会追加 recovered，故终态事件只需存在、不必在末尾。
                assert "completed" in kinds, kinds
                assert "task_completed" in kinds
                state = handle.state()
                assert state is not None and state["state"] == "Completed"
        finally:
            reader.stop()

    def test_start_flow_rejects_unregistered_name(self) -> None:
        with Runtime() as rt, pytest.raises(NotFoundError):
            start_flow("no-such-flow", runtime=rt)

    def test_submit_without_local_definition_uses_explicit_version(
        self, tmp_path
    ) -> None:
        """提交方未导入定义时，凭显式 version 即可落盘 invocation。"""
        rt = _runtime(str(tmp_path))
        rt.start()
        try:
            handle = rt.start_flow(
                "client-only-flow",
                7,
                version=3,
                timeout_ms=5_000,
                failure_strategy="continue",
            )
            invocation = rt.get_workflow_invocation(handle.workflow_id)
            assert invocation is not None
            assert invocation["name"] == "client-only-flow"
            assert invocation["version"] == 3
            args, kwargs = cloudpickle.loads(invocation["args"])
            assert (args, kwargs) == ((7,), {})
            assert rt.get_workflow_state(handle.workflow_id) is not None
        finally:
            rt.stop()

    def test_explicit_version_must_match_registered_definition(self) -> None:
        with Runtime() as rt, pytest.raises(
            ValueError, match="explicit version"
        ):
            start_flow("detach-flow", runtime=rt, version=99)

    def test_unregistered_definition_rejects_invalid_failure_strategy(
        self, tmp_path
    ) -> None:
        rt = _runtime(str(tmp_path))
        rt.start()
        try:
            with pytest.raises(ValueError, match="failure_strategy"):
                rt.start_flow(
                    "client-only-flow", version=1, failure_strategy="best_effort"
                )
        finally:
            rt.stop()

    def test_host_flows_skips_missing_definition(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """定义缺失时不静默、不崩溃：warning 后跳过，工作流保持活跃。"""
        data_dir = str(tmp_path)
        original = FLOW_REGISTRY.get("detach-flow")
        assert original is not None

        rt = _runtime(data_dir)
        rt.start()
        try:
            handle = rt.start_flow("detach-flow", 1)
            # 模拟执行进程里没有导入该 flow 定义。
            monkeypatch.setattr(FLOW_REGISTRY, "get", lambda name: None)
            assert rt.host_flows() == []
            state = rt.get_workflow_state(handle.workflow_id)
            assert state is not None and state["state"] != "Completed"
        finally:
            rt.stop()

    def test_definition_replay_uses_persisted_args(self, tmp_path) -> None:
        """执行方使用的入参来自 invocation，而非本地登记。"""
        data_dir = str(tmp_path)
        rt = _runtime(data_dir)
        rt.start()
        try:
            handle = rt.start_flow("detach-flow", 5, step=3)
        finally:
            rt.stop()

        # 用同名但默认入参不同的临时定义注册，验证 host 仍用持久化入参。
        original = FLOW_REGISTRY.get("detach-flow")
        assert original is not None

        def _wrong_defaults(base: int, *, step: int = 1000) -> int:
            return _double.submit(base + step).result()

        host = _runtime(data_dir)
        host.start()
        try:
            FLOW_REGISTRY.register(
                FlowDefinition(name="detach-flow", func=_wrong_defaults, version=2)
            )
            try:
                assert host.host_flows() == [handle.workflow_id]
            finally:
                FLOW_REGISTRY.register(original)
            assert FlowHandle(host, handle.workflow_id, "detach-flow").result(
                timeout_ms=10_000
            ) == (5 + 3) * 2
        finally:
            host.stop()
