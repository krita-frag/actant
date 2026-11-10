"""Journal & invocation 契约测试。

journal 是工作流历史的**稳定消费面**：``get_workflow_journal`` 导出小写事件名、
任务/等待点标识、业务字节与提交描述（invocation），供任意进程或非 Python host
读取。本文件固定以下契约：

1. 顺序与字段：``submitted`` 在前，节点/任务事件齐全，``seq`` 严格递增；
2. invocation：flow 名、版本与入参字节随 ``Submitted`` 落盘，
   入参可经 ``cloudpickle`` 还原；
3. 业务字节：``NodeAdded`` 携带节点入参、``TaskCompleted`` 携带结果帧，
   可直接导出为 JSON（bytes base64）；
4. 跨重启：同一 ``data_dir`` 重启后既有 journal 前缀逐字段不变（历史是持久的
   事实源，重启只可能追加恢复事件）；
5. 未知工作流：返回空 journal / ``None``，不抛错。
"""

from __future__ import annotations

import base64
import json

import cloudpickle

from actant import Runtime, flow, task
from actant.workflow import current_workflow_id

_SEEN: list[str] = []


@task(name="journal_add")
def _add(v: int) -> int:
    """最小任务：返回确定性结果，供断言结果帧。"""
    return v + 1


@flow(name="journal-flow", version=3)
def _pipeline(v: int) -> int:
    _SEEN.append(current_workflow_id() or "")
    return _add.submit(v).result()


def _run_pipeline(rt: Runtime, v: int) -> str:
    """在当前 Runtime 上跑一遍 flow，返回 workflow_id。"""
    _SEEN.clear()
    assert _pipeline(v) == v + 1
    return _SEEN[0]


class TestJournalContract:
    def test_journal_exposes_invocation_and_ordered_events(self) -> None:
        with Runtime(name="journal-node") as rt:
            wid = _run_pipeline(rt, 41)
            journal = rt.get_workflow_journal(wid)
            invocation = rt.get_workflow_invocation(wid)

        kinds = [e["kind"] for e in journal]
        assert kinds[0] == "submitted", kinds
        assert "node_added" in kinds, kinds
        assert kinds.count("task_completed") == 1, kinds
        assert kinds[-1] == "completed", kinds

        seqs = [e["seq"] for e in journal]
        assert seqs == sorted(seqs) and len(seqs) == len(set(seqs)), seqs
        assert all(e["workflow_id"] == wid for e in journal), journal

        assert journal[0]["invocation"] == invocation
        assert invocation is not None
        assert invocation["workflow_id"] == wid
        assert invocation["name"] == "journal-flow"
        assert invocation["version"] == 3

        args, kwargs = cloudpickle.loads(invocation["args"])
        assert (args, kwargs) == ((41,), {})

        done = next(e for e in journal if e["kind"] == "task_completed")
        assert done["task_id"], done
        success, value = cloudpickle.loads(done["payload"])
        assert success is True
        assert value == 42

        node_added = next(e for e in journal if e["kind"] == "node_added")
        assert isinstance(node_added["payload"], bytes) and node_added["payload"]

    def test_journal_is_durable_across_restart(self, tmp_path) -> None:
        data_dir = str(tmp_path)
        rt = Runtime(name="journal-node", data_dir=data_dir)
        rt.start()
        try:
            wid = _run_pipeline(rt, 7)
            before = rt.get_workflow_journal(wid)
        finally:
            rt.stop()

        rt2 = Runtime(name="journal-node", data_dir=data_dir)
        rt2.start()
        try:
            after = rt2.get_workflow_journal(wid)
            invocation = rt2.get_workflow_invocation(wid)
        finally:
            rt2.stop()

        # 既有历史逐字段保持；重启只追加恢复事件，不改写已有记录。
        assert after[: len(before)] == before
        assert {e["kind"] for e in after[len(before) :]} <= {"recovered"}
        assert invocation is not None and invocation["name"] == "journal-flow"
        assert invocation["args"] == before[0]["invocation"]["args"]

    def test_journal_exports_to_json_with_base64_payloads(self) -> None:
        with Runtime(name="journal-node") as rt:
            wid = _run_pipeline(rt, 1)
            journal = rt.get_workflow_journal(wid)

        exported: list[dict[str, object]] = []
        for record in journal:
            item = dict(record)
            if isinstance(item["payload"], bytes):
                item["payload"] = base64.b64encode(item["payload"]).decode("ascii")
            inv = item["invocation"]
            if isinstance(inv, dict):
                inv = dict(inv)
                inv["args"] = base64.b64encode(inv["args"]).decode("ascii")
                item["invocation"] = inv
            exported.append(item)

        restored = json.loads(json.dumps(exported))
        assert restored[0]["kind"] == "submitted"
        args, kwargs = cloudpickle.loads(
            base64.b64decode(restored[0]["invocation"]["args"])
        )
        assert (args, kwargs) == ((1,), {})

    def test_unknown_workflow_returns_empty(self) -> None:
        with Runtime(name="journal-node") as rt:
            assert rt.get_workflow_journal("no-such-workflow") == []
            assert rt.get_workflow_invocation("no-such-workflow") is None
