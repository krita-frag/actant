"""提交幂等键的跨重启去重契约。

`idempotency_key` 随节点 ``data_dir`` 的 Store 持久化
（``__actant__:idem:{key} → task_id``）。重启后同键再提交时内存中没有可复用的
句柄，因此以 ``AlreadyExistsError`` 拒绝——关键的契约是**不会产生第二次执行**。
登记不自动过期，可经 ``Runtime.forget("idempotency_key", key)`` 显式清除。
"""

from __future__ import annotations

from pathlib import Path

import pytest

from actant import Runtime, task, use_runtime
from actant.exceptions import AlreadyExistsError


def _append_once(path: str) -> str:
    with open(path, "a") as fh:
        fh.write("run\n")
    return path


_append_once_task = task(name="idem_restart_append_once")(_append_once)


def _read_lines(path: str) -> list[str]:
    p = Path(path)
    return p.read_text().splitlines() if p.exists() else []


class TestIdempotencyPersistence:
    def test_key_survives_restart_and_blocks_second_execution(self, tmp_path) -> None:
        data_dir = str(tmp_path / "data")
        marker = str(tmp_path / "once.txt")

        rt = Runtime(name="idem-restart", data_dir=data_dir)
        with rt:
            with use_runtime(rt):
                handle = _append_once_task.submit(marker, idempotency_key="job-restart")
            assert handle.result() == marker
            first_task_id = handle.task_id
        assert _read_lines(marker) == ["run"]

        # 同一 data_dir 的新 Runtime（换 name 证明作用域是 data_dir 而非实例）。
        rt2 = Runtime(name="idem-restart-b", data_dir=data_dir)
        with rt2, use_runtime(rt2), pytest.raises(AlreadyExistsError) as exc:
            _append_once_task.submit(marker, idempotency_key="job-restart")

        message = str(exc.value)
        assert "job-restart" in message
        assert first_task_id in message
        assert _read_lines(marker) == ["run"], "重启后同键不得产生第二次执行"

    def test_unused_key_runs_after_restart(self, tmp_path) -> None:
        """未使用过的键在重启后照常执行（持久层只拦已登记的键）。"""
        data_dir = str(tmp_path / "data")
        marker = str(tmp_path / "fresh.txt")

        with Runtime(name="idem-restart", data_dir=data_dir):
            pass

        rt = Runtime(name="idem-restart", data_dir=data_dir)
        with rt:
            with use_runtime(rt):
                handle = _append_once_task.submit(marker, idempotency_key="job-fresh")
            assert handle.result() == marker
        assert _read_lines(marker) == ["run"]

    def test_forget_key_allows_resubmission(self, tmp_path) -> None:
        """删除登记后同键可重新提交（并真正执行一次）。"""
        data_dir = str(tmp_path / "data")
        marker = str(tmp_path / "forget.txt")

        rt = Runtime(name="idem-forget", data_dir=data_dir)
        with rt:
            with use_runtime(rt):
                first = _append_once_task.submit(marker, idempotency_key="job-forget")
                assert first.result() == marker
            # 同进程内同键提交复用内存中的首次句柄（不重复执行）。
            with use_runtime(rt):
                reused = _append_once_task.submit(marker, idempotency_key="job-forget")
                assert reused.task_id == first.task_id
            rt.forget("idempotency_key", "job-forget")
            with use_runtime(rt):
                second = _append_once_task.submit(marker, idempotency_key="job-forget")
                assert second.result() == marker
                assert second.task_id != first.task_id
        assert _read_lines(marker) == ["run", "run"]
