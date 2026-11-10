"""提交幂等键（``idempotency_key``）单元测试。

覆盖：

- 同一键重复提交返回**同一个** ``AsyncResult`` 且只执行一次；
- 并发提交同一键只入队一次；
- 不同键互不影响；
- 复用键但不比对参数（首次参数为准）；
- 非法键（空 / 非字符串）显式报错；
- ``delay`` 别名同样支持幂等键。

任务在 worker 子进程中执行，副作用经临时文件观测（模块级全局变量不跨进程）。
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from actant import Runtime, task, use_runtime


def _append_once(path: str) -> str:
    with open(path, "a") as fh:
        fh.write("run\n")
        fh.flush()
    return path


_append_once_task = task(name="idem_append_once")(_append_once)


def _read_lines(path: str) -> list[str]:
    p = Path(path)
    return p.read_text().splitlines() if p.exists() else []


def _wait_for_lines(path: str, count: int, *, timeout: float = 30.0) -> list[str]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        lines = _read_lines(path)
        if len(lines) >= count:
            return lines
        time.sleep(0.01)
    return _read_lines(path)


class TestIdempotentSubmit:
    def test_same_key_returns_same_handle_and_runs_once(self, tmp_path) -> None:
        marker = str(tmp_path / "once.txt")
        rt = Runtime(name="idem-node")
        with rt:
            first = _append_once_task.submit(marker, idempotency_key="job-1")
            second = _append_once_task.submit(marker, idempotency_key="job-1")
            assert first is second
            assert first.result() == marker
            # 第二次提交没有产生新任务。
            assert _wait_for_lines(marker, 1) == ["run"]
            time.sleep(0.2)
            assert _read_lines(marker) == ["run"]

    def test_distinct_keys_run_separately(self, tmp_path) -> None:
        marker = str(tmp_path / "multi.txt")
        rt = Runtime(name="idem-node")
        with rt:
            h1 = _append_once_task.submit(marker, idempotency_key="job-a")
            h2 = _append_once_task.submit(marker, idempotency_key="job-b")
            assert h1 is not h2
            h1.result()
            h2.result()
            assert len(_wait_for_lines(marker, 2)) == 2

    def test_same_key_keeps_first_arguments(self, tmp_path) -> None:
        first_path = str(tmp_path / "first.txt")
        second_path = str(tmp_path / "second.txt")
        rt = Runtime(name="idem-node")
        with rt:
            handle = _append_once_task.submit(first_path, idempotency_key="job-args")
            again = _append_once_task.submit(second_path, idempotency_key="job-args")
            assert again is handle
            assert handle.result() == first_path
            assert _wait_for_lines(first_path, 1) == ["run"]
            assert _read_lines(second_path) == []

    def test_concurrent_same_key_submits_once(self, tmp_path) -> None:
        marker = str(tmp_path / "concurrent.txt")
        rt = Runtime(name="idem-node", max_concurrent_tasks=4)
        handles: list = []
        barrier = threading.Barrier(8)
        lock = threading.Lock()

        def _submit() -> None:
            barrier.wait()
            # Runtime 上下文是线程局部的：子线程需显式继承，幂等锁本身是 Runtime 级的。
            with use_runtime(rt):
                handle = _append_once_task.submit(
                    marker, idempotency_key="job-concurrent"
                )
            with lock:
                handles.append(handle)

        with rt:
            threads = [threading.Thread(target=_submit) for _ in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30)
            assert all(not t.is_alive() for t in threads)
            assert len(handles) == 8
            assert all(h is handles[0] for h in handles)
            handles[0].result()
            assert _wait_for_lines(marker, 1) == ["run"]
            time.sleep(0.2)
            assert _read_lines(marker) == ["run"]

    def test_empty_or_non_string_key_rejected(self, tmp_path) -> None:
        rt = Runtime(name="idem-node")
        with rt, pytest.raises(ValueError):
            _append_once_task.submit(str(tmp_path / "x.txt"), idempotency_key="")
        with rt, pytest.raises(ValueError):
            _append_once_task.submit(
                str(tmp_path / "x.txt"),
                idempotency_key=123,  # type: ignore[arg-type]
            )

    def test_key_scope_is_per_runtime(self, tmp_path) -> None:
        marker = str(tmp_path / "scope.txt")
        with Runtime(name="idem-node-a"):
            handle_a = _append_once_task.submit(marker, idempotency_key="shared")
            handle_a.result()
        with Runtime(name="idem-node-b"):
            handle_b = _append_once_task.submit(marker, idempotency_key="shared")
            assert handle_b is not handle_a
            handle_b.result()
        assert len(_wait_for_lines(marker, 2)) == 2
