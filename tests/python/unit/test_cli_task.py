"""``actant.cli`` task 子命令单元测试。"""
from __future__ import annotations

import argparse

import pytest

import actant
from actant.cli import cmd_task
from actant.task import task


@task
def _sample_task() -> str:
    return "ok"


@task
def _long_task() -> str:
    import time

    for _ in range(100):
        time.sleep(0.05)
    return "done"


def test_cmd_task_list_without_runtime_is_honest(capsys: pytest.CaptureFixture[str]) -> None:
    """无 runtime 注入时 list 不再新建空 Runtime 制造 "no tasks" 假象。"""
    args = argparse.Namespace(action="list")
    assert cmd_task(args) == 1
    captured = capsys.readouterr()
    assert "in-memory" in captured.err
    assert "actant status" in captured.err


def test_cmd_task_list_empty_with_runtime(capsys: pytest.CaptureFixture[str]) -> None:
    args = argparse.Namespace(action="list")
    with actant.Runtime.with_defaults() as rt:
        assert cmd_task(args, runtime=rt) == 0
    captured = capsys.readouterr()
    assert "no tasks" in captured.out


def test_cmd_task_list_with_running_task(capsys: pytest.CaptureFixture[str]) -> None:
    import time

    args = argparse.Namespace(action="list")
    with actant.Runtime.with_defaults() as rt:
        handle = _long_task.submit()
        time.sleep(0.3)
        assert cmd_task(args, runtime=rt) == 0
        rt.cancel_task(handle.task_id)
    captured = capsys.readouterr()
    assert "state=" in captured.out


def test_cmd_task_cancel_existing(capsys: pytest.CaptureFixture[str]) -> None:
    args = argparse.Namespace(action="cancel", task_id="nonexistent")
    assert cmd_task(args) == 1
    captured = capsys.readouterr()
    assert "not found" in captured.out


def test_cmd_task_cancel_started_task(capsys: pytest.CaptureFixture[str]) -> None:
    import time

    args = argparse.Namespace(action="cancel", task_id="")
    with actant.Runtime.with_defaults() as rt:
        handle = _long_task.submit()
        # 等待任务被 worker 拉取
        time.sleep(0.3)
        args.task_id = handle.task_id
        assert cmd_task(args, runtime=rt) == 0
        captured = capsys.readouterr()
        assert "cancelled:" in captured.out


def test_cmd_task_unknown_action(capsys: pytest.CaptureFixture[str]) -> None:
    args = argparse.Namespace(action="unknown")
    assert cmd_task(args) == 1
    captured = capsys.readouterr()
    assert "unknown action" in captured.err
