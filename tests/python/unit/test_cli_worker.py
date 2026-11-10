"""``actant.cli`` worker 子命令单元测试。"""
from __future__ import annotations

import argparse
from typing import Any
from unittest.mock import MagicMock

import pytest

from actant.cli import cmd_worker


def _worker_namespace(**overrides: Any) -> argparse.Namespace:
    """构造 ``actant worker`` 的解析结果，未覆盖字段取默认值。"""
    values: dict[str, Any] = {
        "name": "test-worker",
        "data_dir": None,
        "log_level": "info",
        "max_concurrent_tasks": None,
        "default_task_timeout_ms": None,
        "drain_timeout_secs": None,
        "remote_fallback_delay_ms": None,
        "scheduler": None,
        "bootstrap_nodes": None,
        "listen_port": None,
        "payload_signing_key": None,
        "metrics_port": None,
        "resume": False,
        "host_flows": False,
        "role": None,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def test_cmd_worker_starts_and_stops(monkeypatch: pytest.MonkeyPatch) -> None:
    """worker 子命令应启动 Runtime、注册事件 handler、等待信号并优雅停止。"""
    mock_rt = MagicMock()
    mock_rt.node_id = "worker-test-node"

    created: list[Any] = []

    def _new_runtime(*, name: str, **kwargs: Any) -> Any:
        created.append((name, kwargs))
        return mock_rt

    monkeypatch.setattr("actant.Runtime", _new_runtime)

    event = MagicMock()
    monkeypatch.setattr("threading.Event", lambda: event)

    signals: list[tuple[int, Any]] = []
    monkeypatch.setattr("signal.signal", lambda sig, handler: signals.append((sig, handler)))

    args = _worker_namespace()

    # 第一次 wait 返回后设置停止标志
    wait_calls: list[int] = []

    def _wait() -> None:
        wait_calls.append(1)
        event.is_set.return_value = True

    event.wait.side_effect = _wait
    event.is_set.return_value = False

    assert cmd_worker(args) == 0

    assert len(created) == 1
    assert created[0][0] == "test-worker"
    mock_rt.start.assert_called_once_with(auto_resume=False)
    # start() 内部已经调用 serve()；CLI 再调一次会 spawn 第二个执行循环。
    mock_rt.serve.assert_not_called()
    mock_rt.stop.assert_called_once()


def test_cmd_worker_rejects_host_flows_on_worker_role() -> None:
    """``--role worker`` 与 ``--host-flows`` 互斥，直接返回错误。"""
    assert cmd_worker(_worker_namespace(role="worker", host_flows=True)) == 1


def test_cmd_worker_host_flows_starts_flow_host(monkeypatch: pytest.MonkeyPatch) -> None:
    """``--host-flows`` 应拉起常驻 flow host 线程并在关停时 join。"""
    mock_rt = MagicMock()
    mock_rt.node_id = "worker-test-node"
    monkeypatch.setattr("actant.Runtime", lambda **_: mock_rt)

    event = MagicMock()
    monkeypatch.setattr("threading.Event", lambda: event)

    def _wait() -> None:
        event.is_set.return_value = True

    event.wait.side_effect = _wait
    event.is_set.return_value = False
    monkeypatch.setattr("signal.signal", lambda _sig, _handler: None)

    host_thread = MagicMock()
    host_calls: list[Any] = []
    monkeypatch.setattr(
        "actant.cli._start_flow_host",
        lambda rt, _stop: host_calls.append(rt) or host_thread,
    )

    assert cmd_worker(_worker_namespace(host_flows=True)) == 0

    mock_rt.start.assert_called_once_with(auto_resume=False)
    assert host_calls == [mock_rt]
    host_thread.join.assert_called_once()


def test_cmd_worker_rejects_resume_with_host_flows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--resume`` 与 ``--host-flows`` 互斥：不应启动 Runtime。"""
    monkeypatch.setattr(
        "actant.Runtime",
        lambda **_: pytest.fail("互斥参数不应启动 Runtime"),
    )

    assert cmd_worker(_worker_namespace(resume=True, host_flows=True)) == 1
