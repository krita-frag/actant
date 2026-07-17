"""``actant.cli`` worker 子命令单元测试。"""
from __future__ import annotations

import argparse
from typing import Any
from unittest.mock import MagicMock

import pytest

from actant.cli import cmd_worker


def test_cmd_worker_starts_and_stops(monkeypatch: pytest.MonkeyPatch) -> None:
    """worker 子命令应启动 Runtime、注册事件 handler、等待信号并优雅停止。"""
    mock_rt = MagicMock()
    mock_rt.node_id = "worker-test-node"

    created: list[Any] = []

    def _with_defaults(*, name: str, **kwargs: Any) -> Any:
        created.append((name, kwargs))
        return mock_rt

    monkeypatch.setattr("actant.Runtime.with_defaults", _with_defaults)

    event = MagicMock()
    monkeypatch.setattr("threading.Event", lambda: event)

    signals: list[tuple[int, Any]] = []
    monkeypatch.setattr("signal.signal", lambda sig, handler: signals.append((sig, handler)))

    args = argparse.Namespace(
        name="test-worker",
        data_dir=None,
        log_level="info",
        max_concurrent_tasks=None,
        default_task_timeout_ms=None,
        drain_timeout_secs=None,
        remote_fallback_delay_ms=None,
        scheduler=None,
        bootstrap_nodes=None,
        listen_port=None,
        payload_signing_key=None,
    )

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
    mock_rt.start.assert_called_once()
    mock_rt.serve.assert_called_once()
    mock_rt.stop.assert_called_once()
