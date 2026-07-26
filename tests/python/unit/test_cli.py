"""``actant.cli`` 单元测试（不启动完整 Runtime）。"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import pytest

import actant
from actant import TaskEvent
from actant.cli import (
    _build_config,
    _configure_logging,
    _payload_signing_key,
    _print_task_event,
    _split_comma,
    main,
)


def _make_args(**kwargs: object) -> argparse.Namespace:
    """构造带有默认 worker 参数的 argparse.Namespace。"""
    defaults: dict[str, object] = {
        "payload_signing_key": "",
        "max_concurrent_tasks": None,
        "default_task_timeout_ms": None,
        "drain_timeout_secs": None,
        "remote_fallback_delay_ms": None,
        "scheduler": None,
        "bootstrap_nodes": None,
        "listen_port": None,
        "heartbeat_interval_ms": None,
        "failure_timeout_ms": None,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_split_comma_empty() -> None:
    assert _split_comma(None) is None
    assert _split_comma("") is None
    assert _split_comma("  ") == []


def test_split_comma_splits_and_trims() -> None:
    assert _split_comma("a, b ,c") == ["a", "b", "c"]


def test_payload_signing_key_from_args() -> None:
    args = _make_args(payload_signing_key="secret")
    assert _payload_signing_key(args) == "secret"


def test_payload_signing_key_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ACTANT_PAYLOAD_KEY", "env-secret")
    args = _make_args(payload_signing_key="")
    assert _payload_signing_key(args) == "env-secret"


def test_payload_signing_key_generates_random() -> None:
    args = _make_args(payload_signing_key="")
    key = _payload_signing_key(args)
    assert len(key) == 64  # 32 bytes hex


def test_build_config_returns_none_for_defaults() -> None:
    args = _make_args()
    assert _build_config(args) is None


def test_build_config_with_worker_params() -> None:
    args = _make_args(
        max_concurrent_tasks=4,
        scheduler="fifo",
        payload_signing_key="k",
    )
    config = _build_config(args)
    assert config is not None
    assert config.max_concurrent_tasks == 4
    assert config.scheduler == "fifo"
    assert config.payload_signing_key == "k"


def test_build_config_with_network_params() -> None:
    args = _make_args(
        bootstrap_nodes="a,b",
        listen_port=1234,
        payload_signing_key="k",
    )
    config = _build_config(args)
    assert config is not None
    assert config.network is not None
    assert config.network.bootstrap_nodes == ["a", "b"]
    assert config.network.listen_port == 1234
    assert config.gossip is not None


def test_print_task_event(capsys: pytest.CaptureFixture[str]) -> None:
    event = TaskEvent(
        kind="completed",
        task_id="t-1",
        workflow_id="wf-1",
        attempt=2,
        next_attempt=0,
        error="",
    )
    _print_task_event(event)
    captured = capsys.readouterr()
    assert "completed" in captured.err
    assert "t-1" in captured.err
    assert "wf-1" in captured.err
    assert "attempt=2" in captured.err


def test_print_task_event_retried() -> None:
    event = TaskEvent(
        kind="retried",
        task_id="t-1",
        workflow_id="",
        attempt=1,
        next_attempt=2,
        error="boom",
    )
    # 只验证不抛异常
    _print_task_event(event)


def test_configure_logging_does_not_raise() -> None:
    _configure_logging("debug")


def test_main_version(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["version"]) == 0
    captured = capsys.readouterr()
    assert captured.out.strip() == actant.__version__


def test_main_no_subcommand_prints_help(capsys: pytest.CaptureFixture[str]) -> None:
    assert main([]) == 1
    captured = capsys.readouterr()
    assert "usage:" in captured.out


def test_main_status_without_data_dir(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["status"]) == 0
    captured = capsys.readouterr()
    assert "actant version:" in captured.out


def test_main_status_with_data_dir_reads_meta(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    data_dir = tmp_path / "actant-status"
    data_dir.mkdir()
    meta = {"node_id": "node-x", "last_started": "2024-01-01T00:00:00"}
    with open(os.path.join(str(data_dir), "node_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f)

    assert main(["status", "--data-dir", str(data_dir)]) == 0
    captured = capsys.readouterr()
    assert "node-x" in captured.out


def test_main_task_cancel_requires_id(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["task", "cancel"]) == 1
    captured = capsys.readouterr()
    assert "requires" in captured.err
