"""``actant.cli`` status / identity 子命令单元测试。"""
from __future__ import annotations

import argparse
import json
import os
import tempfile

import pytest

from actant.cli import cmd_identity_init, cmd_identity_show, cmd_status, main


def test_cmd_status_without_data_dir(capsys: pytest.CaptureFixture[str]) -> None:
    args = argparse.Namespace(data_dir=None)
    assert cmd_status(args) == 0
    captured = capsys.readouterr()
    assert "actant version" in captured.out
    assert "capabilities" in captured.out


def test_cmd_status_with_data_dir_no_meta(capsys: pytest.CaptureFixture[str]) -> None:
    with tempfile.TemporaryDirectory() as data_dir:
        args = argparse.Namespace(data_dir=data_dir)
        assert cmd_status(args) == 0
        captured = capsys.readouterr()
        assert "node_meta.json not found" in captured.err


def test_cmd_status_with_data_dir_and_meta(capsys: pytest.CaptureFixture[str]) -> None:
    with tempfile.TemporaryDirectory() as data_dir:
        meta = {"node_id": "test-node", "last_started": "2026-01-01T00:00:00"}
        with open(os.path.join(data_dir, "node_meta.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f)
        args = argparse.Namespace(data_dir=data_dir)
        assert cmd_status(args) == 0
        captured = capsys.readouterr()
        assert "test-node" in captured.out
        assert "2026-01-01T00:00:00" in captured.out


def test_cmd_status_with_corrupt_meta(capsys: pytest.CaptureFixture[str]) -> None:
    with tempfile.TemporaryDirectory() as data_dir:
        with open(os.path.join(data_dir, "node_meta.json"), "w", encoding="utf-8") as f:
            f.write("not-json")
        args = argparse.Namespace(data_dir=data_dir)
        assert cmd_status(args) == 0
        captured = capsys.readouterr()
        assert "warning" in captured.err


def test_cmd_identity_init_then_show_returns_same_id(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        # init 负责创建目录：部署流水线可以先算 id，再登记白名单。
        data_dir = os.path.join(tmp, "nested", "node-a")
        assert cmd_identity_init(argparse.Namespace(data_dir=data_dir)) == 0
        created = capsys.readouterr().out.strip()
        assert created

        assert cmd_identity_show(argparse.Namespace(data_dir=data_dir)) == 0
        assert capsys.readouterr().out.strip() == created


def test_cmd_identity_show_missing_key(capsys: pytest.CaptureFixture[str]) -> None:
    with tempfile.TemporaryDirectory() as data_dir:
        assert cmd_identity_show(argparse.Namespace(data_dir=data_dir)) == 1
        captured = capsys.readouterr()
        assert "identity.key not found" in captured.err
        assert "actant identity init" in captured.err


def test_cmd_identity_show_corrupt_key(capsys: pytest.CaptureFixture[str]) -> None:
    with tempfile.TemporaryDirectory() as data_dir:
        with open(os.path.join(data_dir, "identity.key"), "wb") as f:
            f.write(b"short")
        assert cmd_identity_show(argparse.Namespace(data_dir=data_dir)) == 1
        captured = capsys.readouterr()
        assert "identity show failed" in captured.err
        assert "identity.key corrupt" in captured.err


def test_identity_subcommand_dispatch(capsys: pytest.CaptureFixture[str]) -> None:
    """走真实 argparse 注册路径，而不只是直接调用 cmd_* 函数。"""
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = os.path.join(tmp, "node-a")

        assert main(["identity", "init", "--data-dir", data_dir]) == 0
        created = capsys.readouterr().out.strip()
        assert created

        assert main(["identity", "show", "--data-dir", data_dir]) == 0
        assert capsys.readouterr().out.strip() == created
