"""``actant.cli`` status 子命令单元测试。"""
from __future__ import annotations

import argparse
import json
import os
import tempfile

import pytest

from actant.cli import cmd_status


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
