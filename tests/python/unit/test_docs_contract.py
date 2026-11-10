"""文档契约：公共 API 必须有 docstring，生成参考页必须与源码一致。"""

from __future__ import annotations

import inspect
import subprocess
import sys
from pathlib import Path

import actant

ROOT = Path(__file__).resolve().parents[3]


def test_public_api_symbols_have_docstrings() -> None:
    missing: list[str] = []
    for name in actant.__all__:
        if name == "__version__":
            continue
        obj = getattr(actant, name)
        if not (inspect.isclass(obj) or inspect.isfunction(obj) or inspect.ismethod(obj)):
            continue
        if not inspect.getdoc(obj):
            missing.append(name)
    assert not missing, f"公共 API 缺少 docstring: {missing}"


def test_reference_pages_exist() -> None:
    expected = (
        "python-api.md",
        "pyo3-api.md",
        "configuration.md",
        "cli.md",
        "rust-api.md",
    )
    reference_dir = ROOT / "docs" / "reference"
    missing = [name for name in expected if not (reference_dir / name).is_file()]
    assert not missing, f"参考页缺失: {missing}"


def test_generated_reference_is_current() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "gen_api_reference.py"), "--check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_api_surface_snapshot_is_current() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "api_surface.py"), "--check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
