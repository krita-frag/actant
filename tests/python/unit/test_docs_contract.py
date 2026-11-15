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


def test_workflow_config_defaults_match_rust() -> None:
    """`_ActantConfig` 属性说明里的 Rust 默认值必须与 `WorkflowConfig::default` 同源。

    `configuration.md` 的 `None` 是 Python 层「不覆盖」语义；真正的默认值在 Rust
    的 `Default` 实现里。此用例防止 .pyi 的说明与 Rust 默认值漂移。
    """
    import re

    config = (ROOT / "crates" / "actant-common" / "src" / "common" / "config.rs").read_text()
    block = config.split("impl Default for WorkflowConfig {", 1)[1].split("\n}", 1)[0]
    defaults: dict[str, str] = dict(re.findall(r"(\w+):\s*(\d+),", block))
    pyi = (ROOT / "actant" / "actant.pyi").read_text()

    # .pyi 属性名 → Rust 字段名（`max_history_events` 是跨语言别名）。
    aliases = {
        "completed_retention_count": "completed_retention_count",
        "max_history_events": "event_log_max_events_per_workflow",
        "persist_flush_interval_ms": "persist_flush_interval_ms",
        "state_poll_interval_ms": "state_poll_interval_ms",
    }
    for prop, rust_field in aliases.items():
        expected = defaults.get(rust_field)
        assert expected is not None, f"WorkflowConfig::default 缺少 {rust_field}"
        doc = re.search(
            rf"def {prop}\(self\)[^:]*:\s*\"\"\"([^\"]*)\"\"\"", pyi
        )
        assert doc is not None, f"_ActantConfig.{prop} 缺少默认值说明"
        assert f"Rust 默认 ``{expected}``" in doc.group(1), (
            f"{prop}: .pyi 写的是 {doc.group(1)!r}，Rust 默认是 {expected}"
        )
