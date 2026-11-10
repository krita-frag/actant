"""文档可执行契约：教程示例能跑、stub 默认值与实现一致、正文路径可达。

现有文档门禁只比较「生成页 == actant.pyi」，从不校验「.pyi == Rust 实现」，
也没有任何「示例能否执行」检查。本模块补上这三类断言：

1. 教程页的 ```python 块按文档顺序拼接后必须能在干净工作目录跑通；
2. `.pyi` 声明的网络配置默认值、Rust `DEFAULT_*` 常量、运行时实例三方一致；
3. 文档正文引用的仓库内路径必须存在。
"""

from __future__ import annotations

import ast
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]

# 完整教程页：每页的 python 块按出现顺序拼接后可独立运行。
# 片段式示例页（tasks.md / flows.md / service-layer.md）需要额外上下文，改成
# 可运行示例前不要加入本列表，也不要为了纳入它们而降低断言。
TUTORIAL_PAGES = (
    "README.md",
    "docs/start/quickstart.md",
    "docs/start/concepts.md",
    "docs/index.md",
    "docs/guide/capabilities.md",
    "docs/guide/deployment.md",
)

# CHANGELOG 的历史条目引用的是当时的工作树路径，页首已声明可能失效，故不扫描。
PROSE_PAGES = (ROOT / "README.md", ROOT / "AGENTS.md", *sorted((ROOT / "docs").rglob("*.md")))

PATH_RE = re.compile(
    r"(?<![\w./-])"
    r"((?:docs|actant|src|crates|tests|benches|examples|scripts|extensions|plans)/"
    r"[\w./\-]+\.(?:py|rs|md|toml|yml|yaml|json|sh|pyi|ipynb))"
    r"(?![\w])"
)

CONFIG_RS = ROOT / "crates" / "actant-common" / "src" / "common" / "config.rs"


def _python_blocks(page: Path) -> list[str]:
    text = page.read_text(encoding="utf-8")
    return [m.group(1) for m in re.finditer(r"^```python\n(.*?)^```", text, re.S | re.M)]


@pytest.mark.parametrize("page", TUTORIAL_PAGES)
def test_tutorial_examples_execute(page: str) -> None:
    blocks = _python_blocks(ROOT / page)
    assert blocks, f"{page} 没有 python 代码块"
    # 用系统临时目录而非 pytest 的 tmp_path：示例里的 data_dir="./data" 必须落在
    # 干净目录，且本测试在任何 pytest basetemp 配置下都能独立运行。
    with tempfile.TemporaryDirectory() as workdir:
        script = Path(workdir) / "doc_examples.py"
        script.write_text("\n\n".join(blocks), encoding="utf-8")
        result = subprocess.run(
            [sys.executable, str(script)],
            cwd=workdir,
            env={**os.environ, "ACTANT_DISCOVERY": "none"},
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
    assert result.returncode == 0, (
        f"{page} 的教程示例无法执行（exit={result.returncode}）:\n"
        f"{result.stdout}\n{result.stderr}"
    )


def _network_pyi_defaults() -> dict[str, object]:
    """取 `_NetworkConfig.__new__` 中带字面量默认值的参数。"""
    tree = ast.parse((ROOT / "actant" / "actant.pyi").read_text(encoding="utf-8"))
    cls = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "_NetworkConfig"
    )
    ctor = next(
        node
        for node in cls.body
        if isinstance(node, ast.FunctionDef) and node.name == "__new__"
    )
    names = [arg.arg for arg in ctor.args.args][1:]
    defaults = [ast.literal_eval(node) for node in ctor.args.defaults]
    declared = dict(zip(names[-len(defaults) :], defaults, strict=True))
    # Option<T> 参数在 Python 侧归一化为 [] / 'local'，None 声明不与实例值直接对应。
    return {key: value for key, value in declared.items() if value is not None}


def _rust_constant(name: str) -> int:
    match = re.search(
        rf"pub const {name}: \w+ = ([^;]+);", CONFIG_RS.read_text(encoding="utf-8")
    )
    assert match, f"{CONFIG_RS} 中找不到 {name}"
    return ast.literal_eval(match.group(1).strip())


def test_network_config_defaults_agree_across_stub_rust_and_runtime() -> None:
    from actant.actant import _NetworkConfig

    instance = _NetworkConfig()
    for key, declared in _network_pyi_defaults().items():
        actual = getattr(instance, key)
        assert actual == declared, f".pyi 的 {key}={declared!r} 与运行时 {actual!r} 不一致"

    rust_keys = {
        "event_channel_capacity": "DEFAULT_EVENT_CHANNEL_CAPACITY",
    }
    for key, constant in rust_keys.items():
        assert getattr(instance, key) == _rust_constant(constant), (
            f"Rust {constant} 与运行时 {key} 不一致"
        )


def test_prose_repository_paths_exist() -> None:
    missing: list[str] = []
    for page in PROSE_PAGES:
        lines = page.read_text(encoding="utf-8", errors="ignore").splitlines()
        for line_no, line in enumerate(lines, 1):
            for match in PATH_RE.finditer(line):
                target = match.group(1)
                if "*" in target or (ROOT / target).exists():
                    continue
                missing.append(f"{page.relative_to(ROOT)}:{line_no} -> {target}")
    assert not missing, "文档引用了不存在的仓库路径:\n" + "\n".join(missing)
