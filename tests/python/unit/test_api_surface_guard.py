"""回归门禁：已清理的死路径不得回流，文档引用的 Runtime 方法必须存在。

这些断言只覆盖"会静默漂移"的事实，不追求扫描所有公开符号：
- 被删除的单实现接缝 / 死分支一旦被重新引入，测试立即失败；
- 文档中的 ``Runtime.<method>(...)`` 必须与真实运行时一致。
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import actant

_REPO_ROOT = Path(__file__).resolve().parents[3]


def test_removed_dead_paths_do_not_reappear() -> None:
    assert importlib.util.find_spec("actant._internal.tasks.backend") is None

    from actant._internal.runtime_base import _RuntimeBase
    from actant._internal.tasks import helpers

    assert not hasattr(helpers, "_EventBatcherScope")
    assert not hasattr(helpers, "_get_event_batcher")
    assert hasattr(_RuntimeBase, "_perform_one")
    assert not hasattr(_RuntimeBase, "_perform_one_async")


def test_public_all_resolves_without_duplicates() -> None:
    assert len(actant.__all__) == len(set(actant.__all__))
    for name in actant.__all__:
        assert getattr(actant, name) is not None


def test_docs_runtime_methods_exist() -> None:
    pattern = re.compile(r"\bRuntime\.([a-zA-Z_][a-zA-Z0-9_]*)\(")
    docs = _REPO_ROOT / "docs"
    missing: dict[str, list[str]] = {}
    for md in docs.rglob("*.md"):
        for match in pattern.finditer(md.read_text(encoding="utf-8")):
            name = match.group(1)
            if not hasattr(actant.Runtime, name):
                missing.setdefault(name, []).append(str(md.relative_to(docs)))
    assert not missing, f"docs reference non-existent Runtime methods: {missing}"
