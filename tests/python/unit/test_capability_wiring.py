"""Capability 接线契约。

所有公开内置 capability 必须有真实调用点。路由/调度/重试等放置类策略不属于
ERH 数据面：它们由 Rust ``RoutePolicy`` / ``Scheduler`` 或第 3 层外部调度器
承担，不得以"内置 capability"的形式重新出现。

集合相等只能证明"名字登记过了"，证明不了"引擎真的会调用它"。因此本模块
直接扫描 ``actant/`` 生产代码的 effect 发射点：新增一个只有声明、没有调用点
的 capability 会在此处失败。
"""

from __future__ import annotations

import ast
from pathlib import Path

import actant
from actant.capabilities import (
    BUILTIN_CAPABILITIES,
    PYTHON_ONLY_CAPABILITIES,
    RUST_BACKED_CAPABILITIES,
)

_REMOVED_POLICY_CAPABILITIES = ("Routing", "Scheduling", "RetryPolicy")

_REMOVED_EXPORTS = (
    "ROUTING",
    "SCHEDULING",
    "RETRY_POLICY",
)


def test_policy_capabilities_are_not_builtin() -> None:
    for name in _REMOVED_POLICY_CAPABILITIES:
        assert name not in BUILTIN_CAPABILITIES


def test_removed_policy_exports_are_gone() -> None:
    for name in _REMOVED_EXPORTS:
        assert name not in actant.__all__
        assert not hasattr(actant, name)


def test_python_only_is_subset_of_builtin() -> None:
    assert PYTHON_ONLY_CAPABILITIES.issubset(BUILTIN_CAPABILITIES)


def test_rust_backed_is_builtin_minus_python_only() -> None:
    assert (frozenset(BUILTIN_CAPABILITIES) - PYTHON_ONLY_CAPABILITIES) == (
        RUST_BACKED_CAPABILITIES
    )


_EFFECT_VERBS = frozenset(
    {
        "ask",
        "perform",
        "emit",
        "_dispatch_ask",
        "_dispatch_perform",
        "_dispatch_emit",
        "_dispatch_ask_async",
        "_dispatch_perform_async",
        "_dispatch_perform_batch_async",
        "_emit",
    }
)

# 已登记但引擎无发射点、也无默认 handler 的 capability。当前为空：内置的
# 4 个 capability 全部有生产发射点。若确需临时引入未接线项，在此显式登记，
# 接线后必须同步移除（下面的 isdisjoint 断言会强制这一点）。
_KNOWN_UNWIRED: frozenset[str] = frozenset()


def _package_root() -> Path:
    """tests/python/unit/test_capability_wiring.py -> <repo>/actant。"""
    return Path(__file__).resolve().parents[3] / "actant"


def _constant_names(root: Path) -> dict[str, str]:
    """读取 capabilities.py 的模块级字符串常量（如 ``VALUE_STORE``）。"""
    tree = ast.parse((root / "capabilities.py").read_text(encoding="utf-8"))
    out: dict[str, str] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Constant):
            continue
        if not isinstance(node.value.value, str):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id.isupper():
                out[target.id] = node.value.value
    return out


def _observed_capability_calls(root: Path) -> set[str]:
    """扫描 actant/ 生产代码，收集 effect 动词的第一个 capability 实参。"""
    constants = _constant_names(root)
    observed: set[str] = set()
    for path in root.rglob("*.py"):
        if path.name == "capabilities.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            func = node.func
            if isinstance(func, ast.Name):
                verb: str | None = func.id
            elif isinstance(func, ast.Attribute):
                verb = func.attr
            else:
                verb = None
            if verb not in _EFFECT_VERBS:
                continue
            arg = node.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                observed.add(arg.value)
            elif isinstance(arg, ast.Name) and arg.id in constants:
                observed.add(constants[arg.id])
    return observed


def test_wiring_scanner_sees_known_call_sites() -> None:
    """扫描器自检：不会因为模式写错而恒返回空集合造成假绿。"""
    observed = _observed_capability_calls(_package_root())
    assert {"Store", "ValueStore", "TaskLifecycle", "WorkflowLifecycle"} <= observed


def test_every_builtin_capability_has_a_production_call_site() -> None:
    declared = frozenset(BUILTIN_CAPABILITIES)
    observed = _observed_capability_calls(_package_root())
    missing = declared - observed - _KNOWN_UNWIRED
    assert not missing, (
        f"capability 已登记但 actant/ 生产代码无 effect 发射点: {sorted(missing)}; "
        "要么接线，要么在 _KNOWN_UNWIRED 中显式标注"
    )
    assert declared >= _KNOWN_UNWIRED, "允许列表含已删除的 capability，请同步清理"
    assert observed.isdisjoint(_KNOWN_UNWIRED), (
        "允许列表中的 capability 已有生产发射点，请从 _KNOWN_UNWIRED 移除"
    )
