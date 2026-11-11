"""生成并校验公开 API 快照。

快照覆盖三处公开面，任一处的增删都会在 CI 中失败，迫使变更被显式声明：

- ``actant/__init__.py`` 的 ``__all__``
- ``actant/actant.pyi`` 中每个类的公开方法/属性
- ``src/py/*.rs`` 中 ``#[pymethods]`` 暴露给 Python 的成员

此外校验 ``__all__`` 不超过 :data:`PYTHON_ALL_BUDGET`，防止顶层命名空间膨胀。

用法::

    uv run python scripts/api_surface.py           # 写入快照
    uv run python scripts/api_surface.py --check   # 校验快照是否最新
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INIT = ROOT / "actant" / "__init__.py"
STUB = ROOT / "actant" / "actant.pyi"
PYO3_DIR = ROOT / "src" / "py"
SNAPSHOT = ROOT / "scripts" / "api_surface_snapshot.json"

_PYO3_NAME = re.compile(r'pyo3\s*\(\s*name\s*=\s*"([^"]+)"')
_FN = re.compile(r"^\s*fn\s+([A-Za-z_][A-Za-z0-9_]*)")
_IMPL = re.compile(r"^impl<(?:[^>]*)>\s+([A-Za-z_][A-Za-z0-9_]*)")
_IMPL_PLAIN = re.compile(r"^impl\s+([A-Za-z_][A-Za-z0-9_]*)")

#: 顶层 ``__all__`` 的硬上限。新增名字必须显式上调这里，迫使变更被评审，
#: 避免 0.x 期间顶层命名空间再次膨胀。
PYTHON_ALL_BUDGET = 20

#: 公开子命名空间模块与其 ``__all__`` 预算。顶层受限后，新 API 应落到这里；
#: 同样设硬上限，避免收敛目标被“挪进子命名空间”绕过。
NAMESPACE_MODULES: dict[str, Path] = {
    "actant.tasks": ROOT / "actant" / "tasks.py",
    "actant.workflow": ROOT / "actant" / "workflow.py",
    "actant.aio": ROOT / "actant" / "aio.py",
    "actant.effects": ROOT / "actant" / "effects.py",
    "actant.capabilities": ROOT / "actant" / "capabilities.py",
    "actant.exceptions": ROOT / "actant" / "exceptions.py",
    "actant.testkit": ROOT / "actant" / "testkit.py",
}

#: 命名空间规模预算。**已冻结，不再上调**：新增公开名必须先在同类接口中合并
#: 或删除同等数量（净零规则），否则 ``--check`` 失败。预算只降不升。
NAMESPACE_BUDGETS: dict[str, int] = {
    "actant.tasks": 11,
    "actant.workflow": 15,
    "actant.aio": 7,
    "actant.effects": 4,
    "actant.capabilities": 19,
    "actant.exceptions": 23,
    "actant.testkit": 5,
}

#: 稳定性分级。公开面新增时必须落入其中一级；分级决定文档生成与兼容承诺。
#: - ``stable``：跨 0.x 尽量不破坏的正式入口；
#: - ``advanced``：可用的底层能力，随内部实现调整；
#: - ``test-only``：仅测试使用，进入生产可能破坏集群状态；
#: - ``internal``：``_`` 前缀私有实现，不承诺兼容。
STABILITY_TIERS = ("stable", "advanced", "test-only", "internal")

#: 每个公开面的默认级别。新增公开面必须先在此登记，否则 ``--check`` 失败。
SURFACE_TIERS: dict[str, str] = {
    "python_all": "stable",
    "python_namespaces": "stable",
    "stub_classes": "advanced",
    "pyo3_classes": "advanced",
}

#: 按 ``<surface>:<owner>`` 覆盖整组名字的级别（成员级 ``_test_`` 前缀优先）。
MEMBER_TIER_OVERRIDES: dict[str, str] = {
    "python_namespaces:actant.testkit": "test-only",
}


def member_tier(surface: str, owner: str, member: str) -> str:
    """返回一个公开名字的稳定性级别。

    成员名前缀优先于整组覆盖：``_test_*`` 一律 test-only，其余 ``_`` 前缀为
    internal。未登记的表面回退到 ``internal``（新增表面须先登记）。
    """
    override = MEMBER_TIER_OVERRIDES.get(f"{surface}:{owner}")
    if override is not None:
        return override
    if member.startswith("_test_"):
        return "test-only"
    if member.startswith("_"):
        return "internal"
    return SURFACE_TIERS.get(surface, "internal")



def _module_all(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets
        ):
            return sorted(ast.literal_eval(node.value))
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "__all__"
            and node.value is not None
        ):
            return sorted(ast.literal_eval(node.value))
    raise SystemExit(f"{path}: __all__ not found")


def _stub_surface() -> dict[str, list[str]]:
    tree = ast.parse(STUB.read_text(encoding="utf-8"))
    surface: dict[str, list[str]] = {}
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        members = []
        for item in node.body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if not item.name.startswith("_"):
                    members.append(item.name)
            elif (
                isinstance(item, ast.AnnAssign)
                and isinstance(item.target, ast.Name)
                and not item.target.id.startswith("_")
            ):
                members.append(item.target.id)
        surface[node.name] = sorted(members)
    return surface


def _pyo3_surface() -> dict[str, list[str]]:
    surface: dict[str, list[str]] = {}
    for path in sorted(PYO3_DIR.glob("*.rs")):
        current: str | None = None
        in_pymethods = False
        pending_attrs: list[str] = []
        lines = path.read_text(encoding="utf-8").splitlines()
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("#[pymethods]"):
                in_pymethods = True
                pending_attrs = []
                continue
            if not in_pymethods:
                continue
            if current is None:
                match = _IMPL.match(line) or _IMPL_PLAIN.match(line)
                if match:
                    current = match.group(1)
                    surface.setdefault(current, [])
                continue
            if line.startswith("}"):
                in_pymethods = False
                current = None
                pending_attrs = []
                continue
            if stripped.startswith("#["):
                pending_attrs.append(stripped)
                continue
            match = _FN.match(line)
            if match:
                name = match.group(1)
                for attr in pending_attrs:
                    override = _PYO3_NAME.search(attr)
                    if override:
                        name = override.group(1)
                        break
                surface[current].append(name)
                pending_attrs = []
            elif stripped:
                pending_attrs = []
    return {name: sorted(members) for name, members in surface.items()}


def build_snapshot() -> dict[str, object]:
    return {
        "python_all": _module_all(INIT),
        "python_namespaces": {
            name: _module_all(path) for name, path in NAMESPACE_MODULES.items()
        },
        "stub_classes": _stub_surface(),
        "pyo3_classes": _pyo3_surface(),
        "stability": {
            "surfaces": SURFACE_TIERS,
            "overrides": MEMBER_TIER_OVERRIDES,
        },
    }


def _validate_stability(snapshot: dict[str, object]) -> list[str]:
    """校验稳定性分级登记完整，且 test-only 名字未混入稳定公开面。"""
    errors: list[str] = []
    # 反向检查：快照里出现的公开面必须已登记，否则新面会静默回退为 internal。
    for surface in snapshot:
        if surface != "stability" and surface not in SURFACE_TIERS:
            errors.append(f"快照包含未登记的公开面 {surface}")

    for surface, tier in SURFACE_TIERS.items():
        if tier not in STABILITY_TIERS:
            errors.append(f"公开面 {surface} 的级别 {tier!r} 非法")
        if surface not in snapshot:
            errors.append(f"SURFACE_TIERS 登记了不存在的公开面 {surface}")

    for key, tier in MEMBER_TIER_OVERRIDES.items():
        surface, _, owner = key.partition(":")
        if tier not in STABILITY_TIERS:
            errors.append(f"{key} 的级别 {tier!r} 非法")
        members = snapshot.get(surface)
        if not isinstance(members, dict) or owner not in members:
            errors.append(f"{key} 指向不存在的公开面所有者")

    for member in snapshot["python_all"]:
        if member_tier("python_all", "actant", member) == "test-only":
            errors.append(f"actant.__all__ 含 test-only 名字 {member!r}")

    namespaces = snapshot["python_namespaces"]
    assert isinstance(namespaces, dict)
    for module, members in namespaces.items():
        for member in members:
            if member_tier("python_namespaces", module, member) == "test-only":
                override = MEMBER_TIER_OVERRIDES.get(f"python_namespaces:{module}")
                if override != "test-only":
                    errors.append(f"{module}.__all__ 含 test-only 名字 {member!r}")
    return errors


def _diff(old: object, new: object, path: str) -> list[str]:
    if isinstance(old, dict) and isinstance(new, dict):
        lines: list[str] = []
        for key in sorted(set(old) | set(new)):
            if key not in old:
                lines.append(f"+ {path}.{key}")
            elif key not in new:
                lines.append(f"- {path}.{key}")
            else:
                lines.extend(_diff(old[key], new[key], f"{path}.{key}"))
        return lines
    if isinstance(old, list) and isinstance(new, list):
        lines = []
        for item in sorted(set(new) - set(old)):
            lines.append(f"+ {path}: {item}")
        for item in sorted(set(old) - set(new)):
            lines.append(f"- {path}: {item}")
        return lines
    if old != new:
        return [f"~ {path}: {old!r} -> {new!r}"]
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="只校验快照是否最新，不写入",
    )
    args = parser.parse_args()

    snapshot = build_snapshot()
    names = snapshot["python_all"]
    if len(names) > PYTHON_ALL_BUDGET:
        print(
            f"actant.__all__ 有 {len(names)} 个名字，超过预算 {PYTHON_ALL_BUDGET}。",
            file=sys.stderr,
        )
        print(
            "请把新 API 放到职责子命名空间（actant.tasks / workflow / effects / "
            "capabilities / exceptions），或显式上调 scripts/api_surface.py 的 "
            "PYTHON_ALL_BUDGET。",
            file=sys.stderr,
        )
        return 1

    for module, members in snapshot["python_namespaces"].items():
        budget = NAMESPACE_BUDGETS[module]
        if len(members) > budget:
            print(
                f"{module}.__all__ 有 {len(members)} 个名字，超过预算 {budget}。",
                file=sys.stderr,
            )
            print(
                "公开面只收缩不扩张；如需新增，请先评估能否复用现有入口，"
                "再显式上调 NAMESPACE_BUDGETS。",
                file=sys.stderr,
            )
            return 1

    stability_errors = _validate_stability(snapshot)
    if stability_errors:
        print("稳定性分级校验失败：", file=sys.stderr)
        for error in stability_errors:
            print(f"  - {error}", file=sys.stderr)
        print(
            "请在 scripts/api_surface.py 的 SURFACE_TIERS / MEMBER_TIER_OVERRIDES "
            "中登记公开面的稳定性级别。",
            file=sys.stderr,
        )
        return 1

    rendered = json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if not args.check:
        SNAPSHOT.write_text(rendered, encoding="utf-8")
        print(f"wrote {SNAPSHOT.relative_to(ROOT)}")
        return 0

    if not SNAPSHOT.exists():
        print(f"{SNAPSHOT.relative_to(ROOT)} 不存在；请先运行本脚本生成快照", file=sys.stderr)
        return 1

    current = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    if current == snapshot:
        print("API surface 与快照一致")
        return 0

    changes = _diff(current, snapshot, "api")
    print("API surface 与快照不一致：", file=sys.stderr)
    for change in changes:
        print(f"  {change}", file=sys.stderr)
    print(
        "\n如变更属预期，请运行 `uv run python scripts/api_surface.py` 更新快照并提交。",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
