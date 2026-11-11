#!/usr/bin/env python3
"""从源码生成 API 参考页。

生成内容：

- ``docs/reference/pyo3-api.md``：``actant/actant.pyi`` 中的 PyO3 接口
- ``docs/reference/configuration.md``：``_ActantConfig`` 等配置类字段
- ``docs/reference/cli.md``：``actant`` CLI 的 ``--help`` 快照

用法::

    uv run python scripts/gen_api_reference.py          # 写入页面
    uv run python scripts/gen_api_reference.py --check  # 校验页面是否最新
"""

from __future__ import annotations

import argparse
import ast
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STUB = ROOT / "actant" / "actant.pyi"
REFERENCE_DIR = ROOT / "docs" / "reference"

HEADER = "> 本页由 `scripts/gen_api_reference.py` 从源码生成，请勿手工编辑。\n"
CONFIG_CLASSES = (
    "_ActantConfig",
    "_NetworkConfig",
    "_FailoverConfig",
    "_GossipConfig",
    "_RetryPolicy",
    "_StoreConfig",
)
CLI_COMMANDS: tuple[tuple[str, ...], ...] = (
    (),
    ("worker",),
    ("status",),
    ("version",),
    ("metrics",),
)

#: 运行时读取的环境变量。读取位置一并列出，便于核对声明是否仍然有效。
ENV_VARS: tuple[tuple[str, str, str], ...] = (
    (
        "ACTANT_DISCOVERY",
        "src/py/config.rs",
        "覆盖 `network.preset`；CI 用 `ACTANT_DISCOVERY=none` 强制离线",
    ),
    ("ACTANT_PAYLOAD_KEY", "actant/cli.py", "`actant worker` 的 payload 签名密钥"),
    (
        "ACTANT_TRACING",
        "crates/actant-core/src/observability.rs",
        "设为 `1` 启用 Rust `tracing` 结构化日志",
    ),
    (
        "ACTANT_TOKIO_CONSOLE",
        "crates/actant-core/src/observability.rs",
        "设为 `1` 启用 tokio-console（需 `tokio-console` feature）",
    ),
)


def _first_line(text: str | None) -> str:
    if not text:
        return ""
    return text.strip().splitlines()[0].strip()


def _escape_table_cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ").strip()


def _has_decorator(node: ast.FunctionDef | ast.AsyncFunctionDef, name: str) -> bool:
    return any(
        isinstance(decorator, ast.Name) and decorator.id == name
        for decorator in node.decorator_list
    )


def _is_setter(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """`@x.setter` 的装饰器是 Attribute，不是 Name，故 `_has_decorator` 匹配不到。"""
    return any(
        isinstance(decorator, ast.Attribute) and decorator.attr == "setter"
        for decorator in node.decorator_list
    )


def _iter_args(
    args: ast.arguments, *, drop_first: bool = False
) -> list[tuple[ast.arg, ast.expr | None]]:
    positional = list(args.posonlyargs) + list(args.args)
    defaults = [None] * (len(positional) - len(args.defaults)) + list(args.defaults)
    pairs = list(zip(positional, defaults, strict=True))
    if drop_first and pairs:
        pairs = pairs[1:]
    pairs.extend(zip(args.kwonlyargs, args.kw_defaults, strict=True))
    return pairs


def _format_args(args: ast.arguments, *, drop_first: bool = False) -> str:
    parts: list[str] = []
    positional = list(args.posonlyargs) + list(args.args)
    defaults = [None] * (len(positional) - len(args.defaults)) + list(args.defaults)
    pairs = list(zip(positional, defaults, strict=True))
    if drop_first and pairs:
        pairs = pairs[1:]
    for arg, default in pairs:
        text = arg.arg
        if arg.annotation is not None:
            text += f": {ast.unparse(arg.annotation)}"
        if default is not None:
            text += f" = {ast.unparse(default)}"
        parts.append(text)
    if args.vararg is not None:
        parts.append(f"*{args.vararg.arg}")
    elif args.kwonlyargs:
        parts.append("*")
    for arg, default in zip(args.kwonlyargs, args.kw_defaults, strict=True):
        text = arg.arg
        if arg.annotation is not None:
            text += f": {ast.unparse(arg.annotation)}"
        if default is not None:
            text += f" = {ast.unparse(default)}"
        parts.append(text)
    if args.kwarg is not None:
        parts.append(f"**{args.kwarg.arg}")
    return ", ".join(parts)


def _format_function(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    prefix = "async def" if isinstance(node, ast.AsyncFunctionDef) else "def"
    returns = f" -> {ast.unparse(node.returns)}" if node.returns is not None else ""
    return f"{prefix} {node.name}({_format_args(node.args)}){returns}"


def _constructor(node: ast.ClassDef) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    for child in node.body:
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) and child.name in (
            "__new__",
            "__init__",
        ):
            return child
    return None


def _has_details(node: ast.ClassDef) -> bool:
    if ast.get_docstring(node, clean=True) or _constructor(node) is not None:
        return True
    for child in node.body:
        if (
            isinstance(child, ast.AnnAssign)
            and isinstance(child.target, ast.Name)
            and not child.target.id.startswith("__")
        ):
            return True
        if not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if child.name.startswith("__") and child.name not in ("__new__", "__init__"):
            continue
        return True
    return False


def _hidden_member(owner: str, member: str) -> bool:
    """按稳定性分级过滤：test-only / internal 成员不进入公开参考页。"""
    from api_surface import member_tier

    return member_tier("stub_classes", owner, member) in ("test-only", "internal")


def _render_class(node: ast.ClassDef) -> str:
    bases = ", ".join(ast.unparse(base) for base in node.bases)
    heading = f"### `{node.name}`"
    if bases:
        heading += f" ({bases})"
    lines = [heading, ""]
    docstring = ast.get_docstring(node, clean=True)
    if docstring:
        lines.extend([docstring, ""])

    init = _constructor(node)
    if init is not None:
        signature = f"{node.name}({_format_args(init.args, drop_first=True)})"
        lines.extend(["**构造**", "", "```python", signature, "```", ""])

    properties: list[ast.FunctionDef | ast.AsyncFunctionDef] = []
    methods: list[ast.FunctionDef | ast.AsyncFunctionDef] = []
    for child in node.body:
        if not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if child.name in ("__new__", "__init__") or _is_setter(child):
            continue
        if _hidden_member(node.name, child.name):
            continue
        if _has_decorator(child, "property"):
            properties.append(child)
            continue
        if child.name.startswith("__"):
            continue
        methods.append(child)

    # 类级注解（`x: T`）也是公开属性；PyO3 的 `#[pyo3(get, set)]` 字段在存根里
    # 以这种形式声明，不是 `@property`。
    property_names = {prop.name for prop in properties}
    attributes: list[tuple[str, str]] = []
    for child in node.body:
        if not isinstance(child, ast.AnnAssign) or not isinstance(child.target, ast.Name):
            continue
        if child.target.id in property_names or child.target.id.startswith("__"):
            continue
        if _hidden_member(node.name, child.target.id):
            continue
        annotation = ast.unparse(child.annotation) if child.annotation is not None else "Any"
        attributes.append((child.target.id, annotation))

    if properties or attributes:
        lines.extend(
            [
                "**属性**",
                "",
                "| 属性 | 类型 | 说明 |",
                "| --- | --- | --- |",
            ]
        )
        for prop in properties:
            annotation = ast.unparse(prop.returns) if prop.returns is not None else "Any"
            description = _first_line(ast.get_docstring(prop))
            lines.append(
                f"| `{prop.name}` | `{_escape_table_cell(annotation)}` | "
                f"{_escape_table_cell(description)} |"
            )
        for name, annotation in attributes:
            lines.append(f"| `{name}` | `{_escape_table_cell(annotation)}` |  |")
        lines.append("")

    if methods:
        lines.extend(["**方法**", ""])
        for method in methods:
            lines.extend(["```python", _format_function(method), "```", ""])
            docstring = ast.get_docstring(method, clean=True)
            if docstring:
                lines.extend([docstring, ""])

    return "\n".join(lines).rstrip() + "\n"


def _parse_stub() -> ast.Module:
    if not STUB.exists():
        raise FileNotFoundError(f"找不到类型存根：{STUB}")
    return ast.parse(STUB.read_text(encoding="utf-8"), filename=str(STUB))


def generate_pyo3_api() -> str:
    module = _parse_stub()
    functions = [
        node
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and not node.name.startswith("_")
    ]
    classes = [node for node in module.body if isinstance(node, ast.ClassDef)]

    lines = [
        "# PyO3 绑定 API",
        "",
        HEADER,
        "本页覆盖 Python 可直接导入的 `actant.actant` 接口。纯 Python 公共 API 见 "
        "[Python API](python-api.md)，配置摘要见 [配置项](configuration.md)，"
        "命令行见 [CLI](cli.md)。",
        "",
        "稳定性分级：`stable`（跨 0.x 尽量不破坏）/ `advanced`（可用，随内部实现"
        "调整）/ `test-only`（仅测试使用）/ `internal`（私有，不承诺兼容）。"
        "本页不列出 test-only 与 internal 成员，分级规则见 "
        "`scripts/api_surface.py`。",
        "",
    ]
    if functions:
        lines.extend(["## 函数", ""])
        for function in functions:
            lines.extend(["```python", _format_function(function), "```", ""])
            docstring = ast.get_docstring(function, clean=True)
            if docstring:
                lines.extend([docstring, ""])
    if classes:
        detailed = [klass for klass in classes if _has_details(klass)]
        compact = [klass for klass in classes if not _has_details(klass)]
        if detailed:
            lines.extend(["## 类", ""])
            for klass in detailed:
                lines.append(_render_class(klass))
        if compact:
            lines.extend(["## 其他类型", "", "| 类型 | 基类 |", "| --- | --- |"])
            for klass in compact:
                bases = ", ".join(ast.unparse(base) for base in klass.bases) or "—"
                lines.append(f"| `{klass.name}` | {_escape_table_cell(bases)} |")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def generate_configuration() -> str:
    module = _parse_stub()
    classes = {node.name: node for node in module.body if isinstance(node, ast.ClassDef)}
    lines = [
        "# 配置项",
        "",
        HEADER,
        "字段名与默认值来自 `actant/actant.pyi` 的构造签名。运行时行为说明见 "
        "[Worker 运行模型](../architecture/worker-model.md) 与 "
        "[部署与发现](../guide/deployment.md)。",
        "",
    ]
    for name in CONFIG_CLASSES:
        klass = classes.get(name)
        if klass is None:
            raise ValueError(f"类型存根中缺少配置类 {name}")
        init = _constructor(klass)
        if init is None:
            raise ValueError(f"配置类 {name} 缺少 __new__ / __init__")
        lines.extend([f"## `{name}`", ""])
        docstring = ast.get_docstring(klass, clean=True)
        if docstring:
            lines.extend([docstring, ""])
        lines.extend(["| 参数 | 类型 | 默认值 |", "| --- | --- | --- |"])
        for arg, default in _iter_args(init.args, drop_first=True):
            annotation = ast.unparse(arg.annotation) if arg.annotation is not None else "Any"
            default_text = ast.unparse(default) if default is not None else "必填"
            lines.append(
                f"| `{arg.arg}` | `{_escape_table_cell(annotation)}` | "
                f"`{_escape_table_cell(default_text)}` |"
            )
        lines.append("")
    lines.extend(
        [
            "## 环境变量",
            "",
            "| 变量 | 读取位置 | 作用 |",
            "| --- | --- | --- |",
        ]
    )
    for name, source, effect in ENV_VARS:
        lines.append(f"| `{name}` | `{source}` | {effect} |")
    lines.extend(
        [
            "",
            "测试专用：`ACTANT_SKIP_BUILD_CHECK=1` 跳过原生扩展新鲜度检查，"
            "见 [测试](../contributing/testing.md)。",
            "",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _run_help(command: tuple[str, ...]) -> str:
    env = {**os.environ, "COLUMNS": "100", "PYTHONIOENCODING": "utf-8"}
    result = subprocess.run(
        [sys.executable, "-m", "actant.cli", *command, "--help"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"生成 CLI 帮助失败（{' '.join(command) or 'actant'}）：{result.stderr.strip()}"
        )
    return result.stdout.rstrip()


def generate_cli() -> str:
    lines = [
        "# CLI",
        "",
        HEADER,
        "命令的完整参数以 `actant --help` 为准；本页由 CLI 的 `--help` 输出生成。",
        "",
    ]
    for command in CLI_COMMANDS:
        title = "actant" if not command else f"actant {' '.join(command)}"
        lines.extend([f"## `{title}`", "", "```text", _run_help(command), "```", ""])
    return "\n".join(lines).rstrip() + "\n"


def _relative(path: Path) -> str:
    return str(path.relative_to(ROOT))


def generate_pages() -> dict[Path, str]:
    return {
        REFERENCE_DIR / "pyo3-api.md": generate_pyo3_api(),
        REFERENCE_DIR / "configuration.md": generate_configuration(),
        REFERENCE_DIR / "cli.md": generate_cli(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="只校验页面是否与源码一致，不写文件",
    )
    args = parser.parse_args(argv)

    pages = generate_pages()
    if args.check:
        stale = [
            path
            for path, content in pages.items()
            if not path.exists() or path.read_text(encoding="utf-8") != content
        ]
        if stale:
            print("以下文档已过期，请运行 uv run python scripts/gen_api_reference.py：")
            for path in stale:
                print(f"  - {_relative(path)}")
            return 1
        print("文档与源码一致。")
        return 0

    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    for path, content in pages.items():
        path.write_text(content, encoding="utf-8")
        print(f"已生成 {_relative(path)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
