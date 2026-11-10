#!/usr/bin/env python3
"""从源码生成 API 参考页。

生成内容：

- ``docs/reference/pyo3-api.md``：``actant/actant.pyi`` 中的 PyO3 接口
- ``docs/reference/configuration.md``：``_ActantConfig`` 等配置类字段
- ``docs/reference/cli.md``：``actant`` CLI 的 ``--help`` 快照
- ``docs/reference/extension-points.md``：``actant/extension_points.json`` 的扩展点清单

用法::

    uv run python scripts/gen_api_reference.py          # 写入页面
    uv run python scripts/gen_api_reference.py --check  # 校验页面是否最新
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STUB = ROOT / "actant" / "actant.pyi"
REFERENCE_DIR = ROOT / "docs" / "reference"
EXTENSION_POINTS = ROOT / "actant" / "extension_points.json"

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
    ("extensions",),
    ("version",),
    ("metrics",),
)

#: 验收强度等级。登记时显式声明，避免「只有装配断言」被当成行为级 e2e。
ACCEPTANCE_LEVELS: tuple[tuple[str, str], ...] = (
    ("behavior", "真实驱动生产路径的 e2e，断言注入实现影响了引擎行为"),
    ("wiring", "经装配或公共入口证明注入实现被调用，未跑完整任务链路"),
    ("unit", "只验证实现自身，不经注入路径"),
)

#: 扩展点的解析时机。`hot_path=true` 的点只能是 `build_time`，
#: 保证 Rust 引擎热路径运行期只有一次 vtable 调用。
RESOLUTIONS: tuple[tuple[str, str], ...] = (
    ("build_time", "构造 / 装配期定稿为 trait 句柄，运行期不再查注册表"),
    ("runtime", "运行期经 handler 链动态解析，可增删覆盖"),
)

#: 扩展点分层顺序与说明。生成页按此顺序分节，新增层必须在此登记。
EXTENSION_LAYERS: tuple[tuple[str, str], ...] = (
    (
        "rust",
        "编译期 trait，经 `RuntimeBuilder::with_*` 注入；实现随引擎部署，failover 后仍在本节点可用。",
    ),
    (
        "bridge",
        "Python 代码到 Rust 热路径或提交路径的桥；实现是进程内对象，failover 后不随工作流迁移。",
    ),
    (
        "python",
        "用户态 capability handler，经 `rt.layer(name).chain(handler)` 登记；只作用于 Python 侧发起的 effect。",
    ),
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
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and not node.name.startswith("_")
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
    classes = {
        node.name: node for node in module.body if isinstance(node, ast.ClassDef)
    }
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


def generate_extension_points() -> str:
    data = json.loads(EXTENSION_POINTS.read_text(encoding="utf-8"))
    points: list[dict[str, object]] = data["points"]
    known_layers = {name for name, _ in EXTENSION_LAYERS}
    unknown = {str(p["layer"]) for p in points} - known_layers
    if unknown:
        raise ValueError(f"扩展点清单含未登记的层: {sorted(unknown)}")
    known_levels = {name for name, _ in ACCEPTANCE_LEVELS}
    bad_levels = {str(p["acceptance_level"]) for p in points} - known_levels
    if bad_levels:
        raise ValueError(f"扩展点清单含未登记的验收等级: {sorted(bad_levels)}")
    known_resolutions = {name for name, _ in RESOLUTIONS}
    bad_resolutions = {str(p["resolution"]) for p in points} - known_resolutions
    if bad_resolutions:
        raise ValueError(f"扩展点清单含未登记的解析时机: {sorted(bad_resolutions)}")
    runtime_hot = [str(p["id"]) for p in points if p["hot_path"] and p["resolution"] == "runtime"]
    if runtime_hot:
        raise ValueError(f"热路径扩展点必须构建期定稿: {sorted(runtime_hot)}")

    lines = [
        "# 扩展点清单",
        "",
        HEADER,
        "单一事实源：`actant/extension_points.json`。生成器、CLI 与 "
        "`tests/python/unit/test_extension_contract.py` 都读这份数据。",
        "",
        "本页列出**当前可用**的扩展点，不表示某个 Runtime 实例已注入某个实现。"
        "Python 无从得知 Rust 编译期注入了哪个 trait 实现；命令行用 "
        "`actant extensions` 查询同一份清单。",
        "",
        "## 登记判据",
        "",
        "每个条目必须同时具备七项，缺一不允许登记：",
        "",
        "1. `entrypoint` 与 `production_call_site` 指向真实存在的生产符号（不得是测试或示例）；"
        "`production_call_site` 记录**装配 / 绑定位置**，不表示运行期调用频率；",
        "2. `cross_node_safe` 按层固定：rust 为 `true`，bridge / python 为 `false`；",
        "3. `acceptance_test` 指向真实存在的测试或由 CI 运行的 example；",
        "4. `acceptance_level` 显式声明验收强度，未达到 behavior 的条目不得"
        "宣称已通过完整生产路径验证；",
        "5. 接线类验收不直接调用被接线函数；",
        "6. `resolution` 与 `hot_path` 组合合法：热路径点必须是 `build_time`；",
        "7. 热路径白名单文件不得出现 capability 动态分派调用或 GIL 附加，"
        "由 `test_extension_contract.py` 静态扫描。",
        "",
        "验收等级：`"
        + "` / `".join(name for name, _ in ACCEPTANCE_LEVELS)
        + "`。"
        + "；".join(f"`{name}` = {desc}" for name, desc in ACCEPTANCE_LEVELS)
        + "。低于 behavior 的条目仍是可用扩展点，但差距在"
        "「尚未提供契约的扩展意图」里显式列出。",
        "",
        "## 执行域",
        "",
        "统一的是**契约**（清单、词汇、门禁、验收），不是调用路径。",
        "",
        "`resolution` 取值："
        + "；".join(f"`{name}` = {desc}" for name, desc in RESOLUTIONS)
        + "。",
        "",
        "`hot_path` 表示实现位于 Rust 引擎的每任务 / 每消息路径：路由、调度、任务分发、"
        "传输广播、事件日志落盘。这类点在构造期二选一并冻结为 `Arc<dyn Trait>` 句柄，"
        "运行期只有一次 vtable 调用，不生成 PyO3 setter；Python 侧仍可在 "
        "`actant extensions` 查到它们，但不能在运行期替换。构造期定稿后不再解析实现，"
        "换实现必须重建 Runtime：「查得到」不等于「运行期装得上」。",
        "",
        "`CapabilityRuntime` 的 `ask` / `perform` / `emit` 需经注册表查表、编解码与 "
        "Actor 邮箱往返，只用于 `resolution=runtime` 的数据面与观测面，不进入热路径。"
        "这一边界由静态扫描守住，不依赖性能阈值断言。",
        "",
    ]
    for layer, description in EXTENSION_LAYERS:
        lines.extend([f"## `{layer}` 层", "", description, ""])
        lines.extend(
            [
                "| id | 入口 | 生产调用点 | 跨节点安全 | 解析时机 | 热路径 | "
                "验收强度 | 验收 | 说明 |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for point in points:
            if point["layer"] != layer:
                continue
            safe = "是" if point["cross_node_safe"] else "否"
            hot = "是" if point["hot_path"] else "否"
            lines.append(
                f"| `{point['id']}` | `{point['entrypoint']}` | "
                f"`{point['production_call_site']}` | {safe} | "
                f"`{point['resolution']}` | {hot} | "
                f"`{point['acceptance_level']}` | `{point['acceptance_test']}` | "
                f"{_escape_table_cell(str(point['summary']))} |"
            )
        lines.append("")

    lines.extend(
        [
            "## 尚未闭合的差距与方向",
            "",
            "`wiring_level_acceptance_upgrade` 记录已有扩展点的验收强度差距，",
            "其余两项是尚缺外部契约、目前**未接线**的方向。它们不参与门禁的符号校验，",
            "也不得出现在 `actant extensions` 的可用列表里。",
            "",
            "| 意图 | 说明 | 未覆盖原因 |",
            "| --- | --- | --- |",
        ]
    )
    for intent in data["uncovered_intents"]:
        lines.append(
            f"| `{intent['id']}` | {_escape_table_cell(str(intent['summary']))} | "
            f"{_escape_table_cell(str(intent['reason']))} |"
        )
    lines.extend(
        [
            "",
            "新增、删除或修改扩展点的流程见"
            "[贡献指南：扩展点维护](../contributing/extensions.md)。",
            "",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _relative(path: Path) -> str:
    return str(path.relative_to(ROOT))


def generate_pages() -> dict[Path, str]:
    return {
        REFERENCE_DIR / "pyo3-api.md": generate_pyo3_api(),
        REFERENCE_DIR / "configuration.md": generate_configuration(),
        REFERENCE_DIR / "cli.md": generate_cli(),
        REFERENCE_DIR / "extension-points.md": generate_extension_points(),
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
