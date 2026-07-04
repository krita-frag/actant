"""CLI 共享基础设施:退出码、输出格式化、引用解析、临时提交节点。

子命令处理器通过此处提供的工具与运行时交互,避免重复样板代码。
"""

from __future__ import annotations

import json
import sys
from typing import Any, NoReturn

# ---------------------------------------------------------------------------
# 退出码常量 — 所有子命令统一使用
# ---------------------------------------------------------------------------

EXIT_OK = 0
EXIT_USAGE = 1
EXIT_RUNTIME = 2
EXIT_TIMEOUT = 3


def die(message: str, code: int = EXIT_USAGE) -> NoReturn:
    """向 stderr 打印错误信息并退出。

    标注为 NoReturn:调用方不需要在 die 之后写 return / else 分支。
    """
    print(f"error: {message}", file=sys.stderr)
    sys.exit(code)


# ---------------------------------------------------------------------------
# 引用解析: "module:attr" 形式
# ---------------------------------------------------------------------------


def import_reference(ref: str) -> Any:
    """解析 "module.path:attribute" 引用并返回属性对象。

    例如 "myapp.flows:my_workflow" → importlib.import_module("myapp.flows") + getattr(mod, "my_workflow")
    """
    if ":" not in ref:
        die(
            f"invalid reference '{ref}': expected 'module:attr' format (e.g. myapp.flows:my_workflow)"
        )
    module_path, attr_name = ref.split(":", 1)
    module_path = module_path.strip()
    attr_name = attr_name.strip()
    if not module_path or not attr_name:
        die(f"invalid reference '{ref}': module and attr must be non-empty")

    import importlib

    try:
        mod = importlib.import_module(module_path)
    except ImportError as e:
        die(f"failed to import module '{module_path}': {e}")

    try:
        return getattr(mod, attr_name)
    except AttributeError:
        die(f"module '{module_path}' has no attribute '{attr_name}'")


def parse_arg_value(raw: str) -> Any:
    """将命令行字符串参数解析为 Python 值。

    依次尝试:int / float / bool / null / JSON / 原始字符串。
    """
    lowered = raw.lower()
    if lowered in ("true", "false"):
        return lowered == "true"
    if lowered in ("null", "none"):
        return None
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    if (raw.startswith("{") and raw.endswith("}")) or (raw.startswith("[") and raw.endswith("]")):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass
    return raw


def parse_kv_args(items: list[str]) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """解析 --arg key=value 列表为 (args, kwargs)。

    - "x=1" → kwargs["x"] = 1
    - "_pos=..." → 追加到 args(下划线前缀表示位置参数)
    - 纯值(无等号)→ 追加到 args
    """
    args: list[Any] = []
    kwargs: dict[str, Any] = {}
    for item in items or []:
        if "=" in item:
            key, val = item.split("=", 1)
            if key.startswith("_"):
                args.append(parse_arg_value(val))
            else:
                kwargs[key] = parse_arg_value(val)
        else:
            args.append(parse_arg_value(item))
    return tuple(args), kwargs


# ---------------------------------------------------------------------------
# 输出格式化
# ---------------------------------------------------------------------------


def print_output(data: Any, fmt: str = "text") -> None:
    """按指定格式打印数据到 stdout。

    text: 人类可读(对 dict/list 用 key=value 表格)
    json: 紧凑 JSON
    yaml: YAML(依赖 PyYAML,缺失时回退到 json)
    """
    if fmt == "json":
        print(json.dumps(data, default=str, ensure_ascii=False, indent=2))
        return
    if fmt == "yaml":
        try:
            import yaml
        except ImportError:
            print(json.dumps(data, default=str, ensure_ascii=False, indent=2))
            return
        print(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), end="")
        return
    _print_text(data)


def _print_text(data: Any, indent: int = 0) -> None:
    """递归打印人类可读文本。"""
    pad = "  " * indent
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, (dict, list)) and v:
                print(f"{pad}{k}:")
                _print_text(v, indent + 1)
            else:
                print(f"{pad}{k}: {v}")
    elif isinstance(data, (list, tuple)):
        for item in data:
            if isinstance(item, (dict, list)):
                print(f"{pad}-")
                _print_text(item, indent + 1)
            else:
                print(f"{pad}- {item}")
    else:
        print(f"{pad}{data}")


# ---------------------------------------------------------------------------
# 输入辅助
# ---------------------------------------------------------------------------


def add_format_arg(parser: Any) -> None:
    """为子命令添加 --format 全局参数。"""
    parser.add_argument(
        "--format",
        default="text",
        choices=["text", "json", "yaml"],
        help="Output format (default: text)",
    )
