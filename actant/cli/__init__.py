"""Actant CLI 命令行工具入口。

子命令结构(精简):
    worker  — 计算节点生命周期 (start/stop/status/list)
    status  — 集群与 workflow 观测 (peers/workflows/workflow/cancel)

设计原则:
    - CLI 仅用于节点管理与观测,工作流提交通过 Python API(actant.submit)

用法:
    actant worker start --daemon          # 启动后台 worker
    actant worker start                   # 前台 worker
    actant worker stop                    # 停止 daemon
    actant worker list                    # 列出本地 daemon
    actant status peers                   # 查看集群拓扑
    actant status workflows               # 列出活跃 workflow
    actant status workflow <id>           # 查看 workflow 详情
    actant status cancel <id>             # 取消 workflow
"""

from __future__ import annotations

import argparse
import sys

from actant.cli import new, worker

# worker 子命令名集合,用于 argv 预处理判断(快捷语法)
_WORKER_SUBCOMMANDS = {"start", "stop", "status", "list"}


def _preprocess_argv(argv: list[str]) -> list[str]:
    """预处理 argv,为快捷语法插入隐式子命令。

    - `actant worker --daemon`        → `actant worker start --daemon`

    若 worker 后无已知子命令,则插入 "start"。
    若包含 --help/-h 则不预处理,保留原始语义。
    """
    if not argv:
        return argv

    result = list(argv)

    # --help/-h 时不预处理,让 argparse 显示对应层级的帮助
    if "-h" in result or "--help" in result:
        return result

    # 检查是否是 `worker` 命令且缺少子命令
    if result[0] == "worker" and len(result) > 1:
        # 找到 worker 后第一个非 flag 参数
        for i in range(1, len(result)):
            if not result[i].startswith("-"):
                if result[i] not in _WORKER_SUBCOMMANDS:
                    # 插入 "start" 子命令
                    result.insert(i, "start")
                break
        else:
            # 所有后续参数都是 flags → 隐式 start
            result.insert(1, "start")

    return result


def build_parser() -> argparse.ArgumentParser:
    """构建顶层 argparse 解析器与子命令树。"""
    parser = argparse.ArgumentParser(
        prog="actant",
        description="Actant — cross-platform distributed task orchestration engine",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        default=False,
        help="Print version and exit",
    )
    subparsers = parser.add_subparsers(dest="command", required=False)

    worker.register(subparsers)
    new.register(subparsers)

    return parser


def main(argv: list[str] | None = None) -> None:
    """CLI 主入口。"""
    if argv is None:
        argv = sys.argv[1:]

    # 预处理:为 `actant worker <flags>` 等快捷语法插入隐式子命令
    argv = _preprocess_argv(argv)

    parser = build_parser()
    args = parser.parse_args(argv)

    if getattr(args, "version", False):
        from actant import __version__

        print(__version__)
        return

    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        sys.exit(1)

    exit_code = handler(args)
    sys.exit(exit_code or 0)
