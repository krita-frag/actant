"""Actant CLI — P2P 分布式任务编排引擎的命令行入口。

与 ray/prefect/celery 等中心化编排系统不同，Actant 是 P2P 对等架构：
`actant worker` 启动的节点无需连接中心服务器，通过 iroh 自动发现对端节点。
每个节点同时是编排器与执行器。
"""
from __future__ import annotations

import argparse
import signal
import sys
import threading

import actant


def cmd_worker(args: argparse.Namespace) -> int:
    """启动一个 Worker 节点作为后台常驻进程。

    节点启动后通过 P2P 自动发现对端，订阅任务 topic 并执行分配到本节点的任务。
    SIGINT/SIGTERM 触发优雅 drain（等待在途任务完成）后退出。
    """
    rt = actant.Runtime.with_defaults()
    rt.start()
    rt.serve()  # 非阻塞：worker.run() 在 tokio 后台 spawn

    node_id = rt.node_id
    print(f"actant worker started: node_id={node_id}", file=sys.stderr)
    print("P2P auto-discovery active (no central server needed)", file=sys.stderr)
    print("press Ctrl+C to drain in-flight tasks and shutdown", file=sys.stderr)

    stop_event = threading.Event()

    def _handle_signal(_signum: int, _frame: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    stop_event.wait()

    print("draining in-flight tasks and shutting down...", file=sys.stderr)
    rt.stop()
    print("worker stopped", file=sys.stderr)
    return 0


def cmd_version(_args: argparse.Namespace) -> int:
    print(actant.__version__)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="actant",
        description="Actant CLI — P2P 分布式任务编排引擎",
    )
    sub = parser.add_subparsers(dest="command")

    p_worker = sub.add_parser(
        "worker",
        help="启动一个 Worker 节点（后台常驻，P2P 自动发现对端）",
    )
    p_worker.set_defaults(func=cmd_worker)

    p_version = sub.add_parser("version", help="显示版本号")
    p_version.set_defaults(func=cmd_version)

    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 1
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
