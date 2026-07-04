"""worker 子命令:启动计算节点。

设计要点:
    - 纯 flag 驱动,无子命令结构(职责单一:启动 worker)
    - 不接受位置参数 modules(去除预加载模式,统一通过 @actant.task 全局注册)
    - daemon 模式与 PID 管理复用 actant.pidfile
    - 用户业务模块通过 PYTHONPATH 或 actant.task 全局注册表自动发现
"""

from __future__ import annotations

import argparse
import contextlib
import os
import re
import signal
import sys
import threading
from typing import Any

import actant
from actant.cli._common import EXIT_OK, EXIT_RUNTIME, EXIT_USAGE, die
from actant.cli._pidfile import (
    list_workers,
    remove_pid,
    stop_worker,
    worker_status,
    write_pid,
    write_pid_value,
)


def register(subparsers: Any) -> None:
    """注册 worker 子命令。

    子命令:
        start  — 启动 worker(前台或后台 daemon)
        stop   — 停止运行中的 daemon
        status — 查看单个 daemon 状态
        list   — 列出本地所有 daemon
    """
    worker_parser = subparsers.add_parser(
        "worker",
        help="Worker node lifecycle: start/stop/status/list",
    )
    worker_sub = worker_parser.add_subparsers(dest="worker_command", required=True)

    # start
    start_parser = worker_sub.add_parser("start", help="Start a worker node")
    start_parser.add_argument("--name", default=None, help="Worker name (default: actant-worker)")
    start_parser.add_argument("--listen-ip", default=None, help="Listen IP (default: 0.0.0.0)")
    start_parser.add_argument("--port", type=int, default=None, help="Listen port (default: 0 = random)")
    start_parser.add_argument("--node-id", default=None, help="Node ID (default: auto-generated)")
    start_parser.add_argument("--data-dir", default=None, help="Data directory for persistence")
    start_parser.add_argument(
        "--max-concurrent-tasks", type=int, default=None, help="Max concurrent tasks"
    )
    start_parser.add_argument(
        "--heartbeat-interval", type=float, default=None, help="Heartbeat interval in seconds"
    )
    start_parser.add_argument(
        "--heartbeat-interval-ms",
        type=int,
        default=None,
        help="[deprecated] Heartbeat interval in ms; use --heartbeat-interval instead",
    )
    start_parser.add_argument(
        "--failure-timeout", type=float, default=None, help="Failure timeout in seconds"
    )
    start_parser.add_argument(
        "--failure-timeout-ms",
        type=int,
        default=None,
        help="[deprecated] Failure timeout in ms; use --failure-timeout instead",
    )
    start_parser.add_argument(
        "--discovery",
        default=None,
        choices=["mdns", "kad", "both", "none"],
        help=(
            "Discovery mode shorthand (default: both). "
            "Maps to NetworkConfig.preset: mdns→mdns, kad→local, both→local, none→none. "
            "Unknown presets are rejected by Rust at startup."
        ),
    )
    start_parser.add_argument(
        "--bootstrap",
        action="append",
        default=[],
        help="Bootstrap node address (can be repeated)",
    )
    start_parser.add_argument(
        "--allowed-peer-id",
        action="append",
        default=[],
        dest="allowed_peer_ids",
        help=(
            "Restrict direct P2P connections to these iroh EndpointId strings "
            "(can be repeated). WARNING: by default allowed_peer_ids is empty, "
            "meaning ANY iroh peer can join the cluster. For production deployments "
            "always set an explicit whitelist."
        ),
    )
    start_parser.add_argument("--metrics-port", type=int, default=None, help="Metrics HTTP server port")
    start_parser.add_argument(
        "--metrics-bind-address",
        default=None,
        help="Metrics HTTP server bind address (default: 127.0.0.1)",
    )
    start_parser.add_argument(
        "--log-level", default=None, choices=["debug", "info", "warn", "error"], help="Log level"
    )
    start_parser.add_argument(
        "--signing-key", default=None, help="[deprecated] Payload signing key for MAC verification"
    )
    start_parser.add_argument(
        "--signing-key-file",
        default=None,
        help="Path to file containing the payload signing key (preferred over --signing-key)",
    )
    start_parser.add_argument(
        "--daemon", action="store_true", default=False, help="Run as background daemon"
    )
    start_parser.add_argument(
        "--pid-dir", default=None, help="Directory for PID files (default: ~/.actant)"
    )
    start_parser.set_defaults(handler=cmd_start)

    # stop
    stop_parser = worker_sub.add_parser("stop", help="Stop a running daemon")
    stop_parser.add_argument("--name", default="actant-worker", help="Worker name to stop")
    stop_parser.add_argument(
        "--pid-dir", default=None, help="Directory for PID files (default: ~/.actant)"
    )
    stop_parser.set_defaults(handler=cmd_stop)

    # status
    status_parser = worker_sub.add_parser("status", help="Show daemon status")
    status_parser.add_argument("--name", default="actant-worker", help="Worker name")
    status_parser.add_argument(
        "--pid-dir", default=None, help="Directory for PID files (default: ~/.actant)"
    )
    status_parser.set_defaults(handler=cmd_status)

    # list
    list_parser = worker_sub.add_parser("list", help="List all local daemons")
    list_parser.add_argument(
        "--pid-dir", default=None, help="Directory for PID files (default: ~/.actant)"
    )
    list_parser.set_defaults(handler=cmd_list)


# ---------------------------------------------------------------------------
# start 实现
# ---------------------------------------------------------------------------


def cmd_start(args: argparse.Namespace) -> int:
    """启动 worker 节点。

    Worker 通过 @actant.task 全局注册表自动发现任务(业务模块需通过
    PYTHONPATH 或 site-packages 可导入)。无需命令行指定模块路径。
    所有任务通过 __actant_generic__ handler 执行 cloudpickle payload,
    即使业务模块未预加载也能正常工作。
    """
    # 应用默认值
    if args.name is None:
        args.name = "actant-worker"
    if args.listen_ip is None:
        args.listen_ip = "0.0.0.0"
    if args.port is None:
        args.port = 0
    if args.discovery is None:
        args.discovery = "both"
    if args.log_level is None:
        args.log_level = "info"

    # 校验
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", args.name):
        die(
            f"invalid name '{args.name}': must contain only alphanumeric, hyphen, or underscore"
        )
    if not (0 <= args.port <= 65535):
        die(f"invalid port {args.port}: must be 0..65535")
    if args.metrics_port is not None and not (0 <= args.metrics_port <= 65535):
        die(f"invalid metrics_port {args.metrics_port}: must be 0..65535")

    heartbeat_interval = _resolve_seconds(
        args.heartbeat_interval,
        args.heartbeat_interval_ms,
        "--heartbeat-interval",
        "--heartbeat-interval-ms",
    )
    failure_timeout = _resolve_seconds(
        args.failure_timeout,
        args.failure_timeout_ms,
        "--failure-timeout",
        "--failure-timeout-ms",
    )

    # discovery 字符串映射到 NetworkConfig preset
    # mdns → mDNS 局域网发现; kad/both → iroh local 预设(DNS+relay); none → 纯手动
    discovery_preset = {
        "mdns": "mdns",
        "kad": "local",
        "both": "local",
        "none": "none",
    }.get(args.discovery, "local")

    # --daemon:fork 到后台
    if args.daemon:
        _daemonize(args.name, args.pid_dir)
        # daemon 模式下，PID 文件在 _daemonize 内已写入（使用 fork 后的
        # 子进程 PID），避免父进程退出到子进程写 PID 之间的竞态窗口。
    elif os.environ.get("ACTANT_PID_WRITTEN") == "1":
        # Windows daemon 子进程：PID 已由父进程写入，跳过
        os.environ.pop("ACTANT_PID_WRITTEN", None)
    else:
        # 前台模式：在节点启动前写入 PID 文件，防止重复启动
        write_pid(args.name, args.pid_dir)

    # 解析 payload signing key，优先顺序：
    # 1. --signing-key-file
    # 2. ACTANT_SIGNING_KEY 环境变量
    # 3. --signing-key（已弃用，保留以兼容旧脚本）
    signing_key = _resolve_signing_key(
        file_path=args.signing_key_file,
        env_var=os.environ.get("ACTANT_SIGNING_KEY"),
        cli_value=args.signing_key,
    )
    if signing_key is None:
        die(
            "missing payload signing key: provide --signing-key-file, "
            "set ACTANT_SIGNING_KEY environment variable, or use --signing-key"
        )

    # 配置日志
    from actant._logging import configure_logging

    configure_logging(args.log_level, force=True)

    # 启动常驻节点(接收并执行任务)
    # 若 start 失败需清理 PID 文件，否则残留 PID 会阻止后续启动。
    try:
        node = actant.start(
            name=args.name,
            listen_ip=args.listen_ip,
            port=args.port,
            node_id=args.node_id,
            data_dir=args.data_dir,
            max_concurrent_tasks=args.max_concurrent_tasks,
            heartbeat_interval=heartbeat_interval,
            failure_timeout=failure_timeout,
            network=actant.NetworkConfig(
                preset=discovery_preset,
                bootstrap_nodes=args.bootstrap,
                allowed_peer_ids=tuple(args.allowed_peer_ids),
            ),
            log_level=args.log_level,
            signing_key=signing_key,
        )
    except Exception:
        remove_pid(args.name, args.pid_dir)
        raise

    # 通过全局注册表自动发现任务(@actant.task 装饰的函数)
    task_names = node.task_names
    if task_names:
        print(f"worker started: node_id={node.node_id}")
        print(f"discovered tasks ({len(task_names)}): {', '.join(task_names)}")
    else:
        print(f"worker started (pure compute, generic dispatch): node_id={node.node_id}")

    addrs = node.listen_addresses()
    for addr in addrs.get("direct_addrs", []) or []:
        print(f"listening on: {addr}")
    if addrs.get("relay_url"):
        print(f"relay: {addrs['relay_url']}")

    if args.metrics_port is not None:
        bind_addr = args.metrics_bind_address or "127.0.0.1"
        actual_port = node.start_metrics_server(port=args.metrics_port, bind_address=bind_addr)
        print(f"metrics server listening on {bind_addr}:{actual_port}")

    # 等待关闭信号
    shutdown_event = threading.Event()

    def _signal_handler(_signum: int, _frame: object) -> None:
        print("\nshutting down...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    shutdown_event.wait()

    # 优雅关闭
    actant.stop()
    remove_pid(args.name, args.pid_dir)
    print("worker stopped.")
    return EXIT_OK


def _resolve_signing_key(
    file_path: str | None,
    env_var: str | None,
    cli_value: str | None,
) -> str | None:
    """按优先级解析 signing key 来源，并给出弃用警告。"""
    if file_path is not None:
        try:
            with open(file_path, encoding="utf-8") as f:
                return f.read().strip()
        except OSError as exc:
            die(f"cannot read signing key file '{file_path}': {exc}")
    if env_var is not None:
        return env_var
    if cli_value is not None:
        import warnings

        warnings.warn(
            "--signing-key is deprecated: use --signing-key-file or ACTANT_SIGNING_KEY instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return cli_value
    return None


def _resolve_seconds(
    seconds_value: float | None,
    ms_value: int | None,
    seconds_flag: str,
    ms_flag: str,
) -> float | None:
    """统一秒与毫秒 CLI 选项，优先使用秒。"""
    if seconds_value is not None and ms_value is not None:
        die(f"cannot use both {seconds_flag} and {ms_flag}: provide only one")
    if seconds_value is not None:
        return seconds_value
    if ms_value is not None:
        import warnings

        warnings.warn(
            f"{ms_flag} is deprecated: use {seconds_flag} (seconds) instead",
            DeprecationWarning,
            stacklevel=3,
        )
        return ms_value / 1000.0
    return None


# ---------------------------------------------------------------------------
# stop / status / list
# ---------------------------------------------------------------------------


def cmd_stop(args: argparse.Namespace) -> int:
    """停止运行中的 daemon。"""
    if stop_worker(args.name, args.pid_dir):
        print(f"worker '{args.name}' stopped")
        return EXIT_OK
    print(f"worker '{args.name}' is not running", file=sys.stderr)
    return EXIT_USAGE


def cmd_status(args: argparse.Namespace) -> int:
    """显示单个 daemon 状态。"""
    info = worker_status(args.name, args.pid_dir)
    if info["running"]:
        print(f"worker '{info['name']}' is running (pid {info['pid']})")
        return EXIT_OK
    print(f"worker '{info['name']}' is not running")
    return EXIT_RUNTIME


def cmd_list(args: argparse.Namespace) -> int:
    """列出本地所有 daemon。"""
    workers = list_workers(args.pid_dir)
    if not workers:
        print("no workers registered")
        return EXIT_OK
    print(f"{'NAME':<30} {'PID':<10} {'STATUS'}")
    for w in workers:
        status = "running" if w["running"] else "stale"
        pid = str(w.get("pid", "-"))
        print(f"{w['name']:<30} {pid:<10} {status}")
    return EXIT_OK


# ---------------------------------------------------------------------------
# daemon 化
# ---------------------------------------------------------------------------


def _daemonize(name: str, pid_dir: str | None = None) -> None:
    """将当前进程转为后台 daemon。"""
    if sys.platform == "win32":
        _daemonize_windows(name, pid_dir)
    else:
        _daemonize_unix(name, pid_dir)


def _daemonize_unix(name: str, _pid_dir: str | None = None) -> None:
    """Unix 双 fork 守护进程化。

    在第二次 fork 后立即写入 PID 文件，消除父进程退出到子进程
    写 PID 之间的竞态窗口。父进程在最终退出前会短暂等待，验证
    daemon 进程是否仍在运行，避免误报成功。
    """
    pid = os.fork()
    if pid > 0:
        # 顶层父进程：等待孙进程写入 PID 并验证其存活
        with contextlib.suppress(OSError):
            # 等待子进程结束（它只会快速退出）
            os.waitpid(pid, 0)

        # 给孙进程一点时间完成启动或快速失败
        import time

        time.sleep(0.5)

        info = worker_status(name, _pid_dir)
        if not info["running"]:
            die(f"daemon '{name}' failed to start: process not found after fork", EXIT_RUNTIME)
        sys.exit(0)

    os.setsid()

    pid = os.fork()
    if pid > 0:
        sys.exit(0)

    # 在 daemon 进程内立即写入 PID 文件，防止竞态
    write_pid(name, _pid_dir)

    _redirect_stdio(name)


# Windows 子进程创建标志（等价于 subprocess.DETACHED_PROCESS / CREATE_NEW_PROCESS_GROUP）。
# 使用字面量而非平台相关属性，避免在 macOS/Linux 上触发 mypy 属性错误，
# 同时 _daemonize_windows 仅在 sys.platform == "win32" 时才会被调用。
_WIN_DETACHED_PROCESS = 0x00000008
_WIN_CREATE_NEW_PROCESS_GROUP = 0x00000200


def _daemonize_windows(name: str, pid_dir: str | None = None) -> None:
    """Windows 守护进程化:以分离子进程方式重新启动。

    父进程在退出前用子进程 PID 写入 PID 文件，并等待验证子进程是否
    成功启动。若子进程在短暂时间内退出，父进程会输出日志中的错误
    信息并返回非零退出码，避免误报成功。
    """
    import subprocess

    log_dir = os.path.join(os.path.expanduser("~"), ".actant", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{name}.log")

    cmd = [sys.executable, *sys.argv]
    cmd = [a for a in cmd if a != "--daemon"]

    # 传递环境变量给子进程，告知 PID 已由父进程写入
    env = os.environ.copy()
    env["ACTANT_PID_WRITTEN"] = "1"

    with open(log_path, "a", encoding="utf-8") as log_f:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=log_f,
            stderr=log_f,
            env=env,
            creationflags=_WIN_DETACHED_PROCESS | _WIN_CREATE_NEW_PROCESS_GROUP,
        )

    # 父进程代子进程写入 PID 文件，消除竞态窗口
    write_pid_value(proc.pid, name, pid_dir)

    # 等待验证子进程是否成功启动
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        # 子进程仍在运行，daemon 启动成功
        sys.exit(0)

    # 子进程已退出，启动失败
    remove_pid(name, pid_dir)
    last_lines = _tail(log_path, lines=8)
    details = f"\n{last_lines}" if last_lines else ""
    die(f"daemon failed to start (exit {proc.returncode}){details}", EXIT_RUNTIME)


def _tail(path: str, lines: int = 8) -> str:
    """读取文件最后 n 行，用于 daemon 启动失败时输出日志摘要。"""
    try:
        with open(path, encoding="utf-8") as f:
            all_lines = f.readlines()
            return "".join(all_lines[-lines:]).rstrip()
    except OSError:
        return ""


def _redirect_stdio(name: str) -> None:
    """为 daemon 模式重定向 stdin/stdout/stderr。"""
    sys.stdout.flush()
    sys.stderr.flush()

    log_dir = os.path.join(os.path.expanduser("~"), ".actant", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{name}.log")

    with open(os.devnull) as devnull_r:
        os.dup2(devnull_r.fileno(), sys.stdin.fileno())
    with open(log_path, "a", encoding="utf-8") as log_f:
        os.dup2(log_f.fileno(), sys.stdout.fileno())
        os.dup2(log_f.fileno(), sys.stderr.fileno())
