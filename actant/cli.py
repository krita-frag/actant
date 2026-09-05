"""Actant CLI — P2P 分布式任务编排引擎的命令行入口。

与 ray/prefect/celery 等中心化编排系统不同，Actant 是 P2P 对等架构：
`actant worker` 启动的节点无需连接中心服务器，通过 iroh 自动发现对端节点。
每个节点同时是编排器与执行器。

CLI 保持极简：只负责 worker 启动与状态查询（节点、任务、集群），不直接执行或
提交任务。任务提交应通过用户代码调用 `task.submit()` / `flow()` 完成。
"""

from __future__ import annotations

import argparse
import http.server
import logging
import os
import signal
import sys
import threading
from typing import Any

import actant
from actant import TaskEvent

_logger = logging.getLogger("actant.cli")


class _MetricsHandler(http.server.BaseHTTPRequestHandler):
    """Prometheus ``/metrics`` 端点 handler。

    CLI 自有的 HTTP 托管：核心 ``Runtime`` 只提供 ``metrics_text()`` 文本，
    不持有 HTTP server（外置默认守则）。
    """

    def do_GET(self) -> None:
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        body = actant.metrics_text().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        _logger.debug("metrics: " + format, *args)


def _start_metrics_server(port: int) -> tuple[http.server.ThreadingHTTPServer, int]:
    """在 ``port`` 上启动线程化 metrics HTTP 服务器（``0`` = OS 分配端口）。

    返回 ``(server, actual_port)``；调用方负责 ``shutdown()`` + ``server_close()``。
    绑定 ``0.0.0.0``：``--metrics-port`` / ``actant metrics`` 均为显式 opt-in，
    供 Prometheus 跨机抓取；指标端点无认证，网络隔离由部署方负责。
    """
    server = http.server.ThreadingHTTPServer(("0.0.0.0", port), _MetricsHandler)
    return server, server.server_address[1]


def _configure_logging(level: str) -> None:
    """配置 Python 侧日志级别（Rust 侧 tracing 由 RUST_LOG 控制）。"""
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )


def _print_task_event(event: TaskEvent) -> None:
    """默认 TaskLifecycle handler：将任务事件输出到 stderr。"""
    suffix = ""
    if event.kind == "retried":
        suffix = f" attempt={event.attempt} next={event.next_attempt}"
    elif event.attempt > 0:
        suffix = f" attempt={event.attempt}"
    if event.error:
        suffix += f" error={event.error}"
    print(
        f"[task] {event.kind:<10} id={event.task_id} wf={event.workflow_id or '-'}{suffix}",
        file=sys.stderr,
    )


def _payload_signing_key(args: argparse.Namespace) -> str:
    """解析 payload 签名密钥（CLI 参数 > 环境变量 > 随机生成）。"""
    if args.payload_signing_key:
        return str(args.payload_signing_key)
    env_key = os.environ.get("ACTANT_PAYLOAD_KEY", "").strip()
    if env_key:
        return env_key
    import secrets

    return secrets.token_hex(32)


def _build_config(args: argparse.Namespace) -> Any:
    """根据 CLI 参数构造 ``_ActantConfig``；无自定义参数时返回 ``None``（用默认配置）。"""
    has_custom = any(
        getattr(args, k, None) is not None
        for k in (
            "max_concurrent_tasks",
            "default_task_timeout_ms",
            "drain_timeout_secs",
            "remote_fallback_delay_ms",
            "scheduler",
            "bootstrap_nodes",
            "listen_port",
        )
    )
    if not has_custom:
        return None

    from actant.actant import (
        _ActantConfig,
        _FailoverConfig,
        _GossipConfig,
        _NetworkConfig,
    )

    network = None
    if any(getattr(args, k, None) is not None for k in ("bootstrap_nodes", "listen_port")):
        network = _NetworkConfig(
            bootstrap_nodes=_split_comma(args.bootstrap_nodes),
            listen_port=args.listen_port or 0,
        )

    failover = None
    if any(
        getattr(args, k, None) is not None for k in ("heartbeat_interval_ms", "failure_timeout_ms")
    ):
        failover = _FailoverConfig(
            heartbeat_interval_ms=args.heartbeat_interval_ms,
            failure_timeout_ms=args.failure_timeout_ms,
        )

    return _ActantConfig(
        payload_signing_key=_payload_signing_key(args),
        network=network,
        failover=failover,
        gossip=_GossipConfig() if network is not None else None,
        max_concurrent_tasks=args.max_concurrent_tasks,
        default_task_timeout_ms=args.default_task_timeout_ms,
        drain_timeout_secs=args.drain_timeout_secs,
        remote_fallback_delay_ms=args.remote_fallback_delay_ms,
        scheduler=args.scheduler,
    )


def _split_comma(value: str | None) -> list[str] | None:
    """将逗号分隔字符串拆分为列表；空值返回 None。"""
    if not value:
        return None
    return [v.strip() for v in value.split(",") if v.strip()]


def cmd_metrics(args: argparse.Namespace) -> int:
    """启动独立的 Prometheus HTTP exporter。

    仅暴露 ``/metrics`` 端点，不启动 worker；适合在边车或测试场景中
    验证 exporter 配置（OTLP 端点、Prometheus 抓取）。无 worker 运行时，
    抓取到的指标只有 SDK 启动时初始化的零值计数器。

    若需在 worker 中同时暴露指标，使用 ``actant worker --metrics-port``。
    """
    _configure_logging(args.log_level)

    # 必须创建一个 _RuntimeCore 以触发 metrics::init()——否则全局 registry 为空，
    # /metrics 返回空字符串。轻量：不订阅任务 topic，不参与 P2P 发现。
    rt = actant.Runtime.with_defaults(name=f"metrics-{os.getpid()}")
    rt.start()
    # 不调用 rt.serve()——不参与 worker 调度循环。

    server, actual_port = _start_metrics_server(args.port)
    print(
        f"actant metrics exporter listening on http://0.0.0.0:{actual_port}/metrics",
        file=sys.stderr,
    )
    print("press Ctrl+C to shutdown", file=sys.stderr)

    stop_event = threading.Event()

    def _handle_signal(_signum: int, _frame: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    stop_event.wait()
    print("shutting down...", file=sys.stderr)
    server.shutdown()
    server.server_close()
    rt.stop()
    print("stopped", file=sys.stderr)
    return 0


def cmd_worker(args: argparse.Namespace) -> int:
    """启动一个 Worker 节点作为后台常驻进程。

    节点启动后通过 P2P 自动发现对端，订阅任务 topic 并执行分配到本节点的任务。
    SIGINT/SIGTERM 触发优雅 drain（等待在途任务完成）后退出。

    若指定 ``--metrics-port``，会在该端口启动一个 HTTP 服务器暴露
    Prometheus exposition format（``/metrics`` 端点），供 Prometheus 抓取。
    服务器在独立线程中运行，不阻塞 worker 主循环。
    """
    _configure_logging(args.log_level)

    config = _build_config(args)
    rt = actant.Runtime.with_defaults(
        name=args.name,
        data_dir=args.data_dir,
        config=config,
    )
    # 默认订阅 TaskLifecycle，将任务事件打印到 stderr。
    rt.layer("TaskLifecycle").chain(_print_task_event)
    rt.start()
    rt.serve()  # 非阻塞：worker.run() 在 tokio 后台 spawn

    # 可选：启动 Prometheus HTTP exporter（CLI 自有托管，见 _start_metrics_server）。
    metrics_server: http.server.ThreadingHTTPServer | None = None
    metrics_port = None
    if args.metrics_port:
        metrics_server, metrics_port = _start_metrics_server(args.metrics_port)

    node_id = rt.node_id
    print(f"actant worker started: node_id={node_id}", file=sys.stderr)
    if args.data_dir:
        print(f"data_dir: {args.data_dir}", file=sys.stderr)
    if metrics_port is not None:
        print(
            f"metrics: http://0.0.0.0:{metrics_port}/metrics",
            file=sys.stderr,
        )
    if args.data_dir:
        print(
            "P2P auto-discovery active (no central server needed)",
            file=sys.stderr,
        )
    else:
        # 显式 data_dir 缺省时 Runtime 生成临时目录并以 "none" preset 运行
        # （不启动 iroh）：单进程 worker，不参与 P2P 发现。
        print(
            "P2P disabled: no --data-dir given, running as an isolated single-process worker",
            file=sys.stderr,
        )
    print("press Ctrl+C to drain in-flight tasks and shutdown", file=sys.stderr)

    stop_event = threading.Event()

    def _handle_signal(_signum: int, _frame: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    stop_event.wait()

    print("draining in-flight tasks and shutting down...", file=sys.stderr)
    rt.stop()
    if metrics_server is not None:
        metrics_server.shutdown()
        metrics_server.server_close()
    print("worker stopped", file=sys.stderr)
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """查询本地 Runtime 状态（节点 ID、capability、handler 数、任务数）。

    读取 data_dir 元数据而非启动完整 Runtime。
    若未提供 ``--data-dir``，仅打印静态信息（版本、capability 列表）。
    若提供了 ``--data-dir``，尝试从持久化存储中读取节点信息。
    """
    print(f"actant version: {actant.__version__}")
    print(f"capabilities: {len(actant.BUILTIN_CAPABILITIES)}")
    for name in sorted(actant.BUILTIN_CAPABILITIES):
        meta = actant.BUILTIN_CAPABILITIES[name]
        print(f"  {name}: kind={meta.kind}")
    data_dir = getattr(args, "data_dir", None)
    if data_dir:
        print(f"data_dir: {data_dir}")
        import json
        import os

        meta_path = os.path.join(data_dir, "node_meta.json")
        if os.path.exists(meta_path):
            try:
                with open(meta_path, encoding="utf-8") as f:
                    meta = json.load(f)
                print(f"node_id: {meta.get('node_id', 'unknown')}")
                print(f"last_started: {meta.get('last_started', 'unknown')}")
            except (OSError, ValueError) as e:
                print(f"warning: failed to read node_meta.json: {e}", file=sys.stderr)
        else:
            print(
                "node_meta.json not found (Runtime may not have been started with this data_dir)",
                file=sys.stderr,
            )
    else:
        print("data_dir: (none specified; use --data-dir to inspect a persisted node)")
    return 0


def cmd_task(args: argparse.Namespace, *, runtime: actant.Runtime | None = None) -> int:
    """本地任务状态查询/取消（极简，不涉及跨节点调度）。

    ``list`` 读取的本地任务注册表是**进程内内存态**：仅在注入活跃 ``runtime``
    时有意义。无 ``runtime`` 注入时直接提示并返回 1，而不是新建空 Runtime
    制造"永远 no tasks"的假象。``cancel`` 同样依赖内存注册表，保持既有行为。
    """
    if args.action == "list" and runtime is None:
        print(
            "task registry is in-memory and requires a running runtime; "
            "start a runtime or use `actant status`",
            file=sys.stderr,
        )
        return 1
    rt = runtime if runtime is not None else actant.Runtime.with_defaults().start()
    owns_runtime = runtime is None
    try:
        if args.action == "list":
            task_ids = rt.list_tasks()
            if not task_ids:
                print("no tasks")
                return 0
            for tid in task_ids:
                handle = rt.get_task(tid)
                print(f"{tid} state={handle.state if handle else 'unknown'}")
        elif args.action == "cancel":
            cancelled = rt.cancel_task(args.task_id)
            if cancelled:
                print(f"cancelled: {args.task_id}")
            else:
                print(f"task not found or already done: {args.task_id}")
                return 1
        else:
            print(f"unknown action: {args.action}", file=sys.stderr)
            return 1
    finally:
        if owns_runtime:
            rt.stop()
    return 0


def cmd_version(_args: argparse.Namespace) -> int:
    print(actant.__version__)
    return 0


def _add_worker_args(p: argparse.ArgumentParser) -> None:
    """worker 子命令共享参数。"""
    p.add_argument(
        "--name",
        default=None,
        help="节点名称（默认自动生成）",
    )
    p.add_argument(
        "--data-dir",
        default=None,
        help="持久化数据目录（默认使用系统临时目录）",
    )
    p.add_argument(
        "--log-level",
        default="info",
        choices=["debug", "info", "warning", "error"],
        help="Python 侧日志级别（默认 info；Rust 侧用 RUST_LOG 环境变量）",
    )
    p.add_argument(
        "--max-concurrent-tasks",
        type=int,
        default=None,
        help="单节点最大并发任务数（默认 CPU 核数）",
    )
    p.add_argument(
        "--default-task-timeout-ms",
        type=int,
        default=None,
        help="任务默认超时毫秒（默认 30000）",
    )
    p.add_argument(
        "--drain-timeout-secs",
        type=int,
        default=None,
        help="退出时等待在途任务的最长秒数（默认 30）",
    )
    p.add_argument(
        "--remote-fallback-delay-ms",
        type=int,
        default=None,
        help="本地无法执行时任务重新入队前的延迟毫秒（默认 500）",
    )
    p.add_argument(
        "--scheduler",
        default=None,
        choices=["priority", "fifo"],
        help="调度器类型（默认 priority）",
    )
    p.add_argument(
        "--payload-signing-key",
        default=None,
        help="P2P payload 签名密钥（也可用 ACTANT_PAYLOAD_KEY 环境变量）",
    )
    p.add_argument(
        "--bootstrap-nodes",
        default=None,
        help="逗号分隔的 bootstrap 节点地址",
    )
    p.add_argument(
        "--listen-port",
        type=int,
        default=None,
        help="P2P 监听端口（默认随机端口）",
    )
    p.add_argument(
        "--heartbeat-interval-ms",
        type=int,
        default=None,
        help="故障转移心跳间隔毫秒",
    )
    p.add_argument(
        "--failure-timeout-ms",
        type=int,
        default=None,
        help="故障转移超时判定毫秒",
    )
    p.add_argument(
        "--metrics-port",
        type=int,
        default=None,
        help="若指定，在该端口启动 Prometheus HTTP exporter（/metrics 端点）",
    )


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
    _add_worker_args(p_worker)
    p_worker.set_defaults(func=cmd_worker)

    p_status = sub.add_parser(
        "status",
        help="查询本地 Runtime 状态（版本、capability、持久化节点信息）",
    )
    p_status.add_argument(
        "--data-dir",
        default=None,
        help="持久化数据目录（读取 node_meta.json 获取节点信息）",
    )
    p_status.set_defaults(func=cmd_status)

    p_task = sub.add_parser(
        "task",
        help="本地任务查询/取消（list / cancel <task-id>）",
    )
    p_task.add_argument(
        "action",
        choices=["list", "cancel"],
        help="操作类型",
    )
    p_task.add_argument(
        "task_id",
        nargs="?",
        default=None,
        help="任务 ID（cancel 时必需）",
    )
    p_task.set_defaults(func=cmd_task)

    p_version = sub.add_parser("version", help="显示版本号")
    p_version.set_defaults(func=cmd_version)

    p_metrics = sub.add_parser(
        "metrics",
        help="启动独立的 Prometheus HTTP exporter（/metrics 端点）",
    )
    p_metrics.add_argument(
        "--port",
        type=int,
        default=9100,
        help="HTTP 监听端口（默认 9100）",
    )
    p_metrics.add_argument(
        "--log-level",
        default="info",
        choices=["debug", "info", "warning", "error"],
        help="Python 侧日志级别",
    )
    p_metrics.set_defaults(func=cmd_metrics)

    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 1
    if args.command == "task" and args.action == "cancel" and not args.task_id:
        print("error: task cancel requires <task-id>", file=sys.stderr)
        return 1
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
