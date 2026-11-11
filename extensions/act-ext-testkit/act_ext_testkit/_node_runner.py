"""进程外节点运行器（``python -m act_ext_testkit._node_runner``）。

stdout 是 JSON 行协议：首行 ``{"type": "ready", ...}``，之后为命令响应
``{"id": N, "ok": bool, "result": ...}`` 与异步事件
``{"type": "event", "event": {...}}``。所有日志走 stderr，避免污染协议。
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
from pathlib import Path
from typing import Any

from actant import Runtime, metrics_text
from actant.testkit import close_peer, delay_heartbeat, inject_hlc_drift, reopen_peer

from .node import NodeConfig

_WRITE_LOCK = threading.Lock()


def send(message: dict[str, Any]) -> None:
    """线程安全地写一行 JSON 到 stdout。"""
    line = json.dumps(message, ensure_ascii=False, default=str)
    with _WRITE_LOCK:
        sys.stdout.write(line + "\n")
        sys.stdout.flush()


def lifecycle_event(event: Any) -> dict[str, Any]:
    """把 ``TaskEvent`` 转为协议事件。"""
    payload = getattr(event, "result_payload", b"") or b""
    return {
        "kind": str(getattr(event, "kind", "")),
        "task_id": str(getattr(event, "task_id", "")),
        "workflow_id": str(getattr(event, "workflow_id", "")),
        "attempt": int(getattr(event, "attempt", 0) or 0),
        "next_attempt": int(getattr(event, "next_attempt", 0) or 0),
        "error": str(getattr(event, "error", "") or ""),
        "payload_size": len(payload) if isinstance(payload, (bytes, bytearray)) else 0,
        "data": {},
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="act-ext-testkit 进程外节点")
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)

    payload = json.loads(Path(args.config).read_text())
    name = str(payload["name"])
    config = NodeConfig.from_dict(dict(payload["config"]))

    runtime = Runtime(
        name=name,
        data_dir=str(Path(args.config).parent),
        config=config.to_actant_config(),
        route_policy=config.route_policy,
    )

    try:
        runtime.layer("TaskLifecycle").chain(
            lambda event: send({"type": "event", "event": lifecycle_event(event)})
        )
    except Exception as exc:  # 事件订阅失败属于基础设施错误，必须显式上报
        send({"type": "fatal", "error": f"failed to subscribe TaskLifecycle: {exc}"})
        return 2

    runtime.start()

    runtime.on_task_log(
        lambda event: send(
            {
                "type": "event",
                "event": {
                    "kind": "log",
                    "task_id": str(event.get("task_id", "")),
                    "data": {
                        "level": event.get("level", ""),
                        "message": event.get("message", ""),
                    },
                },
            }
        )
    )
    runtime.on_worker_state(
        lambda event: send(
            {
                "type": "event",
                "event": {
                    "kind": "worker",
                    "data": {
                        "state": event.get("state", ""),
                        "node_id": event.get("node_id", ""),
                    },
                },
            }
        )
    )

    send(
        {
            "type": "ready",
            "name": name,
            "node_id": runtime.node_id,
            "peer_id": runtime.peer_id,
            "listen": runtime.listen_addresses(),
        }
    )

    try:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            request: dict[str, Any] = {}
            try:
                request = json.loads(line)
                request_id = request.get("id")
                result = _dispatch(runtime, request)
            except Exception as exc:  # 单条命令失败不能杀死控制面
                send(
                    {
                        "id": request.get("id"),
                        "ok": False,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                continue
            send({"id": request_id, "ok": True, "result": result})
            if request.get("cmd") == "stop":
                break
    finally:
        try:
            runtime.stop(timeout=30.0)
        except Exception as exc:  # 退出阶段仍需尽量关闭；错误写 stderr
            print(f"runtime stop failed: {exc}", file=sys.stderr)
    return 0


def _dispatch(runtime: Runtime, request: dict[str, Any]) -> Any:
    cmd = str(request.get("cmd", ""))
    args = dict(request.get("args", {}))
    if cmd == "ping":
        return "pong"
    if cmd == "node_id":
        return runtime.node_id
    if cmd == "peer_id":
        return runtime.peer_id
    if cmd == "listen_addresses":
        return runtime.listen_addresses()
    if cmd == "peers":
        return runtime.peers()
    if cmd == "dial":
        runtime.dial(str(args["addr"]))
        return None
    if cmd == "add_gossip_peer":
        runtime.add_gossip_peer(str(args["peer_id"]))
        return None
    if cmd == "close_peer":
        close_peer(runtime, str(args["node_id"]))
        return None
    if cmd == "reopen_peer":
        reopen_peer(runtime, str(args["node_id"]))
        return None
    if cmd == "delay_heartbeat":
        delay_heartbeat(runtime, int(args["ms"]))
        return None
    if cmd == "inject_hlc_drift":
        inject_hlc_drift(runtime, int(args["ms"]))
        return None
    if cmd == "set_max_concurrent_tasks":
        runtime.set_max_concurrent_tasks(int(args["new_max"]))
        return None
    if cmd == "metrics_text":
        return metrics_text()
    if cmd == "stop":
        return {"stopping": True, "timeout_s": float(args.get("timeout_s_arg", 30.0))}
    raise ValueError(f"unsupported command: {cmd!r}")


if __name__ == "__main__":
    raise SystemExit(main())
