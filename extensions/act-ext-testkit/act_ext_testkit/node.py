"""节点句柄：同进程 Runtime 与真正独立子进程的统一控制面。

``InProcessNode`` 适合大多数功能/一致性测试（快、易调试）；``ProcessNode``
提供真实的进程隔离——SIGSTOP 冻结、SIGKILL 崩溃、独立 ``data_dir``/环境，
用于验证崩溃恢复、硬超时与"父进程不受污染"。

两种句柄暴露同一组控制操作（dial/peers/partition/kill/...），``TestCluster``
只面向句柄编程。进程模式的 IPC 是 stdin/stdout 上的 JSON 行协议（见
``_node_runner``），仅用于控制面，不承载任务载荷。
"""

from __future__ import annotations

import contextlib
import json
import os
import queue
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from actant import testkit as _testkit

from .contract import ClockMode, EventCodec, ResourceLimits


class NodeError(RuntimeError):
    """节点控制面错误（启动失败、进程退出、RPC 超时等）。"""


@dataclass
class NodeConfig:
    """Actant 节点运行时配置（可序列化，供进程内/进程外共用）。"""

    discovery: str = "local"
    heartbeat_interval_ms: int = 500
    failure_timeout_ms: int = 3_000
    starvation_timeout_ms: int = 5_000
    labels: dict[str, str] = field(default_factory=dict)
    max_concurrent_tasks: int = 1
    num_worker_processes: int | None = None
    node_quotas: dict[str, int] = field(default_factory=dict)
    crash_failover_max_attempts: int = 3
    route_policy: str | None = None
    signing_key: str = ""
    require_payload_signing: bool = False
    require_signed_records: bool = False
    allowed_peer_ids: list[str] = field(default_factory=list)
    network: dict[str, Any] = field(default_factory=dict)

    def to_actant_config(self) -> Any:
        """构造 Actant 运行时配置。

        延迟导入 ``actant``：``NodeConfig`` 本身可在未安装 Actant 的环境中
        导入，只有真正使用 Actant 后端时才要求依赖存在。
        """
        from actant.actant import _ActantConfig, _FailoverConfig, _NetworkConfig

        network_kwargs: dict[str, Any] = dict(self.network)
        network_kwargs.setdefault("preset", self.discovery)
        if self.allowed_peer_ids:
            network_kwargs["allowed_peer_ids"] = list(self.allowed_peer_ids)
        if self.require_signed_records:
            network_kwargs["require_signed_records"] = True
        return _ActantConfig(
            payload_signing_key=self.signing_key,
            require_payload_signing=self.require_payload_signing,
            network=_NetworkConfig(**network_kwargs),
            failover=_FailoverConfig(
                heartbeat_interval_ms=self.heartbeat_interval_ms,
                failure_timeout_ms=self.failure_timeout_ms,
            ),
            node_labels=dict(self.labels),
            max_concurrent_tasks=self.max_concurrent_tasks,
            num_worker_processes=self.num_worker_processes,
            node_quotas=dict(self.node_quotas),
            starvation_timeout_ms=self.starvation_timeout_ms,
            crash_failover_max_attempts=self.crash_failover_max_attempts,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "discovery": self.discovery,
            "heartbeat_interval_ms": self.heartbeat_interval_ms,
            "failure_timeout_ms": self.failure_timeout_ms,
            "starvation_timeout_ms": self.starvation_timeout_ms,
            "labels": dict(self.labels),
            "max_concurrent_tasks": self.max_concurrent_tasks,
            "num_worker_processes": self.num_worker_processes,
            "node_quotas": dict(self.node_quotas),
            "crash_failover_max_attempts": self.crash_failover_max_attempts,
            "route_policy": self.route_policy,
            "signing_key": self.signing_key,
            "require_payload_signing": self.require_payload_signing,
            "require_signed_records": self.require_signed_records,
            "allowed_peer_ids": list(self.allowed_peer_ids),
            "network": dict(self.network),
        }

    def redacted(self) -> dict[str, Any]:
        """报告用配置摘要（签名密钥只保留长度）。"""
        data = self.to_dict()
        key = str(data.get("signing_key", ""))
        data["signing_key"] = f"<redacted:{len(key)}>" if key else ""
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> NodeConfig:
        return cls(
            discovery=str(data.get("discovery", "local")),
            heartbeat_interval_ms=int(data.get("heartbeat_interval_ms", 500)),
            failure_timeout_ms=int(data.get("failure_timeout_ms", 3_000)),
            starvation_timeout_ms=int(data.get("starvation_timeout_ms", 5_000)),
            labels=dict(data.get("labels", {})),
            max_concurrent_tasks=int(data.get("max_concurrent_tasks", 1)),
            num_worker_processes=data.get("num_worker_processes"),
            node_quotas=dict(data.get("node_quotas", {})),
            crash_failover_max_attempts=int(data.get("crash_failover_max_attempts", 3)),
            route_policy=(
                None if data.get("route_policy") is None else str(data.get("route_policy"))
            ),
            signing_key=str(data.get("signing_key", "")),
            require_payload_signing=bool(data.get("require_payload_signing", False)),
            require_signed_records=bool(data.get("require_signed_records", False)),
            allowed_peer_ids=list(data.get("allowed_peer_ids", [])),
            network=dict(data.get("network", {})),
        )


class InProcessNode:
    """同进程 ``Runtime`` 的句柄。

    节点 ID / peer ID / 监听地址在构造时缓存：节点被 kill 后仍可用于断言
    （例如"其他节点最终不再看到这个 id"）。
    """

    mode = "in-process"

    def __init__(
        self,
        name: str,
        runtime: Any,
        *,
        codec: EventCodec | None = None,
    ) -> None:
        self.name = name
        self._runtime: Any | None = runtime
        self._codec = codec
        self._node_id = runtime.node_id
        self._peer_id = runtime.peer_id
        self._addresses = runtime.listen_addresses()
        self._alive = True
        self._lock = threading.Lock()

    @property
    def runtime(self) -> Any | None:
        return self._runtime

    @property
    def node_id(self) -> str:
        return str(self._node_id)

    @property
    def peer_id(self) -> str:
        return str(self._peer_id)

    @property
    def alive(self) -> bool:
        return self._alive

    def _require_runtime(self) -> Any:
        runtime = self._runtime
        if runtime is None or not self._alive:
            raise NodeError(f"node {self.name!r} is not running")
        return runtime

    def listen_addresses(self) -> dict[str, Any]:
        return dict(self._addresses)

    def peers(self) -> list[dict[str, Any]]:
        if not self._alive or self._runtime is None:
            return []
        try:
            return list(self._runtime.peers())
        except Exception:  # 节点停止竞态：视为无可见 peer
            return []

    def dial(self, addr: str) -> None:
        self._require_runtime().dial(addr)

    def add_gossip_peer(self, peer_id: str) -> None:
        self._require_runtime().add_gossip_peer(peer_id)

    def close_peer(self, node_id: str) -> None:
        _testkit.close_peer(self._require_runtime(), node_id)

    def reopen_peer(self, node_id: str) -> None:
        _testkit.reopen_peer(self._require_runtime(), node_id)

    def delay_heartbeat(self, ms: int) -> None:
        _testkit.delay_heartbeat(self._require_runtime(), ms)

    def inject_hlc_drift(self, ms: int) -> None:
        _testkit.inject_hlc_drift(self._require_runtime(), ms)

    def set_max_concurrent_tasks(self, new_max: int) -> None:
        self._require_runtime().set_max_concurrent_tasks(new_max)

    def metrics_text(self) -> str:
        if not self._alive or self._runtime is None:
            return ""
        return str(self._runtime.metrics_text())

    def subscribe(self, sink: Callable[[dict[str, Any]], None]) -> None:
        """订阅生命周期/日志/Worker 事件。

        Actant 专有钩子在这里完成最后一次耦合；其他后端实现自己的
        ``subscribe``（或由 :class:`EventCodec` 适配），``TraceRecorder``
        只依赖该通用入口。
        """
        runtime = self._require_runtime()
        codec = self._codec
        lifecycle_codec = codec.lifecycle if codec else _default_lifecycle
        log_codec = codec.log if codec else _default_log
        worker_codec = codec.worker if codec else _default_worker
        try:
            runtime.layer("TaskLifecycle").chain(
                lambda raw: _emit(sink, lifecycle_codec(raw))
            )
            runtime.on_task_log(lambda raw: _emit(sink, log_codec(raw)))
            runtime.on_worker_state(lambda raw: _emit(sink, worker_codec(raw)))
        except AttributeError as exc:
            raise NodeError(
                f"node {self.name!r} runtime does not expose Actant observation hooks: {exc}"
            ) from exc

    def crash(self) -> None:
        raise NodeError(
            f"node {self.name!r} is in-process; use mode='process' for a real crash"
        )

    def stop(self, *, timeout_s: float = 30.0) -> None:
        with self._lock:
            if not self._alive or self._runtime is None:
                self._alive = False
                return
            self._alive = False
            runtime = self._runtime
            self._runtime = None
        runtime.stop(timeout=timeout_s)

    def kill(self) -> None:
        with self._lock:
            if not self._alive or self._runtime is None:
                self._alive = False
                return
            self._alive = False
            runtime = self._runtime
            self._runtime = None
        _testkit.kill_node(runtime)

    def freeze(self) -> None:
        raise NodeError(
            f"node {self.name!r} is in-process; use mode='process' for SIGSTOP freeze"
        )

    def thaw(self) -> None:
        raise NodeError(
            f"node {self.name!r} is in-process; use mode='process' for SIGCONT thaw"
        )

    def describe(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "mode": self.mode,
            "node_id": self._node_id,
            "peer_id": self._peer_id,
            "alive": self._alive,
            "endpoint_addr": self._addresses.get("endpoint_addr", ""),
        }


class ProcessNode:
    """独立子进程中的节点句柄（真实崩溃/冻结隔离）。

    控制协议：stdin 写入 ``{"id": N, "cmd": "...", "args": {...}}``，stdout
    读取 ``{"id": N, "ok": bool, "result": ...}``；异步事件以
    ``{"type": "event", "event": {...}}`` 推送。
    """

    mode = "process"

    def __init__(
        self,
        name: str,
        *,
        data_dir: str | Path,
        config: NodeConfig,
        runtime_name: str | None = None,
        env: dict[str, str] | None = None,
        python: str | None = None,
        call_timeout_s: float = 20.0,
        startup_timeout_s: float = 30.0,
        stderr_path: str | Path | None = None,
        codec: EventCodec | None = None,
        resources: ResourceLimits | None = None,
        cwd: str | Path | None = None,
        command: Sequence[str] | None = None,
        cleanup_command: Sequence[str] | None = None,
        clock_mode: ClockMode | None = None,
        clock_offset_ms: int = 0,
    ) -> None:
        self.name = name
        self.runtime_name = runtime_name or name
        self.config = config
        self._codec = codec
        self._data_dir = Path(data_dir)
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._config_path = self._data_dir / "testkit_node_config.json"
        self._write_config(config)
        self._stderr_path = (
            Path(stderr_path) if stderr_path is not None else self._data_dir / "node.stderr.log"
        )
        self._stderr_path.parent.mkdir(parents=True, exist_ok=True)
        self._python = python or sys.executable
        self._call_timeout_s = call_timeout_s
        self._startup_timeout_s = startup_timeout_s
        self._resources = resources
        self._cwd = Path(cwd) if cwd is not None else None
        self._command = list(command) if command is not None else None
        self._cleanup_command = (
            list(cleanup_command) if cleanup_command is not None else None
        )
        self._cleanup_ran = False
        self._clock_mode = clock_mode
        self._clock_offset_ms = clock_offset_ms
        self._proc: subprocess.Popen[str] | None = None
        self._stderr_file: Any = None
        self._ready = threading.Event()
        self._exited = threading.Event()
        self._handshake: dict[str, Any] = {}
        self._replies: queue.Queue[dict[str, Any]] = queue.Queue()
        self._events: deque[dict[str, Any]] = deque(maxlen=10_000)
        self._sinks: list[Any] = []
        self._sink_errors: list[BaseException] = []
        self._sink_lock = threading.Lock()
        self._rpc_lock = threading.Lock()
        self._next_id = 0
        self._alive = False
        self._reader: threading.Thread | None = None
        self._env = dict(env) if env is not None else None
        if resources is not None:
            _validate_limits(resources)
        self._start()

    # ------------------------------------------------------------------
    # 启动 / 握手
    # ------------------------------------------------------------------

    def _write_config(self, config: NodeConfig) -> None:
        payload = {"name": self.runtime_name, "config": config.to_dict()}
        self._config_path.write_text(json.dumps(payload))
        os.chmod(self._config_path, 0o600)

    def _build_command(self) -> list[str]:
        if self._command is not None:
            base = list(self._command)
        else:
            base = [
                self._python,
                "-u",
                "-m",
                "act_ext_testkit._node_runner",
                "--config",
                str(self._config_path),
            ]
        if self._clock_mode == ClockMode.REAL:
            faketime = shutil.which("faketime")
            if faketime is None:
                raise NodeError(
                    "clock_mode='real' requires the 'faketime' binary on PATH; "
                    "install libfaketime or use clock_mode='hlc'"
                )
            offset = f"{self._clock_offset_ms / 1000:+.3f}s"
            return [faketime, "-f", offset, *base]
        return base

    def _start(self) -> None:
        command = self._build_command()
        self._stderr_file = self._stderr_path.open("a", encoding="utf-8")
        try:
            self._proc = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=self._stderr_file,
                text=True,
                bufsize=1,
                env=self._env,
                cwd=str(self._cwd) if self._cwd is not None else None,
                preexec_fn=_resource_preexec(self._resources),
                start_new_session=True,
            )
        except OSError as exc:
            self._stderr_file.close()
            raise NodeError(f"failed to spawn node {self.name!r}: {exc}") from exc
        self._alive = True
        self._reader = threading.Thread(
            target=self._read_stdout,
            name=f"testkit-node-{self.name}",
            daemon=True,
        )
        self._reader.start()
        if not self._ready.wait(timeout=self._startup_timeout_s):
            tail = self._stderr_tail()
            self.kill()
            raise NodeError(
                f"node {self.name!r} did not become ready within "
                f"{self._startup_timeout_s}s; stderr tail:\n{tail}"
            )

    def _read_stdout(self) -> None:
        proc = self._proc
        if proc is None or proc.stdout is None:
            self._exited.set()
            return
        try:
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    message = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if message.get("type") == "ready":
                    self._handshake = dict(message)
                    self._ready.set()
                elif message.get("type") == "event":
                    self._dispatch_event(dict(message.get("event", {})))
                else:
                    self._replies.put(message)
        finally:
            self._exited.set()

    def _dispatch_event(self, event: dict[str, Any]) -> None:
        event.setdefault("node", self.name)
        with self._sink_lock:
            sinks = list(self._sinks)
            if not sinks:
                self._events.append(event)
                return
        for sink in sinks:
            try:
                sink(event)
            except Exception as exc:  # sink 是观测面，不能阻塞/杀死控制面
                with self._sink_lock:
                    self._sink_errors.append(exc)

    def add_trace_sink(self, sink: Any) -> None:
        """注册事件接收器，并回放注册前缓存的事件。"""
        with self._sink_lock:
            self._sinks.append(sink)
            buffered = list(self._events)
            self._events.clear()
        for event in buffered:
            sink(event)

    def subscribe(self, sink: Callable[[dict[str, Any]], None]) -> None:
        """订阅节点事件；``EventCodec`` 在 sink 前完成归一化。"""
        codec = self._codec
        if codec is None:
            self.add_trace_sink(sink)
            return

        def decoded(event: dict[str, Any]) -> None:
            kind = str(event.get("kind", ""))
            if kind in {"started", "completed", "failed", "retried", "cancelled"}:
                mapped = codec.lifecycle(event)
            elif kind == "log":
                mapped = codec.log(event)
            elif kind == "worker":
                mapped = codec.worker(event)
            else:
                mapped = event
            _emit(sink, mapped)

        self.add_trace_sink(decoded)

    def crash(self) -> None:
        """真实进程崩溃（SIGKILL 整个进程组）。"""
        self.kill()

    @property
    def sink_errors(self) -> list[BaseException]:
        with self._sink_lock:
            return list(self._sink_errors)

    # ------------------------------------------------------------------
    # RPC
    # ------------------------------------------------------------------

    def _call(self, cmd: str, timeout_s: float | None = None, **args: Any) -> Any:
        if not self._alive or self._proc is None:
            raise NodeError(f"node {self.name!r} is not running")
        timeout = self._call_timeout_s if timeout_s is None else timeout_s
        with self._rpc_lock:
            self._next_id += 1
            request_id = self._next_id
            payload = {"id": request_id, "cmd": cmd, "args": args}
            stdin = self._proc.stdin
            if stdin is None:
                raise NodeError(f"node {self.name!r} stdin is closed")
            try:
                stdin.write(json.dumps(payload) + "\n")
                stdin.flush()
            except (BrokenPipeError, OSError) as exc:
                self._alive = False
                raise NodeError(f"node {self.name!r} control channel closed: {exc}") from exc
            deadline = time.monotonic() + timeout
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise NodeError(
                        f"node {self.name!r} command {cmd!r} timed out after {timeout}s"
                    )
                try:
                    reply = self._replies.get(timeout=min(remaining, 0.2))
                except queue.Empty:
                    if self._proc.poll() is not None:
                        self._alive = False
                        raise NodeError(
                            f"node {self.name!r} exited during command {cmd!r}; "
                            f"stderr tail:\n{self._stderr_tail()}"
                        ) from None
                    continue
                if reply.get("id") != request_id:
                    continue
                if not reply.get("ok", False):
                    raise NodeError(
                        f"node {self.name!r} command {cmd!r} failed: {reply.get('error')}"
                    )
                return reply.get("result")

    # ------------------------------------------------------------------
    # 控制面
    # ------------------------------------------------------------------

    @property
    def runtime(self) -> Any | None:
        """进程外节点没有本地 Runtime，返回 ``None``。"""
        return None

    @property
    def node_id(self) -> str:
        return str(self._handshake.get("node_id", ""))

    @property
    def peer_id(self) -> str:
        return str(self._handshake.get("peer_id", ""))

    @property
    def alive(self) -> bool:
        return self._alive and self._proc is not None and self._proc.poll() is None

    @property
    def pid(self) -> int | None:
        return self._proc.pid if self._proc is not None else None

    def listen_addresses(self) -> dict[str, Any]:
        return dict(self._handshake.get("listen", {}))

    def peers(self) -> list[dict[str, Any]]:
        if not self.alive:
            return []
        result = self._call("peers")
        return [dict(item) for item in (result or [])]

    def dial(self, addr: str) -> None:
        self._call("dial", addr=addr)

    def add_gossip_peer(self, peer_id: str) -> None:
        self._call("add_gossip_peer", peer_id=peer_id)

    def close_peer(self, node_id: str) -> None:
        self._call("close_peer", node_id=node_id)

    def reopen_peer(self, node_id: str) -> None:
        self._call("reopen_peer", node_id=node_id)

    def delay_heartbeat(self, ms: int) -> None:
        self._call("delay_heartbeat", ms=ms)

    def inject_hlc_drift(self, ms: int) -> None:
        self._call("inject_hlc_drift", ms=ms)

    def set_max_concurrent_tasks(self, new_max: int) -> None:
        self._call("set_max_concurrent_tasks", new_max=new_max)

    def metrics_text(self) -> str:
        if not self.alive:
            return ""
        return str(self._call("metrics_text") or "")

    def stop(self, *, timeout_s: float = 30.0) -> None:
        if not self.alive:
            self._cleanup_process(force=False)
            return
        with contextlib.suppress(NodeError):
            self._call("stop", timeout_s=timeout_s + 5.0, timeout_s_arg=timeout_s)
        self._alive = False
        proc = self._proc
        if proc is not None:
            try:
                proc.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                self._kill_group(signal.SIGTERM)
                try:
                    proc.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    self._kill_group(signal.SIGKILL)
        self._cleanup_process(force=False)

    def kill(self) -> None:
        """SIGKILL 整个进程组（节点 + 其 worker 子进程）。"""
        if self._proc is not None:
            self._kill_group(signal.SIGKILL)
            with contextlib.suppress(subprocess.TimeoutExpired):
                self._proc.wait(timeout=5.0)
        self._alive = False
        self._cleanup_process(force=True)

    def freeze(self) -> None:
        """SIGSTOP 冻结整个进程组（模拟节点假死/网络黑洞）。"""
        if not self.alive:
            raise NodeError(f"node {self.name!r} is not running")
        self._kill_group(signal.SIGSTOP)

    def thaw(self) -> None:
        """SIGCONT 恢复被冻结的进程组。"""
        if self._proc is None:
            raise NodeError(f"node {self.name!r} has no process")
        self._kill_group(signal.SIGCONT)

    def _kill_group(self, sig: int) -> None:
        proc = self._proc
        if proc is None:
            return
        try:
            os.killpg(os.getpgid(proc.pid), sig)
        except ProcessLookupError:
            return

    def _cleanup_process(self, *, force: bool) -> None:
        self._run_cleanup()
        proc = self._proc
        if proc is not None:
            for stream in (proc.stdin, proc.stdout):
                if stream is not None:
                    with contextlib.suppress(OSError):
                        stream.close()
        if self._stderr_file is not None:
            with contextlib.suppress(OSError):
                self._stderr_file.close()
            self._stderr_file = None
        if force:
            self._replies.put({"id": -1, "ok": False, "error": "node killed"})

    def _run_cleanup(self) -> None:
        if self._cleanup_command is None or self._cleanup_ran:
            return
        self._cleanup_ran = True
        try:
            result = subprocess.run(
                self._cleanup_command,
                capture_output=True,
                text=True,
                timeout=10.0,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            self._note_cleanup(f"cleanup command failed: {exc}")
            return
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip()
            self._note_cleanup(
                f"cleanup command exited {result.returncode}: {detail[:500]}"
            )

    def _note_cleanup(self, message: str) -> None:
        try:
            with self._stderr_path.open("a", encoding="utf-8") as handle:
                handle.write(f"[testkit cleanup] {message}\n")
        except OSError:
            # 清理诊断信息写不进去时不应掩盖原始失败。
            pass

    def _stderr_tail(self, limit: int = 4_000) -> str:
        try:
            text = self._stderr_path.read_text(errors="replace")
        except OSError:
            return "<stderr unavailable>"
        return text[-limit:]

    def describe(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "mode": self.mode,
            "node_id": self.node_id,
            "peer_id": self.peer_id,
            "pid": self.pid,
            "alive": self.alive,
            "data_dir": str(self._data_dir),
            "stderr": str(self._stderr_path),
            "endpoint_addr": self.listen_addresses().get("endpoint_addr", ""),
            "config": self.config.redacted(),
            "resources": self._resources.to_dict() if self._resources else None,
            "clock_mode": (
                self._clock_mode.value if self._clock_mode is not None else ClockMode.SHARED.value
            ),
        }


def _emit(
    sink: Callable[[dict[str, Any]], None],
    event: dict[str, Any] | None,
) -> None:
    if event is not None:
        sink(event)


def _default_lifecycle(raw: Any) -> dict[str, Any] | None:
    if isinstance(raw, dict):
        data = dict(raw)
        payload = data.get("result_payload", b"")
    else:
        data = {
            "kind": getattr(raw, "kind", ""),
            "task_id": getattr(raw, "task_id", ""),
            "workflow_id": getattr(raw, "workflow_id", ""),
            "attempt": getattr(raw, "attempt", 0),
            "next_attempt": getattr(raw, "next_attempt", 0),
            "error": getattr(raw, "error", ""),
        }
        payload = getattr(raw, "result_payload", b"")
    data["payload_size"] = len(payload) if isinstance(payload, (bytes, bytearray)) else 0
    return data


def _default_log(raw: Any) -> dict[str, Any] | None:
    data = raw if isinstance(raw, dict) else {}
    return {
        "kind": "log",
        "task_id": str(data.get("task_id", "")),
        "data": {
            "level": data.get("level", ""),
            "message": data.get("message", ""),
        },
    }


def _default_worker(raw: Any) -> dict[str, Any] | None:
    data = raw if isinstance(raw, dict) else {}
    return {
        "kind": "worker",
        "data": {
            "state": data.get("state", ""),
            "node_id": data.get("node_id", ""),
        },
    }


def _validate_limits(limits: ResourceLimits) -> None:
    if limits.is_empty():
        return
    if os.name != "posix":
        raise NodeError("resource limits require a POSIX platform")
    if limits.cpu_cores is not None and not hasattr(os, "sched_setaffinity"):
        raise NodeError(
            "cpu_cores limit is not supported on this platform "
            "(os.sched_setaffinity unavailable)"
        )


def _resource_preexec(limits: ResourceLimits | None) -> Callable[[], None] | None:
    """构造子进程 ``preexec_fn``，在 exec 前设置 rlimit 与 umask。

    ``preexec_fn`` 在多线程父进程中调用需要谨慎；这里只做同步、无分配的
    ``setrlimit``/``umask``/CPU 亲和性设置，不获取任何 Python 锁。
    """
    if limits is None or limits.is_empty():
        return None

    def apply() -> None:
        import resource

        if limits.memory_mb is not None:
            value = int(limits.memory_mb) * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (value, value))
        if limits.cpu_seconds is not None:
            value = int(limits.cpu_seconds)
            resource.setrlimit(resource.RLIMIT_CPU, (value, value))
        if limits.open_files is not None:
            value = int(limits.open_files)
            resource.setrlimit(resource.RLIMIT_NOFILE, (value, value))
        if limits.pids is not None:
            value = int(limits.pids)
            resource.setrlimit(resource.RLIMIT_NPROC, (value, value))
        if limits.disk_mb is not None:
            value = int(limits.disk_mb) * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_FSIZE, (value, value))
        if limits.cpu_cores is not None:
            cores = max(1, int(limits.cpu_cores))
            sched_setaffinity = getattr(os, "sched_setaffinity", None)
            if sched_setaffinity is not None:
                sched_setaffinity(0, set(range(cores)))
        os.umask(0o077)

    return apply

