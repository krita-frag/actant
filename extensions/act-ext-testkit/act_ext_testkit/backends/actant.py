"""Actant 后端：把 ``actant`` 运行时适配到 testkit 契约。

除本模块与 ``_node_runner`` 外，testkit 的编排/断言/报告代码不导入
``actant``。因此其他项目可以注册自己的后端，衍生应用也可以只复用
testkit 的隔离、拓扑与报告能力。
"""

from __future__ import annotations

import shutil
from dataclasses import replace
from pathlib import Path
from typing import Any

from ..contract import (
    BackendCapabilities,
    BackendError,
    BackendUnavailable,
    ClockMode,
    EventCodec,
    FaultKind,
    IsolationLevel,
    LaunchContext,
    NetworkFidelity,
    NodeHandle,
    NodeSpec,
)
from ..node import InProcessNode, NodeConfig, ProcessNode

_PROCESS_FAULTS = frozenset(
    {
        FaultKind.KILL,
        FaultKind.CRASH,
        FaultKind.FREEZE,
        FaultKind.THAW,
        FaultKind.PARTITION,
        FaultKind.HEAL,
        FaultKind.ISOLATE,
        FaultKind.SPLIT,
        FaultKind.PAUSE_HEARTBEAT,
        FaultKind.CLOCK_DRIFT,
    }
)
_SEMANTIC_FAULTS = _PROCESS_FAULTS - {FaultKind.CRASH, FaultKind.FREEZE, FaultKind.THAW}


class ActantEventCodec:
    """把 Actant 生命周期/日志/Worker 事件归一化为 trace 字典。"""

    def lifecycle(self, raw: Any) -> dict[str, Any] | None:
        if isinstance(raw, dict):
            data = dict(raw)
            payload = data.pop("result_payload", None)
            if payload is None:
                payload = data.pop("result", None)
        else:
            data = {
                "kind": getattr(raw, "kind", ""),
                "task_id": getattr(raw, "task_id", ""),
                "workflow_id": getattr(raw, "workflow_id", ""),
                "attempt": getattr(raw, "attempt", 0),
                "next_attempt": getattr(raw, "next_attempt", 0),
                "error": getattr(raw, "error", ""),
            }
            payload = getattr(raw, "result_payload", None)
        data["payload_size"] = len(payload) if isinstance(payload, (bytes, bytearray)) else 0
        data.setdefault("kind", "")
        data.setdefault("task_id", "")
        data.setdefault("workflow_id", "")
        return data

    def log(self, raw: Any) -> dict[str, Any] | None:
        data = raw if isinstance(raw, dict) else {}
        return {
            "kind": "log",
            "task_id": str(data.get("task_id", "")),
            "data": {
                "level": str(data.get("level", "")),
                "message": str(data.get("message", "")),
            },
        }

    def worker(self, raw: Any) -> dict[str, Any] | None:
        data = raw if isinstance(raw, dict) else {}
        return {
            "kind": "worker",
            "data": {
                "state": str(data.get("state", "")),
                "node_id": str(data.get("node_id", "")),
            },
        }


class ActantBackend:
    """基于 ``actant.Runtime`` 的默认后端。"""

    name = "actant"

    def __init__(self) -> None:
        self._codec = ActantEventCodec()

    @property
    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(
            isolation=IsolationLevel.PROCESS,
            network=NetworkFidelity.SEMANTIC,
            clock=ClockMode.HLC,
            faults=_PROCESS_FAULTS,
            resources=True,
            black_box=True,
        )

    def capabilities_for(self, specs: list[NodeSpec]) -> BackendCapabilities:
        """按实际节点模式收敛能力声明，避免夸大隔离等级。"""
        modes = {spec.mode for spec in specs}
        if not modes or "in-process" in modes:
            return replace(
                self.capabilities,
                isolation=IsolationLevel.NONE,
                faults=_SEMANTIC_FAULTS,
                resources=False,
            )
        if "container" in modes:
            return replace(self.capabilities, isolation=IsolationLevel.CONTAINER)
        return self.capabilities

    def codec(self) -> EventCodec:
        return self._codec

    def launch(self, spec: NodeSpec, context: LaunchContext) -> NodeHandle:
        config = self._resolve_config(spec, context)
        clock_mode = self._clock_mode(spec, context)
        if spec.mode == "in-process":
            return self._launch_in_process(spec, context, config)
        if spec.mode == "process":
            return self._launch_process(spec, context, config, clock_mode)
        if spec.mode == "container":
            return self._launch_container(spec, context, config, clock_mode)
        raise BackendError(f"unknown Actant node mode: {spec.mode!r}")

    # ------------------------------------------------------------------
    # 配置解析
    # ------------------------------------------------------------------

    def _resolve_config(self, spec: NodeSpec, context: LaunchContext) -> NodeConfig:
        defaults = context.defaults
        if isinstance(spec.config, NodeConfig):
            config = replace(spec.config)
        else:
            config = NodeConfig(
                discovery=str(defaults.get("discovery", "local")),
                heartbeat_interval_ms=int(defaults.get("heartbeat_interval_ms", 500)),
                failure_timeout_ms=int(defaults.get("failure_timeout_ms", 3_000)),
                starvation_timeout_ms=int(defaults.get("starvation_timeout_ms", 5_000)),
            )
        config.labels = dict(spec.labels) or dict(config.labels)
        if spec.max_concurrent_tasks is not None:
            config.max_concurrent_tasks = spec.max_concurrent_tasks
        return config

    def _clock_mode(self, spec: NodeSpec, context: LaunchContext) -> ClockMode:
        raw = spec.options.get("clock_mode", context.defaults.get("clock_mode", ClockMode.HLC))
        return raw if isinstance(raw, ClockMode) else ClockMode(str(raw))

    # ------------------------------------------------------------------
    # 启动
    # ------------------------------------------------------------------

    def _launch_in_process(
        self,
        spec: NodeSpec,
        context: LaunchContext,
        config: NodeConfig,
    ) -> NodeHandle:
        try:
            from actant import Runtime
        except ImportError as exc:  # pragma: no cover - 依赖缺失时的明确报错
            raise BackendUnavailable(
                "the 'actant' package is required for the default backend; "
                "install act-ext-testkit[actant] or register another backend"
            ) from exc
        if spec.resources is not None and not spec.resources.is_empty():
            raise BackendError(
                "resource limits require mode='process' or mode='container'; "
                "in-process nodes share the pytest process"
            )
        if spec.command is not None or spec.image is not None or spec.env:
            raise BackendError(
                "command/image/env require mode='process' or mode='container'; "
                "in-process nodes share the pytest environment"
            )
        runtime = Runtime(
            name=context.runtime_name,
            data_dir=str(context.data_dir),
            config=config.to_actant_config(),
            route_policy=config.route_policy,
        )
        runtime.start()
        return InProcessNode(spec.name, runtime, codec=self._codec)

    def _launch_process(
        self,
        spec: NodeSpec,
        context: LaunchContext,
        config: NodeConfig,
        clock_mode: ClockMode,
    ) -> NodeHandle:
        return ProcessNode(
            spec.name,
            runtime_name=context.runtime_name,
            data_dir=context.data_dir,
            config=config,
            env=context.sandbox.child_env(extra=spec.env),
            stderr_path=context.sandbox.log_path(spec.name),
            codec=self._codec,
            resources=spec.resources,
            cwd=spec.options.get("cwd"),
            command=spec.command,
            clock_mode=clock_mode,
            clock_offset_ms=int(spec.options.get("clock_offset_ms", 0)),
        )

    def _launch_container(
        self,
        spec: NodeSpec,
        context: LaunchContext,
        config: NodeConfig,
        clock_mode: ClockMode,
    ) -> NodeHandle:
        image = spec.image or spec.options.get("image")
        if not image:
            raise BackendError("mode='container' requires spec.image")
        if clock_mode is ClockMode.REAL:
            raise BackendError(
                "clock_mode='real' is not supported for mode='container': "
                "per-container wall-clock offset requires image-level faketime"
            )
        docker = shutil.which("docker")
        if docker is None:
            raise BackendUnavailable(
                "mode='container' requires the docker CLI on PATH"
            )
        container_name = f"{context.runtime_name}-{context.node_name}".replace("/", "-")
        inner = list(spec.command) if spec.command else [
            "python",
            "-u",
            "-m",
            "act_ext_testkit._node_runner",
            "--config",
            "/data/testkit_node_config.json",
        ]
        command = [docker, "run", "--rm", "-i", "--init", "--name", container_name]
        network = spec.options.get("network")
        if network:
            command += ["--network", str(network)]
        resources = spec.resources
        if resources is not None:
            if resources.cpu_cores is not None:
                command += ["--cpus", str(resources.cpu_cores)]
            if resources.memory_mb is not None:
                command += ["--memory", f"{resources.memory_mb}m"]
            if resources.pids is not None:
                command += ["--pids-limit", str(resources.pids)]
        for key, value in spec.env.items():
            command += ["-e", f"{key}={value}"]
        command += [
            "-v",
            f"{Path(context.data_dir)}:/data",
            "-w",
            "/data",
            image,
            *inner,
        ]
        return ProcessNode(
            spec.name,
            runtime_name=context.runtime_name,
            data_dir=context.data_dir,
            config=config,
            env=context.sandbox.child_env(),
            stderr_path=context.sandbox.log_path(spec.name),
            codec=self._codec,
            command=command,
            cleanup_command=[docker, "kill", container_name],
            clock_mode=ClockMode.HLC,
        )
