"""轻量 soak：本机多进程集群 + 连续负载 + 周期故障。

定位：不引入容器、虚拟机或自托管 runner，只用 POSIX 进程边界
（``Sandbox`` + ``ProcessNode``）构造"本机多机"：

- 提交节点在父进程内（不开 worker），Worker 节点是独立进程组；
- 每个 Worker 有独立 ``data_dir``、``TMPDIR``、``HOME`` 与 iroh endpoint；
- 周期注入 SIGSTOP/SIGCONT、单向分区、心跳延迟与 SIGKILL + 补位；
- 持续提交任务，统计完成/失败/丢失、延迟分位与吞吐；
- 结束时校验集群收敛、进程清理与失败率，并输出 JSON 报告。

这不是安全沙箱，也不替代真实跨机网络；它回答的是"在真实进程崩溃、
冻结和分区下，任务是否仍能收敛，集群是否留下脏状态"。

仅支持 POSIX。Windows 没有 ``SIGSTOP``/进程组语义，直接拒绝。
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .cluster import TestCluster
from .metrics import PerformanceSampler
from .node import NodeConfig


class SoakError(RuntimeError):
    """soak 配置或编排失败。"""


@dataclass
class SoakConfig:
    """soak 参数。

    ``duration_s`` 是稳定负载时长，不含启动/收敛/排空；夜间任务通常设为
    900–1800 秒，本地冒烟用 10–30 秒。
    """

    namespace: str = "soak"
    workers: int = 2
    duration_s: float = 60.0
    warmup_s: float = 2.0
    concurrency: int = 4
    rate_limit_per_s: float = 20.0
    task_sleep_ms: int = 25
    task_timeout_s: float = 60.0
    fault_interval_s: float = 10.0
    fault_hold_s: float = 2.0
    convergence_timeout_s: float = 20.0
    drain_timeout_s: float = 90.0
    max_failure_rate: float = 0.01
    discovery: str = "none"
    seed: int = 0
    kill_faults: bool = True
    keep_data: bool = False
    report_path: Path | None = None

    def __post_init__(self) -> None:
        if self.workers < 1:
            raise ValueError("workers must be >= 1")
        if self.duration_s <= 0:
            raise ValueError("duration_s must be positive")
        if self.concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        if self.rate_limit_per_s < 0:
            raise ValueError("rate_limit_per_s must be >= 0")
        if self.task_timeout_s <= 0:
            raise ValueError("task_timeout_s must be positive")
        if self.fault_interval_s <= 0:
            raise ValueError("fault_interval_s must be positive")
        if self.fault_hold_s < 0:
            raise ValueError("fault_hold_s must be >= 0")
        if not 0.0 <= self.max_failure_rate <= 1.0:
            raise ValueError("max_failure_rate must be within [0, 1]")

    def to_dict(self) -> dict[str, Any]:
        return {
            "namespace": self.namespace,
            "workers": self.workers,
            "duration_s": self.duration_s,
            "warmup_s": self.warmup_s,
            "concurrency": self.concurrency,
            "rate_limit_per_s": self.rate_limit_per_s,
            "task_sleep_ms": self.task_sleep_ms,
            "task_timeout_s": self.task_timeout_s,
            "fault_interval_s": self.fault_interval_s,
            "fault_hold_s": self.fault_hold_s,
            "convergence_timeout_s": self.convergence_timeout_s,
            "drain_timeout_s": self.drain_timeout_s,
            "max_failure_rate": self.max_failure_rate,
            "discovery": self.discovery,
            "seed": self.seed,
            "kill_faults": self.kill_faults,
            "keep_data": self.keep_data,
            "report_path": None if self.report_path is None else str(self.report_path),
        }


@dataclass
class SoakResult:
    """soak 结果。``ok`` 为 False 时 CLI 返回非零。"""

    config: SoakConfig
    submitted: int
    completed: int
    failed: int
    lost: int
    fault_count: int
    fault_errors: list[str] = field(default_factory=list)
    convergence_violations: list[str] = field(default_factory=list)
    cleanup_errors: list[str] = field(default_factory=list)
    failure_samples: list[str] = field(default_factory=list)
    latency_ms: dict[str, float] = field(default_factory=dict)
    throughput_per_s: float = 0.0
    duration_s: float = 0.0
    report_path: Path | None = None

    @property
    def failure_rate(self) -> float:
        return self.failed / self.submitted if self.submitted else 0.0

    @property
    def ok(self) -> bool:
        return (
            self.submitted > 0
            and self.lost == 0
            and not self.fault_errors
            and not self.convergence_violations
            and not self.cleanup_errors
            and self.failure_rate <= self.config.max_failure_rate
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "config": self.config.to_dict(),
            "submitted": self.submitted,
            "completed": self.completed,
            "failed": self.failed,
            "lost": self.lost,
            "failure_rate": round(self.failure_rate, 6),
            "fault_count": self.fault_count,
            "fault_errors": list(self.fault_errors),
            "convergence_violations": list(self.convergence_violations),
            "cleanup_errors": list(self.cleanup_errors),
            "failure_samples": list(self.failure_samples),
            "latency_ms": dict(self.latency_ms),
            "throughput_per_s": self.throughput_per_s,
            "duration_s": self.duration_s,
            "report_path": None if self.report_path is None else str(self.report_path),
        }


class _RatePacer:
    """固定速率节流；``rate <= 0`` 表示不限制。"""

    def __init__(self, rate_per_s: float) -> None:
        self._interval = 0.0 if rate_per_s <= 0 else 1.0 / rate_per_s
        self._next = 0.0

    def wait(self) -> None:
        if self._interval <= 0:
            return
        now = time.monotonic()
        if self._next > now:
            time.sleep(self._next - now)
            now = time.monotonic()
        self._next = max(self._next, now) + self._interval


def _make_task(task_timeout_ms: int) -> Any:
    """构造极简任务；延迟导入避免 testkit 包导入即依赖 actant。"""
    from actant.tasks import task

    @task(tags=["engine=worker"], timeout_ms=task_timeout_ms)
    def soak_task(seq: int, sleep_ms: int) -> int:
        time.sleep(sleep_ms / 1000)
        return seq

    return soak_task


def _run_one(
    task: Any,
    runtime: Any,
    seq: int,
    sleep_ms: int,
    timeout_s: float,
) -> float:
    """提交一次并等待结果，返回端到端延迟（毫秒）。"""
    from actant import use_runtime

    started = time.perf_counter()
    with use_runtime(runtime):
        result = task.submit(seq, sleep_ms).result(timeout=timeout_s)
    if result != seq:
        raise SoakError(f"task {seq} returned {result!r}, expected {seq!r}")
    return (time.perf_counter() - started) * 1000.0


def _worker_config(config: SoakConfig) -> NodeConfig:
    return NodeConfig(
        discovery=config.discovery,
        heartbeat_interval_ms=300,
        failure_timeout_ms=1_500,
        starvation_timeout_ms=4_000,
        labels={"engine": "worker"},
        max_concurrent_tasks=2,
        crash_failover_max_attempts=5,
    )


def _apply_fault(
    cluster: TestCluster,
    config: SoakConfig,
    rng: random.Random,
    live_workers: list[str],
    round_no: int,
    sampler: PerformanceSampler,
) -> str:
    """注入一次可恢复故障，返回故障类型。"""
    choices = ["freeze", "partition", "heartbeat"]
    if config.kill_faults and len(live_workers) >= 2:
        choices.append("kill")
    kind = rng.choice(choices)

    if kind == "freeze":
        victim = rng.choice(live_workers)
        cluster.report.record_fault({"kind": "freeze", "node": victim, "round": round_no})
        cluster.freeze(victim)
        time.sleep(config.fault_hold_s)
        cluster.thaw(victim)
        return kind

    if kind == "partition":
        if len(live_workers) < 2:
            return "partition-skipped"
        source, target = rng.sample(live_workers, 2)
        cluster.report.record_fault(
            {"kind": "partition", "source": source, "target": target, "round": round_no}
        )
        cluster.partition(source, target)
        time.sleep(config.fault_hold_s)
        cluster.heal(source, target)
        return kind

    if kind == "heartbeat":
        victim = rng.choice(live_workers)
        cluster.report.record_fault(
            {"kind": "pause_heartbeat", "node": victim, "round": round_no}
        )
        cluster.pause_heartbeat(victim, ms=int(config.fault_hold_s * 1000))
        time.sleep(config.fault_hold_s)
        return kind

    victim = rng.choice(live_workers)
    cluster.report.record_fault({"kind": "kill", "node": victim, "round": round_no})
    cluster.kill(victim)
    cluster.leave(victim, timeout_s=5.0)
    live_workers.remove(victim)
    replacement = f"{victim}-r{round_no}"
    handle = cluster.join(
        replacement,
        labels={"engine": "worker"},
        mode="process",
        config=_worker_config(config),
        connect_to=["submit", *live_workers],
        connect_timeout_s=config.convergence_timeout_s,
    )
    live_workers.append(replacement)
    _watch_process(sampler, replacement, handle)
    return kind


def _watch_process(sampler: PerformanceSampler, name: str, handle: Any) -> None:
    """把进程节点加入 RSS 采样；同进程节点没有 pid。"""
    pid = getattr(handle, "pid", None)
    if pid:
        sampler.watch_process(int(pid), label=name)


def _fault_loop(
    cluster: TestCluster,
    config: SoakConfig,
    deadline: float,
    live_workers: list[str],
    stop: threading.Event,
    errors: list[str],
    violations: list[str],
    sampler: PerformanceSampler,
) -> None:
    rng = random.Random(config.seed ^ 0x5A17)
    round_no = 0
    while not stop.wait(config.fault_interval_s):
        if time.monotonic() >= deadline:
            return
        round_no += 1
        try:
            _apply_fault(cluster, config, rng, live_workers, round_no, sampler)
            cluster.wait_converged(timeout_s=config.convergence_timeout_s)
        except Exception as exc:  # 批量编排：记录后继续，最终由 result.ok 判定
            message = f"fault round {round_no}: {type(exc).__name__}: {exc}"
            errors.append(message)
            violations.append(message)


def run_soak(config: SoakConfig) -> SoakResult:
    """运行一次 soak；仅支持 POSIX。"""
    if os.name == "nt":
        raise SoakError("soak 仅支持 POSIX：Windows 缺少 SIGSTOP/进程组语义")

    result = SoakResult(
        config=config,
        submitted=0,
        completed=0,
        failed=0,
        lost=0,
        fault_count=0,
    )
    live_workers = [f"w{index}" for index in range(config.workers)]
    sampler = PerformanceSampler(interval_s=1.0)
    fault_stop = threading.Event()
    fault_thread: threading.Thread | None = None
    started_at = time.monotonic()

    cluster = TestCluster(
        config.namespace,
        discovery=config.discovery,
        heartbeat_interval_ms=300,
        failure_timeout_ms=1_500,
        starvation_timeout_ms=4_000,
        seed=config.seed,
        keep_data=config.keep_data,
        strict_env=True,
    )

    try:
        cluster.add_node("submit", labels={"role": "submit"}, max_concurrent_tasks=0)
        for name in live_workers:
            cluster.add_node(
                name,
                labels={"engine": "worker"},
                mode="process",
                config=_worker_config(config),
            )
        cluster.start(topology="mesh", connect_timeout_s=30.0)
        cluster.report.record("soak_started", **config.to_dict())

        sampler.start()
        for name in live_workers:
            _watch_process(sampler, name, cluster.handle(name))

        task = _make_task(max(int(config.task_timeout_s * 1000), 60_000))
        runtime = cluster.node("submit")
        time.sleep(config.warmup_s)
        deadline = time.monotonic() + config.duration_s

        fault_thread = threading.Thread(
            target=_fault_loop,
            args=(
                cluster,
                config,
                deadline,
                live_workers,
                fault_stop,
                result.fault_errors,
                result.convergence_violations,
                sampler,
            ),
            name="actant-soak-faults",
            daemon=True,
        )
        fault_thread.start()

        pacer = _RatePacer(config.rate_limit_per_s)
        failures = 0
        futures: dict[Future[float], int] = {}
        with ThreadPoolExecutor(
            max_workers=config.concurrency, thread_name_prefix="actant-soak"
        ) as pool:
            while time.monotonic() < deadline:
                while len(futures) < config.concurrency and time.monotonic() < deadline:
                    pacer.wait()
                    seq = result.submitted
                    result.submitted += 1
                    futures[
                        pool.submit(
                            _run_one,
                            task,
                            runtime,
                            seq,
                            config.task_sleep_ms,
                            config.task_timeout_s,
                        )
                    ] = seq
                if not futures:
                    break
                done, _ = wait(
                    tuple(futures), timeout=0.2, return_when=FIRST_COMPLETED
                )
                for future in done:
                    seq = futures.pop(future)
                    try:
                        latency_ms = future.result()
                    except Exception as exc:  # 单任务失败：计数并采样
                        failures += 1
                        if len(result.failure_samples) < 20:
                            result.failure_samples.append(
                                f"task {seq}: {type(exc).__name__}: {exc}"
                            )
                    else:
                        result.completed += 1
                        sampler.record_latency_ms(latency_ms)
                        sampler.record_completed()

            if futures:
                done, not_done = wait(futures, timeout=config.drain_timeout_s)
                for future in done:
                    seq = futures.pop(future)
                    try:
                        latency_ms = future.result()
                    except Exception as exc:  # 排空阶段失败同样计数
                        failures += 1
                        if len(result.failure_samples) < 20:
                            result.failure_samples.append(
                                f"task {seq}: {type(exc).__name__}: {exc}"
                            )
                    else:
                        result.completed += 1
                        sampler.record_latency_ms(latency_ms)
                        sampler.record_completed()
                result.lost = len(not_done)

        result.failed = failures
        result.fault_count = len(cluster.report.snapshot()["faults"])
        result.latency_ms = sampler.percentiles()
        result.throughput_per_s = sampler.throughput()
    finally:
        fault_stop.set()
        if fault_thread is not None:
            fault_thread.join(timeout=config.fault_hold_s + 10.0)
        try:
            sampler.stop()
        except Exception as exc:  # 清理失败需进入结果，不吞掉
            result.cleanup_errors.append(f"sampler.stop: {type(exc).__name__}: {exc}")
        try:
            cluster.stop()
        except Exception as exc:  # 清理失败需进入结果，不吞掉
            result.cleanup_errors.append(f"cluster.stop: {type(exc).__name__}: {exc}")

    result.duration_s = round(time.monotonic() - started_at, 3)
    cluster.report.set_manifest(
        {
            "soak": result.to_dict(),
            "nodes": {
                "submit": {"mode": "in-process"},
                **{name: {"mode": "process"} for name in live_workers},
            },
        }
    )
    cluster.report.attach_metrics(sampler)
    if config.report_path is not None:
        path = Path(config.report_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        cluster.report.dump(path)
        result.report_path = path
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Actant testkit 本机多进程 soak（POSIX only）"
    )
    parser.add_argument("--namespace", default="soak")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--duration-seconds", type=float, default=60.0)
    parser.add_argument("--warmup-seconds", type=float, default=2.0)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--rate", type=float, default=20.0)
    parser.add_argument("--task-sleep-ms", type=int, default=25)
    parser.add_argument("--task-timeout", type=float, default=60.0)
    parser.add_argument("--fault-interval", type=float, default=10.0)
    parser.add_argument("--fault-hold", type=float, default=2.0)
    parser.add_argument("--max-failure-rate", type=float, default=0.01)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--no-kill", action="store_true")
    parser.add_argument("--keep-data", action="store_true")
    parser.add_argument("--report", type=Path, default=None)
    args = parser.parse_args(argv)

    config = SoakConfig(
        namespace=args.namespace,
        workers=args.workers,
        duration_s=args.duration_seconds,
        warmup_s=args.warmup_seconds,
        concurrency=args.concurrency,
        rate_limit_per_s=args.rate,
        task_sleep_ms=args.task_sleep_ms,
        task_timeout_s=args.task_timeout,
        fault_interval_s=args.fault_interval,
        fault_hold_s=args.fault_hold,
        max_failure_rate=args.max_failure_rate,
        seed=args.seed,
        kill_faults=not args.no_kill,
        keep_data=args.keep_data,
        report_path=args.report,
    )
    try:
        result = run_soak(config)
    except Exception as exc:  # CLI 边界：打印并返回非零，不伪装成功
        print(f"soak failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
