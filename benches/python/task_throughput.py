"""任务吞吐基线：短任务吞吐、任务超时、worker 崩溃恢复。

用法::

    uv run --extra dev python benches/python/task_throughput.py --tasks 500 --workers 8 --task-ms 5
    uv run --extra dev python benches/python/task_throughput.py --tasks 100 --timeout-case
    uv run --extra dev python benches/python/task_throughput.py --tasks 20 --crash-case

这些数据是后续 TaskSource / WorkerLease 改造的对照基线，不做通过/失败判定。
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from actant import Runtime, task, use_runtime

_CRASH_MARKER = ""


@task(name="bench_work")
def _work(index: int, sleep_ms: int) -> int:
    if sleep_ms > 0:
        time.sleep(sleep_ms / 1000.0)
    return index


@task(name="bench_slow")
def _slow(sleep_ms: int) -> int:
    time.sleep(sleep_ms / 1000.0)
    return sleep_ms


@task(name="bench_crash_once")
def _crash_once(marker: str) -> str:
    """首次尝试直接杀掉 worker 进程，第二次尝试成功。"""
    path = Path(marker)
    if not path.exists():
        path.write_text("crashed", encoding="utf-8")
        os._exit(17)
    return "ok"


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(round((pct / 100.0) * (len(ordered) - 1)), len(ordered) - 1)
    return ordered[index]


@dataclass
class ThroughputResult:
    tasks: int
    workers: int
    task_ms: int
    submit_seconds: float
    total_seconds: float
    throughput_per_sec: float
    latency_p50_ms: float | None
    latency_p95_ms: float | None
    errors: int


@dataclass
class TimeoutCase:
    configured_timeout_ms: int
    task_sleep_ms: int
    observed_seconds: float
    error: str | None


@dataclass
class CrashCase:
    observed_seconds: float
    result: str | None
    error: str | None


def _run_throughput(tasks: int, workers: int, task_ms: int) -> ThroughputResult:
    data_dir = tempfile.mkdtemp(prefix="actant-bench-task-")
    rt = Runtime(
        name="bench-task",
        data_dir=data_dir,
        max_concurrent_tasks=workers,
    )
    latencies: list[float] = []
    errors = 0
    rt.start()
    try:
        with use_runtime(rt):
            started = time.monotonic()
            handles = [_work.submit(index, task_ms) for index in range(tasks)]
            submitted = time.monotonic()
            for handle in handles:
                try:
                    handle.result()
                except BaseException:
                    errors += 1
                latencies.append((time.monotonic() - started) * 1000.0)
            finished = time.monotonic()
    finally:
        rt.stop()

    total = finished - started
    return ThroughputResult(
        tasks=tasks,
        workers=workers,
        task_ms=task_ms,
        submit_seconds=round(submitted - started, 3),
        total_seconds=round(total, 3),
        throughput_per_sec=round(tasks / total, 2) if total > 0 else 0.0,
        latency_p50_ms=round(statistics.median(latencies), 1) if latencies else None,
        latency_p95_ms=round(_percentile(latencies, 95) or 0.0, 1) if latencies else None,
        errors=errors,
    )


def _run_timeout_case(configured_timeout_ms: int = 100, task_sleep_ms: int = 1000) -> TimeoutCase:
    data_dir = tempfile.mkdtemp(prefix="actant-bench-timeout-")
    rt = Runtime(
        name="bench-timeout",
        data_dir=data_dir,
        max_concurrent_tasks=1,
        default_task_timeout_ms=configured_timeout_ms,
    )
    error: str | None = None
    rt.start()
    started = time.monotonic()
    try:
        with use_runtime(rt):
            handle = _slow.submit(task_sleep_ms)
            try:
                handle.result()
            except BaseException as exc:  # 超时是预期结果，记录类型即可
                error = type(exc).__name__
    finally:
        rt.stop()
    return TimeoutCase(
        configured_timeout_ms=configured_timeout_ms,
        task_sleep_ms=task_sleep_ms,
        observed_seconds=round(time.monotonic() - started, 3),
        error=error,
    )


def _run_crash_case() -> CrashCase:
    data_dir = tempfile.mkdtemp(prefix="actant-bench-crash-")
    marker = str(Path(data_dir) / "crash-once.marker")
    rt = Runtime(name="bench-crash", data_dir=data_dir, max_concurrent_tasks=1)
    result: str | None = None
    error: str | None = None
    rt.start()
    started = time.monotonic()
    try:
        with use_runtime(rt):
            handle = _crash_once.submit(marker)
            try:
                result = handle.result()
            except BaseException as exc:  # 崩溃恢复失败时记录异常类型
                error = type(exc).__name__
    finally:
        rt.stop()
    return CrashCase(
        observed_seconds=round(time.monotonic() - started, 3),
        result=result,
        error=error,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", type=int, default=200)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--task-ms", type=int, default=5)
    parser.add_argument("--timeout-case", action="store_true")
    parser.add_argument("--crash-case", action="store_true")
    args = parser.parse_args()

    if args.tasks < 1:
        parser.error("--tasks 必须大于 0")
    if args.workers < 1:
        parser.error("--workers 必须大于 0")

    payload: dict[str, object] = {
        "throughput": asdict(_run_throughput(args.tasks, args.workers, args.task_ms)),
    }
    if args.timeout_case:
        payload["timeout_case"] = asdict(_run_timeout_case())
    if args.crash_case:
        payload["crash_case"] = asdict(_run_crash_case())
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
