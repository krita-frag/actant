"""Flow 并发基线：量化当前 driven / blocking 模型的并发上限。

两种基线：

- ``threads``：当前用户常用的“每 flow 一线程”写法，测线程数、RSS、唤醒延迟。
- ``detached``：``start_flow`` + 单 ``host_flows`` 驱动，测串行 host loop 能同时
  park 多少个 flow。当前预期为 1，这正是 WP1 Flow Driver 要解决的问题。

用法::

    uv run --extra dev python benches/python/flow_concurrency.py --mode threads --flows 200
    uv run --extra dev python benches/python/flow_concurrency.py --mode detached --flows 200
    uv run --extra dev python benches/python/flow_concurrency.py --mode detached --flows 20 --replay

``10,000`` 个线程通常会在 macOS/Linux 上触发线程创建上限；这正是基线的一部分，
脚本会报告已成功 park 的数量而不是直接崩溃。
"""

from __future__ import annotations

import argparse
import json
import platform
import resource
import statistics
import tempfile
import threading
import time
from dataclasses import asdict, dataclass

from actant import Runtime, flow, host_flows, task, use_runtime
from actant.workflow import current_workflow_id, wait_signal

_LOCK = threading.Lock()
_READY: list[str] = []
_DONE: list[tuple[str, float]] = []


@task(name="bench_noop")
def _noop(value: int) -> int:
    return value


@flow(name="bench-park")
def _park_flow(index: int) -> str:
    """完成一个节点后 park，模拟长流程在等待外部事件。"""
    _noop.submit(index).result()
    workflow_id = current_workflow_id()
    with _LOCK:
        _READY.append(workflow_id)
    payload = wait_signal("go", timeout_ms=60_000)
    with _LOCK:
        _DONE.append((workflow_id, time.monotonic()))
    return payload.decode() if payload else "timeout"


@flow(name="bench-park-detached")
def _park_detached(index: int) -> str:
    _noop.submit(index).result()
    workflow_id = current_workflow_id()
    with _LOCK:
        _READY.append(workflow_id)
    return wait_signal("go", timeout_ms=60_000).decode()


def _rss_mb() -> float:
    raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if platform.system() == "Darwin":
        return raw / (1024 * 1024)
    return raw / 1024


def _wait_for(predicate, *, timeout: float, what: str) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


@dataclass
class BenchResult:
    mode: str
    requested: int
    parked: int
    completed: int
    park_seconds: float
    park_throughput_per_sec: float
    threads: int
    rss_mb: float
    wake_p50_ms: float | None
    wake_p95_ms: float | None
    wake_max_ms: float | None
    replay_to_first_park_ms: float | None
    errors: int
    data_dir: str


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(round((pct / 100.0) * (len(ordered) - 1)), len(ordered) - 1)
    return ordered[index]


def _run_threads(flows: int, park_timeout: float, signal_timeout: float) -> BenchResult:
    _READY.clear()
    _DONE.clear()
    data_dir = tempfile.mkdtemp(prefix="actant-bench-threads-")
    rt = Runtime(
        name="bench-threads",
        data_dir=data_dir,
        max_concurrent_tasks=min(flows, 64),
    )
    rt.start()
    errors: list[BaseException] = []
    threads: list[threading.Thread] = []

    def _worker(index: int) -> None:
        try:
            with use_runtime(rt):
                _park_flow(index)
        except BaseException as exc:  # 强停/park 超时都属于基线观测
            errors.append(exc)

    started = time.monotonic()
    for index in range(flows):
        thread = threading.Thread(target=_worker, args=(index,), daemon=True)
        thread.start()
        threads.append(thread)
    _wait_for(lambda: len(_READY) >= flows, timeout=park_timeout, what="all parked")
    park_elapsed = time.monotonic() - started
    parked = len(_READY)
    thread_count = threading.active_count()
    rss_mb = _rss_mb()

    signal_times: dict[str, float] = {}
    for workflow_id in list(_READY):
        signal_times[workflow_id] = time.monotonic()
        rt.signal_wait_point(workflow_id, "go", b"go")
    _wait_for(lambda: len(_DONE) >= len(signal_times), timeout=signal_timeout, what="all woken")

    with _LOCK:
        done = list(_DONE)
    latencies = [
        (finished - signal_times[workflow_id]) * 1000.0
        for workflow_id, finished in done
        if workflow_id in signal_times
    ]
    for thread in threads:
        thread.join(timeout=5)
    rt.stop()

    return BenchResult(
        mode="threads",
        requested=flows,
        parked=parked,
        completed=len(done),
        park_seconds=round(park_elapsed, 3),
        park_throughput_per_sec=round(parked / park_elapsed, 2) if park_elapsed > 0 else 0.0,
        threads=thread_count,
        rss_mb=round(rss_mb, 1),
        wake_p50_ms=round(statistics.median(latencies), 1) if latencies else None,
        wake_p95_ms=round(_percentile(latencies, 95) or 0.0, 1) if latencies else None,
        wake_max_ms=round(max(latencies), 1) if latencies else None,
        replay_to_first_park_ms=None,
        errors=len(errors),
        data_dir=data_dir,
    )


def _run_detached(
    flows: int,
    park_window: float,
    replay: bool,
) -> BenchResult:
    _READY.clear()
    _DONE.clear()
    data_dir = tempfile.mkdtemp(prefix="actant-bench-detached-")
    rt = Runtime(name="bench-detached", data_dir=data_dir)
    rt.start()
    errors: list[BaseException] = []
    stopping = threading.Event()

    with use_runtime(rt):
        for index in range(flows):
            rt.start_flow("bench-park-detached", index)

    def _drive(runtime: Runtime) -> None:
        try:
            with use_runtime(runtime):
                host_flows(runtime=runtime, once=False)
        except BaseException as exc:  # 停止 host loop 属于预期
            if not stopping.is_set():
                errors.append(exc)

    driver = threading.Thread(target=_drive, args=(rt,), daemon=True)
    started = time.monotonic()
    driver.start()
    _wait_for(lambda: bool(_READY), timeout=park_window, what="first parked")
    park_elapsed = time.monotonic() - started
    parked = len(_READY)
    stopping.set()
    rt.stop()
    driver.join(timeout=10)

    replay_ms: float | None = None
    if replay:
        _READY.clear()
        rt2 = Runtime(name="bench-detached-replay", data_dir=data_dir)
        rt2.start()
        stopping.clear()
        driver2 = threading.Thread(target=_drive, args=(rt2,), daemon=True)
        replay_started = time.monotonic()
        driver2.start()
        _wait_for(lambda: bool(_READY), timeout=park_window, what="first re-parked")
        replay_ms = (time.monotonic() - replay_started) * 1000.0
        stopping.set()
        rt2.stop()
        driver2.join(timeout=10)

    return BenchResult(
        mode="detached",
        requested=flows,
        parked=parked,
        completed=0,
        park_seconds=round(park_elapsed, 3),
        park_throughput_per_sec=round(parked / park_elapsed, 4) if park_elapsed > 0 else 0.0,
        threads=threading.active_count(),
        rss_mb=round(_rss_mb(), 1),
        wake_p50_ms=None,
        wake_p95_ms=None,
        wake_max_ms=None,
        replay_to_first_park_ms=round(replay_ms, 1) if replay_ms is not None else None,
        errors=len(errors),
        data_dir=data_dir,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["threads", "detached"], default="threads")
    parser.add_argument("--flows", type=int, default=200)
    parser.add_argument("--park-timeout", type=float, default=120.0)
    parser.add_argument("--signal-timeout", type=float, default=120.0)
    parser.add_argument("--park-window", type=float, default=5.0)
    parser.add_argument("--replay", action="store_true", help="detached 模式下测重启后首次 park")
    args = parser.parse_args()

    if args.flows < 1:
        parser.error("--flows 必须大于 0")
    if args.mode == "threads" and args.flows > 2000:
        print(
            json.dumps(
                {"warning": "超过 2000 线程可能触及系统上限，结果仅为基线"},
                ensure_ascii=False,
            )
        )

    if args.mode == "threads":
        result = _run_threads(args.flows, args.park_timeout, args.signal_timeout)
    else:
        result = _run_detached(args.flows, args.park_window, args.replay)
    print(json.dumps(asdict(result), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
