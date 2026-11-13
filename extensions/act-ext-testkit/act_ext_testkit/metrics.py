"""性能观测：只采样、不设阈值。

分布式测试里的性能断言极易抖动。本模块提供**观察型**采样器：调用方决定
记录哪些延迟、在报告里看 p50/p95/p99 与吞吐，阈值由调用方在明确硬件/负载
边界时自行断言。

RSS 采样默认覆盖本进程；可通过 :meth:`PerformanceSampler.watch_process`
登记 worker/节点 PID，逐进程采样（依赖 POSIX ``ps``）。RSS 采样仅在 POSIX
可用；Windows 上自动跳过，延迟/吞吐统计不受影响。
"""

from __future__ import annotations

try:
    import resource
except ImportError:  # Windows 没有 resource 模块
    resource = None  # type: ignore[assignment]

import shutil
import statistics
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any


class PerformanceError(RuntimeError):
    """采样器生命周期错误。"""


class PerformanceSampler:
    """任务延迟/吞吐/RSS 采样器。

    用法::

        sampler = PerformanceSampler(interval_s=0.1).start()
        with sampler.measure():
            handle.result(timeout=30)
        summary = sampler.stop().summary()
    """

    def __init__(self, *, interval_s: float = 0.1) -> None:
        if interval_s <= 0:
            raise ValueError("interval_s must be positive")
        self.interval_s = interval_s
        self._latencies_ms: list[float] = []
        self._completed = 0
        self._rss_samples: list[int] = []
        self._process_samples: dict[str, list[int]] = {}
        self._processes: dict[int, str] = {}
        self._started_at: float | None = None
        self._stopped_at: float | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._metrics_text = ""

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    def start(self) -> PerformanceSampler:
        if self._thread is not None:
            raise PerformanceError("sampler already started")
        self._started_at = time.monotonic()
        self._stop.clear()
        self._thread = threading.Thread(target=self._sample_loop, name="testkit-metrics", daemon=True)
        self._thread.start()
        return self

    def _sample_loop(self) -> None:
        while not self._stop.wait(self.interval_s):
            self._record_rss()

    def _record_rss(self) -> None:
        if resource is None:
            return
        usage = resource.getrusage(resource.RUSAGE_SELF)
        rss = int(usage.ru_maxrss)
        # macOS 的 ru_maxrss 单位是字节，Linux 是 KB。
        if sys.platform != "darwin":
            rss *= 1024
        with self._lock:
            self._rss_samples.append(rss)
            processes = dict(self._processes)
        for pid, label in processes.items():
            value = _process_rss_bytes(pid)
            if value is not None:
                with self._lock:
                    self._process_samples.setdefault(label, []).append(value)

    def watch_process(self, pid: int, *, label: str | None = None) -> None:
        """登记一个 worker/节点 PID 进行独立 RSS 采样。"""
        if pid <= 0:
            raise ValueError("pid must be positive")
        if shutil.which("ps") is None:
            raise PerformanceError("per-process sampling requires the 'ps' binary")
        with self._lock:
            self._processes[pid] = label or f"pid:{pid}"

    def stop(self, *, metrics_text: str | None = None) -> PerformanceSampler:
        if self._thread is None or self._stopped_at is not None:
            return self
        self._stop.set()
        self._thread.join(timeout=5.0)
        self._record_rss()
        self._stopped_at = time.monotonic()
        if metrics_text is not None:
            self._metrics_text = metrics_text
        return self

    @property
    def running(self) -> bool:
        return self._thread is not None and self._stopped_at is None

    # ------------------------------------------------------------------
    # 记录
    # ------------------------------------------------------------------

    def record_latency_ms(self, latency_ms: float) -> None:
        if latency_ms < 0:
            raise ValueError("latency must be non-negative")
        with self._lock:
            self._latencies_ms.append(float(latency_ms))

    def record_completed(self, count: int = 1) -> None:
        if count < 0:
            raise ValueError("count must be non-negative")
        with self._lock:
            self._completed += count

    @contextmanager
    def measure(self) -> Iterator[None]:
        """记录一段代码耗时并计一次完成。"""
        started = time.perf_counter()
        try:
            yield
        finally:
            elapsed_ms = (time.perf_counter() - started) * 1000
            self.record_latency_ms(elapsed_ms)
            self.record_completed()

    # ------------------------------------------------------------------
    # 统计
    # ------------------------------------------------------------------

    @property
    def duration_s(self) -> float:
        if self._started_at is None:
            return 0.0
        end = self._stopped_at if self._stopped_at is not None else time.monotonic()
        return max(0.0, end - self._started_at)

    def percentiles(self) -> dict[str, float]:
        """返回 p50/p90/p95/p99（无样本时全为 0）。"""
        with self._lock:
            values = sorted(self._latencies_ms)
        if not values:
            return {"p50": 0.0, "p90": 0.0, "p95": 0.0, "p99": 0.0}

        def pick(q: float) -> float:
            index = min(len(values) - 1, max(0, round(q * (len(values) - 1))))
            return round(values[index], 3)

        return {"p50": pick(0.50), "p90": pick(0.90), "p95": pick(0.95), "p99": pick(0.99)}

    def throughput(self) -> float:
        duration = self.duration_s
        with self._lock:
            completed = self._completed
        return round(completed / duration, 3) if duration > 0 else 0.0

    def peak_rss_bytes(self) -> int:
        with self._lock:
            return max(self._rss_samples) if self._rss_samples else 0

    def summary(self) -> dict[str, Any]:
        with self._lock:
            latencies = list(self._latencies_ms)
            completed = self._completed
            rss_samples = list(self._rss_samples)
            process_samples = {
                label: list(values) for label, values in self._process_samples.items()
            }
        return {
            "duration_s": round(self.duration_s, 3),
            "completed": completed,
            "throughput_per_s": self.throughput(),
            "latency_ms": {
                "count": len(latencies),
                "mean": round(statistics.fmean(latencies), 3) if latencies else 0.0,
                **self.percentiles(),
                "max": round(max(latencies), 3) if latencies else 0.0,
            },
            "rss_bytes": {
                "samples": len(rss_samples),
                "peak": max(rss_samples) if rss_samples else 0,
            },
            "processes": {
                label: {
                    "samples": len(values),
                    "peak": max(values) if values else 0,
                }
                for label, values in sorted(process_samples.items())
            },
            "metrics_text_chars": len(self._metrics_text),
        }


def _process_rss_bytes(pid: int) -> int | None:
    """读取单个进程 RSS（字节）；进程退出/权限不足时返回 ``None``。"""
    try:
        result = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=2.0,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    text = result.stdout.strip()
    if result.returncode != 0 or not text:
        return None
    try:
        return int(text.split()[0]) * 1024
    except (ValueError, IndexError):
        return None
