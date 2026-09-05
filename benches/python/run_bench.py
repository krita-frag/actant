"""Python 基准测试独立运行入口。

不依赖 pytest-benchmark，使用 ``timeit.repeat`` 进行多次采样，
输出 Markdown 表格 + 可选 JSON 报告，便于 CI 对比与外部可视化。

用法::

    .venv/bin/python benches/python/run_bench.py
    .venv/bin/python benches/python/run_bench.py --quick
    .venv/bin/python benches/python/run_bench.py --json /tmp/bench.json
    .venv/bin/python benches/python/run_bench.py --filter gather

设计原则：
- 每个 benchmark 函数返回 ``(name, callable, samples_config)``
- 主循环按 ``number`` 次调用为单位计时，避免单次太快分辨率不足
- 输出：每个 benchmark 给出 median/mean/min/stdev/op_time
"""

from __future__ import annotations

import argparse
import gc
import json
import logging
import os
import statistics
import sys
import timeit
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any

# 在 import actant 之前设置，避免 iroh DNS 阻塞
os.environ.setdefault("ACTANT_DISCOVERY", "none")
# 压制 Rust tracing，清除基准测量中的 span/log 记录开销。热路径每任务 span
# （py.submit_task / task executing / dispatching）均为 debug 级，默认 info
# 过滤器本就不会记录；此处再压到 error，连 info 级生命周期日志也不落盘，
# 保证 timeit 采样的纯执行时延。
os.environ.setdefault("RUST_LOG", "error")

# 让 benches/ 目录可被 import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import actant

# 压制 actant 日志输出，仅保留 bench 自己的进度信息，避免干扰测量与输出整洁。
logging.getLogger("actant").setLevel(logging.ERROR)
logging.getLogger("actant.task").setLevel(logging.ERROR)
logging.getLogger("actant.flow").setLevel(logging.ERROR)
logging.getLogger("actant.runtime").setLevel(logging.ERROR)


@dataclass
class BenchResult:
    """单个 benchmark 结果。"""

    name: str
    group: str
    number: int  # 每采样内调用 fn 的次数（timeit `number`）
    repeat: int  # 采样次数
    samples_sec: list[float] = field(default_factory=list)  # 每次采样的耗时（秒）
    ops_per_call: int = 1  # 每次 fn 调用内部执行的操作数（单任务内核=1）
    median_sec: float = 0.0
    mean_sec: float = 0.0
    min_sec: float = 0.0
    stdev_sec: float = 0.0
    median_call_ms: float = 0.0  # 单次 fn 调用（非每操作）的 median 毫秒
    op_time_us: float = 0.0  # 每 op 的 median 微秒数（= sample/(number*ops_per_call)）

    def compute(self) -> None:
        if not self.samples_sec:
            return
        self.median_sec = statistics.median(self.samples_sec)
        self.mean_sec = statistics.fmean(self.samples_sec)
        self.min_sec = min(self.samples_sec)
        self.stdev_sec = statistics.stdev(self.samples_sec) if len(self.samples_sec) > 1 else 0.0
        calls = self.number * self.ops_per_call
        self.median_call_ms = self.median_sec / self.number * 1000.0
        self.op_time_us = self.median_sec / calls * 1_000_000


# 每次 fn 调用内部执行的 op 数（把 sample 耗时折算为每 op 时延）。
# 仅批量内核 >1：gather / concurrency / events / flow 每次 fn 提交多个任务。
OPS_PER_CALL: dict[str, int] = {
    "gather/gather_10": 10,
    "gather/gather_100": 100,
    "gather/serial_result_100": 100,
    "concurrency/concurrent_1000": 1000,
    "concurrency/silent_1000": 1000,
    "events/normal_100": 100,
    "events/silent_100": 100,
    "flow/imperative_10": 10,
    "flow/imperative_50": 50,
}


def _make_tasks() -> dict[str, Callable[..., Any]]:
    """创建基准测试用的 task 对象。"""

    @actant.task
    def noop() -> int:
        return 42

    @actant.task(silent=True)
    def silent_noop() -> int:
        return 42

    @actant.task
    def add(a: int, b: int) -> int:
        return a + b

    @actant.task
    def echo(data: bytes) -> bytes:
        return data

    @actant.task
    def make_payload(size: int) -> bytes:
        return b"x" * size

    return {
        "noop": noop,
        "silent_noop": silent_noop,
        "add": add,
        "echo": echo,
        "make_payload": make_payload,
    }


def _make_flows() -> dict[str, Callable[..., Any]]:
    """创建 flow 对象（命令式）。"""
    import time as _time

    @actant.task
    def _compute(x: int) -> int:
        _time.sleep(0.001)  # 1ms 模拟 IO
        return x * 2

    @actant.flow
    def fanout_imperative(n: int) -> list[int]:
        handles = [_compute.submit(i) for i in range(n)]
        return actant.gather(*handles, timeout=10)

    return {
        "fanout_imperative": fanout_imperative,
    }


# ---------------------------------------------------------------------------
# Benchmark 定义：每个函数返回 (name, group, callable, number, repeat)
# ---------------------------------------------------------------------------


def _bench_definitions(quick: bool = False) -> list[tuple[str, str, Callable, int, int]]:
    """返回所有 benchmark 定义列表。

    每个 entry: (name, group, fn, number, repeat)
        name:   基准名称
        group:  所属分组（用于聚合报告）
        fn:     被测函数（执行一次单位工作）
        number: timeit 单次采样内 fn 重复次数
        repeat: 采样次数（取 median）
    """
    tasks = _make_tasks()
    flows = _make_flows()

    # 默认采样配置
    # 注意：actant submit 同步阻塞 ~7-12ms/op，单次采样 number=50 即 ~500ms
    if quick:
        nr_normal = (20, 3)  # (number, repeat)
        nr_slow = (5, 3)
        nr_fast = (500, 3)
        nr_big_batch = (1, 3)  # 1000 任务并发场景
    else:
        nr_normal = (100, 5)
        nr_slow = (10, 5)
        nr_fast = (2000, 7)
        nr_big_batch = (3, 3)

    defs: list[tuple[str, str, Callable, int, int]] = []

    # === task_dispatch ===
    noop = tasks["noop"]
    add = tasks["add"]
    silent = tasks["silent_noop"]

    def _noop_dispatch():
        noop.submit().result(timeout=5)

    defs.append(("task_dispatch/noop", "task_dispatch", _noop_dispatch, *nr_normal))

    def _silent_dispatch():
        silent.submit().result(timeout=5)

    defs.append(("task_dispatch/silent", "task_dispatch", _silent_dispatch, *nr_normal))

    def _add_dispatch():
        add.submit(3, 4).result(timeout=5)

    defs.append(("task_dispatch/add", "task_dispatch", _add_dispatch, *nr_normal))

    def _direct_call():
        noop()

    defs.append(("task_dispatch/direct_call", "task_dispatch", _direct_call, *nr_fast))

    # === gather ===
    def _gather_10():
        handles = [noop.submit() for _ in range(10)]
        actant.gather(*handles, timeout=10)

    defs.append(("gather/gather_10", "gather", _gather_10, *nr_normal))

    def _gather_100():
        handles = [noop.submit() for _ in range(100)]
        actant.gather(*handles, timeout=30)

    defs.append(("gather/gather_100", "gather", _gather_100, *nr_slow))

    def _serial_100():
        handles = [noop.submit() for _ in range(100)]
        for h in handles:
            h.result(timeout=5)

    defs.append(("gather/serial_result_100", "gather", _serial_100, *nr_slow))

    # === concurrency ===
    def _concurrent_1000():
        handles = [noop.submit() for _ in range(1000)]
        actant.gather(*handles, timeout=60)

    defs.append(("concurrency/concurrent_1000", "concurrency", _concurrent_1000, *nr_big_batch))

    def _concurrent_silent_1000():
        handles = [silent.submit() for _ in range(1000)]
        actant.gather(*handles, timeout=60)

    defs.append(("concurrency/silent_1000", "concurrency", _concurrent_silent_1000, *nr_big_batch))

    # === events ===
    def _normal_events_100():
        for _ in range(100):
            noop.submit().result(timeout=5)

    defs.append(("events/normal_100", "events", _normal_events_100, *nr_slow))

    def _silent_events_100():
        for _ in range(100):
            silent.submit().result(timeout=5)

    defs.append(("events/silent_100", "events", _silent_events_100, *nr_slow))

    # === payload ===
    echo = tasks["echo"]
    make_payload = tasks["make_payload"]

    for size in [0, 1024, 10 * 1024, 100 * 1024]:
        data = b"x" * size

        # 闭包按值捕获 size 与 data
        def _make_echo_input(d=data, s=size):
            echo.submit(d).result(timeout=10)

        defs.append((f"payload/input_{size}_bytes", "payload", _make_echo_input, *nr_slow))

    for size in [0, 1024, 10 * 1024, 100 * 1024]:

        def _make_output(s=size):
            make_payload.submit(s).result(timeout=10)

        defs.append((f"payload/output_{size}_bytes", "payload", _make_output, *nr_slow))

    # === flow ===
    fanout_imperative = flows["fanout_imperative"]

    def _flow_imperative_10():
        fanout_imperative(10)

    defs.append(("flow/imperative_10", "flow", _flow_imperative_10, *nr_slow))

    def _flow_imperative_50():
        fanout_imperative(50)

    defs.append(("flow/imperative_50", "flow", _flow_imperative_50, *nr_slow))

    return defs


# ---------------------------------------------------------------------------
# 主运行循环
# ---------------------------------------------------------------------------


def run_benchmarks(
    *,
    quick: bool = False,
    name_filter: str | None = None,
    json_path: str | None = None,
) -> list[BenchResult]:
    """运行所有 benchmark，返回结果列表。"""
    results: list[BenchResult] = []

    print("=== Actant Python Benchmark ===", flush=True)
    print(f"actant version: {actant.__version__}", flush=True)
    print(f"python: {sys.version.split()[0]}", flush=True)
    print(f"mode: {'quick' if quick else 'full'}", flush=True)
    print(f"filter: {name_filter or '(none)'}", flush=True)
    print(flush=True)

    with actant.Runtime.with_defaults():
        # 触发一次预热：让 iroh endpoint / tokio runtime 完成初始化
        _warmup_in_runtime()

        defs = _bench_definitions(quick=quick)
        if name_filter:
            name_filter_lower = name_filter.lower()
            defs = [d for d in defs if name_filter_lower in d[0].lower()]

        print(
            f"{'name':<40}  {'call_ms':>10}  {'per_op_us':>12}  "
            f"{'min_call_ms':>12}  {'stdev_call_ms':>12}  {'steps':>10}  {'op/call':>7}",
            flush=True,
        )
        print("-" * 108, flush=True)

        for name, group, fn, number, repeat in defs:
            # 关闭 GC 防止意外触发影响测量
            gc_was_enabled = gc.isenabled()
            gc.disable()
            try:
                samples = timeit.repeat(fn, number=number, repeat=repeat)
            except Exception as exc:
                print(f"  {name:<40}  FAILED: {exc}", flush=True)
                if gc_was_enabled:
                    gc.enable()
                continue
            finally:
                if gc_was_enabled:
                    gc.enable()

            result = BenchResult(
                name=name,
                group=group,
                number=number,
                repeat=repeat,
                samples_sec=list(samples),
                ops_per_call=OPS_PER_CALL.get(name, 1),
            )
            result.compute()
            results.append(result)

            # call_ms = 单次 fn 调用 median 毫秒；per_op_us = 每 op median 微秒，
            # op/call 列标识该内核每次 fn 内部执行的任务数（批量内核 >1）。
            print(
                f"  {name:<40}  {result.median_call_ms:>9.3f}  "
                f"{result.op_time_us:>9.3f}  "
                f"{result.min_sec / result.number * 1000:>9.3f}  "
                f"{result.stdev_sec / result.number * 1000:>9.3f}  "
                f"{number}*{repeat:>2}  {result.ops_per_call:>6}",
                flush=True,
            )

        print("-" * 100, flush=True)
        print(f"Total: {len(results)} benchmarks", flush=True)

    if json_path:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump([asdict(r) for r in results], f, indent=2)
        print(f"\nJSON report written to: {json_path}", flush=True)

    return results


def _warmup_in_runtime() -> None:
    """在 Runtime 内预热：触发 Rust 端代码路径加载与 JIT 优化。"""

    @actant.task
    def _warmup() -> int:
        return 0

    h = _warmup.submit()
    h.result(timeout=5)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Actant Python benchmark runner",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Quick mode: fewer samples (CI smoke test)",
    )
    parser.add_argument(
        "--filter",
        type=str,
        default=None,
        help="Only run benchmarks whose name contains this string (case-insensitive)",
    )
    parser.add_argument(
        "--json",
        type=str,
        default=None,
        help="Write JSON report to the given path",
    )
    args = parser.parse_args()

    results = run_benchmarks(
        quick=args.quick,
        name_filter=args.filter,
        json_path=args.json,
    )

    # 退出码：所有 benchmark 失败则非零
    return 0 if results else 1


if __name__ == "__main__":
    sys.exit(main())
