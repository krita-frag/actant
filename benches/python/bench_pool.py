"""进程池专项基准：冷启动、池化收益、超时强杀回收延迟。

进程级隔离的 worker 进程池在 dispatcher 构造时**预拉起** N 个 worker
（见 `src/runtime/dispatcher.rs::ProcessTaskDispatcher::new`）。本基准量化
三项特性，供 3.3「进程池冷启动与池化收益、超时强杀回收延迟」数据入档：

- ``pool/start_ms_n`` — Runtime 启动耗时（含 N 个 worker 子进程 spawn）。
- ``pool/first_dispatch_ms_n`` — 全新 Runtime 上**首个**任务端到端延迟
  （冷启动惩罚：子进程解释器启动 + `_worker.py` 模块导入 + IPC 预热）。
- ``pool/steady_state_us`` — 热池稳态分发延迟（worker 复用，无进程创建）：
  ``first_dispatch`` 与 ``steady_state`` 的差值即一次性冷启动成本，
  差值示意"若不池化、每任务新建进程"的浪费。
- ``pool/timeout_reclaim`` — 硬超时恢复周期分项：``submit→error``（提交到
  ``TimeoutError`` 暴露，含强杀 + 替补拉起）、``error→next_done``（错误暴露到
  下一任务完成）、``reclaim_extra``（后者扣除稳态分发基线的纯回收开销，理想为 0）。

运行方式::

    .venv/bin/python benches/python/bench_pool.py
    .venv/bin/python benches/python/bench_pool.py --json /tmp/pool_bench.json
    .venv/bin/python benches/python/bench_pool.py --workers 1 4 8 --reclaim-runs 5

环境依赖：``maturin develop --release`` 先行编译 Rust 扩展。
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time

# 必须在 import actant 之前设置，避免 iroh DNS 阻塞 / tracing 日志污染测量。
os.environ.setdefault("ACTANT_DISCOVERY", "none")
os.environ.setdefault("RUST_LOG", "error")

import actant
from actant.actant import _ActantConfig, _NetworkConfig


def _make_runtime(num_workers: int) -> actant.Runtime:
    """按进程池大小构造 Runtime。

    Rust 层将 ``max_concurrent_tasks`` 映射为 ``num_worker_processes``
    （进程池容量），二者一致。``preset="none"`` 禁用 P2P 发现抖动。
    """
    cfg = _ActantConfig(
        payload_signing_key="",
        network=_NetworkConfig(preset="none"),
        max_concurrent_tasks=num_workers,
    )
    return actant.Runtime.with_defaults(config=cfg)


def _measure_cold_start(num_workers: int, repeats: int) -> dict[str, float]:
    """冷启动测量：Runtime 启动 + 首次任务端到端延迟。

    每次重复都新建一个含 ``num_workers`` 个 worker 的全新 Runtime，
    测量启动耗时与首次任务延迟，取 median。
    """
    @actant.task
    def noop() -> int:
        return 42

    start_sec: list[float] = []
    first_dispatch_sec: list[float] = []

    for _ in range(repeats):
        t0 = time.perf_counter()
        rt = _make_runtime(num_workers)
        rt.start()
        t1 = time.perf_counter()
        try:
            first_submit = time.perf_counter()
            noop.submit().result(timeout=10)
            first_dispatch_sec.append(time.perf_counter() - first_submit)
            start_sec.append(t1 - t0)
        finally:
            rt.stop()

    return {
        "workers": float(num_workers),
        "start_ms": statistics.median(start_sec) * 1000,
        "first_dispatch_ms": statistics.median(first_dispatch_sec) * 1000,
    }


def _measure_steady_state(
    rt: actant.Runtime, num_workers: int, number: int, repeats: int
) -> float:
    """热池稳态分发：复用已启动的 Runtime，测量 worker 池化复用后的分发延迟。

    返回单次 noop 分发的 median 毫秒数。
    """
    @actant.task
    def noop() -> int:
        return 42

    _ = num_workers  # 池大小由 Runtime 已决定，此处仅需复用

    samples_ms: list[float] = []
    for _ in range(repeats):
        # 每次采样连续提交 number 个任务，取平均，稳定到单个任务粒度
        t0 = time.perf_counter()
        for _ in range(number):
            noop.submit().result(timeout=10)
        samples_ms.append((time.perf_counter() - t0) / number * 1000)

    return statistics.median(samples_ms)


def _measure_timeout_reclaim(
    rt: actant.Runtime,
    *,
    timeout_ms: int,
    runs: int,
    steady_state_ms: float,
) -> dict[str, float]:
    """超时强杀回收：硬超时后系统恢复到可执行下一任务的分项测量。

    进程池容量为 1，唯一 worker 被阻塞任务占用；该任务硬超时后 Rust 在
    ``terminate_and_replace`` 中完成强杀 + 替补并归还槽位（此过程先于
    ``TimeoutError`` 暴露结束）。故分项测量整个恢复周期：
    - ``submit→error``：提交到超时错误暴露（含强杀与替补）
    - ``error→next_done``：错误暴露到下一任务完成
    - ``reclaim_extra``：扣除稳态分发基线后的纯回收开销（理想为 0）
    """
    def _blocker_impl() -> None:
        # 休眠远大于超时，保证被 Rust 侧硬超时强杀而非自行返回
        time.sleep(timeout_ms / 1000 * 10)
    # @task(timeout_ms=...) 经 `task` 的 `-> Any` 返回，无法保留函数类型；
    # 故先用 typed 实现函数再显式包装，避免 mypy untyped-decorator 告警。
    blocker = actant.task(timeout_ms=timeout_ms)(_blocker_impl)

    @actant.task
    def noop() -> int:
        return 42

    from actant.exceptions import ActantError

    # 每次重复记录一整个超时恢复周期（提交 → 超时暴露 → 下一任务完成）的分项。
    error_latency_ms: list[float] = []   # 提交到 TimeoutError 暴露（含强杀 + 替补）
    reclaim_done_ms: list[float] = []    # TimeoutError 暴露到下一任务完成
    reclaim_extra_ms: list[float] = []   # 上一项扣除稳态分发基线后的纯回收开销

    for _ in range(runs):
        t_submit = time.perf_counter()

        # 唯一 worker 被阻塞任务占用；task 级 timeout_ms 生效后被 Rust 强杀。
        try:
            blocker.submit().result(timeout=10)
        except ActantError as exc:
            # 期望 TimeoutError；若为业务失败则不应出现在本测量中
            if "timeout" not in str(exc).lower():
                raise
        t_error = time.perf_counter()

        # 槽位已在超时暴露前完成归还并替补（terminate_and_replace），
        # 立即提交 noop 验证系统已恢复可执行。
        noop.submit().result(timeout=10)
        t_done = time.perf_counter()

        error_latency_ms.append((t_error - t_submit) * 1000)
        reclaim_done_ms.append((t_done - t_error) * 1000)
        reclaim_extra_ms.append(
            max(0.0, (t_done - t_error) * 1000 - steady_state_ms)
        )

    return {
        "timeout_ms": float(timeout_ms),
        "runs": float(runs),
        "submit_to_error_ms": statistics.median(error_latency_ms),
        "error_to_done_ms": statistics.median(reclaim_done_ms),
        "reclaim_extra_ms": statistics.median(reclaim_extra_ms),
    }


def _main() -> int:
    parser = argparse.ArgumentParser(
        description="Actant worker 进程池专项基准",
    )
    parser.add_argument(
        "--workers", type=int, nargs="+", default=[1, 4, 8],
        help="冷启动测量的进程池大小（默认 1 4 8）",
    )
    parser.add_argument(
        "--cold-repeats", type=int, default=3,
        help="冷启动每次池大小的重复次数（取 median）",
    )
    parser.add_argument(
        "--steady-number", type=int, default=100,
        help="稳态测量每次采样提交的任务数",
    )
    parser.add_argument(
        "--steady-repeats", type=int, default=5,
        help="稳态测量的采样次数（取 median）",
    )
    parser.add_argument(
        "--timeout-ms", type=int, default=200,
        help="回收延迟测量的阻塞任务超时毫秒",
    )
    parser.add_argument(
        "--reclaim-runs", type=int, default=5,
        help="回收延迟测量重复次数（取 median）",
    )
    parser.add_argument(
        "--json", type=str, default=None,
        help="写入 JSON 报告的路径",
    )
    args = parser.parse_args()

    print("=== Actant Worker 进程池专项基准 ===", flush=True)
    print(f"actant version: {actant.__version__}", flush=True)
    print(f"python: {sys.version.split()[0]}", flush=True)

    cold_results: dict[str, dict[str, float]] = {}
    steady: dict[int, float] = {}

    # 1) 冷启动：每个池大小独立测量
    print("\n--- 进程池冷启动 ---", flush=True)
    for n in args.workers:
        res = _measure_cold_start(n, args.cold_repeats)
        cold_results[str(n)] = res
        print(
            f"  workers={res['workers']:>2}  start={res['start_ms']:>8.2f} ms  "
            f"first_dispatch={res['first_dispatch_ms']:>8.2f} ms",
            flush=True,
        )

    # 2) 稳态 + 3) 回收延迟：复用容量=1 的 Runtime 隔离测量
    print("\n--- 池化收益（容量=1 热池稳态分发） ---", flush=True)
    with _make_runtime(1) as rt:
        rt.start()
        steady_1 = _measure_steady_state(
            rt, 1, args.steady_number, args.steady_repeats
        )
        steady[1] = steady_1
        print(f"  steady_state={steady_1:>8.3f} ms/op", flush=True)

        print("\n--- 超时强杀回收延迟（容量=1） ---", flush=True)
        reclaim = _measure_timeout_reclaim(
            rt,
            timeout_ms=args.timeout_ms,
            runs=args.reclaim_runs,
            steady_state_ms=steady_1,
        )
        print(
            f"  per-task timeout={reclaim['timeout_ms']:.0f} ms  "
            f"submit→error={reclaim['submit_to_error_ms']:.1f} ms  "
            f"error→next_done={reclaim['error_to_done_ms']:.1f} ms  "
            f"reclaim_extra={reclaim['reclaim_extra_ms']:.1f} ms",
            flush=True,
        )

    report = {
        "version": actant.__version__,
        "python": sys.version.split()[0],
        "cold_start": cold_results,
        "steady_state_ms": steady,
        "timeout_reclaim": reclaim,
    }

    print("\n=== 汇总 ===", flush=True)
    for n, r in cold_results.items():
        # 冷启动惩罚 = 首次调度 - 稳态；示意不池化则每任务付出此成本
        penalty = r["first_dispatch_ms"] - steady.get(int(n), 0.0)
        print(
            f"  workers={n}: 冷启动惩罚 = first_dispatch({r['first_dispatch_ms']:.2f}) "
            f"- steady_state({steady.get(int(n), 0.0):.2f}) = {penalty:.2f} ms/次",
            flush=True,
        )

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(f"\nJSON report written to: {args.json}", flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(_main())
