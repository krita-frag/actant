"""分布式命名配额（信号量）集成测试。

覆盖：
- 未声明配额 → Defer → starvation 超时转 Failed（错误信息含 quota）；
- 容量 1 时两个任务不会并发执行（进程级时间区间不重叠）。
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from actant import Runtime, flow
from actant.actant import _ActantConfig, _NetworkConfig
from actant.tasks import task


def _config(*, quotas: dict[str, int], starvation_ms: int = 50) -> _ActantConfig:
    return _ActantConfig(
        payload_signing_key="",
        network=_NetworkConfig(preset="none"),
        max_concurrent_tasks=2,
        num_worker_processes=2,
        starvation_timeout_ms=starvation_ms,
        node_quotas=quotas,
    )


class TestQuota:
    def test_unknown_quota_fails_after_starvation(self) -> None:
        rt = Runtime(config=_config(quotas={}, starvation_ms=50))
        rt.start()
        try:

            @task(quota="gpu")
            def compute(x: int) -> int:
                return x

            handle = compute.submit(1)
            with pytest.raises(Exception, match="quota"):
                handle.result(timeout=30)
        finally:
            rt.stop()

    def test_flow_task_quota_is_propagated(self) -> None:
        """flow 节点必须携带任务配额诉求：未声明配额时应 starvation 失败。

        回归：`_flow_submit` 曾在构造 DagNode 时丢弃 `quota`，导致 flow 内
        带配额的任务绕过准入直接执行。
        """
        rt = Runtime(config=_config(quotas={}, starvation_ms=50))
        rt.start()
        try:

            @task(quota="gpu")
            def compute(x: int) -> int:
                return x

            @flow
            def pipeline() -> int:
                return compute.submit(1).result()

            with pytest.raises(Exception, match="quota"):
                pipeline()
        finally:
            rt.stop()

    def test_flow_task_tags_are_propagated(self) -> None:
        """flow 节点必须携带标签诉求：无匹配节点时应 starvation 失败。"""
        rt = Runtime(config=_config(quotas={}, starvation_ms=50))
        rt.start()
        try:

            @task(tags=["gpu-required"])
            def placed(x: int) -> int:
                return x

            @flow
            def pipeline() -> int:
                return placed.submit(1).result()

            with pytest.raises(Exception, match="required labels"):
                pipeline()
        finally:
            rt.stop()

    def test_zero_starvation_timeout_means_unbounded_wait(self) -> None:
        """``starvation_timeout_ms=0`` 表示不设上限，任务不会立即失败。"""
        rt = Runtime(config=_config(quotas={}, starvation_ms=0))
        rt.start()
        try:

            @task(tags=["never-matches"])
            def blocked(x: int) -> int:
                return x

            handle = blocked.submit(1)
            time.sleep(0.3)
            assert not handle.done()
        finally:
            rt.stop()

    def test_capacity_one_serializes_tasks(self, tmp_path: Path) -> None:
        path = tmp_path / "intervals.log"
        rt = Runtime(config=_config(quotas={"gpu": 1}, starvation_ms=30_000))
        rt.start()
        try:

            @task(quota="gpu")
            def timed(tag: str) -> None:
                with path.open("a", encoding="utf-8") as fh:
                    fh.write(f"{tag} start {time.monotonic()}\n")
                    fh.flush()
                    time.sleep(0.2)
                    fh.write(f"{tag} end {time.monotonic()}\n")
                    fh.flush()

            a = timed.submit("a")
            b = timed.submit("b")
            a.result(timeout=30)
            b.result(timeout=30)

            events: list[tuple[str, str, float]] = []
            for line in path.read_text(encoding="utf-8").splitlines():
                tag, kind, ts = line.split()
                events.append((tag, kind, float(ts)))
            starts = [ts for _, kind, ts in events if kind == "start"]
            ends = [ts for _, kind, ts in events if kind == "end"]
            assert len(starts) == 2 and len(ends) == 2

            # 扫描线：任意时刻并发执行的任务数不得超过配额容量 1。
            active = 0
            max_active = 0
            for _, kind, _ in sorted(events, key=lambda e: e[2]):
                active += 1 if kind == "start" else -1
                max_active = max(max_active, active)
            assert max_active == 1, f"quota=1 violated: {sorted(events)}"
        finally:
            rt.stop()
