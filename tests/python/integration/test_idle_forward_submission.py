"""空闲窗口后的跨节点 forward 提交回归（F-B5-01，P1）。

缺陷：failover 心跳只在「路由字段变化」时失效路由候选快照，纯保活心跳不
刷新 `last_heartbeat_ms`——集群空闲超过 failure_timeout 后，快照时间戳冻结，
所有活 peer 被 `is_eligible` 的 TTL 判死，forward 提交只能 defer 到饥饿失败。

修复：每条心跳都使候选快照失效。判别力：修复前本用例红——空闲窗口后的
提交在 starvation_timeout 处以 ``no node satisfies required labels`` 失败
（failover.rs 的保活 bump 被还原成条件 bump 即可复现）。
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from actant import Runtime, use_runtime
from actant.actant import _ActantConfig, _FailoverConfig, _NetworkConfig
from actant.tasks import task
from tests.python._helpers import connect_peers, wait_until


def _config(*, max_concurrent: int, labels: dict[str, str] | None) -> _ActantConfig:
    # 心跳/失联超时收窄：1.2s 的空闲窗口必然跨越 TTL（500ms）数倍，
    # 放大缺陷窗口；starvation 给足预算，修复后任务必然在窗口内路由成功。
    return _ActantConfig(
        payload_signing_key="",
        network=_NetworkConfig(preset="none"),
        failover=_FailoverConfig(heartbeat_interval_ms=200, failure_timeout_ms=500),
        max_concurrent_tasks=max_concurrent,
        starvation_timeout_ms=8000,
        node_labels=labels,
        num_worker_processes=1,
    )


@task(tags=["engine=worker"])
def _echo(x: int) -> int:
    return x


@pytest.mark.regression
def test_forward_submission_survives_idle_window(tmp_path: Path) -> None:
    submit = Runtime(
        name="idle-submit",
        data_dir=str(tmp_path / "submit"),
        config=_config(max_concurrent=0, labels={"role": "submit"}),
    )
    worker = Runtime(
        name="idle-worker",
        data_dir=str(tmp_path / "worker"),
        config=_config(max_concurrent=2, labels={"engine": "worker"}),
    )
    submit.start()
    worker.start()
    try:
        assert connect_peers(submit, worker), "peers must discover each other"
        # 提交节点必须看到带 engine=worker 标签的 worker（初始路由基线）。
        # 标签经心跳传播（间隔 200ms），iroh 层发现后需再等一轮心跳，轮询等待。
        assert wait_until(
            lambda: any(
                (p.get("labels") or {}).get("engine") == "worker"
                for p in submit.peers()
            ),
            timeout_s=5.0,
        ), "worker labels must be visible before the idle window"

        # 空闲窗口：> TTL(500ms) 数倍，期间无任何容量/成员变更（缺陷路径）。
        time.sleep(1.2)

        with use_runtime(submit):
            assert _echo.submit(7).result(timeout=30) == 7
            assert _echo.submit(8).result(timeout=30) == 8
    finally:
        submit.stop()
        worker.stop()
