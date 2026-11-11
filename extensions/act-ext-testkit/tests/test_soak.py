"""soak 冒烟：真实进程节点 + 连续负载 + 周期故障。"""

from __future__ import annotations

import json
import os

import pytest
from act_ext_testkit.soak import SoakConfig, run_soak

pytestmark = pytest.mark.skipif(os.name == "nt", reason="soak 仅支持 POSIX")


@pytest.mark.timeout(180)
def test_soak_smoke(tmp_path) -> None:
    report_path = tmp_path / "soak.json"
    result = run_soak(
        SoakConfig(
            namespace="soak-smoke",
            workers=2,
            duration_s=8.0,
            warmup_s=1.0,
            concurrency=2,
            rate_limit_per_s=10.0,
            task_sleep_ms=20,
            task_timeout_s=30.0,
            fault_interval_s=2.0,
            fault_hold_s=0.5,
            max_failure_rate=0.05,
            seed=5,
            report_path=report_path,
        )
    )

    assert result.submitted >= 10
    assert result.completed >= 10
    assert result.lost == 0
    assert result.fault_count >= 1
    assert result.ok, result.to_dict()

    data = json.loads(report_path.read_text())
    assert data["manifest"]["soak"]["submitted"] == result.submitted
    assert data["manifest"]["soak"]["ok"] is True
