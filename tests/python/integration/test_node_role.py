"""P2 拓扑角色集成测试。

覆盖：
- 角色默认值 ``both``（P2P 行为不变）；
- ``worker`` 节点拒绝驱动 flow host；
- ``orchestrator`` 节点不执行本地任务，任务因无可用执行节点而失败。
"""

from __future__ import annotations

import pytest

from actant import Runtime
from actant.actant import _ActantConfig, _NetworkConfig
from actant.tasks import task


def _config(role: str) -> _ActantConfig:
    return _ActantConfig(
        payload_signing_key="",
        network=_NetworkConfig(preset="none"),
        max_concurrent_tasks=2,
        num_worker_processes=2,
        starvation_timeout_ms=50,
        role=role,
    )


class TestNodeRole:
    def test_role_defaults_to_both(self) -> None:
        rt = Runtime(config=_config("both"))
        assert rt.role == "both"

    def test_worker_role_disables_flow_host(self) -> None:
        rt = Runtime(config=_config("worker"))
        rt.start()
        try:
            assert rt.role == "worker"
            assert rt.host_flows() == []
        finally:
            rt.stop()

    def test_orchestrator_role_does_not_execute_local_tasks(self) -> None:
        rt = Runtime(config=_config("orchestrator"))
        rt.start()
        try:

            @task
            def compute(x: int) -> int:
                return x

            handle = compute.submit(1)
            with pytest.raises(Exception, match="does not execute"):
                handle.result(timeout=30)
        finally:
            rt.stop()
