"""PyO3 直接提交路径的 payload 签名集成测试。

回归：`submit_task` / `submit_tasks_batch` 曾把未签名 payload 交给 dispatcher，
在 `require_payload_signing=True` 时被以“unsigned payload rejected”拒绝，
而 `@flow` 路径（orchestrator 派发时签名）一直正常。
"""

from __future__ import annotations

import uuid

from actant import Runtime, flow
from actant.actant import _ActantConfig, _NetworkConfig
from actant.tasks import task


def _config() -> _ActantConfig:
    return _ActantConfig(
        payload_signing_key="signing-test-key",
        network=_NetworkConfig(preset="none"),
        max_concurrent_tasks=1,
        num_worker_processes=1,
        require_payload_signing=True,
    )


class TestPayloadSigning:
    def test_direct_task_submit_is_signed(self) -> None:
        rt = Runtime(config=_config())
        rt.start()
        try:

            @task
            def add(x: int, y: int) -> int:
                return x + y

            assert add.submit(20, 22).result(timeout=30) == 42
        finally:
            rt.stop()

    def test_flow_root_task_is_signed(self) -> None:
        rt = Runtime(config=_config())
        rt.start()
        try:

            @task
            def add(x: int, y: int) -> int:
                return x + y

            @flow
            def pipeline() -> int:
                return add.submit(1, 2).result()

            assert pipeline() == 3
        finally:
            rt.stop()

    def test_batch_submit_is_signed(self) -> None:
        rt = Runtime(config=_config())
        rt.start()
        try:

            @task
            def double(x: int) -> int:
                return x * 2

            handles = double.submit_batch([0, 1, 2], batch_id=f"batch-{uuid.uuid4().hex}")
            assert [handle.result(timeout=30) for handle in handles] == [0, 2, 4]
        finally:
            rt.stop()
