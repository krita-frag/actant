"""Store capability 覆盖的生产路径契约。

只经 ``Runtime`` 公共入口驱动（``@task`` 提交 / ``result()`` / 终态记录写入），
不直接调用 ``_dispatch_perform`` 或 ``_store_*``。断言依据是 Python 侧终态
记录 writer 的写入确实经过 ``_layers`` 的 handler，而不是「单元测试全绿但
生产路径未接线」。

同时作为 ``rt.layer("Store").chain(...)`` 这一公开扩展面的验收：Python
handler 优先于 Rust ``StoreHandler``，且只影响 Python 侧发起的 Store 调用。
Rust 内部（Worker 执行 / Orchestrator 状态持久化）直接读写 LMDB，不经过
这条 handler 链。
"""

from __future__ import annotations

import time
from typing import Any

from actant import Runtime
from actant.actant import _ActantConfig, _NetworkConfig
from actant.capabilities import StoreReq
from actant.tasks import task


def _config() -> _ActantConfig:
    return _ActantConfig(
        payload_signing_key="",
        network=_NetworkConfig(preset="none"),
        max_concurrent_tasks=2,
        num_worker_processes=1,
        role="both",
    )


class _RecordingStore:
    """记录请求并透传到 Rust 默认 handler，避免替换后破坏运行时持久化。"""

    def __init__(self, runtime: Runtime) -> None:
        self._runtime = runtime
        self.requests: list[StoreReq] = []

    def __call__(self, req: StoreReq) -> Any:
        self.requests.append(req)
        return self._runtime._rust_runtime.perform("Store", req)


def test_engine_persistence_goes_through_python_store_handler() -> None:
    rt = Runtime(config=_config())
    recorder = _RecordingStore(rt)
    # start() 前注册：Rust-backed capability 在 start 后追加会告警。
    rt.layer("Store").chain(recorder)
    rt.start()
    try:

        @task
        def add_one(x: int) -> int:
            return x + 1

        handle = add_one.submit(41)
        assert handle.result(timeout=30) == 42

        # 终态记录由后台 writer 批量刷出，等待其经过 Store handler 链。
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline and not recorder.requests:
            time.sleep(0.01)
        assert recorder.requests, "引擎持久化未经过 Python Store handler 链"
        assert any(
            req.op in ("put", "put_batch") for req in recorder.requests
        ), f"未见引擎侧持久化写入: {[req.op for req in recorder.requests]}"
    finally:
        rt.stop()
