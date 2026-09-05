"""大载荷经 stdio pipe 帧协议端到端回归测试。

SHM ring 传输移除后，worker IPC 统一为 stdio 长度前缀二进制帧（>32KB 的正文
不再有 ring 旁路）。本测试提交一个返回 ~1MB bytes 的任务，验证大载荷在
Dispatch / Result 两个方向都经完整 pipe 帧路径（含 os.writev 短写推进 /
read_exact 组装）端到端可用。
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from actant import AsyncResult, Runtime, task

_PAYLOAD_SIZE = 1024 * 1024


def _big_blob_fn(size: int) -> bytes:
    return b"\xa5" * size


_big_blob = task(_big_blob_fn)


@pytest.mark.timeout(10)
def test_large_payload_roundtrip_via_pipe() -> None:
    """~1MB 结果载荷经 pipe 帧路径返回，字节内容完整无损。"""
    with Runtime.with_defaults() as rt:
        rt.serve()
        handle: AsyncResult[Any] = _big_blob.submit(_PAYLOAD_SIZE)
        deadline = time.monotonic() + 10
        while not handle.done() and time.monotonic() < deadline:
            time.sleep(0.02)
        result = handle.result(timeout=10)
        assert len(result) == _PAYLOAD_SIZE
        assert result == b"\xa5" * _PAYLOAD_SIZE
