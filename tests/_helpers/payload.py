"""payload 构造辅助函数。"""

from __future__ import annotations

import struct
from typing import TYPE_CHECKING

from actant._serialization import TAG_UPSTREAM_PREFIX

if TYPE_CHECKING:
    from collections.abc import Sequence


def pack_upstream_prefix(
    upstream_results: Sequence[bytes],
    inner_payload: bytes,
) -> bytes:
    """构造 TAG_UPSTREAM_PREFIX 包装的 payload。

    ``upstream_results`` 应为已序列化的字节列表；``inner_payload`` 为
    任务默认 payload（如 pack_positional/pack_single 的输出）。
    """
    if not upstream_results:
        return inner_payload

    buf = bytearray()
    buf.append(TAG_UPSTREAM_PREFIX)
    buf += struct.pack("<I", len(upstream_results))
    for data in upstream_results:
        buf += struct.pack("<I", len(data))
        buf += data
    buf += inner_payload
    return bytes(buf)
