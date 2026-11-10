"""L2 服务层：跨进程提交、定义寻址与协议传输。

服务层只把现有 Runtime 原语映射为稳定的进程边界，不新增执行语义。当前包含：

- :mod:`actant.service.definitions`：``DefinitionRef`` 与定义校验；
- :mod:`actant.service.protocol`：HTTP/JSON + SSE 协议 v1。

pull worker 与读模型尚未实现；它们依赖的租约语义已在核心侧就绪。
"""

from __future__ import annotations

from actant.service.definitions import (
    DefinitionError,
    DefinitionHashMismatchError,
    DefinitionNotFoundError,
    DefinitionRef,
    DefinitionResolver,
    DefinitionVersionMismatchError,
    definition_hash,
)
from actant.service.protocol import PROTOCOL_VERSION, ServiceClient, ServiceServer

__all__ = [
    "PROTOCOL_VERSION",
    "DefinitionError",
    "DefinitionHashMismatchError",
    "DefinitionNotFoundError",
    "DefinitionRef",
    "DefinitionResolver",
    "DefinitionVersionMismatchError",
    "ServiceClient",
    "ServiceServer",
    "definition_hash",
]
