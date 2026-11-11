"""L2 服务层：跨进程提交、定义寻址与协议传输。

服务层把现有 Runtime 原语映射为稳定的进程边界，不新增执行语义。它是**可选的
传输扩展**，不属于 ``actant`` 核心包：核心包只提供引擎与进程内 SDK，HTTP/JSON
入口由本扩展按需安装。包含：

- :mod:`act_ext_service.definitions`：``DefinitionRef`` 与定义校验；
- :mod:`act_ext_service.protocol`：HTTP/JSON + SSE 协议 v1。

pull worker 与读模型尚未实现；它们依赖的租约语义已在核心侧就绪。

.. danger::
   协议 v1 **没有鉴权**，且会 ``cloudpickle.loads`` 客户端提交的载荷——能连上端口
   的人即可执行代码。只允许监听 ``127.0.0.1``，或置于已完成身份认证与网络隔离的
   可信反向代理之后；不要暴露到公网或未隔离的容器网络。
"""

from __future__ import annotations

from act_ext_service.definitions import (
    DefinitionError,
    DefinitionHashMismatchError,
    DefinitionNotFoundError,
    DefinitionRef,
    DefinitionResolver,
    DefinitionVersionMismatchError,
    definition_hash,
)
from act_ext_service.protocol import PROTOCOL_VERSION, ServiceClient, ServiceServer

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
