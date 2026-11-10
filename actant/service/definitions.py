"""流程定义的跨进程寻址与校验。

提交方只需持有 ``DefinitionRef = name + version + hash``，无需在本进程导入
flow 代码；宿主进程用 :class:`DefinitionResolver` 把引用解析回
:class:`~actant._internal.flow.FlowDefinition`，并在版本或内容不一致时返回
稳定错误码（``kind``），由服务协议原样透传给客户端。

指纹口径：函数字节码 + 嵌套代码 + 常量 + 装饰时声明的执行参数。``co_filename``
与 ``co_firstlineno`` 在计算前归一化，故同一份代码部署在不同路径/不同行偏移
下指纹一致；被 flow 体引用的**模块级全局实现**不在指纹内——那是 Python 导入
图的事实，不是定义本身的一部分。
"""

from __future__ import annotations

import hashlib
import marshal
import types
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from actant._internal.flow import FLOW_REGISTRY, FlowDefinition, FlowRegistry
from actant.exceptions import ActantError

__all__ = [
    "DefinitionError",
    "DefinitionHashMismatchError",
    "DefinitionNotFoundError",
    "DefinitionRef",
    "DefinitionResolver",
    "DefinitionVersionMismatchError",
    "definition_hash",
]


class DefinitionError(ActantError):
    """定义解析失败。

    ``kind`` 是跨进程稳定的错误码，服务协议以它为客户端分支依据；不要依赖
    异常消息文本。
    """

    kind = "definition"

    def __init__(self, message: str) -> None:
        super().__init__(message, kind=type(self).kind)


class DefinitionNotFoundError(DefinitionError):
    """当前进程未登记该 flow 定义。"""

    kind = "definition_not_found"


class DefinitionVersionMismatchError(DefinitionError):
    """提交方声明的版本与宿主登记定义的版本不一致。"""

    kind = "definition_version_mismatch"


class DefinitionHashMismatchError(DefinitionError):
    """版本一致但函数内容指纹不一致（定义漂移）。"""

    kind = "definition_hash_mismatch"


@dataclass(frozen=True)
class DefinitionRef:
    """流程定义的跨进程引用。

    Attributes:
        name: ``@flow(name=...)`` 登记的名字。
        version: 提交方认知的定义版本（非负整数）。
        hash: :func:`definition_hash` 的结果，用于检测同版本下的代码漂移。
    """

    name: str
    version: int
    hash: str

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("DefinitionRef.name must be a non-empty string")
        if not isinstance(self.version, int) or isinstance(self.version, bool):
            raise ValueError("DefinitionRef.version must be an integer")
        if self.version < 0:
            raise ValueError("DefinitionRef.version must be >= 0")
        if not isinstance(self.hash, str) or not self.hash:
            raise ValueError("DefinitionRef.hash must be a non-empty string")

    @classmethod
    def from_definition(cls, definition: FlowDefinition) -> DefinitionRef:
        """用进程内登记的定义构造引用。"""
        return cls(
            name=definition.name,
            version=definition.version,
            hash=definition_hash(definition),
        )

    def to_dict(self) -> dict[str, object]:
        """转为协议可传输的 JSON 对象。"""
        return {"name": self.name, "version": self.version, "hash": self.hash}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DefinitionRef:
        """从协议对象构造引用；字段缺失或类型错误抛 ``ValueError``。"""
        try:
            name = data["name"]
            version = data["version"]
            digest = data["hash"]
        except KeyError as exc:
            raise ValueError(f"definition reference missed field {exc.args[0]!r}") from exc
        return cls(name=name, version=version, hash=digest)


def definition_hash(definition: FlowDefinition) -> str:
    """计算定义内容指纹（SHA-256 十六进制）。

    同一份代码在不同部署路径、不同行偏移下指纹一致；修改函数体、常量、
    嵌套函数或装饰参数都会改变指纹。Python 主/次版本升级可能改变字节码，
    因此跨版本升级应同步提升 ``version``。
    """
    digest = hashlib.sha256()
    parts = (
        definition.name,
        str(definition.version),
        str(definition.timeout_ms),
        definition.failure_strategy or "",
        "async" if definition.is_async else "sync",
    )
    for part in parts:
        digest.update(part.encode("utf-8"))
        digest.update(b"\x00")
    digest.update(marshal.dumps(_normalized_code(definition.func.__code__)))
    return digest.hexdigest()


def _normalized_code(code: types.CodeType) -> types.CodeType:
    """抹去部署位置差异，只保留与行为相关的代码结构。"""
    consts = tuple(
        _normalized_code(const) if isinstance(const, types.CodeType) else const
        for const in code.co_consts
    )
    return code.replace(co_filename="", co_firstlineno=1, co_consts=consts)


class DefinitionResolver:
    """把 :class:`DefinitionRef` 解析为宿主进程内的定义。"""

    def __init__(self, registry: FlowRegistry | None = None) -> None:
        self._registry = FLOW_REGISTRY if registry is None else registry

    def reference(self, name: str) -> DefinitionRef:
        """返回宿主侧对某 flow 的当前引用。"""
        definition = self._registry.get(name)
        if definition is None:
            raise DefinitionNotFoundError(
                f"flow definition {name!r} is not registered in this process; "
                f"known flows: {self._registry.names()}"
            )
        return DefinitionRef.from_definition(definition)

    def resolve(self, ref: DefinitionRef) -> FlowDefinition:
        """校验版本与内容指纹，返回宿主登记的定义。

        Raises:
            DefinitionNotFoundError: 宿主未登记该名字。
            DefinitionVersionMismatchError: 版本不一致。
            DefinitionHashMismatchError: 版本一致但内容指纹不一致。
        """
        definition = self._registry.get(ref.name)
        if definition is None:
            raise DefinitionNotFoundError(
                f"flow definition {ref.name!r} is not registered in this process; "
                f"known flows: {self._registry.names()}"
            )
        if definition.version != ref.version:
            raise DefinitionVersionMismatchError(
                f"flow definition {ref.name!r} version mismatch: client declared "
                f"{ref.version}, host has {definition.version}"
            )
        actual = definition_hash(definition)
        if actual != ref.hash:
            raise DefinitionHashMismatchError(
                f"flow definition {ref.name!r} content hash mismatch at version "
                f"{ref.version}: client declared {ref.hash}, host has {actual}"
            )
        return definition
