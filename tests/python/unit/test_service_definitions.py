"""DefinitionRef 与定义解析契约。"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from actant._internal.flow import FlowDefinition, FlowRegistry
from actant.service import (
    DefinitionHashMismatchError,
    DefinitionNotFoundError,
    DefinitionRef,
    DefinitionResolver,
    DefinitionVersionMismatchError,
    definition_hash,
)


def _sample(value: int) -> int:
    return value + 1


def _other(value: int) -> int:
    return value + 2


def _definition(
    *,
    name: str = "svc-sample",
    func: Callable[[int], int] = _sample,
    version: int = 3,
    timeout_ms: int = 1_000,
    failure_strategy: str | None = "continue",
) -> FlowDefinition:
    return FlowDefinition(
        name=name,
        func=func,
        version=version,
        timeout_ms=timeout_ms,
        failure_strategy=failure_strategy,
    )


def _compile_function(source: str, filename: str) -> Callable[[int], int]:
    namespace: dict[str, Any] = {}
    exec(compile(source, filename, "exec"), namespace)
    return namespace["f"]


def test_hash_is_stable_and_sensitive_to_code() -> None:
    assert definition_hash(_definition()) == definition_hash(_definition())
    assert definition_hash(_definition()) != definition_hash(_definition(func=_other))


def test_hash_ignores_deployment_path_and_line_offset() -> None:
    source = "def f(value):\n    return value + 1\n"
    first = _compile_function(source, "/deploy/a/flows.py")
    shifted = _compile_function("\n\n\n" + source, "/deploy/b/flows.py")
    assert definition_hash(_definition(func=first)) == definition_hash(
        _definition(func=shifted)
    )


def test_ref_roundtrip_and_validation() -> None:
    definition = _definition()
    ref = DefinitionRef.from_definition(definition)
    assert ref == DefinitionRef(ref.name, ref.version, ref.hash)
    assert DefinitionRef.from_dict(ref.to_dict()) == ref

    with pytest.raises(ValueError, match="name"):
        DefinitionRef("", 1, "abc")
    with pytest.raises(ValueError, match="version"):
        DefinitionRef("svc-sample", -1, "abc")
    with pytest.raises(ValueError, match="hash"):
        DefinitionRef("svc-sample", 1, "")
    with pytest.raises(ValueError, match="missed field"):
        DefinitionRef.from_dict({"name": "svc-sample", "version": 1})


def test_resolver_accepts_matching_reference() -> None:
    registry = FlowRegistry()
    definition = _definition()
    registry.register(definition)
    resolver = DefinitionResolver(registry)

    ref = resolver.reference("svc-sample")
    assert ref == DefinitionRef.from_definition(definition)
    assert resolver.resolve(ref) is definition


def test_resolver_reports_stable_error_kinds() -> None:
    registry = FlowRegistry()
    definition = _definition()
    registry.register(definition)
    resolver = DefinitionResolver(registry)
    ref = resolver.reference("svc-sample")

    with pytest.raises(DefinitionNotFoundError) as missing:
        resolver.resolve(DefinitionRef("svc-missing", 1, "0" * 64))
    assert missing.value.kind == "definition_not_found"

    with pytest.raises(DefinitionVersionMismatchError) as version:
        resolver.resolve(DefinitionRef(ref.name, ref.version + 1, ref.hash))
    assert version.value.kind == "definition_version_mismatch"

    with pytest.raises(DefinitionHashMismatchError) as digest:
        resolver.resolve(DefinitionRef(ref.name, ref.version, "0" * 64))
    assert digest.value.kind == "definition_hash_mismatch"
