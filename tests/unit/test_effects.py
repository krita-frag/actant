"""`actant._effects` 纯单元测试。

覆盖不依赖 Rust 运行时的路径：
- `impossible()` 总是抛 RuntimeError。
- `effect()` 的 kind 校验（未知 kind 抛 ValueError）。
- `ask`/`perform`/`emit`/`effect` 在无活跃 Runtime 时抛 RuntimeError
  （`_resolve_meta` 的 `get_current_runtime() is None` 分支）。
"""

from __future__ import annotations

import pytest

from actant._effects import ask, effect, emit, impossible, perform


def test_impossible_raises_runtime_error() -> None:
    with pytest.raises(RuntimeError, match="impossible: nope"):
        impossible("nope")


def test_impossible_default_detail() -> None:
    with pytest.raises(RuntimeError, match="unreachable"):
        impossible()


def test_impossible_is_no_return() -> None:
    """impossible 标记为 NoReturn，调用方不应继续执行。"""
    # NoReturn 在运行时不暴露为类型对象，断言函数可调用且抛异常即可
    with pytest.raises(RuntimeError):
        impossible("stop")


def test_effect_unknown_kind_raises_value_error() -> None:
    """effect 对未知 kind 抛 ValueError，在访问 Runtime 之前。"""
    with pytest.raises(ValueError, match="kind must be 'ask'/'perform'/'emit'"):
        effect("Whatever", "bogus", object())


def test_ask_without_runtime_raises_runtime_error() -> None:
    with pytest.raises(RuntimeError, match="no active Runtime"):
        ask("Routing", {"x": 1})


def test_perform_without_runtime_raises_runtime_error() -> None:
    with pytest.raises(RuntimeError, match="no active Runtime"):
        perform("Execute", {"x": 1})


def test_emit_without_runtime_raises_runtime_error() -> None:
    with pytest.raises(RuntimeError, match="no active Runtime"):
        emit("TaskLifecycle", {"x": 1})


def test_effect_ask_without_runtime_raises_runtime_error() -> None:
    """effect('ask', ...) 会转调 ask，无 Runtime 时同样抛 RuntimeError。"""
    with pytest.raises(RuntimeError, match="no active Runtime"):
        effect("Routing", "ask", {"x": 1})


def test_effect_perform_without_runtime_raises_runtime_error() -> None:
    with pytest.raises(RuntimeError, match="no active Runtime"):
        effect("Execute", "perform", {"x": 1})


def test_effect_emit_without_runtime_raises_runtime_error() -> None:
    with pytest.raises(RuntimeError, match="no active Runtime"):
        effect("TaskLifecycle", "emit", {"x": 1})


def test_resolve_meta_without_runtime_includes_capability_name() -> None:
    """错误消息应包含被请求的 capability 名称，便于定位。"""
    with pytest.raises(RuntimeError) as ei:
        ask("MyCap", None)
    assert "MyCap" in str(ei.value)
