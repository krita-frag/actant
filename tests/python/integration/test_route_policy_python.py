"""Python 路由策略（callable）集成测试。

驱动方式：全部用例只经 ``Runtime(route_policy=...)`` 公共入口驱动，不直接构造
Rust 侧的 ``PyRoutePolicy``。断言依据是用户 callable 的真实调用（计数/入参）
与任务最终结果，避免「单元测试全绿但生产路径未接线」。

覆盖：
- callable 被真实调用，且入参形状与文档一致；
- 返回 ``None`` 时弃权，交回内置策略；
- 回调抛异常 / 返回非法值时降级，任务仍能完成；
- 非字符串且不可调用的参数在启动时被拒绝；
- 字符串策略（``label_affinity``）行为不回归。
"""

from __future__ import annotations

from typing import Any

import pytest

from actant import Runtime
from actant.actant import _ActantConfig, _NetworkConfig
from actant.tasks import task


def _config() -> _ActantConfig:
    return _ActantConfig(
        payload_signing_key="",
        network=_NetworkConfig(preset="none"),
        max_concurrent_tasks=2,
        num_worker_processes=2,
        starvation_timeout_ms=500,
        role="both",
    )


class TestPythonRoutePolicy:
    def test_callable_is_invoked_and_task_completes(self) -> None:
        calls: list[tuple[dict[str, Any], list[dict[str, Any]]]] = []

        def policy(ctx: dict[str, Any], candidates: list[dict[str, Any]]) -> str:
            calls.append((ctx, candidates))
            return "local"

        rt = Runtime(config=_config(), route_policy=policy)
        rt.start()
        try:

            @task
            def add_one(x: int) -> int:
                return x + 1

            assert add_one.submit(41).result(timeout=30) == 42
        finally:
            rt.stop()

        assert calls, "python route_policy was never invoked by the routing path"
        ctx, candidates = calls[0]
        assert ctx["local_role"] == "both"
        assert ctx["local_draining"] is False
        assert isinstance(ctx["local_available_slots"], int)
        assert isinstance(ctx["heartbeat_ttl_ms"], int)
        assert isinstance(ctx["local_labels"], dict)
        assert isinstance(ctx["required_tags"], list)
        assert isinstance(ctx["local_available_quotas"], dict)
        assert isinstance(candidates, list)

    def test_none_defers_to_builtin_policy(self) -> None:
        seen: list[int] = []

        def policy(ctx: dict[str, Any], candidates: list[dict[str, Any]]) -> None:
            seen.append(1)
            return None

        rt = Runtime(config=_config(), route_policy=policy)
        rt.start()
        try:

            @task
            def identity(x: int) -> int:
                return x

            assert identity.submit(7).result(timeout=30) == 7
        finally:
            rt.stop()

        assert seen, "python route_policy was never invoked"

    def test_raised_exception_falls_back(self) -> None:
        def policy(ctx: dict[str, Any], candidates: list[dict[str, Any]]) -> str:
            raise RuntimeError("policy boom")

        rt = Runtime(config=_config(), route_policy=policy)
        rt.start()
        try:

            @task
            def identity(x: int) -> int:
                return x

            assert identity.submit(11).result(timeout=30) == 11
        finally:
            rt.stop()

    def test_invalid_return_type_falls_back(self) -> None:
        def policy(ctx: dict[str, Any], candidates: list[dict[str, Any]]) -> Any:
            return 12345

        rt = Runtime(config=_config(), route_policy=policy)
        rt.start()
        try:

            @task
            def identity(x: int) -> int:
                return x

            assert identity.submit(13).result(timeout=30) == 13
        finally:
            rt.stop()

    def test_non_callable_non_string_is_rejected(self) -> None:
        rt = Runtime(config=_config(), route_policy=42)
        with pytest.raises(TypeError, match="route_policy must be"):
            rt.start()

    def test_named_policy_still_works(self) -> None:
        rt = Runtime(config=_config(), route_policy="label_affinity")
        rt.start()
        try:

            @task
            def add(a: int, b: int) -> int:
                return a + b

            assert add.submit(2, 3).result(timeout=30) == 5
        finally:
            rt.stop()
