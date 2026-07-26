"""P2-3: Runtime.test() 内存模式测试。

验证：
- ``Runtime.test()`` 创建的 Runtime 无 P2P 网络开销。
- 临时 data_dir 自动创建与清理。
- 任务提交与执行正常工作。
- 较短的 drain 超时加速 teardown。
- 与 ``with_defaults`` 行为兼容（默认 handler 已注册）。
"""

from __future__ import annotations

import os
import tempfile
import time

import pytest

import actant
from actant import Runtime


@pytest.mark.integration
class TestRuntimeTestMode:
    """``Runtime.test()`` 内存模式。"""

    def test_test_mode_creates_runtime(self) -> None:
        rt = Runtime.test()
        try:
            assert rt is not None
            assert not rt._started
        finally:
            rt.stop()

    def test_test_mode_default_name(self) -> None:
        rt = Runtime.test()
        try:
            assert "test-" in rt._name
            assert str(os.getpid()) in rt._name
        finally:
            rt.stop()

    def test_test_mode_custom_name(self) -> None:
        rt = Runtime.test(name="my-test-rt")
        try:
            assert rt._name == "my-test-rt"
        finally:
            rt.stop()

    def test_test_mode_uses_temp_data_dir(self) -> None:
        rt = Runtime.test()
        try:
            assert rt._data_dir is not None
            assert rt._data_dir.startswith(tempfile.gettempdir())
            assert rt._owns_data_dir is True
        finally:
            rt.stop()

    def test_test_mode_cleans_up_temp_dir_on_stop(self) -> None:
        rt = Runtime.test()
        data_dir = rt._data_dir
        rt.stop()
        # stop() 后临时目录应被清理
        assert not os.path.exists(data_dir)

    def test_test_mode_short_drain_timeout(self) -> None:
        rt = Runtime.test()
        try:
            # 默认 drain_timeout_secs=5（而非 30）
            config = rt._config
            assert config is not None
            assert config.drain_timeout_secs == 5
        finally:
            rt.stop()

    def test_test_mode_custom_drain_timeout(self) -> None:
        rt = Runtime.test(drain_timeout_secs=10)
        try:
            config = rt._config
            assert config is not None
            assert config.drain_timeout_secs == 10
        finally:
            rt.stop()

    def test_test_mode_disables_p2p(self) -> None:
        rt = Runtime.test()
        try:
            config = rt._config
            assert config is not None
            # 临时 data_dir 自动触发 preset="none"
            assert config.network.preset == "none"
        finally:
            rt.stop()

    def test_test_mode_no_payload_signing(self) -> None:
        rt = Runtime.test()
        try:
            config = rt._config
            assert config is not None
            assert config.payload_signing_key == ""
            assert config.require_payload_signing is False
        finally:
            rt.stop()

    def test_test_mode_starts_and_executes_tasks(self) -> None:
        @actant.task
        def add(a: int, b: int) -> int:
            return a + b

        with Runtime.test():
            h = add.submit(3, 4)
            result = h.result(timeout=10)
            assert result == 7

    def test_test_mode_registers_default_handlers(self) -> None:
        with Runtime.test() as rt:
            # 默认 handler 已注册（Routing、Scheduling 等）
            assert "Routing" in rt._layers
            assert "Scheduling" in rt._layers
            assert len(rt._layers["Routing"]) >= 1

    def test_test_mode_context_manager(self) -> None:
        with Runtime.test() as rt:
            assert rt._started
        assert not rt._started

    def test_test_mode_fast_start(self) -> None:
        """test 模式启动应比 with_defaults 更快（无网络初始化）。"""
        start = time.monotonic()
        with Runtime.test():
            pass
        elapsed = time.monotonic() - start
        # 无 P2P 发现，启动应 < 3s
        assert elapsed < 3.0, f"test mode start took {elapsed:.2f}s"

    def test_test_mode_supports_flow(self) -> None:
        @actant.task
        def double(x: int) -> int:
            return x * 2

        @actant.flow
        def pipeline(x: int) -> int:
            a = double.submit(x)
            b = double.submit(a)
            return b.result()

        with Runtime.test():
            result = pipeline(5)
            assert result == 20

    def test_test_mode_gather(self) -> None:
        @actant.task
        def square(x: int) -> int:
            return x * x

        with Runtime.test():
            handles = [square.submit(i) for i in range(5)]
            results = actant.gather(*handles, timeout=10)
            assert results == [0, 1, 4, 9, 16]
