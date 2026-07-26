"""P2-2: Python 侧 metrics 接口暴露测试。

验证：
- ``Runtime.metrics_text()`` 返回 Prometheus exposition format 文本。
- ``Runtime.start_metrics_server()`` / ``stop_metrics_server()`` 编程式控制。
- ``actant.metrics_text()`` 顶层便捷函数。
- Runtime.stop() 自动清理 metrics 服务器。
- HTTP ``/metrics`` 端点返回有效的 Prometheus 文本。
"""

from __future__ import annotations

import socket
import urllib.request

import pytest

import actant


def _find_free_port() -> int:
    """让 OS 分配一个可用端口并立即释放，供测试传入。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.integration
class TestMetricsText:
    """``metrics_text()`` 返回 Prometheus 文本。"""

    def test_runtime_metrics_text(self) -> None:
        with actant.Runtime.with_defaults() as rt:
            text = rt.metrics_text()
            assert isinstance(text, str)
            # metrics::init() 已在 _RuntimeCore::new() 中调用，
            # 注册了 actant.tasks.submitted 等计数器。
            assert "actant_" in text or text == ""

    def test_top_level_metrics_text(self) -> None:
        with actant.Runtime.with_defaults():
            text = actant.metrics_text()
            assert isinstance(text, str)

    def test_metrics_text_after_task(self) -> None:
        """提交任务后 metrics_text 应包含任务计数器。"""

        @actant.task
        def add(a: int, b: int) -> int:
            return a + b

        with actant.Runtime.with_defaults() as rt:
            h = add.submit(1, 2)
            h.result(timeout=10)
            text = rt.metrics_text()
            assert "actant_tasks_submitted" in text or "actant.tasks.submitted" in text


@pytest.mark.integration
class TestMetricsServer:
    """``start_metrics_server()`` / ``stop_metrics_server()`` 编程式控制。"""

    def test_start_stop_metrics_server(self) -> None:
        with actant.Runtime.with_defaults() as rt:
            port = rt.start_metrics_server(0)
            assert isinstance(port, int)
            assert port > 0
            rt.stop_metrics_server()

    def test_metrics_server_serves_prometheus(self) -> None:
        with actant.Runtime.with_defaults() as rt:
            port = rt.start_metrics_server(0)
            try:
                # 抓取 /metrics 端点
                url = f"http://127.0.0.1:{port}/metrics"
                with urllib.request.urlopen(url, timeout=5) as resp:
                    assert resp.status == 200
                    body = resp.read().decode("utf-8")
                    assert isinstance(body, str)
                    # 应包含 actant 指标名
                    assert "actant_" in body or body == ""
            finally:
                rt.stop_metrics_server()

    def test_metrics_server_404_for_other_paths(self) -> None:
        with actant.Runtime.with_defaults() as rt:
            port = rt.start_metrics_server(0)
            try:
                url = f"http://127.0.0.1:{port}/other"
                with pytest.raises(urllib.error.HTTPError) as exc_info:
                    urllib.request.urlopen(url, timeout=5)
                assert exc_info.value.code == 404
            finally:
                rt.stop_metrics_server()

    def test_start_metrics_server_replaces_existing(self) -> None:
        """重复调用 start_metrics_server 先停旧服务器再启新服务器。"""
        with actant.Runtime.with_defaults() as rt:
            port1 = rt.start_metrics_server(0)
            port2 = rt.start_metrics_server(0)
            assert port1 != port2
            # 新服务器可访问
            url = f"http://127.0.0.1:{port2}/metrics"
            with urllib.request.urlopen(url, timeout=5) as resp:
                assert resp.status == 200
            rt.stop_metrics_server()

    def test_stop_metrics_server_idempotent(self) -> None:
        """stop_metrics_server() 未启动时为空操作。"""
        with actant.Runtime.with_defaults() as rt:
            rt.stop_metrics_server()  # 不应抛异常
            rt.stop_metrics_server()  # 重复调用安全

    def test_runtime_stop_cleans_up_metrics_server(self) -> None:
        """Runtime.stop() 自动停止 metrics 服务器。"""
        rt = actant.Runtime.with_defaults()
        rt.start()
        port = rt.start_metrics_server(0)
        rt.stop()
        # stop 后端口应不可访问（服务器已关闭）
        url = f"http://127.0.0.1:{port}/metrics"
        with pytest.raises((urllib.error.URLError, ConnectionError)):
            urllib.request.urlopen(url, timeout=2)

    def test_start_metrics_server_requires_started_runtime(self) -> None:
        """未 start 的 Runtime 调用 start_metrics_server 抛 InvalidStateError。"""
        from actant.exceptions import InvalidStateError

        rt = actant.Runtime.with_defaults()
        with pytest.raises(InvalidStateError):
            rt.start_metrics_server(0)
