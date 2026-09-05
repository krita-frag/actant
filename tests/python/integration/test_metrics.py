"""Python 侧 metrics 接口暴露测试。

验证：
- ``Runtime.metrics_text()`` 返回 Prometheus exposition format 文本。
- ``actant.metrics_text()`` 顶层便捷函数。

HTTP 托管（``/metrics`` 端点）已从核心层外置：不提供自动化测试，
托管样例见 ``examples/metrics_server.py`` 与 ``actant worker --metrics-port``。
"""

from __future__ import annotations

import pytest

import actant


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
