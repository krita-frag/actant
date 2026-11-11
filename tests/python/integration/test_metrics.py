"""Python 侧 metrics 接口暴露测试。

验证：
- ``actant.metrics_text()`` 顶层便捷函数返回 Prometheus exposition format 文本。

HTTP 托管（``/metrics`` 端点）已从核心层外置：不提供自动化测试，
托管样例见 ``examples/metrics_server.py`` 与 ``actant worker --metrics-port``。
"""

from __future__ import annotations

import pytest

import actant
from actant import metrics_text


@pytest.mark.integration
class TestMetricsText:
    """``metrics_text()`` 返回 Prometheus 文本。"""

    def test_top_level_metrics_text(self) -> None:
        with actant.Runtime():
            text = metrics_text()
            assert isinstance(text, str)
            # metrics::init() 已在 _RuntimeCore::new() 中调用，
            # 注册了 actant.tasks.submitted 等计数器。
            assert "actant_" in text or text == ""

    def test_metrics_text_after_task(self) -> None:
        """提交任务后 metrics_text 应包含任务计数器。"""

        @actant.task
        def add(a: int, b: int) -> int:
            return a + b

        with actant.Runtime() as _rt:
            h = add.submit(1, 2)
            h.result(timeout=10)
            text = metrics_text()
            assert "actant_tasks_submitted" in text or "actant.tasks.submitted" in text

    def test_internal_store_dispatch_is_not_counted(self, monkeypatch) -> None:
        """框架内部 Store 读写不得计入业务 capability 计数。"""
        counted: list[tuple[str, str]] = []
        with actant.Runtime() as rt:
            monkeypatch.setattr(
                rt,
                "_record_effect",
                lambda name, effect: counted.append((name, effect)),
            )
            rt._store_put("metrics-probe", b"value")
            assert counted == []

    def test_capability_invocation_counter(self) -> None:
        """Python 分发入口按 capability / effect 记录调用次数。"""
        with actant.Runtime() as rt:
            rt.layer("Probe", "perform").chain(lambda request: request)

            assert actant.perform("Probe", 1) == 1

            text = metrics_text()
            assert "actant_capability_invocations_total" in text
            assert 'capability="Probe"' in text
            assert 'effect="perform"' in text
