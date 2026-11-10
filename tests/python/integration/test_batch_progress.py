"""batch/array job 逐项进度集成测试。

验证核心 `BatchTracker` 经 EventBus 终态事件计数：成功/失败分档、
`pending` 收敛到 0、`finished` 置位、重复/未知查询的边界行为。
"""

from __future__ import annotations

import uuid

import pytest

from actant import Runtime
from actant.tasks import task


class TestBatchProgress:
    def test_progress_counts_success_and_failure(self) -> None:
        rt = Runtime.test()
        rt.start()
        try:

            @task
            def maybe_fail(x: int) -> int:
                if x < 0:
                    raise ValueError("negative")
                return x * x

            batch_id = f"batch-{uuid.uuid4().hex}"
            handles = maybe_fail.submit_batch([1, 2, 3, -1], batch_id=batch_id)

            for handle in handles[:3]:
                assert handle.result(timeout=30) > 0
            with pytest.raises(Exception, match="negative"):
                handles[3].result(timeout=30)

            progress = rt.batch_progress(batch_id)
            assert progress is not None
            assert progress.batch_id == batch_id
            assert progress.total == 4
            assert progress.completed == 3
            assert progress.failed == 1
            assert progress.cancelled == 0
            assert progress.pending == 0
            assert progress.finished

            assert rt.remove_batch(batch_id)
            assert rt.batch_progress(batch_id) is None
        finally:
            rt.stop()

    def test_unknown_batch_returns_none(self) -> None:
        rt = Runtime.test()
        rt.start()
        try:
            assert rt.batch_progress("missing") is None
            assert not rt.remove_batch("missing")
        finally:
            rt.stop()

    def test_batch_without_id_is_not_tracked(self) -> None:
        rt = Runtime.test()
        rt.start()
        try:

            @task
            def identity(x: int) -> int:
                return x

            handles = identity.submit_batch([1, 2])
            for handle in handles:
                assert handle.result(timeout=30) in (1, 2)
            assert rt.batch_progress("any") is None
        finally:
            rt.stop()
