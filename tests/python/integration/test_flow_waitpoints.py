"""Flow 等待点集成测试：持久化挂起、外部唤醒与跨重启恢复。

覆盖等待点（持久化挂起、外部唤醒、跨重启恢复）的可执行断言：

1. ``sleep_until`` 真实 park（不返回得太早）→ 到期自动唤醒 → 工作流收敛；
2. ``wait_signal`` 在外部递交信号后被唤醒（flow 体跑在另一个线程真实挂起）；
3. 信号是**闭锁**语义：同一名字重复 await 立即返回已收到的 payload；
4. 有界等待超时返回 ``None``（用户自带处理）；
5. 等待点随工作流快照落盘，**跨重启**仍能被递交（信号挂起跨节点续跑的持久化侧）。

无界 park 的用例一律加 ``join(timeout=...)`` + ``is_alive`` 断言：失败要表现为
**测试失败**，而不是把整个测试套件挂死。
"""

from __future__ import annotations

import queue
import threading
import time

import pytest

from actant import (
    Runtime,
    current_workflow_id,
    flow,
    sleep_until,
    task,
    use_runtime,
    wait_signal,
)
from actant.exceptions import InvalidStateError


@task(name="wp_inc")
def _inc(v: int) -> int:
    return v + 1


def _epoch_ms() -> int:
    return int(time.time() * 1000)


def _signal_once(rt, workflow_id: str, name: str) -> bytes | None:
    """**只递交一次**信号，不重试。

    返回值 ``None`` 表示"已入缓冲、此刻无等待点被唤醒"，不再是"信号被丢弃"。
    """
    return rt.signal_wait_point(workflow_id, name)


class _Events:
    """收集 WorkflowLifecycle 事件（用于取 workflow_id）。"""

    def __init__(self) -> None:
        self.items: list[dict[str, str]] = []

    def __call__(self, e) -> None:  # type: ignore[no-untyped-def]
        self.items.append({"workflow_id": e.workflow_id, "kind": e.kind})

    def await_workflow_id(self, timeout: float = 10.0) -> str:
        """轮询等待首个事件携带的 workflow_id（提交事件在函数体前同步广播）。"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.items:
                return self.items[0]["workflow_id"]
            time.sleep(0.01)
        raise AssertionError("no WorkflowLifecycle event observed")


class TestFlowWaitPoints:
    """flow 侧 ``sleep_until`` / ``wait_signal``。"""

    def test_sleep_until_parks_until_deadline(self) -> None:
        """``sleep_until`` 真实挂起：返回不早于 deadline，且顺序正确。"""
        order: list[str] = []

        @flow(name="wf-sleep")
        def pipeline() -> int:
            _inc.submit(1)
            order.append("before")
            sleep_until(_epoch_ms() + 250)
            order.append("after")
            return _inc.submit(2).result()

        with Runtime.with_defaults():
            started = time.monotonic()
            result = pipeline()
            elapsed = time.monotonic() - started

        assert result == 3, result
        assert order == ["before", "after"], order
        assert elapsed >= 0.25, f"sleep_until 未真实挂起（{elapsed:.3f}s）"
        # 唤醒延迟上界 = Rust watcher 轮询周期（默认 500ms），放宽到 3s 只做
        # 退化保护：真正要守住的是"不会永久挂起"。
        assert elapsed < 3.0, f"sleep_until 唤醒过慢（{elapsed:.3f}s）"

    def test_wait_signal_unblocks_on_external_signal(self) -> None:
        """flow 无任务即挂起（惰性建壳）→ 外部信号唤醒 → 工作流收敛。"""
        seen: list[str] = []
        outcome: list[str] = []

        @flow(name="wf-signal")
        def pipeline() -> str:
            seen.append("parking")
            wait_signal("go")
            seen.append("resumed")
            return "done"

        events = _Events()
        with Runtime.with_defaults() as rt:
            rt.layer("WorkflowLifecycle", "emit").chain(events)

            def _run() -> None:
                # 子线程需显式继承 Runtime 上下文（threading.local 不跨线程传播）。
                with use_runtime(rt):
                    outcome.append(pipeline())

            thread = threading.Thread(target=_run, daemon=True)
            thread.start()
            wf_id = events.await_workflow_id()

            # 单次递交（不重试）：若等待点此刻未注册则入缓冲，注册时命中。
            _signal_once(rt, wf_id, "go")
            thread.join(timeout=20)
            assert not thread.is_alive(), "flow did not resume after signal"

        assert outcome == ["done"], outcome
        assert seen == ["parking", "resumed"], seen

    def test_wait_signal_is_latching(self) -> None:
        """同一信号名重复 await 立即返回（闭锁语义，重放体幂等前提）。"""
        results: list[str] = []

        @flow(name="wf-latch")
        def pipeline() -> None:
            first = wait_signal("go", timeout_ms=5000)
            second = wait_signal("go", timeout_ms=5000)
            results.append(f"{first is not None}:{second is not None}")

        events = _Events()
        with Runtime.with_defaults() as rt:
            rt.layer("WorkflowLifecycle", "emit").chain(events)

            def _run() -> None:
                with use_runtime(rt):
                    pipeline()

            thread = threading.Thread(target=_run, daemon=True)
            thread.start()
            wf_id = events.await_workflow_id()
            _signal_once(rt, wf_id, "go")
            thread.join(timeout=20)
            assert not thread.is_alive(), "latch replay did not return immediately"

        # 只递交了一次信号：第二次 await 必须因"已 Signaled"立即返回。
        assert results == ["True:True"], results

    def test_signal_arrives_before_wait_point_is_registered(self) -> None:
        """信号可先于等待点抵达：注册时命中缓冲，递交方**无需重试**。

        信号在等待点注册前抵达会被缓冲，注册即命中，函数在注册时刻立即返回。

        时序设计：flow 先 ``submit`` 建工作流，再进入 1.5s 的纯睡眠段（**此刻
        等待点尚未注册**），然后 ``wait_signal``。主线程在拿到 workflow_id 后
        0.3s **只递交一次**。故耗时必须接近 1.5s，而不是 1.5s + 3s 上界。
        """
        q: queue.Queue = queue.Queue()
        outcome: list[str] = []
        seen: list[str] = []

        @flow(name="wf-buffer")
        def pipeline() -> str:
            _inc.submit(1).result()  # 工作流在此创建
            q.put(current_workflow_id())
            seen.append("sleeping")
            time.sleep(1.5)  # 纯睡眠段：等待点尚未注册
            seen.append("parking")
            wait_signal("go", timeout_ms=3000)
            seen.append("resumed")
            return "done"

        with Runtime.with_defaults() as rt:

            def _run() -> None:
                with use_runtime(rt):
                    try:
                        outcome.append(f"ret:{pipeline()}")
                    except BaseException as exc:
                        outcome.append(f"{type(exc).__name__}: {exc}")

            thread = threading.Thread(target=_run, daemon=True)
            started = time.monotonic()
            thread.start()
            wf_id = q.get(timeout=10)
            time.sleep(0.3)  # 落在"等待点未注册"的窗口内
            assert rt.signal_wait_point(wf_id, "go") is None, (
                "等待点尚未注册，信号应入缓冲而非唤醒某个等待点"
            )
            thread.join(timeout=20)
            elapsed = time.monotonic() - started

        assert outcome == ["ret:done"], outcome
        assert seen == ["sleeping", "parking", "resumed"], seen
        assert not thread.is_alive(), "buffered signal did not unblock the flow"
        # 1.5s 睡眠 + 少量开销；若信号被丢弃则是 1.5s + 3s 上界 ≈ 4.5s。
        assert elapsed < 3.0, f"缓冲未命中，flow 等到了上界（{elapsed:.2f}s）"

    def test_workflow_id_from_lifecycle_event_is_immediately_usable(self) -> None:
        """``submitted`` 事件给出的 ``workflow_id`` **立即可用**（接线证据）。

        ``@flow`` 的 wrapper 曾在**函数体开始前**就 emit ``submitted`` /
        ``started``，而工作流是**惰性创建**的——于是广播出去的 id 在那一刻还不
        存在，外部控制器按它立即递交信号会撞 ``NotFoundError``（实测 1/5 复现）。
        修复后事件在真正建槽**之后**广播，"看到 submitted ⇒ 工作流已存在"成立。

        本用例刻意**不做任何等待**：拿到事件里的 id 就直接递交。
        """
        events = _Events()
        outcome: list[str] = []

        @flow(name="wf-immediate")
        def pipeline() -> str:
            wait_signal("go")
            return "done"

        with Runtime.with_defaults() as rt:
            rt.layer("WorkflowLifecycle", "emit").chain(events)

            def _run() -> None:
                with use_runtime(rt):
                    try:
                        outcome.append(f"ret:{pipeline()}")
                    except BaseException as exc:
                        outcome.append(f"{type(exc).__name__}: {exc}")

            thread = threading.Thread(target=_run, daemon=True)
            thread.start()
            wf_id = events.await_workflow_id()
            # 不做任何等待：信号已缓冲，此刻递交也不会因等待点未注册而抛错。
            rt.signal_wait_point(wf_id, "go")
            thread.join(timeout=20)
            assert outcome == ["ret:done"], outcome
            assert not thread.is_alive(), "递交后 flow 未被唤醒"

    def test_signal_to_unknown_workflow_raises(self) -> None:
        """递给不存在的工作流 → 显式报错（不再静默返回 ``None``）。"""
        from actant.exceptions import NotFoundError

        with Runtime.with_defaults() as rt, pytest.raises(NotFoundError):
            rt.signal_wait_point("no-such-workflow", "go")

    def test_signal_retry_after_consumption_is_safe(self) -> None:
        """信号已被消费、flow 已跑完后再次递交：**不报错**（重试安全）。

        曾在此处让终态工作流抛 ``InvalidStateError``，被本文件的前两条用例推翻：
        它们的时序正是"首次递交入缓冲 → flow 跑完 → 工作流终态"，重试会撞上
        已终态而报错——明明送到了却报错。
        """
        events = _Events()

        @flow(name="wf-retry")
        def pipeline() -> str:
            wait_signal("go")
            return "done"

        with Runtime.with_defaults() as rt:
            rt.layer("WorkflowLifecycle", "emit").chain(events)

            def _run() -> None:
                with use_runtime(rt):
                    pipeline()

            thread = threading.Thread(target=_run, daemon=True)
            thread.start()
            wf_id = events.await_workflow_id()
            _signal_once(rt, wf_id, "go")
            thread.join(timeout=20)
            assert not thread.is_alive(), "flow did not resume"

            # flow 已跑完、工作流已终态：此时重试递交不得报错。
            assert rt.signal_wait_point(wf_id, "go") is not None

    def test_wait_signal_bounded_returns_none_on_timeout(self) -> None:
        """有界等待超时返回 ``None``（不抛错——上界是调用方显式请求的）。"""

        @flow(name="wf-bounded")
        def pipeline() -> str:
            payload = wait_signal("never-arrives", timeout_ms=200)
            return "none" if payload is None else "got"

        with Runtime.with_defaults():
            started = time.monotonic()
            outcome = pipeline()
            elapsed = time.monotonic() - started

        assert outcome == "none", outcome
        assert elapsed >= 0.2, f"bounded wait 提前返回（{elapsed:.3f}s）"

    def test_wait_outside_flow_raises(self) -> None:
        """非 flow 上下文调用等待原语 → 显式报错（不静默 no-op）。"""
        with Runtime.with_defaults(), pytest.raises(InvalidStateError):
            sleep_until(_epoch_ms() + 10)


class TestWaitPointDurability:
    """等待点持久化：重启后仍可被递交，且已唤醒的直接返回。"""

    def test_wait_point_survives_restart(self, tmp_path) -> None:
        data_dir = str(tmp_path / "data")
        wf_id = "wp-restart-wf"

        rt_a = Runtime.with_defaults(name="wp-node", data_dir=data_dir)
        rt_a.start()
        try:
            rt_a.submit_workflow(wf_id)
            rt_a.register_wait_point(wf_id, "go", kind="signal", name="go")
        finally:
            rt_a.stop()

        rt_b = Runtime.with_defaults(name="wp-node", data_dir=data_dir)
        rt_b.start()
        try:
            # 等待点随快照恢复 → 重启后递交的信号仍被接收。
            assert rt_b.signal_wait_point(wf_id, "go") is not None, (
                "wait point must survive restart"
            )
            # 已唤醒的等待点：park 立即返回（"已收到 → 直接返回"）。
            assert rt_b.wait_wait_point(wf_id, "go", timeout_ms=1000) == b""
        finally:
            rt_b.stop()
