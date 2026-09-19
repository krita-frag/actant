"""Flow 挂起/恢复/中止集成测试。

覆盖挂起/恢复/中止（终态转移释放 park 等待者）的可执行断言：

1. ``suspend()`` 真实 park，直到 ``Runtime.resume_suspended`` 才继续；
2. ``resume_suspended`` 幂等：重复调用第二次返回 ``0``；
3. **resume 不冒充业务信号**：park 在 ``wait_signal`` 上的流不会被
   ``resume_suspended`` 唤醒（它是挂起点专用），反之亦然；
4. ``cancel_workflow``（abort）能唤醒 park 中的函数体并抛出
   ``WorkflowCancelledError``——这是核心修复：终态转移释放 park 等待者。
5. 挂起是多周期的：suspend → resume → suspend → resume 各自独立；
6. 有界挂起超时抛 ``ActantTimeoutError``（与 ``sleep_until`` 同契约）；
7. 挂起点随工作流快照落盘：**跨重启**仍可被恢复。

无界 park 的用例一律加 ``join(timeout=...)`` + ``is_alive`` 断言：失败要表现为
**测试失败**，而不是把整个测试套件挂死。
"""

from __future__ import annotations

import threading
import time

import pytest

from actant import Runtime, flow, suspend, use_runtime, wait_signal
from actant.exceptions import ActantTimeoutError


class _Events:
    """收集 WorkflowLifecycle 事件（用于取 workflow_id）。"""

    def __init__(self) -> None:
        self.items: list[str] = []

    def __call__(self, e) -> None:  # type: ignore[no-untyped-def]
        self.items.append(e.workflow_id)

    def await_workflow_id(self, timeout: float = 10.0) -> str:
        """轮询等待首个事件携带的 workflow_id（提交事件在函数体前同步广播）。"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.items:
                return self.items[0]
            time.sleep(0.01)
        raise AssertionError("no WorkflowLifecycle event observed")


def _resume_until(rt, workflow_id: str, *, count: int = 1, timeout: float = 10.0) -> int:
    """反复调用 ``resume_suspended`` 直到累计唤醒 ``count`` 个挂起点。

    需要"反复"是因为挂起点的注册发生在函数体跑到 ``suspend()`` 时，而驱动器
    不知道它何时就绪；返回 ``0`` 表示该时刻还没有挂起中的挂起点，不是失败。
    """
    total = 0
    deadline = time.monotonic() + timeout
    while total < count and time.monotonic() < deadline:
        total += rt.resume_suspended(workflow_id)
        if total < count:
            time.sleep(0.02)
    if total < count:
        raise AssertionError(f"expected {count} suspend resumes, got {total}")
    return total


def _run_flow(rt, fn, outcome: list[str]) -> threading.Thread:
    """在子线程里跑一个 flow，把返回/异常记录进 ``outcome``。"""

    def _run() -> None:
        # 子线程需显式继承 Runtime 上下文（threading.local 不跨线程传播）。
        with use_runtime(rt):
            try:
                outcome.append(f"ret:{fn()}")
            except BaseException as exc:
                outcome.append(f"{type(exc).__name__}: {exc}")

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return thread


class TestSuspendResume:
    """``suspend()`` / ``Runtime.resume_suspended()``。"""

    def test_suspend_parks_until_resumed(self) -> None:
        """挂起真实 park：恢复前不继续，恢复后跑完。"""
        seen: list[str] = []
        outcome: list[str] = []

        @flow(name="wf-suspend")
        def pipeline() -> str:
            seen.append("before")
            suspend()
            seen.append("after")
            return "done"

        events = _Events()
        with Runtime.with_defaults() as rt:
            rt.layer("WorkflowLifecycle", "emit").chain(events)

            thread = _run_flow(rt, pipeline, outcome)
            wf_id = events.await_workflow_id()

            time.sleep(0.5)
            assert seen == ["before"], f"挂起未生效：{seen}"
            assert thread.is_alive(), "flow 在 suspend 之后仍在跑"

            assert _resume_until(rt, wf_id) >= 1
            thread.join(timeout=20)
            assert not thread.is_alive(), "flow 在 resume 之后没有继续"

        assert seen == ["before", "after"], seen
        assert outcome == ["ret:done"], outcome

    def test_resume_suspended_is_idempotent(self) -> None:
        """重复恢复：第二次返回 ``0``（不重复追加唤醒事件）。"""
        outcome: list[str] = []

        @flow(name="wf-suspend-once")
        def pipeline() -> str:
            suspend()
            return "done"

        events = _Events()
        with Runtime.with_defaults() as rt:
            rt.layer("WorkflowLifecycle", "emit").chain(events)

            thread = _run_flow(rt, pipeline, outcome)
            wf_id = events.await_workflow_id()

            assert _resume_until(rt, wf_id) == 1
            thread.join(timeout=20)
            assert not thread.is_alive()

            # 该工作流已无挂起中的挂起点 → 幂等返回 0。
            assert rt.resume_suspended(wf_id) == 0

        assert outcome == ["ret:done"], outcome

    def test_resume_does_not_impersonate_business_signal(self) -> None:
        """resume 只唤醒挂起点，不会替 ``wait_signal`` 递交业务信号。"""
        outcome: list[str] = []

        @flow(name="wf-signal-not-resume")
        def pipeline() -> str:
            wait_signal("go", timeout_ms=8000)
            return "done"

        events = _Events()
        with Runtime.with_defaults() as rt:
            rt.layer("WorkflowLifecycle", "emit").chain(events)

            thread = _run_flow(rt, pipeline, outcome)
            wf_id = events.await_workflow_id()

            # 等信号等待点注册（递交方需重试的既有契约）。
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if rt.resume_suspended(wf_id) == 0:
                    # resume 对 signal 等待点必须无动作。
                    break
                raise AssertionError("resume_suspended 唤醒了 signal 等待点")
            else:
                raise AssertionError("signal 等待点始终未注册")

            time.sleep(0.3)
            assert thread.is_alive(), "flow 不应被 resume 唤醒"

            # 真正的业务信号才能唤醒它。
            assert rt.signal_wait_point(wf_id, "go") is not None
            thread.join(timeout=20)
            assert not thread.is_alive(), "flow 未被业务信号唤醒"

        assert outcome == ["ret:done"], outcome

    def test_suspend_is_multicycle(self) -> None:
        """同一 flow 内多次挂起各自独立（键按等待序号递增）。"""
        seen: list[str] = []
        outcome: list[str] = []

        @flow(name="wf-suspend-cycles")
        def pipeline() -> str:
            suspend()
            seen.append("first")
            suspend()
            seen.append("second")
            return "done"

        events = _Events()
        with Runtime.with_defaults() as rt:
            rt.layer("WorkflowLifecycle", "emit").chain(events)

            thread = _run_flow(rt, pipeline, outcome)
            wf_id = events.await_workflow_id()

            _resume_until(rt, wf_id, count=1)
            _resume_until(rt, wf_id, count=1)  # 第二次挂起是独立的挂起点
            thread.join(timeout=20)
            assert not thread.is_alive(), "两轮挂起未都被恢复"

        assert seen == ["first", "second"], seen
        assert outcome == ["ret:done"], outcome

    def test_suspend_bounded_raises_on_timeout(self) -> None:
        """有界挂起未被恢复 ⇒ 抛 ``ActantTimeoutError``（与 sleep_until 同契约）。

        ``suspend()`` 返回值是 ``None``，静默超时无法与"已恢复"区分，故与
        ``sleep_until`` 一致地抛错，而不是像 ``wait_signal`` 那样返回 ``None``。
        """

        @flow(name="wf-suspend-bounded")
        def pipeline() -> str:
            suspend(timeout_ms=300)
            return "done"

        with Runtime.with_defaults():
            started = time.monotonic()
            with pytest.raises(ActantTimeoutError) as excinfo:
                pipeline()
            elapsed = time.monotonic() - started

        assert "suspend()" in str(excinfo.value)
        assert elapsed >= 0.3, f"有界挂起提前返回（{elapsed:.3f}s）"


class TestAbortParkedFlow:
    """``Runtime.cancel_workflow`` 必须唤醒 park 中的函数体（核心修复）。"""

    def test_abort_wakes_flow_parked_on_signal(self) -> None:
        """park 在 ``wait_signal`` 上的流被 abort ⇒ 抛 ``WorkflowCancelledError``。

        abort 必须释放 park 等待者，否则函数体会永久挂起（工作流已 ``Cancelled``）。
        """
        seen: list[str] = []
        outcome: list[str] = []

        @flow(name="wf-abort-signal")
        def pipeline() -> str:
            seen.append("parking")
            wait_signal("never", timeout_ms=0)   # 无限 park
            seen.append("resumed")
            return "done"

        events = _Events()
        with Runtime.with_defaults() as rt:
            rt.layer("WorkflowLifecycle", "emit").chain(events)

            thread = _run_flow(rt, pipeline, outcome)
            wf_id = events.await_workflow_id()
            time.sleep(0.5)
            assert thread.is_alive()

            rt.cancel_workflow(wf_id)
            thread.join(timeout=10)
            assert not thread.is_alive(), "abort 未能唤醒 park 中的函数体"

            state = rt.get_workflow_state(wf_id)
            assert state is not None and state.get("state") == "Cancelled"

        assert seen == ["parking"], f"函数体在 abort 之后不应继续：{seen}"
        assert len(outcome) == 1 and outcome[0].startswith("WorkflowCancelledError"), outcome

    def test_abort_wakes_suspended_flow(self) -> None:
        """``suspend()`` 挂起中的流被 abort ⇒ 同样被唤醒（挂起不是免死金牌）。"""
        seen: list[str] = []
        outcome: list[str] = []

        @flow(name="wf-abort-suspend")
        def pipeline() -> str:
            suspend()
            seen.append("after-suspend")
            return "done"

        events = _Events()
        with Runtime.with_defaults() as rt:
            rt.layer("WorkflowLifecycle", "emit").chain(events)

            thread = _run_flow(rt, pipeline, outcome)
            wf_id = events.await_workflow_id()
            time.sleep(0.5)
            assert thread.is_alive()

            rt.cancel_workflow(wf_id)
            thread.join(timeout=10)
            assert not thread.is_alive(), "abort 未能唤醒挂起中的函数体"

        assert seen == [], seen
        assert len(outcome) == 1 and outcome[0].startswith("WorkflowCancelledError"), outcome

    def test_abort_wakes_flow_parked_on_timer(self) -> None:
        """``sleep_until`` park 中的流被 abort ⇒ 不等到期即抛错并结束。"""
        outcome: list[str] = []

        @flow(name="wf-abort-timer")
        def pipeline() -> str:
            from actant import sleep_until

            sleep_until(int(time.time() * 1000) + 30_000)   # 30s 后到期
            return "done"

        events = _Events()
        with Runtime.with_defaults() as rt:
            rt.layer("WorkflowLifecycle", "emit").chain(events)

            thread = _run_flow(rt, pipeline, outcome)
            wf_id = events.await_workflow_id()
            time.sleep(0.5)

            started = time.monotonic()
            rt.cancel_workflow(wf_id)
            thread.join(timeout=10)
            elapsed = time.monotonic() - started

            assert not thread.is_alive(), "abort 未能唤醒 timer park 中的函数体"
            assert elapsed < 5.0, f"abort 唤醒过慢（{elapsed:.2f}s，未到期的 sleep 应被立即释放）"

        assert len(outcome) == 1 and outcome[0].startswith("WorkflowCancelledError"), outcome


class TestSuspendDurability:
    """挂起点的持久化：跨重启仍可被恢复。"""

    def test_suspend_survives_restart(self, tmp_path) -> None:
        data_dir = str(tmp_path / "data")
        wf_id = "wf-suspend-restart"

        rt_a = Runtime.with_defaults(name="suspend-node", data_dir=data_dir)
        rt_a.start()
        try:
            rt_a.submit_workflow(wf_id)
            rt_a.register_wait_point(wf_id, "suspend-1", kind="suspend")
            # 重启前未恢复 → 该挂起点仍在等待。
            assert rt_a.resume_suspended(wf_id) == 1
        finally:
            rt_a.stop()

        # 另起一个挂起点（模拟"挂起中崩溃"），重启后应能被恢复。
        rt_b = Runtime.with_defaults(name="suspend-node", data_dir=data_dir)
        rt_b.start()
        try:
            rt_b.register_wait_point(wf_id, "suspend-2", kind="suspend")
        finally:
            rt_b.stop()

        rt_c = Runtime.with_defaults(name="suspend-node", data_dir=data_dir)
        rt_c.start()
        try:
            # 挂起点随快照恢复 → 重启后仍可被恢复（幂等返回 1）。
            assert rt_c.resume_suspended(wf_id) == 1, (
                "suspend wait point must survive restart"
            )
            # 已唤醒的挂起点：再次恢复返回 0。
            assert rt_c.resume_suspended(wf_id) == 0
        finally:
            rt_c.stop()
