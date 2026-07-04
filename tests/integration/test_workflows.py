"""集成测试：单进程多任务工作流执行。

覆盖：
- 线性 DAG（A→B→C）
- 菱形 DAG（A→B,C→D）
- 多输入任务
- 任务返回值传递
- 任务异常处理
- inline 任务（无注册表）
- 大量并行任务
- 任务超时
- 任务优先级（不影响正确性，仅验证可工作）
"""

from __future__ import annotations

import time

import pytest

import actant
from actant.exceptions import WorkflowFailedError

# ---------------------------------------------------------------------------
# 测试任务定义（模块级，可被 worker 注册表发现）
# ---------------------------------------------------------------------------


@actant.task(name="wf_add")
def _add(a, b):
    return a + b


@actant.task(name="wf_mul")
def _mul(a, b):
    return a * b


@actant.task(name="wf_identity")
def _identity(x):
    return x


@actant.task(name="wf_const")
def _const():
    return 42


@actant.task(name="wf_concat")
def _concat(a, b, c):
    return f"{a}-{b}-{c}"


@actant.task(name="wf_raise")
def _raise_error(msg):
    raise ValueError(msg)


@actant.task(name="wf_slow")
def _slow_task(seconds):
    time.sleep(seconds)
    return "done"


@actant.task(name="wf_sum_list")
def _sum_list(items):
    return sum(items)


@actant.task(name="wf_range_n")
def _range_n(n):
    return list(range(n))


# ---------------------------------------------------------------------------
# 线性 DAG
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestLinearDag:
    """A → B → C 线性依赖。"""

    def test_three_stage_linear(self, submit_and_wait):
        @actant.flow
        def f():
            a = _add(1, 2)  # 3
            b = _mul(a, 10)  # 30
            c = _add(b, 5)  # 35
            return c

        result = submit_and_wait(f)
        assert result.is_success
        assert result.value == 35

    def test_chain_with_kwargs(self, submit_and_wait):
        """kwargs 中的 TaskRef 应正确建立依赖并替换。"""
        @actant.flow
        def f():
            a = _add(1, b=2)  # 3
            b = _mul(a=a, b=10)  # 30
            return b

        result = submit_and_wait(f)
        assert result.is_success
        assert result.value == 30

    def test_single_task_flow(self, submit_and_wait):
        @actant.flow
        def f():
            return _const()

        result = submit_and_wait(f)
        assert result.value == 42

    def test_identity_passthrough(self, submit_and_wait):
        @actant.flow
        def f():
            return _identity("hello")

        result = submit_and_wait(f)
        assert result.value == "hello"


# ---------------------------------------------------------------------------
# 菱形 DAG
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestDiamondDag:
    """A → B, A → C, B+C → D。"""

    def test_diamond_dependency(self, submit_and_wait):
        @actant.flow
        def f():
            a = _add(1, 2)  # 3
            b = _mul(a, 10)  # 30
            c = _mul(a, 100)  # 300
            d = _add(b, c)  # 330
            return d

        result = submit_and_wait(f)
        assert result.value == 330

    def test_diamond_three_inputs(self, submit_and_wait):
        @actant.flow
        def f():
            a = _const()  # 42
            b = _identity(a)  # 42
            c = _identity(a)  # 42
            d = _concat(b, c, a)  # "42-42-42"
            return d

        result = submit_and_wait(f)
        assert result.value == "42-42-42"


# ---------------------------------------------------------------------------
# map / reduce
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestMapReduce:
    """Task.map 并行 + Task.reduce 聚合。"""

    def test_map_only(self, submit_and_wait):
        @actant.flow
        def f():
            refs = _identity.map([1, 2, 3, 4, 5])
            # 返回 list of refs → 多结果
            return refs

        result = submit_and_wait(f)
        # 多 sink 节点 → list
        assert isinstance(result.value, list)
        assert sorted(result.value) == [1, 2, 3, 4, 5]

    def test_map_then_reduce(self, submit_and_wait):
        @actant.flow
        def f():
            refs = _identity.map([1, 2, 3, 4, 5])
            total = _sum_list.reduce(refs)
            return total

        result = submit_and_wait(f)
        assert result.value == 15

    def test_map_with_options(self, submit_and_wait):
        @actant.flow
        def f():
            refs = _identity.map([10, 20, 30], priority="high", timeout=5.0)
            return _sum_list.reduce(refs)

        result = submit_and_wait(f)
        assert result.value == 60


# ---------------------------------------------------------------------------
# inline 任务
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestInlineTasks:
    """inline=True 任务：func 内联到 payload，无需注册表。"""

    def test_inline_lambda(self, submit_and_wait):
        """inline 让无业务模块依赖的 worker 也能执行。"""
        inline_task = actant.task(name="_inline_double", func=lambda x: x * 2)

        @actant.flow
        def f():
            return inline_task(5, inline=True)

        result = submit_and_wait(f)
        assert result.value == 10

    def test_inline_in_chain(self, submit_and_wait):
        inline_square = actant.task(name="_inline_sq", func=lambda x: x * x)

        @actant.flow
        def f():
            a = _add(2, 3)  # 5
            b = inline_square(a, inline=True)  # 25
            return _add(b, 1)  # 26

        result = submit_and_wait(f)
        assert result.value == 26


# ---------------------------------------------------------------------------
# 异常与失败
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestTaskFailure:
    """任务异常导致 workflow 失败。"""

    def test_task_raise_propagates(self, submit_and_wait):
        @actant.flow
        def f():
            return _raise_error("boom")

        with pytest.raises(WorkflowFailedError) as ei:
            submit_and_wait(f)
        assert "boom" in str(ei.value)

    def test_failure_in_middle_task(self, submit_and_wait):
        @actant.flow
        def f():
            a = _add(1, 2)
            b = _raise_error("middle failure")
            return _add(a, b)

        with pytest.raises(WorkflowFailedError) as ei:
            submit_and_wait(f)
        assert "middle failure" in str(ei.value)

    def test_failed_task_name_reported(self, submit_and_wait):
        @actant.flow
        def f():
            return _raise_error("named failure")

        with pytest.raises(WorkflowFailedError) as ei:
            submit_and_wait(f)
        # task_name 应被填充（Rust 端结构化失败任务）
        assert ei.value.task_name is not None
        assert "wf_raise" in ei.value.task_name or "raise" in ei.value.task_name


# ---------------------------------------------------------------------------
# 任务超时
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestTaskTimeout:
    """任务超时被运行时中断。"""

    def test_task_with_short_timeout(self, single_node):
        """slow_task 超过 timeout 应失败。

        超时任务应导致 workflow 进入 Failed 终态。
        """
        @actant.flow
        def f():
            # 2 秒任务，0.2 秒超时
            return _slow_task(2.0, timeout=0.2)

        result = single_node.submit(f)
        # 等待 workflow 进入终态（任务超时 + 取消传播）
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            if result.ready():
                break
            time.sleep(0.1)
        assert result.ready(), "workflow 未在超时内进入终态"
        # 超时任务应导致 workflow 失败
        assert result.state() != "Completed", \
            f"超时任务不应成功完成（state={result.state()}）"

    def test_task_completes_within_timeout(self, submit_and_wait):
        @actant.flow
        def f():
            return _slow_task(0.05, timeout=2.0)

        result = submit_and_wait(f, timeout=10.0)
        assert result.is_success
        assert result.value == "done"


# ---------------------------------------------------------------------------
# 大量并行任务
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestParallelTasks:
    """并行任务压力测试。"""

    def test_many_parallel_identity(self, submit_and_wait):
        @actant.flow
        def f():
            refs = _identity.map(list(range(20)))
            return _sum_list.reduce(refs)

        result = submit_and_wait(f, timeout=30.0)
        assert result.value == sum(range(20))

    def test_wide_diamond(self, submit_and_wait):
        """一个 source fan-out 到多个 task，再 fan-in。"""
        @actant.flow
        def f():
            src = _const()  # 42
            # 5 个并行任务都消费 src
            refs = _identity.map([src] * 5)
            return _sum_list.reduce(refs)

        result = submit_and_wait(f)
        assert result.value == 42 * 5
