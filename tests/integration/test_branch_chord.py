"""集成测试：分支、switch、chord、嵌套 flow。

覆盖：
- branch() 条件分支
- switch() 多路分支
- parallel() 显式并行
- Task.reduce chord 语义
- 嵌套 @flow（subflow 扁平化）
- 条件分支作为下游 task 参数
"""

from __future__ import annotations

import pytest

import actant
from actant.exceptions import WorkflowFailedError
from actant.flow import branch, parallel, switch

# ---------------------------------------------------------------------------
# 测试任务
# ---------------------------------------------------------------------------


@actant.task(name="bc_add")
def _add(a, b):
    return a + b


@actant.task(name="bc_gt")
def _gt(x, threshold):
    return x > threshold


@actant.task(name="bc_classify")
def _classify(x):
    if x < 10:
        return "low"
    elif x < 100:
        return "mid"
    else:
        return "high"


@actant.task(name="bc_format")
def _format(label, value):
    return f"{label}:{value}"


@actant.task(name="bc_double")
def _double(x):
    return x * 2

@actant.task(name="bc_square")
def _square(x):
    return x * x

@actant.task(name="bc_sum_list")
def _sum_list(items):
    return sum(items)


@actant.task(name="bc_identity")
def _identity(x):
    return x


@actant.task(name="bc_max_list")
def _max_list(items):
    return max(items)


# ---------------------------------------------------------------------------
# branch() 条件分支
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestBranch:
    """branch() 根据 condition_fn 选择执行 if 或 else 分支。"""

    def test_branch_true_path(self, submit_and_wait):
        """condition_fn 返回 True 时执行 if_ref。"""
        @actant.flow
        def f():
            x = _identity(5)
            # x > 0 → True → double
            br = branch(x, lambda r: r > 0, _double(x), _square(x))
            return _identity(br)

        result = submit_and_wait(f)
        assert result.is_success
        # 5 > 0 → double(5) = 10
        assert result.value == 10

    def test_branch_false_path(self, submit_and_wait):
        """condition_fn 返回 False 时执行 else_ref。"""
        @actant.flow
        def f():
            x = _identity(-3)
            # x > 0 → False → square
            br = branch(x, lambda r: r > 0, _double(x), _square(x))
            return _identity(br)

        result = submit_and_wait(f)
        assert result.is_success
        # -3 > 0 → False → square(-3) = 9
        assert result.value == 9

    def test_branch_zero_threshold(self, submit_and_wait):
        @actant.flow
        def f():
            x = _identity(0)
            br = branch(x, lambda r: r >= 0, _double(x), _square(x))
            return _identity(br)

        result = submit_and_wait(f)
        assert result.value == 0  # double(0) = 0

    def test_branch_in_chain(self, submit_and_wait):
        """branch 结果传入下游 task。"""
        @actant.flow
        def f():
            x = _identity(7)
            br = branch(x, lambda r: r > 5, _double(x), _square(x))
            # br 传入下游
            return _add(br, 100)

        result = submit_and_wait(f)
        # 7 > 5 → double(7)=14, 14+100=114
        assert result.value == 114


# ---------------------------------------------------------------------------
# switch() 多路分支
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestSwitch:
    """switch() 多路分支选择。"""

    def test_switch_all_cases_complete(self, submit_and_wait):
        """switch 返回所有 case TaskRef，运行时编排循环只激活匹配条件的 case。

        x=50 匹配 mid case (r<100)，其他 case 不执行。
        """
        @actant.flow
        def f():
            x = _identity(50)
            cases = [
                ("low", _double(x), lambda r: r < 10),
                ("mid", _square(x), lambda r: r < 100),
                ("high", _identity(x), lambda r: r >= 100),
            ]
            refs = switch(x, *cases)
            # 只有匹配条件的 case 产生结果，reduce 聚合
            return _sum_list.reduce(refs)

        result = submit_and_wait(f)
        assert result.is_success
        # x=50 只匹配 mid (r<100)：square(50)=2500
        assert result.value == 2500

    def test_switch_low_value(self, submit_and_wait):
        """x=5 匹配 low (r<10) 和 mid (r<100)，两个 case 都激活。"""
        @actant.flow
        def f():
            x = _identity(5)
            cases = [
                ("low", _double(x), lambda r: r < 10),
                ("mid", _square(x), lambda r: r < 100),
            ]
            refs = switch(x, *cases)
            return _sum_list.reduce(refs)

        result = submit_and_wait(f)
        assert result.is_success
        # x=5 同时匹配 low 和 mid：double(5)=10, square(5)=25, sum=35
        assert result.value == 35


# ---------------------------------------------------------------------------
# parallel() 显式并行
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestParallel:
    """parallel() 声明显式并行。"""

    def test_parallel_three_tasks(self, submit_and_wait):
        @actant.flow
        def f():
            a = _identity(1)
            b = _identity(2)
            c = _identity(3)
            refs = parallel(a, b, c)
            return _sum_list.reduce(refs)

        result = submit_and_wait(f)
        assert result.value == 6

    def test_parallel_empty(self, submit_and_wait):
        """parallel() 无参数返回空列表。"""
        @actant.flow
        def f():
            refs = parallel()
            return _sum_list.reduce(refs) if refs else _identity(0)

        result = submit_and_wait(f)
        assert result.value == 0

    def test_parallel_with_reduce(self, submit_and_wait):
        @actant.flow
        def f():
            refs = parallel(_double(5), _square(3), _identity(10))
            return _sum_list.reduce(refs)

        result = submit_and_wait(f)
        # 10 + 9 + 10 = 29
        assert result.value == 29

    def test_parallel_rejects_non_taskref(self, submit_and_wait):
        """parallel() 参数必须是 TaskRef。"""
        @actant.flow
        def f():
            return parallel(_identity(1), "not_a_ref")  # type: ignore[arg-type]

        with pytest.raises((WorkflowFailedError, TypeError, Exception)):
            submit_and_wait(f)


# ---------------------------------------------------------------------------
# chord（Task.reduce）
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestChord:
    """Task.reduce 实现 chord 语义：所有上游完成后聚合。"""

    def test_chord_basic(self, submit_and_wait):
        @actant.flow
        def f():
            refs = _identity.map([1, 2, 3, 4, 5])
            return _sum_list.reduce(refs)

        result = submit_and_wait(f)
        assert result.value == 15

    def test_chord_after_parallel(self, submit_and_wait):
        @actant.flow
        def f():
            a = _double(5)  # 10
            b = _square(4)  # 16
            c = _identity(20)  # 20
            refs = parallel(a, b, c)
            return _max_list.reduce(refs)

        result = submit_and_wait(f)
        assert result.value == 20

    def test_chord_empty_upstream(self, submit_and_wait):
        """chord 上游为空列表时 callback 仍执行。"""
        @actant.flow
        def f():
            return _sum_list.reduce([])

        result = submit_and_wait(f)
        # sum([]) = 0
        assert result.value == 0

    def test_chord_nested(self, submit_and_wait):
        """chord 嵌套：第一层 chord 结果作为第二层 chord 输入。"""
        @actant.flow
        def f():
            # 第一组：[1,2,3] → sum=6
            refs1 = _identity.map([1, 2, 3])
            sum1 = _sum_list.reduce(refs1)
            # 第二组：[4,5,6] → sum=15
            refs2 = _identity.map([4, 5, 6])
            sum2 = _sum_list.reduce(refs2)
            # 第三层：聚合两个 chord 结果
            return _sum_list.reduce([sum1, sum2])

        result = submit_and_wait(f)
        assert result.value == 21  # 6 + 15


# ---------------------------------------------------------------------------
# 嵌套 @flow（subflow 扁平化）
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestNestedFlow:
    """子 flow 调用时 DAG 扁平化嵌入父 flow。"""

    def test_subflow_single_return(self, submit_and_wait):
        """子 flow 返回单个 TaskRef，父 flow 直接使用。"""
        @actant.flow
        def child(x):
            a = _double(x)
            b = _add(a, 1)
            return b

        @actant.flow
        def parent():
            r = child(5)  # double(5)+1 = 11
            return _add(r, 100)  # 111

        result = submit_and_wait(parent)
        assert result.value == 111

    def test_subflow_nested_two_levels(self, submit_and_wait):
        @actant.flow
        def inner(x):
            return _double(x)

        @actant.flow
        def middle(x):
            return _add(inner(x), 1)

        @actant.flow
        def outer():
            return middle(10)  # double(10)+1 = 21

        result = submit_and_wait(outer)
        assert result.value == 21

    def test_subflow_with_map_reduce(self, submit_and_wait):
        @actant.flow
        def sum_squares(n):
            refs = _square.map(list(range(1, n + 1)))
            return _sum_list.reduce(refs)

        @actant.flow
        def parent():
            return sum_squares(5)  # 1+4+9+16+25 = 55

        result = submit_and_wait(parent)
        assert result.value == 55

    def test_two_subflows_independent(self, submit_and_wait):
        """两个子 flow 独立执行，结果在父 flow 聚合。"""
        @actant.flow
        def branch_a(x):
            return _double(x)

        @actant.flow
        def branch_b(x):
            return _square(x)

        @actant.flow
        def parent():
            a = branch_a(5)  # 10
            b = branch_b(3)  # 9
            return _add(a, b)  # 19

        result = submit_and_wait(parent)
        assert result.value == 19
