"""flow.py 单元测试：Flow / parallel / branch / switch / _build_ref_payload 等。

覆盖目标：100% 行覆盖 + 分支覆盖。
重点测试 Flow 装饰器、subflow 嵌套、参数校验、循环检测、parallel/branch/switch。
"""

from __future__ import annotations

import pytest

import actant
from actant.flow import (
    BranchRef,
    Flow,
    FlowContext,
    _build_ref_payload,
    _extract_output_refs,
    _find_sinks,
    _is_taskref_collection,
    branch,
    flow,
    parallel,
    switch,
)
from actant.task import TaskRef

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def reset_flow_context_local():
    """每个测试后重置全局 flow context。"""
    import sys

    flow_mod = sys.modules["actant.flow"]
    old = getattr(flow_mod._context_local, "flow_context", None)
    yield
    flow_mod._context_local.flow_context = old


# ---------------------------------------------------------------------------
# _is_taskref_collection
# ---------------------------------------------------------------------------


class TestIsTaskrefCollection:
    def test_empty_list_not_collection(self):
        assert _is_taskref_collection([]) is False

    def test_empty_tuple_not_collection(self):
        assert _is_taskref_collection(()) is False

    def test_list_of_taskrefs(self):
        ref = TaskRef(task_name="t", args=())
        assert _is_taskref_collection([ref]) is True

    def test_tuple_of_taskrefs(self):
        ref = TaskRef(task_name="t", args=())
        assert _is_taskref_collection((ref,)) is True

    def test_non_list_returns_false(self):
        assert _is_taskref_collection("string") is False
        assert _is_taskref_collection(42) is False
        assert _is_taskref_collection(None) is False

    def test_nested_list_not_supported(self):
        """嵌套 list 不视为 collection。"""
        ref = TaskRef(task_name="t", args=())
        assert _is_taskref_collection([[ref]]) is False


# ---------------------------------------------------------------------------
# _extract_output_refs
# ---------------------------------------------------------------------------


class TestExtractOutputRefs:
    def test_single_taskref(self):
        ref = TaskRef(task_name="t", args=())
        assert _extract_output_refs(ref) == [ref]

    def test_branchref(self):
        if_ref = TaskRef(task_name="if", args=())
        else_ref = TaskRef(task_name="else", args=())
        br = BranchRef(if_ref, else_ref)
        assert _extract_output_refs(br) == [if_ref, else_ref]

    def test_list_of_taskrefs(self):
        r1 = TaskRef(task_name="t1", args=())
        r2 = TaskRef(task_name="t2", args=())
        assert _extract_output_refs([r1, r2]) == [r1, r2]

    def test_tuple_of_taskrefs(self):
        r1 = TaskRef(task_name="t1", args=())
        r2 = TaskRef(task_name="t2", args=())
        assert _extract_output_refs((r1, r2)) == [r1, r2]

    def test_nested_list_with_branchref(self):
        if_ref = TaskRef(task_name="if", args=())
        else_ref = TaskRef(task_name="else", args=())
        br = BranchRef(if_ref, else_ref)
        r3 = TaskRef(task_name="t3", args=())
        # [br, r3] → [if_ref, else_ref, r3]
        assert _extract_output_refs([br, r3]) == [if_ref, else_ref, r3]

    def test_non_taskref_value_returns_empty(self):
        assert _extract_output_refs(42) == []
        assert _extract_output_refs("string") == []
        assert _extract_output_refs(None) == []


# ---------------------------------------------------------------------------
# _build_ref_payload
# ---------------------------------------------------------------------------


class TestBuildRefPayload:
    """_build_ref_payload 编码 TaskRef 的参数为 payload。"""

    def test_inline_func_used_when_present(self):
        """ref.inline_func 存在时直接使用（68 行反向）。"""

        def my_inline(a, b):
            return a + b

        ref = TaskRef(task_name="t", args=(1, 2), inline_func=my_inline)
        payload = _build_ref_payload(ref)
        # payload 非空（args 被编码）
        assert payload != b""

    def test_global_task_func_used_when_inline_none(self):
        """ref.inline_func 为 None 时从全局注册表查找（68 行）。"""

        @actant.task
        def global_task(x):
            return x * 2

        # 构造一个 inline_func=None 的 TaskRef
        ref = TaskRef(task_name="global_task", args=(5,), inline_func=None)
        payload = _build_ref_payload(ref)
        assert payload != b""


# ---------------------------------------------------------------------------
# _find_sinks
# ---------------------------------------------------------------------------


class TestFindSinks:
    def test_no_edges_all_nodes_are_sinks(self):
        assert _find_sinks(3, []) == [0, 1, 2]

    def test_linear_chain_last_is_sink(self):
        edges = [(0, 1, None), (1, 2, None)]
        assert _find_sinks(3, edges) == [2]

    def test_diamond_single_sink(self):
        edges = [(0, 1, None), (0, 2, None), (1, 3, None), (2, 3, None)]
        assert _find_sinks(4, edges) == [3]

    def test_multiple_sinks(self):
        edges = [(0, 1, None), (0, 2, None)]
        assert _find_sinks(3, edges) == [1, 2]


# ---------------------------------------------------------------------------
# FlowContext
# ---------------------------------------------------------------------------


class TestFlowContextProperties:
    """FlowContext 的属性访问。"""

    def test_return_value_property(self):
        ctx = FlowContext()
        ctx._return_value = "test-value"
        assert ctx.return_value == "test-value"

    def test_ref_count_empty(self):
        ctx = FlowContext()
        assert ctx.ref_count == 0

    def test_condition_evaluators_empty(self):
        ctx = FlowContext()
        assert ctx.condition_evaluators == {}

    def test_get_ref_by_index(self):
        ctx = FlowContext()
        ref = TaskRef(task_name="t", args=())
        ctx.track(ref)
        assert ctx.get_ref(0) is ref

    def test_merge_into(self):
        """子 context 合并到父 context。"""
        parent = FlowContext()
        child = FlowContext()

        r1 = TaskRef(task_name="t1", args=())
        r2 = TaskRef(task_name="t2", args=())
        child.track(r1)
        child.track(r2)

        offset = child.merge_into(parent)
        assert offset == 0
        assert parent.ref_count == 2
        assert parent.get_ref(0) is r1
        assert parent.get_ref(1) is r2

    def test_find_sinks_with_superseded_edges(self):
        """find_sinks 跳过被取代的无条件边。"""
        ctx = FlowContext()
        r1 = TaskRef(task_name="t1", args=())
        r2 = TaskRef(task_name="t2", args=())
        ctx.track(r1)
        ctx.track(r2)
        # 手动添加边和 superseded 标记
        ctx._edges.append((0, 1, None))
        ctx._superseded_unconditional.add((0, 1))
        # 0 -> 1 的无条件边被取代，0 仍被视为 sink
        sinks = ctx.find_sinks()
        assert 0 in sinks
        assert 1 in sinks


# ---------------------------------------------------------------------------
# Flow 装饰器
# ---------------------------------------------------------------------------


class TestFlowDecorator:
    """flow 装饰器的所有分支。"""

    def test_flow_without_parenthesis(self):
        @flow
        def my_flow(x):
            return x

        assert isinstance(my_flow, Flow)
        assert my_flow.name == "my_flow"

    def test_flow_with_parenthesis_no_args(self):
        @flow()
        def my_flow(x):
            return x

        assert isinstance(my_flow, Flow)
        assert my_flow.name == "my_flow"

    def test_flow_with_explicit_name(self):
        @flow(name="custom-name")
        def my_flow(x):
            return x

        assert isinstance(my_flow, Flow)
        assert my_flow.name == "custom-name"

    def test_flow_with_all_params(self):
        @flow(name="full", timeout=30.0, failure_strategy="fail_fast")
        def my_flow(x):
            return x

        assert isinstance(my_flow, Flow)
        assert my_flow.name == "full"

    def test_flow_repr(self):
        @flow
        def my_flow(x):
            return x

        assert repr(my_flow) == "<Flow my_flow>"


# ---------------------------------------------------------------------------
# Flow.__call__ — 上下文外执行
# ---------------------------------------------------------------------------


class TestFlowCallOutsideContext:
    """Flow.__call__ 在 flow 上下文外直接执行。"""

    def test_call_outside_context_executes_locally(self, reset_flow_context_local):
        @flow
        def my_flow(x):
            return x * 2

        # flow 上下文外调用 → 直接执行
        result = my_flow(5)
        assert result == 10

    def test_call_inside_context_embeds_subflow(self, reset_flow_context_local):
        """flow 上下文内调用另一个 flow → 嵌入子工作流。"""
        from actant.flow import _context_local

        @actant.task
        def add(a, b):
            return a + b

        @flow
        def child(x):
            return add(x, 1)

        @flow
        def parent(x):
            return child(x)

        # 在父 flow 上下文中调用 child
        ctx = FlowContext()
        _context_local.flow_context = ctx
        try:
            result = parent(5)
            # child 返回 TaskRef
            assert isinstance(result, TaskRef)
            assert ctx.ref_count > 0
        finally:
            _context_local.flow_context = None

    def test_subflow_no_tasks_raises(self, reset_flow_context_local):
        """子 flow 不产生任何 task → ValueError。"""
        from actant.flow import _context_local

        @flow
        def empty_child():
            return 42  # 不调用任何 task

        @flow
        def parent():
            return empty_child()

        ctx = FlowContext()
        _context_local.flow_context = ctx
        with pytest.raises(ValueError, match="produced no tasks"):
            parent()

    def test_subflow_returns_multiple_refs(self, reset_flow_context_local):
        """子 flow 返回多个 TaskRef → 返回列表。"""
        from actant.flow import _context_local

        @actant.task
        def task_a():
            return 1

        @actant.task
        def task_b():
            return 2

        @flow
        def child():
            a = task_a()
            b = task_b()
            return [a, b]

        @flow
        def parent():
            return child()

        ctx = FlowContext()
        _context_local.flow_context = ctx
        try:
            result = parent()
            assert isinstance(result, list)
            assert len(result) == 2
        finally:
            _context_local.flow_context = None

    def test_subflow_no_return_uses_sink_detection(self, reset_flow_context_local):
        """子 flow 无显式返回 TaskRef → 回退到 sink 检测。"""
        from actant.flow import _context_local

        @actant.task
        def task_a():
            return 1

        @flow
        def child():
            task_a()  # 不返回

        @flow
        def parent():
            return child()

        ctx = FlowContext()
        _context_local.flow_context = ctx
        try:
            result = parent()
            # 单个 sink → 返回单个 TaskRef
            assert isinstance(result, TaskRef)
        finally:
            _context_local.flow_context = None

    def test_subflow_multiple_sinks_returns_list(self, reset_flow_context_local):
        """子 flow 无显式返回且有多个 sink → 返回列表（425 行）。"""
        from actant.flow import _context_local

        @actant.task
        def task_a():
            return 1

        @actant.task
        def task_b():
            return 2

        @flow
        def child():
            task_a()  # 不返回
            task_b()  # 不返回 —— 两个独立 sink

        @flow
        def parent():
            return child()

        ctx = FlowContext()
        _context_local.flow_context = ctx
        try:
            result = parent()
            # 多个 sink → 返回列表
            assert isinstance(result, list)
            assert len(result) == 2
        finally:
            _context_local.flow_context = None


# ---------------------------------------------------------------------------
# Flow._build_dag — 循环检测
# ---------------------------------------------------------------------------


class TestFlowBuildDagCycleDetection:
    """_build_dag 检测循环并抛出可读错误。"""

    def test_build_dag_no_cycle(self, reset_flow_context_local):
        @actant.task
        def step1(x):
            return x

        @actant.task
        def step2(x):
            return x

        @flow
        def my_flow(x):
            r1 = step1(x)
            return step2(r1)

        nodes, edges, _conds = my_flow._build_dag(5)
        assert len(nodes) == 2
        assert len(edges) == 1

    def test_build_dag_no_tasks_raises(self, reset_flow_context_local):
        @flow
        def empty_flow(x):
            return x

        with pytest.raises(ValueError, match="no tasks"):
            empty_flow._build_dag(5)

    def test_build_dag_with_cycle_raises(self, reset_flow_context_local):
        """构造循环并验证 _build_dag 抛出可读错误（462-464 行）。"""
        # 通过 mock FlowContext 注入循环边
        from unittest.mock import patch

        @actant.task
        def task_a(x):
            return x

        @actant.task
        def task_b(x):
            return x

        @flow
        def my_flow(x):
            a = task_a(x)
            return task_b(a)

        # mock detect_cycle 返回循环路径
        with (
            patch("actant._dag.detect_cycle", return_value=[0, 1, 0]),
            patch("actant._dag.format_cycle_path", return_value="a -> b -> a"),
            pytest.raises(ValueError, match="contains a cycle"),
        ):
            my_flow._build_dag(5)


# ---------------------------------------------------------------------------
# Flow.run — 本地执行模式
# ---------------------------------------------------------------------------


class TestFlowRun:
    def test_run_executes_locally(self, reset_flow_context_local):
        @actant.task
        def add(a, b):
            return a + b

        @flow
        def my_flow(x):
            return add(x, 1)

        # run 直接执行，不构建 DAG
        result = my_flow.run(5)
        assert result == 6


# ---------------------------------------------------------------------------
# parallel
# ---------------------------------------------------------------------------


class TestParallel:
    def test_parallel_empty_returns_empty_list(self):
        assert parallel() == []

    def test_parallel_outside_context_returns_values(self, reset_flow_context_local):
        # flow 上下文外，parallel 直接返回值列表
        result = parallel(1, 2, 3)
        assert result == [1, 2, 3]

    def test_parallel_inside_context_tracks_refs(self, reset_flow_context_local):
        from actant.flow import _context_local

        @actant.task
        def task_a():
            return 1

        @actant.task
        def task_b():
            return 2

        ctx = FlowContext()
        _context_local.flow_context = ctx
        try:
            a = task_a()
            b = task_b()
            result = parallel(a, b)
            assert len(result) == 2
            assert all(isinstance(r, TaskRef) for r in result)
        finally:
            _context_local.flow_context = None

    def test_parallel_non_taskref_raises(self, reset_flow_context_local):
        from actant.flow import _context_local

        ctx = FlowContext()
        _context_local.flow_context = ctx
        try:
            with pytest.raises(TypeError, match="must be TaskRef"):
                parallel(1, 2)  # 非 TaskRef
        finally:
            _context_local.flow_context = None


# ---------------------------------------------------------------------------
# BranchRef
# ---------------------------------------------------------------------------


class TestBranchRef:
    def test_construction_and_properties(self):
        if_ref = TaskRef(task_name="if", args=())
        else_ref = TaskRef(task_name="else", args=())
        br = BranchRef(if_ref, else_ref)
        assert br.if_ref is if_ref
        assert br.else_ref is else_ref

    def test_repr(self):
        if_ref = TaskRef(task_name="if_task", args=())
        else_ref = TaskRef(task_name="else_task", args=())
        br = BranchRef(if_ref, else_ref)
        assert "if_task" in repr(br)
        assert "else_task" in repr(br)


# ---------------------------------------------------------------------------
# branch
# ---------------------------------------------------------------------------


class TestBranch:
    def test_branch_outside_context_returns_branchref(self, reset_flow_context_local):
        @actant.task
        def cond(x):
            return x

        @actant.task
        def if_task(x):
            return x

        @actant.task
        def else_task(x):
            return x

        c = cond(5)
        br = branch(c, lambda r: r > 0, if_task(c), else_task(c))
        assert isinstance(br, BranchRef)

    def test_branch_inside_context_tracks(self, reset_flow_context_local):
        from actant.flow import _context_local

        @actant.task
        def cond(x):
            return x

        @actant.task
        def if_task(x):
            return x

        @actant.task
        def else_task(x):
            return x

        ctx = FlowContext()
        _context_local.flow_context = ctx
        try:
            c = cond(5)
            if_r = if_task(c)
            else_r = else_task(c)
            br = branch(c, lambda r: r > 0, if_r, else_r)
            assert isinstance(br, BranchRef)
            assert ctx.ref_count > 0
        finally:
            _context_local.flow_context = None


# ---------------------------------------------------------------------------
# switch
# ---------------------------------------------------------------------------


class TestSwitch:
    def test_switch_outside_context_evaluates_conditions(self, reset_flow_context_local):
        @actant.task
        def classify(x):
            return x

        @actant.task
        def fast(x):
            return x

        @actant.task
        def slow(x):
            return x

        c = classify("fast")
        result = switch(
            c,
            ("fast", fast(c), lambda r: r == "fast"),
            ("slow", slow(c), lambda r: r == "slow"),
        )
        # 本地模式：只返回条件为 True 的
        assert len(result) == 1

    def test_switch_inside_context_returns_all_refs(self, reset_flow_context_local):
        from actant.flow import _context_local

        @actant.task
        def classify(x):
            return x

        @actant.task
        def fast(x):
            return x

        @actant.task
        def slow(x):
            return x

        ctx = FlowContext()
        _context_local.flow_context = ctx
        try:
            c = classify("fast")
            f = fast(c)
            s = slow(c)
            result = switch(
                c,
                ("fast", f, lambda r: r == "fast"),
                ("slow", s, lambda r: r == "slow"),
            )
            # flow 上下文内：返回所有 case 的 TaskRef
            assert len(result) == 2
        finally:
            _context_local.flow_context = None


# ---------------------------------------------------------------------------
# FlowContext.track_switch — 错误路径
# ---------------------------------------------------------------------------


class TestFlowContextTrackSwitchError:
    def test_track_switch_untracked_condition_raises(self, reset_flow_context_local):
        """track_switch 时 condition_ref 未追踪 → ValueError。"""
        ctx = FlowContext()
        cond_ref = TaskRef(task_name="cond", args=())
        case_ref = TaskRef(task_name="case", args=())
        with pytest.raises(ValueError, match="must be tracked before"):
            ctx.track_switch(
                cond_ref,
                [("label", case_ref, lambda r: True)],
            )


# ---------------------------------------------------------------------------
# FlowContext.track — 已追踪 ref 不重复添加
# ---------------------------------------------------------------------------


class TestFlowContextTrackIdempotent:
    def test_track_same_ref_twice_returns_same_index(self, reset_flow_context_local):
        """同一 TaskRef 重复 track 返回相同索引。"""
        ctx = FlowContext()
        ref = TaskRef(task_name="t", args=())
        idx1 = ctx._ensure_tracked(ref)
        idx2 = ctx._ensure_tracked(ref)
        assert idx1 == idx2

    def test_track_branch_with_branchref_args(self, reset_flow_context_local):
        """track 时 args 包含 BranchRef → 建立对两个分支的依赖边。"""
        ctx = FlowContext()
        if_ref = TaskRef(task_name="if", args=())
        else_ref = TaskRef(task_name="else", args=())
        ctx.track(if_ref)
        ctx.track(else_ref)

        # 下游 task 依赖 BranchRef
        br = BranchRef(if_ref, else_ref)
        downstream = TaskRef(task_name="down", args=(br,))
        ctx.track(downstream)
        # 应有边 if -> down 和 else -> down
        assert ctx.ref_count == 3

    def test_track_with_chord_args(self, reset_flow_context_local):
        """track 时 args 是 TaskRef 列表（chord 模式）。"""
        ctx = FlowContext()
        r1 = TaskRef(task_name="t1", args=())
        r2 = TaskRef(task_name="t2", args=())
        ctx.track(r1)
        ctx.track(r2)

        # 下游 task 依赖 [r1, r2]
        downstream = TaskRef(task_name="down", args=([r1, r2],))
        ctx.track(downstream)
        assert ctx.ref_count == 3

    def test_track_with_kwargs_taskref(self, reset_flow_context_local):
        """track 时 kwargs 包含 TaskRef。"""
        ctx = FlowContext()
        r1 = TaskRef(task_name="t1", args=())
        ctx.track(r1)

        downstream = TaskRef(task_name="down", args=(), kwargs={"dep": r1})
        ctx.track(downstream)
        assert ctx.ref_count == 2

    def test_track_with_kwargs_branchref(self, reset_flow_context_local):
        """track 时 kwargs 包含 BranchRef。"""
        ctx = FlowContext()
        if_ref = TaskRef(task_name="if", args=())
        else_ref = TaskRef(task_name="else", args=())
        ctx.track(if_ref)
        ctx.track(else_ref)

        br = BranchRef(if_ref, else_ref)
        downstream = TaskRef(task_name="down", args=(), kwargs={"br": br})
        ctx.track(downstream)
        assert ctx.ref_count == 3

    def test_track_with_kwargs_chord(self, reset_flow_context_local):
        """track 时 kwargs 包含 TaskRef 列表。"""
        ctx = FlowContext()
        r1 = TaskRef(task_name="t1", args=())
        r2 = TaskRef(task_name="t2", args=())
        ctx.track(r1)
        ctx.track(r2)

        downstream = TaskRef(
            task_name="down", args=(), kwargs={"deps": [r1, r2]}
        )
        ctx.track(downstream)
        assert ctx.ref_count == 3


# ---------------------------------------------------------------------------
# FlowContext — 边界场景
# ---------------------------------------------------------------------------


class TestFlowContextEdgeCases:
    """覆盖 track/merge_into/find_sinks 的边界分支。"""

    def test_track_with_untracked_dependency(self, reset_flow_context_local):
        """track 时依赖未追踪 → dep_idx is None 分支（153->151）。"""
        ctx = FlowContext()
        # r1 未追踪，直接构造依赖 r1 的 downstream
        r1 = TaskRef(task_name="t1", args=())
        downstream = TaskRef(task_name="down", args=(r1,))
        # track downstream 时 r1 未在 _ref_index 中
        ctx.track(downstream)
        # 不应抛异常，但也不应建立 r1 -> down 边
        assert ctx.ref_count == 1  # 只有 downstream

    def test_merge_into_with_superseded_edges(self, reset_flow_context_local):
        """merge_into 时合并 superseded_unconditional（310, 313 行）。"""
        parent = FlowContext()
        child = FlowContext()

        r1 = TaskRef(task_name="t1", args=())
        r2 = TaskRef(task_name="t2", args=())
        child.track(r1)
        child.track(r2)
        # 手动添加 superseded 边
        child._edges.append((0, 1, None))
        child._superseded_unconditional.add((0, 1))

        offset = child.merge_into(parent)
        assert offset == 0
        # superseded 边应合并到 parent
        assert (0, 1) in parent._superseded_unconditional

    def test_find_sinks_skips_superseded_only(self, reset_flow_context_local):
        """find_sinks 跳过被取代的无条件边（322 行 False 分支）。"""
        ctx = FlowContext()
        r1 = TaskRef(task_name="t1", args=())
        r2 = TaskRef(task_name="t2", args=())
        r3 = TaskRef(task_name="t3", args=())
        ctx.track(r1)
        ctx.track(r2)
        ctx.track(r3)
        # 添加被取代的无条件边（0->1）
        ctx._edges.append((0, 1, None))
        ctx._superseded_unconditional.add((0, 1))
        # 添加普通无条件边（1->2，不被取代）→ 322 行
        ctx._edges.append((1, 2, None))
        sinks = ctx.find_sinks()
        # 0 的边被取代 → 0 是 sink
        # 1 有普通出边 → 1 不是 sink
        # 2 无出边 → 2 是 sink
        assert 0 in sinks
        assert 1 not in sinks
        assert 2 in sinks

    def test_iter_ref_deps_kwargs_branchref(self, reset_flow_context_local):
        """_iter_ref_deps 处理 kwargs 中的 BranchRef（246->240 分支）。"""
        ctx = FlowContext()
        if_ref = TaskRef(task_name="if", args=())
        else_ref = TaskRef(task_name="else", args=())
        ctx.track(if_ref)
        ctx.track(else_ref)

        br = BranchRef(if_ref, else_ref)
        downstream = TaskRef(task_name="down", args=(), kwargs={"br": br})
        ctx.track(downstream)
        # 应建立 if -> down 和 else -> down 边
        assert ctx.ref_count == 3

    def test_iter_ref_deps_kwargs_non_taskref_value(self, reset_flow_context_local):
        """_iter_ref_deps 处理 kwargs 中的普通值（246->240 False 分支）。"""
        ctx = FlowContext()
        r1 = TaskRef(task_name="t1", args=())
        ctx.track(r1)

        # kwargs 包含普通字符串值（非 TaskRef/BranchRef/collection）
        downstream = TaskRef(
            task_name="down", args=(), kwargs={"label": "plain-string"}
        )
        ctx.track(downstream)
        assert ctx.ref_count == 2
