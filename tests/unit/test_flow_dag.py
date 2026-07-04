"""Flow DAG 构建单元测试：拓扑、依赖、分支、chord、payload 构建。

覆盖 FlowContext 的 track/build_dag/track_branch/track_switch，
以及 _build_ref_payload 的所有路径（叶子/positional/chord）。

历史上此区域的漏测导致：
- 位置错乱 bug 未被发现（_build_ref_payload 没被测试）
- chord 模式 callback 缺参 bug 未被发现
- BranchRef 序列化错误未被发现
"""

from __future__ import annotations

import pytest

from actant._serialization import (
    TAG_GENERIC,
    TAG_GROUP,
    TAG_POSITIONAL,
    TAG_SINGLE,
    TAG_SINGLE_KW,
    _dispatch_task,
    loads,
)
from actant.flow import (
    BranchRef,
    FlowContext,
    _build_ref_payload,
    _is_taskref_collection,
)
from actant.task import Task, TaskRef

# ---------------------------------------------------------------------------
# detect_cycle / _find_cycle_path / format_cycle_path
# ---------------------------------------------------------------------------


class TestDetectCycle:
    """detect_cycle 使用 Kahn 算法检测 DAG 循环。

    覆盖率目标：所有分支（空图/无环/有环/越界索引/部分节点成环）。
    """

    def test_empty_graph_returns_none(self):
        """node_count == 0 时直接返回 None（36 行）。"""
        from actant._dag import detect_cycle

        assert detect_cycle(0, []) is None

    def test_no_edges_no_cycle(self):
        from actant._dag import detect_cycle

        assert detect_cycle(3, []) is None

    def test_simple_dag_no_cycle(self):
        from actant._dag import detect_cycle

        # 0 → 1 → 2
        edges = [(0, 1, None), (1, 2, None)]
        assert detect_cycle(3, edges) is None

    def test_diamond_no_cycle(self):
        from actant._dag import detect_cycle

        # 0 → 1, 0 → 2, 1 → 3, 2 → 3
        edges = [(0, 1, None), (0, 2, None), (1, 3, None), (2, 3, None)]
        assert detect_cycle(4, edges) is None

    def test_self_loop_detected(self):
        from actant._dag import detect_cycle

        # 0 → 0
        edges = [(0, 0, None)]
        result = detect_cycle(1, edges)
        assert result is not None
        assert result[0] == 0  # 首节点为 0
        assert result[-1] == 0  # 尾节点也为 0（首尾相同）

    def test_simple_two_node_cycle_detected(self):
        from actant._dag import detect_cycle

        # 0 → 1 → 0
        edges = [(0, 1, None), (1, 0, None)]
        result = detect_cycle(2, edges)
        assert result is not None
        # 环路径首尾相同
        assert result[0] == result[-1]
        assert set(result[:-1]) == {0, 1}

    def test_three_node_cycle_detected(self):
        from actant._dag import detect_cycle

        # 0 → 1 → 2 → 0
        edges = [(0, 1, None), (1, 2, None), (2, 0, None)]
        result = detect_cycle(3, edges)
        assert result is not None
        assert result[0] == result[-1]
        assert set(result[:-1]) == {0, 1, 2}

    def test_cycle_with_extra_non_cycle_nodes(self):
        """部分节点成环，其他节点不在环中。"""
        from actant._dag import detect_cycle

        # 0 → 1 → 2 → 1（环在 1-2），3 独立
        edges = [(0, 1, None), (1, 2, None), (2, 1, None)]
        result = detect_cycle(4, edges)
        assert result is not None
        # 环路径应包含 1 和 2
        assert 1 in result and 2 in result

    def test_condition_edges_participate_in_cycle_detection(self):
        """条件边也参与循环检测。"""
        from actant._dag import detect_cycle

        # 0 → 1 → 0，第二条边带条件标签
        edges = [(0, 1, None), (1, 0, "cond_a")]
        result = detect_cycle(2, edges)
        assert result is not None

    def test_out_of_range_indices_ignored(self):
        """越界索引被忽略，不引发异常（43->41 分支）。"""
        from actant._dag import detect_cycle

        # 越界索引：from_idx 和 to_idx 都超出范围
        edges = [(0, 1, None), (5, 6, None)]  # 5, 6 越界（node_count=2）
        # 不应抛异常，且无环（有效边只有 0→1）
        assert detect_cycle(2, edges) is None

    def test_visited_count_equals_node_count_returns_none(self):
        """visited_count == node_count 时返回 None（62-63 行）。

        覆盖所有节点都被访问到的无环情况。
        """
        from actant._dag import detect_cycle

        # 完整拓扑：0 → 1 → 2 → 3
        edges = [(0, 1, None), (1, 2, None), (2, 3, None)]
        assert detect_cycle(4, edges) is None

    def test_in_degree_never_zero_immediate_cycle(self):
        """所有节点入度都 > 0 的完全成环情况。"""
        from actant._dag import detect_cycle

        # 0 → 1, 1 → 0：两个节点入度都为 1
        edges = [(0, 1, None), (1, 0, None)]
        result = detect_cycle(2, edges)
        assert result is not None


class TestFindCyclePath:
    """_find_cycle_path 从环节点提取具体循环路径。"""

    def test_empty_cycle_nodes_returns_empty(self):
        from actant._dag import _find_cycle_path

        assert _find_cycle_path([], []) == []

    def test_extracts_cycle_path_from_adj(self):
        from actant._dag import _find_cycle_path

        # 邻接表：0→1, 1→2, 2→0
        adj = [[1], [2], [0]]
        cycle_nodes = [0, 1, 2]
        result = _find_cycle_path(adj, cycle_nodes)
        # 应返回首尾相同的路径
        assert result[0] == result[-1]
        assert set(result[:-1]) == {0, 1, 2}

    def test_falls_back_to_cycle_nodes_on_failure(self):
        """DFS 无法找到环时回退到 cycle_nodes 本身。

        构造一个 adj 中环节点的邻居都不在 cycle_set 中的场景。
        """
        from actant._dag import _find_cycle_path

        # adj[0] = []（无邻居），无法 DFS → 回退
        adj = [[], [2], [1]]
        cycle_nodes = [0, 1, 2]
        result = _find_cycle_path(adj, cycle_nodes)
        # DFS 从 0 出发无邻居，返回 None → 回退到 cycle_nodes
        assert result == cycle_nodes

    def test_neighbor_not_in_cycle_set_skipped(self):
        """邻居不在 cycle_set 中时跳过（95->94 分支）。

        构造 adj[start] 有邻居，但邻居不在 cycle_set 中。
        """
        from actant._dag import _find_cycle_path

        # adj[0] = [3]，但 3 不在 cycle_nodes=[0] 中 → 跳过
        adj = [[3], [], [], [0]]
        cycle_nodes = [0]
        result = _find_cycle_path(adj, cycle_nodes)
        # DFS 从 0 出发，邻居 3 不在 cycle_set → 跳过 → 回退到 cycle_nodes
        assert result == cycle_nodes

    def test_dfs_returns_none_continues_loop(self):
        """dfs 返回 None 时继续循环下一个邻居（97->94 分支）。

        构造一个场景：第一个邻居的 DFS 返回 None，但第二个邻居能找到环。
        """
        from actant._dag import _find_cycle_path

        # adj[0] = [1, 2]
        # 从 1 出发：adj[1]=[]，DFS 返回 None
        # 从 2 出发：adj[2]=[0]，0 已访问 → 找到环 [0, 2, 0]
        adj = [[1, 2], [], [0]]
        cycle_nodes = [0, 1, 2]
        result = _find_cycle_path(adj, cycle_nodes)
        # 应找到环（0 → 2 → 0），不是回退到 cycle_nodes
        assert result is not None
        assert result[0] == result[-1]  # 首尾相同


class TestFormatCyclePath:
    """format_cycle_path 将索引路径格式化为可读字符串。"""

    def test_empty_indices_returns_empty(self):
        from actant._dag import format_cycle_path

        assert format_cycle_path(["a", "b", "c"], []) == ""

    def test_formats_simple_cycle(self):
        from actant._dag import format_cycle_path

        names = ["a", "b", "c"]
        # 环 0 → 1 → 0
        result = format_cycle_path(names, [0, 1, 0])
        assert result == "a -> b -> a"

    def test_formats_three_node_cycle(self):
        from actant._dag import format_cycle_path

        names = ["prepare", "train", "evaluate"]
        result = format_cycle_path(names, [0, 1, 2, 0])
        assert result == "prepare -> train -> evaluate -> prepare"

    def test_out_of_range_index_uses_placeholder(self):
        """索引越界时回退到 node{idx} 占位符（118-127 行）。"""
        from actant._dag import format_cycle_path

        # 只有 2 个节点名，但索引包含 5
        names = ["a", "b"]
        result = format_cycle_path(names, [0, 5, 0])
        assert "node5" in result
        assert result == "a -> node5 -> a"

    def test_negative_index_uses_placeholder(self):
        """负索引也走占位符分支。"""
        from actant._dag import format_cycle_path

        names = ["a", "b"]
        result = format_cycle_path(names, [-1, 0])
        assert "node-1" in result


# ---------------------------------------------------------------------------
# _is_taskref_collection
# ---------------------------------------------------------------------------


class TestIsTaskrefCollection:
    """检测 list/tuple of TaskRef（chord 模式标识）。"""

    def test_empty_list_is_not_collection(self):
        """空 list 不视为 chord（无依赖）。"""
        assert _is_taskref_collection([]) is False

    def test_empty_tuple_is_not_collection(self):
        assert _is_taskref_collection(()) is False

    def test_list_of_taskrefs(self):
        r1, r2 = TaskRef("a"), TaskRef("b")
        assert _is_taskref_collection([r1, r2]) is True

    def test_tuple_of_taskrefs(self):
        r1, r2 = TaskRef("a"), TaskRef("b")
        assert _is_taskref_collection((r1, r2)) is True

    def test_mixed_list_is_not_collection(self):
        """list 中含非 TaskRef 元素不是 chord。"""
        r1 = TaskRef("a")
        assert _is_taskref_collection([r1, 42]) is False

    def test_non_list_is_not_collection(self):
        assert _is_taskref_collection(42) is False
        assert _is_taskref_collection("string") is False
        assert _is_taskref_collection(TaskRef("a")) is False

    def test_nested_list_not_supported(self):
        """嵌套 list 不被识别（仅顶层 list/tuple of TaskRef）。"""
        r1 = TaskRef("a")
        assert _is_taskref_collection([[r1]]) is False


# ---------------------------------------------------------------------------
# _build_ref_payload：所有路径覆盖
# ---------------------------------------------------------------------------


class TestBuildRefPayload:
    """_build_ref_payload 决定每个 TaskRef 的 default_payload 格式。

    三条路径：
    - chord（list/tuple of TaskRef）→ TAG_GROUP
    - 有 TaskRef/BranchRef 依赖 → TAG_POSITIONAL
    - 叶子任务 → TAG_GENERIC（inline）或 build_payload（named）
    """

    def test_leaf_task_with_inline_func(self):
        """叶子任务 + inline_func → TAG_GENERIC。"""
        fn = lambda x: x + 1  # noqa: E731
        ref = TaskRef("add", args=(41,), inline_func=fn)
        payload = _build_ref_payload(ref)
        assert payload[0] == TAG_GENERIC

    def test_leaf_task_no_inline(self):
        """叶子任务 + 无 inline → build_payload 走 named 路径。"""
        ref = TaskRef("named_task", args=(1, 2))
        payload = _build_ref_payload(ref)
        # build_payload 走 TAG_SINGLE 或 TAG_SINGLE_KW
        assert payload[0] in (TAG_SINGLE, TAG_SINGLE_KW)

    def test_leaf_task_single_arg(self):
        """叶子任务单参数 → TAG_SINGLE。"""
        fn = lambda x: x  # noqa: E731
        ref = TaskRef("f", args=(42,), inline_func=fn)
        payload = _build_ref_payload(ref)
        assert payload[0] == TAG_GENERIC
        # 内部 (fn, args, kwargs)
        _restored_fn, args, _kwargs = loads(payload[1:])
        assert args == (42,)

    def test_dep_at_position_0(self):
        """回归测试：ref 在位置 0，concrete 在位置 1。

        历史 bug：旧设计纯 chain（args 全是 TaskRef）走 generic 路径丢失位置。
        """
        fn = lambda a, b: (a, b)  # noqa: E731
        dep = TaskRef("dep")
        ref = TaskRef("combine", args=(dep, "concrete_b"), inline_func=fn)
        payload = _build_ref_payload(ref)
        assert payload[0] == TAG_POSITIONAL

        _restored_fn, positions, kwargs_keys, concrete_args, _concrete_kwargs = loads(payload[1:])
        assert positions == [0]
        assert kwargs_keys == []
        assert concrete_args == ("concrete_b",)

    def test_dep_at_position_1(self):
        """回归测试：ref 在位置 1，concrete 在位置 0。

        历史 bug：combine(1, ref) 被错位为 combine(ref, 1)。
        """
        fn = lambda a, b: (a, b)  # noqa: E731
        dep = TaskRef("dep")
        ref = TaskRef("combine", args=("concrete_a", dep), inline_func=fn)
        payload = _build_ref_payload(ref)
        assert payload[0] == TAG_POSITIONAL

        _restored_fn, positions, kwargs_keys, concrete_args, _concrete_kwargs = loads(payload[1:])
        assert positions == [1]
        assert kwargs_keys == []
        assert concrete_args == ("concrete_a",)

    def test_multiple_deps_preserve_positions(self):
        """多个 ref 必须保留所有位置。"""
        fn = lambda a, b, c, d: (a, b, c, d)  # noqa: E731
        r1, r2 = TaskRef("r1"), TaskRef("r2")
        # ref 在 0 和 2，concrete 在 1 和 3
        ref = TaskRef("f", args=(r1, "c1", r2, "c2"), inline_func=fn)
        payload = _build_ref_payload(ref)
        assert payload[0] == TAG_POSITIONAL

        _, positions, kwargs_keys, concrete_args, _ = loads(payload[1:])
        assert positions == [0, 2]
        assert kwargs_keys == []
        assert concrete_args == ("c1", "c2")

    def test_all_deps_chain(self):
        """回归测试：纯 chain B(A_result) — args 全是 TaskRef。

        历史 bug：旧设计此场景走 generic 路径丢失位置。
        """
        fn = lambda a, b, c: (a, b, c)  # noqa: E731
        r1, r2, r3 = TaskRef("r1"), TaskRef("r2"), TaskRef("r3")
        ref = TaskRef("chain", args=(r1, r2, r3), inline_func=fn)
        payload = _build_ref_payload(ref)
        assert payload[0] == TAG_POSITIONAL

        _, positions, kwargs_keys, concrete_args, _ = loads(payload[1:])
        assert positions == [0, 1, 2]
        assert kwargs_keys == []
        assert concrete_args == ()

    def test_chord_mode(self):
        """chord 模式：args 含 list of TaskRef → TAG_GROUP。"""
        r1, r2, r3 = TaskRef("r1"), TaskRef("r2"), TaskRef("r3")
        fn = lambda results: sum(results)  # noqa: E731
        ref = TaskRef("sum_all", args=([r1, r2, r3],), inline_func=fn)
        payload = _build_ref_payload(ref)
        assert payload[0] == TAG_GROUP

    def test_chord_with_extra_concrete_args(self):
        """chord + 混合 concrete args：chord 优先（一个 list of TaskRef 即触发）。"""
        r1, r2 = TaskRef("r1"), TaskRef("r2")
        fn = lambda results, scale: sum(results) * scale  # noqa: E731
        ref = TaskRef("scale_sum", args=([r1, r2], 10), inline_func=fn)
        payload = _build_ref_payload(ref)
        # chord 优先，但 extra concrete args 的情况按当前设计仍走 group
        # 注意：当前实现 chord_positions 仅检测，但 pack_group([]) 不保留 extra args
        # 这是已知限制 — chord callback 应仅接收一个 list 参数
        assert payload[0] == TAG_GROUP

    def test_branchref_in_args(self):
        """回归测试：BranchRef 在 args 中走 positional 路径。

        历史 bug：BranchRef 未被过滤，被 cloudpickle 序列化为无意义引用。
        """
        if_ref, else_ref = TaskRef("if_branch"), TaskRef("else_branch")
        branch = BranchRef(if_ref, else_ref)
        fn = lambda x: x  # noqa: E731
        ref = TaskRef("consumer", args=(branch,), inline_func=fn)
        payload = _build_ref_payload(ref)
        assert payload[0] == TAG_POSITIONAL

    def test_kwargs_with_taskref(self):
        """kwargs 中的 TaskRef 也应走 positional 路径。"""
        fn = lambda a, b: (a, b)  # noqa: E731
        dep = TaskRef("dep")
        ref = TaskRef("f", args=("concrete_a",), kwargs={"b": dep}, inline_func=fn)
        payload = _build_ref_payload(ref)
        assert payload[0] == TAG_POSITIONAL

        _, positions, kwargs_keys, concrete_args, concrete_kwargs = loads(payload[1:])
        # args 中无 dep，kwargs 中有 dep
        assert positions == []  # args 中无 dep
        assert kwargs_keys == ["b"]  # kwargs 中有 dep
        assert concrete_args == ("concrete_a",)
        assert concrete_kwargs == {}  # kwargs 中的 dep 被过滤

    def test_none_kwargs_leaf(self):
        """回归测试：kwargs=None 不应破坏 _build_ref_payload。

        历史 bug：pack_generic(fn, args, None) 在 dispatcher 中 **None 崩溃。
        """
        fn = lambda x: x  # noqa: E731
        ref = TaskRef("f", args=(42,), kwargs=None, inline_func=fn)
        payload = _build_ref_payload(ref)
        assert payload[0] == TAG_GENERIC
        # 验证 dispatcher 能处理
        result = _dispatch_task(None, payload)
        assert loads(result) == 42


# ---------------------------------------------------------------------------
# FlowContext.track：依赖检测与边构建
# ---------------------------------------------------------------------------


class TestFlowContextTrack:
    """FlowContext.track 自动检测 TaskRef 参数中的依赖。"""

    def test_track_single_ref(self, fresh_flow_context: FlowContext):
        ctx = fresh_flow_context
        r = TaskRef("a")
        ctx.track(r)
        assert ctx.ref_count == 1

    def test_track_duplicate_ref_ignored(self, fresh_flow_context: FlowContext):
        """重复 track 同一 TaskRef 被忽略（基于 id）。"""
        ctx = fresh_flow_context
        r = TaskRef("a")
        ctx.track(r)
        ctx.track(r)  # 同一实例
        assert ctx.ref_count == 1

    def test_track_creates_edge_for_dep(self, fresh_flow_context: FlowContext):
        """args 中的 TaskRef 自动建立 DAG 边。"""
        ctx = fresh_flow_context
        ra = TaskRef("a")
        rb = TaskRef("b", args=(ra,))
        ctx.track(ra)
        ctx.track(rb)
        nodes, edges = ctx.build_dag()
        assert len(nodes) == 2
        assert (0, 1, None) in edges

    def test_track_no_edge_for_independent_refs(self, fresh_flow_context: FlowContext):
        ctx = fresh_flow_context
        ra = TaskRef("a")
        rb = TaskRef("b")
        ctx.track(ra)
        ctx.track(rb)
        _, edges = ctx.build_dag()
        assert edges == []

    def test_track_chord_creates_multiple_edges(self, fresh_flow_context: FlowContext):
        """chord 模式：args 含 list of TaskRef，建立多条边。"""
        ctx = fresh_flow_context
        r1, r2, r3 = TaskRef("r1"), TaskRef("r2"), TaskRef("r3")
        callback = TaskRef("sum_all", args=([r1, r2, r3],))
        ctx.track(r1)
        ctx.track(r2)
        ctx.track(r3)
        ctx.track(callback)
        _, edges = ctx.build_dag()
        # r1→callback, r2→callback, r3→callback
        edge_set = {(f, t, c) for f, t, c in edges}
        assert (0, 3, None) in edge_set
        assert (1, 3, None) in edge_set
        assert (2, 3, None) in edge_set

    def test_kwargs_dep_creates_edge(self, fresh_flow_context: FlowContext):
        """kwargs 中的 TaskRef 也建立边。"""
        ctx = fresh_flow_context
        ra = TaskRef("a")
        rb = TaskRef("b", kwargs={"x": ra})
        ctx.track(ra)
        ctx.track(rb)
        _, edges = ctx.build_dag()
        assert (0, 1, None) in edges


# ---------------------------------------------------------------------------
# FlowContext.build_dag：节点与边格式
# ---------------------------------------------------------------------------


class TestFlowContextBuildDag:
    """build_dag 生成 Rust 运行时所需的 _DagNode 列表和边列表。"""

    def test_linear_chain(self, fresh_flow_context: FlowContext):
        """A → B → C 线性链。"""
        ctx = fresh_flow_context
        ra = TaskRef("a")
        rb = TaskRef("b", args=(ra,))
        rc = TaskRef("c", args=(rb,))
        ctx.track(ra)
        ctx.track(rb)
        ctx.track(rc)
        nodes, edges = ctx.build_dag()
        names = [n.name for n in nodes]
        assert names == ["a", "b", "c"]
        assert set(edges) == {(0, 1, None), (1, 2, None)}

    def test_diamond(self, fresh_flow_context: FlowContext):
        """菱形：A → [B, C] → D。"""
        ctx = fresh_flow_context
        ra = TaskRef("a")
        rb = TaskRef("b", args=(ra,))
        rc = TaskRef("c", args=(ra,))
        rd = TaskRef("d", args=(rb, rc))
        ctx.track(ra)
        ctx.track(rb)
        ctx.track(rc)
        ctx.track(rd)
        nodes, edges = ctx.build_dag()
        names = [n.name for n in nodes]
        assert names == ["a", "b", "c", "d"]
        assert set(edges) == {(0, 1, None), (0, 2, None), (1, 3, None), (2, 3, None)}

    def test_fan_out(self, fresh_flow_context: FlowContext):
        """扇出：A → [B, C, D]。"""
        ctx = fresh_flow_context
        ra = TaskRef("a")
        rb = TaskRef("b", args=(ra,))
        rc = TaskRef("c", args=(ra,))
        rd = TaskRef("d", args=(ra,))
        ctx.track(ra)
        ctx.track(rb)
        ctx.track(rc)
        ctx.track(rd)
        _, edges = ctx.build_dag()
        edge_set = set(edges)
        assert (0, 1, None) in edge_set
        assert (0, 2, None) in edge_set
        assert (0, 3, None) in edge_set

    def test_node_payload_built(self, fresh_flow_context: FlowContext):
        """build_dag 必须为每个节点构建 payload。"""
        ctx = fresh_flow_context
        fn = lambda x: x  # noqa: E731
        ra = TaskRef("a", args=(42,), inline_func=fn)
        ctx.track(ra)
        nodes, _ = ctx.build_dag()
        assert len(nodes) == 1
        assert nodes[0].name == "a"
        assert isinstance(nodes[0].payload, bytes)
        assert len(nodes[0].payload) > 0

    def test_node_priority_encoded(self, fresh_flow_context: FlowContext):
        """priority 必须编码为 i32 数值。"""
        ctx = fresh_flow_context
        ra = TaskRef("a", priority=5)
        ctx.track(ra)
        nodes, _ = ctx.build_dag()
        assert nodes[0].priority == 5

    def test_node_timeout_in_ms(self, fresh_flow_context: FlowContext):
        """timeout 秒 → timeout_ms 毫秒。"""
        ctx = fresh_flow_context
        ra = TaskRef("a", timeout=2.5)
        ctx.track(ra)
        nodes, _ = ctx.build_dag()
        assert nodes[0].timeout_ms == 2500

    def test_node_timeout_none(self, fresh_flow_context: FlowContext):
        ctx = fresh_flow_context
        ra = TaskRef("a")  # 无 timeout
        ctx.track(ra)
        nodes, _ = ctx.build_dag()
        assert nodes[0].timeout_ms is None


# ---------------------------------------------------------------------------
# 条件分支：track_branch
# ---------------------------------------------------------------------------


class TestFlowContextBranch:
    """track_branch 创建条件边，取代无条件边。"""

    def test_branch_creates_conditional_edges(self, fresh_flow_context: FlowContext):
        """branch 创建两条条件边（true/false tag）。"""
        ctx = fresh_flow_context
        cond = TaskRef("cond")
        if_ref = TaskRef("if_branch")
        else_ref = TaskRef("else_branch")
        ctx.track(cond)
        ctx.track(if_ref)
        ctx.track(else_ref)
        ctx.track_branch(cond, if_ref, else_ref, lambda x: x > 0)
        _, edges = ctx.build_dag()
        edge_set = set(edges)
        # 两条条件边
        cond_to_if = [e for e in edge_set if e[0] == 0 and e[1] == 1]
        cond_to_else = [e for e in edge_set if e[0] == 0 and e[1] == 2]
        assert len(cond_to_if) == 1
        assert len(cond_to_else) == 1
        assert cond_to_if[0][2] is not None  # condition_tag 非 None
        assert cond_to_else[0][2] is not None

    def test_branch_replaces_unconditional_edges(self, fresh_flow_context: FlowContext):
        """branch 的条件边取代自动 track 创建的无条件边。"""
        ctx = fresh_flow_context
        cond = TaskRef("cond")
        if_ref = TaskRef("if_branch")
        else_ref = TaskRef("else_branch")
        ctx.track(cond)
        ctx.track(if_ref)
        ctx.track(else_ref)
        ctx.track_branch(cond, if_ref, else_ref, lambda x: bool(x))
        _, edges = ctx.build_dag()
        # 不应有无条件边 cond→if 或 cond→else
        for f, t, c in edges:
            if f == 0 and t in (1, 2):
                assert c is not None, "branch 边必须带 condition_tag"

    def test_branch_evaluators_registered(self, fresh_flow_context: FlowContext):
        """branch 注册 true/false 评估器。"""
        ctx = fresh_flow_context
        cond = TaskRef("cond")
        if_ref = TaskRef("if_branch")
        else_ref = TaskRef("else_branch")
        ctx.track(cond)
        ctx.track(if_ref)
        ctx.track(else_ref)
        ctx.track_branch(cond, if_ref, else_ref, lambda x: x > 0)
        evaluators = ctx.condition_evaluators
        assert len(evaluators) == 2
        # true evaluator
        true_eval = next(v for k, v in evaluators.items() if "true" in k)
        assert true_eval(5) is True
        assert true_eval(-1) is False
        # false evaluator 是 true 的否定
        false_eval = next(v for k, v in evaluators.items() if "false" in k)
        assert false_eval(5) is False
        assert false_eval(-1) is True

    def test_branch_requires_tracked_condition(self, fresh_flow_context: FlowContext):
        """condition 必须先 track。"""
        ctx = fresh_flow_context
        cond = TaskRef("cond")
        if_ref = TaskRef("if")
        else_ref = TaskRef("else")
        # 不 track cond
        ctx.track(if_ref)
        ctx.track(else_ref)
        with pytest.raises(ValueError, match="must be tracked"):
            ctx.track_branch(cond, if_ref, else_ref, lambda x: bool(x))


# ---------------------------------------------------------------------------
# 条件分支：track_switch
# ---------------------------------------------------------------------------


class TestFlowContextSwitch:
    """track_switch 创建多路条件分支。"""

    def test_switch_creates_conditional_edges(self, fresh_flow_context: FlowContext):
        ctx = fresh_flow_context
        cond = TaskRef("cond")
        case_a = TaskRef("case_a")
        case_b = TaskRef("case_b")
        case_c = TaskRef("case_c")
        ctx.track(cond)
        ctx.track(case_a)
        ctx.track(case_b)
        ctx.track(case_c)
        ctx.track_switch(
            cond,
            [
                ("a", case_a, lambda x: x == 1),
                ("b", case_b, lambda x: x == 2),
                ("c", case_c, lambda x: x == 3),
            ],
        )
        _, edges = ctx.build_dag()
        # 3 条条件边
        cond_edges = [e for e in edges if e[0] == 0]
        assert len(cond_edges) == 3
        for _, _, tag in cond_edges:
            assert tag is not None
            assert tag.startswith("switch_")

    def test_switch_evaluators_registered(self, fresh_flow_context: FlowContext):
        ctx = fresh_flow_context
        cond = TaskRef("cond")
        case_a = TaskRef("a")
        case_b = TaskRef("b")
        ctx.track(cond)
        ctx.track(case_a)
        ctx.track(case_b)
        ctx.track_switch(
            cond,
            [
                ("a", case_a, lambda x: x == 1),
                ("b", case_b, lambda x: x == 2),
            ],
        )
        evaluators = ctx.condition_evaluators
        assert len(evaluators) == 2
        # 找到 "a" case 的 evaluator
        a_eval = next(v for k, v in evaluators.items() if k.endswith("_a"))
        assert a_eval(1) is True
        assert a_eval(2) is False


# ---------------------------------------------------------------------------
# reduce / map：chord 与并行映射
# ---------------------------------------------------------------------------


class TestTaskReduce:
    """Task.reduce 实现 chord 语义。"""

    def test_reduce_creates_callback_with_list_arg(
        self, fresh_flow_context: FlowContext, reset_flow_context_local
    ):
        """reduce 必须设置 callback.args = (list(refs),) 触发 chord 路径。"""
        from actant.flow import _context_local

        ctx = fresh_flow_context
        _context_local.flow_context = ctx

        task = Task("sum_all", func=lambda results: sum(results))
        r1 = TaskRef("r1")
        r2 = TaskRef("r2")
        r3 = TaskRef("r3")
        ctx.track(r1)
        ctx.track(r2)
        ctx.track(r3)

        callback = task.reduce([r1, r2, r3])
        assert isinstance(callback, TaskRef)
        # callback.args 必须是 (list_of_refs,)
        assert len(callback.args) == 1
        assert isinstance(callback.args[0], list)
        assert callback.args[0] == [r1, r2, r3]

        # build_dag 应建立 3 条边到 callback
        nodes, edges = ctx.build_dag()
        callback_idx = len(nodes) - 1
        edge_set = set(edges)
        assert (0, callback_idx, None) in edge_set
        assert (1, callback_idx, None) in edge_set
        assert (2, callback_idx, None) in edge_set

    def test_reduce_callback_payload_is_group(
        self, fresh_flow_context: FlowContext, reset_flow_context_local
    ):
        """reduce callback 的 payload 必须是 TAG_GROUP。"""
        from actant.flow import _context_local

        ctx = fresh_flow_context
        _context_local.flow_context = ctx

        task = Task("sum", func=lambda r: sum(r))
        r1, r2 = TaskRef("r1"), TaskRef("r2")
        ctx.track(r1)
        ctx.track(r2)
        task.reduce([r1, r2])

        nodes, _ = ctx.build_dag()
        callback_node = nodes[-1]
        assert callback_node.payload[0] == TAG_GROUP


class TestTaskMap:
    """Task.map 并行映射每个元素。"""

    def test_map_creates_one_ref_per_item(
        self, fresh_flow_context: FlowContext, reset_flow_context_local
    ):
        from actant.flow import _context_local

        ctx = fresh_flow_context
        _context_local.flow_context = ctx

        task = Task("process", func=lambda x: x * 2)
        refs = task.map([1, 2, 3, 4])
        assert len(refs) == 4
        assert all(isinstance(r, TaskRef) for r in refs)
        assert ctx.ref_count == 4

    def test_map_no_deps_between_items(
        self, fresh_flow_context: FlowContext, reset_flow_context_local
    ):
        """map 的各 item 之间无依赖。"""
        from actant.flow import _context_local

        ctx = fresh_flow_context
        _context_local.flow_context = ctx

        task = Task("f", func=lambda x: x)
        task.map([1, 2, 3])
        _, edges = ctx.build_dag()
        assert edges == []


# ---------------------------------------------------------------------------
# flow 装饰器与 subflow 嵌套
# ---------------------------------------------------------------------------


class TestFlowDecorator:
    """@flow 装饰器构建 DAG。"""

    def test_flow_builds_dag_from_task_calls(
        self, reset_flow_context_local, reset_global_task_registry
    ):
        from actant.flow import flow

        @flow
        def my_flow():
            t1 = Task("a", func=lambda: 1)
            t2 = Task("b", func=lambda x: x + 1)
            r1 = t1()
            r2 = t2(r1)
            return r2

        nodes, edges, _evaluators = my_flow._build_dag()
        names = [n.name for n in nodes]
        assert "a" in names
        assert "b" in names
        # a → b
        a_idx = names.index("a")
        b_idx = names.index("b")
        assert (a_idx, b_idx, None) in edges
