"""序列化系统单元测试：payload 编解码、tag 分发、upstream prefix。

这是测试体系的**第一道防线**：覆盖所有 payload 格式与分发路径，
确保序列化层的正确性。历史上此区域曾出现严重 bug：
- 位置错乱（combine(1, ref) 被错位为 combine(ref, 1)）
- chord 模式 callback 缺参
- kwargs=None 导致 **None 崩溃
- TAG_UPSTREAM_PREFIX 未被任何测试覆盖

每个 bug 都必须有对应的回归测试。
"""

from __future__ import annotations

import struct
from typing import Any

import pytest

from actant._serialization import (
    TAG_GENERIC,
    TAG_GROUP,
    TAG_POSITIONAL,
    TAG_SINGLE,
    TAG_SINGLE_KW,
    _dispatch_empty,
    _dispatch_generic,
    _dispatch_group,
    _dispatch_positional,
    _dispatch_single,
    _dispatch_single_kw,
    _dispatch_task,
    dumps,
    loads,
    pack_generic,
    pack_group,
    pack_positional,
    pack_single,
    pack_single_kw,
    unpack_upstream_prefix,
)

# ---------------------------------------------------------------------------
# 基础序列化 round-trip
# ---------------------------------------------------------------------------


class TestDumpsLoads:
    """cloudpickle round-trip 必须保持对象语义。"""

    @pytest.mark.parametrize("value", [42, "hello", 3.14, True, None, b"\x00\x01"])
    def test_scalar_roundtrip(self, value: Any):
        assert loads(dumps(value)) == value

    def test_list_roundtrip(self):
        data = [1, "two", 3.0, None, b"\x00"]
        assert loads(dumps(data)) == data

    def test_dict_roundtrip(self):
        data = {"a": 1, "b": [2, 3], "c": {"nested": True}}
        assert loads(dumps(data)) == data

    def test_tuple_roundtrip(self):
        data = (1, "two", 3.0)
        assert loads(dumps(data)) == data

    def test_nested_structure_roundtrip(self):
        data = {"list": [1, {"tuple": (2, 3)}], "set": {4, 5}}
        result = loads(dumps(data))
        assert result["list"] == [1, {"tuple": (2, 3)}]
        assert result["set"] == {4, 5}

    def test_lambda_roundtrip(self):
        """cloudpickle 必须能序列化 lambda（任务载荷核心能力）。"""
        fn = lambda x, y: x + y  # noqa: E731
        restored = loads(dumps(fn))
        assert restored(3, 4) == 7

    def test_closure_roundtrip(self):
        """cloudpickle 必须能序列化闭包（捕获变量）。"""
        base = 100
        fn = lambda x: x + base  # noqa: E731
        restored = loads(dumps(fn))
        assert restored(5) == 105


# ---------------------------------------------------------------------------
# pack_* 函数：payload 构建正确性
# ---------------------------------------------------------------------------


class TestPackSingle:
    """pack_single 生成 [TAG_SINGLE, cloudpickle(args_tuple)]。"""

    def test_tag_byte(self):
        payload = pack_single((42,))
        assert payload[0] == TAG_SINGLE

    def test_single_arg_roundtrip(self):
        payload = pack_single((42,))
        assert loads(payload[1:]) == (42,)

    def test_multiple_args_roundtrip(self):
        payload = pack_single((1, "two", 3.0))
        assert loads(payload[1:]) == (1, "two", 3.0)

    def test_empty_tuple(self):
        payload = pack_single(())
        assert loads(payload[1:]) == ()

    def test_nested_objects(self):
        payload = pack_single(({"key": [1, 2]}, (3, 4)))
        assert loads(payload[1:]) == ({"key": [1, 2]}, (3, 4))


class TestPackSingleKw:
    """pack_single_kw 生成 [TAG_SINGLE_KW, cloudpickle((args, kwargs))]。"""

    def test_tag_byte(self):
        payload = pack_single_kw((1,), {"b": 2})
        assert payload[0] == TAG_SINGLE_KW

    def test_args_kwargs_roundtrip(self):
        payload = pack_single_kw((1, 2), {"c": 3, "d": 4})
        args, kwargs = loads(payload[1:])
        assert args == (1, 2)
        assert kwargs == {"c": 3, "d": 4}

    def test_empty_args(self):
        payload = pack_single_kw((), {"x": 1})
        args, kwargs = loads(payload[1:])
        assert args == ()
        assert kwargs == {"x": 1}

    def test_empty_kwargs(self):
        payload = pack_single_kw((1, 2), {})
        args, kwargs = loads(payload[1:])
        assert args == (1, 2)
        assert kwargs == {}


class TestPackGroup:
    """pack_group 生成 [TAG_GROUP, count(u32 LE), len1(u32 LE), data1, ...]。"""

    def test_tag_byte(self):
        payload = pack_group([])
        assert payload[0] == TAG_GROUP

    def test_empty_group(self):
        payload = pack_group([])
        count = struct.unpack_from("<I", payload, 1)[0]
        assert count == 0
        assert len(payload) == 5  # tag + count

    def test_single_result(self):
        payload = pack_group([dumps(42)])
        count = struct.unpack_from("<I", payload, 1)[0]
        assert count == 1
        # 解包第一个结果
        offset = 5
        length = struct.unpack_from("<I", payload, offset)[0]
        offset += 4
        assert loads(payload[offset : offset + length]) == 42

    def test_multiple_results(self):
        results = [dumps(10), dumps("hello"), dumps([1, 2, 3])]
        payload = pack_group(results)
        count = struct.unpack_from("<I", payload, 1)[0]
        assert count == 3

        offset = 5
        unpacked = []
        for _ in range(count):
            length = struct.unpack_from("<I", payload, offset)[0]
            offset += 4
            unpacked.append(loads(payload[offset : offset + length]))
            offset += length
        assert unpacked == [10, "hello", [1, 2, 3]]

    def test_large_result(self):
        """大结果（>64KB）长度前缀必须正确编码。"""
        large_data = b"\x00" * 100_000
        payload = pack_group([large_data])
        count = struct.unpack_from("<I", payload, 1)[0]
        assert count == 1
        offset = 5
        length = struct.unpack_from("<I", payload, offset)[0]
        assert length == 100_000


class TestPackGeneric:
    """pack_generic 生成 [TAG_GENERIC, cloudpickle((fn, args, kwargs))]。"""

    def test_tag_byte(self):
        fn = lambda: None  # noqa: E731
        payload = pack_generic(fn, (), {})
        assert payload[0] == TAG_GENERIC

    def test_fn_args_kwargs_roundtrip(self):
        fn = lambda x, y, z=0: x + y + z  # noqa: E731
        payload = pack_generic(fn, (1, 2), {"z": 3})
        restored_fn, args, kwargs = loads(payload[1:])
        assert restored_fn(*args, **kwargs) == 6

    def test_none_kwargs(self):
        """回归测试：kwargs=None 不应破坏 pack_generic。

        历史 bug：_dispatch_generic 中 **None 导致 TypeError。
        """
        fn = lambda x: x * 2  # noqa: E731
        payload = pack_generic(fn, (5,), None)  # type: ignore[arg-type]
        restored_fn, args, kwargs = loads(payload[1:])
        # dispatcher 必须处理 None kwargs
        assert restored_fn(*args, **(kwargs or {})) == 10


class TestPackPositional:
    """pack_positional 生成 [TAG_POSITIONAL, cloudpickle((fn, positions, kwargs_keys, args, kwargs))]。"""

    def test_tag_byte(self):
        fn = lambda: None  # noqa: E731
        payload = pack_positional(fn, [], [], (), {})
        assert payload[0] == TAG_POSITIONAL

    def test_roundtrip(self):
        fn = lambda a, b, c: (a, b, c)  # noqa: E731
        # ref 在位置 0 和 2，concrete 在位置 1
        payload = pack_positional(fn, [0, 2], [], ("concrete_b",), {})
        _restored_fn, positions, kwargs_keys, args, kwargs = loads(payload[1:])
        assert positions == [0, 2]
        assert kwargs_keys == []
        assert args == ("concrete_b",)
        assert kwargs == {}

    def test_none_fn(self):
        """fn=None 表示走 named 路径，由 handler 通过 task_name 查找。"""
        payload = pack_positional(None, [0], [], (), {})
        restored_fn, positions, _kwargs_keys, _args, _kwargs = loads(payload[1:])
        assert restored_fn is None
        assert positions == [0]

    def test_with_kwargs(self):
        fn = lambda a, b, c: (a, b, c)  # noqa: E731
        payload = pack_positional(fn, [1], ["b"], ("a_val",), {"c": "c_val"})
        _restored_fn, positions, kwargs_keys, _args, kwargs = loads(payload[1:])
        assert positions == [1]
        assert kwargs_keys == ["b"]
        assert kwargs == {"c": "c_val"}


# ---------------------------------------------------------------------------
# unpack_upstream_prefix：Rust 前置结果的解包
# ---------------------------------------------------------------------------


class TestUnpackUpstreamPrefix:
    """unpack_upstream_prefix 解包 TAG_UPSTREAM_PREFIX 包装的 payload。

    这是 Rust↔Python 边界的核心机制，历史上完全未被测试覆盖。
    """

    def test_non_prefix_payload_returns_unchanged(self):
        """非 TAG_UPSTREAM_PREFIX 的 payload 原样返回，upstream 为空。"""
        payload = pack_single((42,))
        upstream, inner = unpack_upstream_prefix(payload)
        assert upstream == []
        assert inner == payload

    def test_empty_payload(self):
        upstream, inner = unpack_upstream_prefix(b"")
        assert upstream == []
        assert inner == b""

    def test_single_upstream(self):
        """单个上游结果 + default_payload。"""
        from tests._helpers import pack_upstream_prefix

        upstream_results = [dumps(42)]
        default_payload = pack_single(("hello",))
        payload = pack_upstream_prefix(upstream_results, default_payload)

        upstream, inner = unpack_upstream_prefix(payload)
        assert upstream == [42]
        assert inner == default_payload

    def test_multiple_upstream(self):
        """多个上游结果保持顺序。"""
        from tests._helpers import pack_upstream_prefix

        upstream_results = [dumps(1), dumps("two"), dumps([3, 4])]
        default_payload = pack_single(("default",))
        payload = pack_upstream_prefix(upstream_results, default_payload)

        upstream, inner = unpack_upstream_prefix(payload)
        assert upstream == [1, "two", [3, 4]]
        assert inner == default_payload

    def test_empty_upstream_with_default(self):
        """Rust 对无前驱任务直接返回 default_payload（不包装）。"""
        # pack_upstream_prefix 对空 upstream 返回 default_payload 原样
        from tests._helpers import pack_upstream_prefix

        default_payload = pack_single((42,))
        payload = pack_upstream_prefix([], default_payload)
        # 无前驱时 Rust 不包装，payload == default_payload
        assert payload == default_payload

        upstream, inner = unpack_upstream_prefix(payload)
        assert upstream == []
        assert inner == default_payload

    def test_upstream_with_complex_objects(self):
        """上游结果可以是任意 cloudpickle 可序列化对象。"""
        from tests._helpers import pack_upstream_prefix

        upstream_results = [dumps({"key": [1, 2, 3]}), dumps(lambda x: x * 2)]
        default_payload = b""
        payload = pack_upstream_prefix(upstream_results, default_payload)

        upstream, inner = unpack_upstream_prefix(payload)
        assert upstream[0] == {"key": [1, 2, 3]}
        assert upstream[1](5) == 10
        assert inner == b""

    def test_preserves_inner_tag(self):
        """解包后 inner_payload 的 tag 必须保留，供后续 dispatcher 分发。"""
        from tests._helpers import pack_upstream_prefix

        # inner 是 TAG_POSITIONAL
        inner_payload = pack_positional(lambda a: a, [0], [], (), {})
        payload = pack_upstream_prefix([dumps(42)], inner_payload)

        upstream, inner = unpack_upstream_prefix(payload)
        assert upstream == [42]
        assert inner[0] == TAG_POSITIONAL


# ---------------------------------------------------------------------------
# _dispatch_* 函数：各 tag 的分发逻辑
# ---------------------------------------------------------------------------


class TestDispatchEmpty:
    """_dispatch_empty: 无 default_payload 的叶子任务。"""

    def test_no_arg_fn(self):
        def fn() -> int:
            return 99

        assert _dispatch_empty(fn, []) == 99

    def test_upstream_ignored(self):
        """设计上仅用于叶子任务，upstream 应为空；但若传入应被忽略（不混入参数）。"""

        def fn() -> str:
            return "leaf"

        # upstream 非空时 _dispatch_empty 仍只无参调用 fn
        assert _dispatch_empty(fn, ["should_be_ignored"]) == "leaf"


class TestDispatchSingle:
    """_dispatch_single: TAG_SINGLE 位置参数调用。"""

    def test_single_arg(self):
        def double(x: int) -> int:
            return x * 2

        payload = pack_single((5,))
        assert _dispatch_single(double, payload[1:], []) == 10

    def test_multiple_args(self):
        def add(x: int, y: int, z: int) -> int:
            return x + y + z

        payload = pack_single((1, 2, 3))
        assert _dispatch_single(add, payload[1:], []) == 6

    def test_non_tuple_arg(self):
        """单参数非元组情况（兼容旧格式）。"""

        def fn(x: int) -> int:
            return x + 1

        # 直接 pickle 单值（非元组）
        payload = dumps(42)
        assert _dispatch_single(fn, payload, []) == 43

    def test_upstream_ignored(self):
        """设计上仅用于叶子任务，upstream 应为空。"""

        def fn(x: int) -> int:
            return x

        payload = pack_single((10,))
        # upstream 非空时不混入
        assert _dispatch_single(fn, payload[1:], ["ignored"]) == 10


class TestDispatchSingleKw:
    """_dispatch_single_kw: TAG_SINGLE_KW 位置+关键字参数。"""

    def test_args_and_kwargs(self):
        def fn(a: int, b: int, c: int = 0) -> int:
            return a + b + c

        payload = pack_single_kw((1, 2), {"c": 3})
        assert _dispatch_single_kw(fn, payload[1:], []) == 6

    def test_only_kwargs(self):
        def fn(a: int = 1, b: int = 2) -> int:
            return a + b

        payload = pack_single_kw((), {"a": 10, "b": 20})
        assert _dispatch_single_kw(fn, payload[1:], []) == 30


class TestDispatchGroup:
    """_dispatch_group: TAG_GROUP chord 模式。"""

    def test_upstream_as_list(self):
        """chord 模式：upstream 作为 list 参数传入 fn。"""

        def sum_all(results: list[int]) -> int:
            return sum(results)

        # chord 任务 default_payload 为 pack_group([])（仅标记 group 语义）
        payload = pack_group([])
        assert _dispatch_group(sum_all, payload[1:], [10, 20, 30]) == 60

    def test_empty_upstream_falls_back_to_payload(self):
        """无 upstream 时从 payload 内部解析（兼容）。"""

        def sum_all(results: list[int]) -> int:
            return sum(results)

        results = [dumps(10), dumps(20), dumps(30)]
        payload = pack_group(results)
        # 注意：_dispatch_group 收到的是 payload[1:]（已剥离 tag）
        assert _dispatch_group(sum_all, payload[1:], []) == 60

    def test_upstream_preserves_order(self):
        """chord 模式必须保持前驱结果顺序。"""

        def collect(results: list[Any]) -> list[Any]:
            return results

        payload = pack_group([])
        result = _dispatch_group(collect, payload[1:], ["a", "b", "c", "d"])
        assert result == ["a", "b", "c", "d"]

    def test_upstream_with_complex_objects(self):
        def fn(results: list) -> int:
            return sum(len(r) for r in results)

        payload = pack_group([])
        upstream = [[1, 2, 3], {"a": 1, "b": 2}, (1, 2)]
        assert _dispatch_group(fn, payload[1:], upstream) == 7


class TestDispatchGeneric:
    """_dispatch_generic: TAG_GENERIC 内联 callable。"""

    def test_inline_fn_execution(self):
        """payload 中的 fn 覆盖 handler 入参 _fn。"""
        fn = lambda x, y: x * y  # noqa: E731
        payload = pack_generic(fn, (6, 7), {})
        # _fn 占位符被 payload 中的 fn 覆盖
        assert _dispatch_generic(None, payload[1:], []) == 42

    def test_with_kwargs(self):
        fn = lambda x, y=10: x + y  # noqa: E731
        payload = pack_generic(fn, (5,), {"y": 20})
        assert _dispatch_generic(None, payload[1:], []) == 25

    def test_none_kwargs_regression(self):
        """回归测试：kwargs=None 不应导致 **None 崩溃。

        历史 bug：_dispatch_generic 中 ``**kwargs`` 当 kwargs=None 时
        抛出 ``TypeError: argument after ** must be a mapping, not NoneType``。
        """
        fn = lambda x: x * 3  # noqa: E731
        # 构造 kwargs=None 的 payload（可能来自旧版序列化）
        payload = _TAG_GENERIC_B = bytes((TAG_GENERIC,))
        import cloudpickle

        payload = bytes((TAG_GENERIC,)) + cloudpickle.dumps((fn, (7,), None))
        assert _dispatch_generic(None, payload[1:], []) == 21

    def test_upstream_ignored(self):
        """设计上仅用于叶子任务，upstream 应为空。"""
        fn = lambda x: x  # noqa: E731
        payload = pack_generic(fn, (42,), {})
        # upstream 非空时不混入
        assert _dispatch_generic(None, payload[1:], ["ignored"]) == 42


class TestDispatchPositional:
    """_dispatch_positional: TAG_POSITIONAL 位置感知合并。

    这是有依赖任务的统一路径，保留 TaskRef 在原始 args 中的位置。
    """

    def test_single_ref_at_position_0(self):
        """ref 在位置 0，concrete 在位置 1。"""
        fn = lambda a, b: (a, b)  # noqa: E731
        payload = pack_positional(fn, [0], [], ("concrete_b",), {})
        # upstream[0] 填入位置 0
        result = _dispatch_positional(None, payload[1:], ["upstream_a"])
        assert result == ("upstream_a", "concrete_b")

    def test_single_ref_at_position_1(self):
        """回归测试：ref 在位置 1，concrete 在位置 0。

        历史 bug：旧设计中 combine(1, ref) 会被错位为 combine(ref, 1)。
        """
        fn = lambda a, b: (a, b)  # noqa: E731
        payload = pack_positional(fn, [1], [], ("concrete_a",), {})
        result = _dispatch_positional(None, payload[1:], ["upstream_b"])
        assert result == ("concrete_a", "upstream_b")

    def test_multiple_refs_preserve_order(self):
        """多个 ref 按原始位置重建。"""
        fn = lambda a, b, c, d: (a, b, c, d)  # noqa: E731
        # ref 在位置 0 和 2，concrete 在位置 1 和 3
        payload = pack_positional(fn, [0, 2], [], ("c1", "c2"), {})
        result = _dispatch_positional(None, payload[1:], ["r0", "r2"])
        assert result == ("r0", "c1", "r2", "c2")

    def test_all_refs(self):
        """所有参数都是 ref（纯 chain 场景 B(A_result)）。"""
        fn = lambda a, b, c: (a, b, c)  # noqa: E731
        payload = pack_positional(fn, [0, 1, 2], [], (), {})
        result = _dispatch_positional(None, payload[1:], ["a", "b", "c"])
        assert result == ("a", "b", "c")

    def test_with_kwargs(self):
        fn = lambda a, b, c=0: (a, b, c)  # noqa: E731
        payload = pack_positional(fn, [1], [], ("a_val",), {"c": 99})
        result = _dispatch_positional(None, payload[1:], ["b_val"])
        assert result == ("a_val", "b_val", 99)

    def test_named_fn_used_when_inline_none(self):
        """inline_fn=None 时使用 handler 入参 fn（named 路径）。"""

        def named_fn(a: str, b: str) -> str:
            return f"{a}-{b}"

        payload = pack_positional(None, [0], [], ("b_concrete",), {})
        result = _dispatch_positional(named_fn, payload[1:], ["a_upstream"])
        assert result == "a_upstream-b_concrete"

    def test_inline_fn_overrides_named(self):
        """inline_fn 非 None 时覆盖 handler 入参 fn。"""
        named = lambda a, b: "wrong"  # noqa: E731
        inline = lambda a, b: f"{a}+{b}"  # noqa: E731
        payload = pack_positional(inline, [0], [], ("b",), {})
        result = _dispatch_positional(named, payload[1:], ["a"])
        assert result == "a+b"

    def test_branchref_positional(self):
        """BranchRef 在位置 0（条件分支延迟引用，运行时仅一个分支产生结果）。"""
        fn = lambda x: x * 10  # noqa: E731
        payload = pack_positional(fn, [0], [], (), {})
        # BranchRef 运行时仅一个分支结果
        result = _dispatch_positional(None, payload[1:], [5])
        assert result == 50


# ---------------------------------------------------------------------------
# _dispatch_task：端到端分发入口
# ---------------------------------------------------------------------------


class TestDispatchTask:
    """_dispatch_task 是 worker 执行任务的入口，整合 upstream prefix 解包与 tag 分发。"""

    def test_empty_payload_leaf_task(self):
        """无 default_payload 的叶子任务。"""

        def fn() -> int:
            return 42

        result = _dispatch_task(fn, b"")
        assert loads(result) == 42

    def test_single_tag(self):
        def add(x: int, y: int) -> int:
            return x + y

        payload = pack_single((3, 4))
        result = _dispatch_task(add, payload)
        assert loads(result) == 7

    def test_single_kw_tag(self):
        def fn(a: int, b: int, c: int = 0) -> int:
            return a + b + c

        payload = pack_single_kw((1, 2), {"c": 3})
        result = _dispatch_task(fn, payload)
        assert loads(result) == 6

    def test_generic_tag(self):
        fn = lambda x: x * 2  # noqa: E731
        payload = pack_generic(fn, (21,), {})
        result = _dispatch_task(None, payload)
        assert loads(result) == 42

    def test_positional_tag_with_upstream_prefix(self):
        """有前驱任务：Rust 用 TAG_UPSTREAM_PREFIX 包装 default_payload。"""
        from tests._helpers import pack_upstream_prefix

        fn = lambda a, b: (a, b)  # noqa: E731
        default_payload = pack_positional(fn, [0], [], ("concrete_b",), {})
        # Rust 前置一个上游结果
        payload = pack_upstream_prefix([dumps("upstream_a")], default_payload)

        result = _dispatch_task(None, payload)
        assert loads(result) == ("upstream_a", "concrete_b")

    def test_group_tag_with_upstream_prefix(self):
        """chord 模式：Rust 前置多个上游结果。"""
        from tests._helpers import pack_upstream_prefix

        def sum_all(results: list[int]) -> int:
            return sum(results)

        default_payload = pack_group([])
        payload = pack_upstream_prefix([dumps(10), dumps(20), dumps(30)], default_payload)

        result = _dispatch_task(sum_all, payload)
        assert loads(result) == 60

    def test_chain_via_positional(self):
        """纯 chain B(A_result)：B 的 args 全是 TaskRef，走 positional 路径。"""
        from tests._helpers import pack_upstream_prefix

        def inc(x: int) -> int:
            return x + 1

        # B 的 args=(ref,) → positional 路径，positions=[0], concrete=()
        default_payload = pack_positional(inc, [0], [], (), {})
        # A 的结果 = 41
        payload = pack_upstream_prefix([dumps(41)], default_payload)

        result = _dispatch_task(None, payload)
        assert loads(result) == 42

    def test_unknown_tag_raises(self):
        with pytest.raises(ValueError, match="unknown payload tag"):
            _dispatch_task(lambda: None, bytes([0xFF]))

    def test_returned_bytes_are_pickle(self):
        """_dispatch_task 返回的 bytes 必须是有效 cloudpickle。"""

        def fn() -> str:
            return "hello"

        result = _dispatch_task(fn, b"")
        assert isinstance(result, bytes)
        assert loads(result) == "hello"


# ---------------------------------------------------------------------------
# Rust 类型编码
# ---------------------------------------------------------------------------


class TestEncodeRetry:
    """encode_retry: Python dict → Rust _RetryPolicy，含单位转换。"""

    def test_none_returns_none(self):
        from actant._serialization import encode_retry

        assert encode_retry(None) is None

    def test_seconds_to_milliseconds(self):
        from actant._serialization import encode_retry

        policy = encode_retry({"max_retries": 3, "delay": 0.5, "max_delay": 10.0})
        assert policy is not None
        assert policy.max_retries == 3
        assert policy.delay_ms == 500
        assert policy.max_delay_ms == 10_000

    def test_milliseconds_key_accepted(self):
        """同时接受毫秒级 key（delay_ms, max_delay_ms）。"""
        from actant._serialization import encode_retry

        policy = encode_retry({"max_retries": 2, "delay_ms": 200, "max_delay_ms": 5000})
        assert policy is not None
        assert policy.delay_ms == 200
        assert policy.max_delay_ms == 5000

    def test_defaults_filled(self):
        """缺失字段使用 Rust 默认值。"""
        from actant._serialization import encode_retry

        policy = encode_retry({"max_retries": 1})
        assert policy is not None
        assert policy.max_retries == 1
        # delay_ms 和 max_delay_ms 使用默认值（非零）
        assert policy.delay_ms > 0
        assert policy.max_delay_ms > 0


class TestEncodePriority:
    """encode_priority: int/str → i32 数值。"""

    def test_none_returns_none(self):
        from actant._serialization import encode_priority

        assert encode_priority(None) is None

    def test_integer_passthrough(self):
        from actant._serialization import encode_priority

        assert encode_priority(5) == 5
        assert encode_priority(-3) == -3
        assert encode_priority(0) == 0

    def test_string_normalization(self):
        from actant._serialization import encode_priority

        # 字符串优先级必须归一化为数值
        assert encode_priority("normal") is not None
        assert isinstance(encode_priority("high"), int)


class TestEncodeFailureStrategy:
    """encode_failure_strategy: str → str 标签。"""

    def test_none_returns_none(self):
        from actant._serialization import encode_failure_strategy

        assert encode_failure_strategy(None) is None

    def test_string_passthrough(self):
        from actant._serialization import encode_failure_strategy

        # 失败策略字符串原样传递（Rust 侧解析）
        assert encode_failure_strategy("fail_fast") == "fail_fast"
        assert encode_failure_strategy("continue") == "continue"


# ---------------------------------------------------------------------------
# build_payload：参数过滤与编码
# ---------------------------------------------------------------------------


class TestBuildPayload:
    """build_payload 应过滤 TaskRef 参数并选择正确的编码路径。

    覆盖率目标：所有分支（args-only / kwargs-only / 混合 / 全 TaskRef）。
    通过 _dispatch_task 验证 payload 可被正确解码执行。
    """

    def test_no_args_no_kwargs_returns_empty(self):
        from actant._serialization import build_payload

        assert build_payload((), None) == b""
        assert build_payload((), {}) == b""

    def test_args_only_path(self):
        from actant._serialization import _dispatch_task, build_payload, loads

        def fn(a, b, c):
            return a + b + c

        payload = build_payload((1, 2, 3), None)
        assert payload != b""
        result = _dispatch_task(fn, payload)
        assert loads(result) == 6

    def test_kwargs_only_path(self):
        from actant._serialization import _dispatch_task, build_payload, loads

        def fn(a=0, b=0):
            return a * 10 + b

        payload = build_payload((), {"a": 1, "b": 2})
        assert payload != b""
        assert loads(_dispatch_task(fn, payload)) == 12

    def test_mixed_args_and_kwargs_path(self):
        from actant._serialization import _dispatch_task, build_payload, loads

        def fn(a, b, c=0):
            return a + b + c

        payload = build_payload((1, 2), {"c": 3})
        assert payload != b""
        assert loads(_dispatch_task(fn, payload)) == 6

    def test_filters_taskref_from_args(self):
        from actant._serialization import _dispatch_task, build_payload, loads
        from actant.task import TaskRef

        def fn(b):
            # TaskRef 被过滤，只剩 1 个具体参数
            return b * 2

        ref = TaskRef(task_name="upstream", args=(1,))
        payload = build_payload((ref, 99), None)
        assert loads(_dispatch_task(fn, payload)) == 198

    def test_filters_taskref_from_kwargs(self):
        from actant._serialization import _dispatch_task, build_payload, loads
        from actant.task import TaskRef

        def fn(a, kept=0):
            return a + kept

        ref = TaskRef(task_name="upstream", args=(1,))
        payload = build_payload((1,), {"ref": ref, "kept": 5})
        assert loads(_dispatch_task(fn, payload)) == 6

    def test_filters_taskref_from_both(self):
        from actant._serialization import _dispatch_task, build_payload, loads
        from actant.task import TaskRef

        def fn(b):
            # (ref, 2) 过滤 ref 后 args=(2,)；{"a": ref, "b": 3} 过滤 ref 后 kwargs={"b": 3}
            # 但 fn 只接受 1 个位置参数，所以这里测试 kwargs 中保留的 b 不传给 fn
            return b * 10

        ref = TaskRef(task_name="up", args=(1,))
        payload = build_payload((ref, 2), {"a": ref, "b": 3})
        # fn(b=?) — _dispatch_single_kw 会把 (2,) 当 args，{"b":3} 当 kwargs
        # fn 签名 fn(b) 无法同时接受位置 2 和 kw b=3 → 改用 fn(a, b)
        def fn2(a, b):
            return a + b
        assert loads(_dispatch_task(fn2, payload)) == 5

    def test_all_args_are_taskref_returns_empty(self):
        from actant._serialization import build_payload
        from actant.task import TaskRef

        ref = TaskRef(task_name="up", args=(1,))
        # 所有 args 都是 TaskRef，concrete_args 为空 → 返回 b""
        assert build_payload((ref,), None) == b""

    def test_all_args_and_kwargs_are_taskref_returns_empty(self):
        from actant._serialization import build_payload
        from actant.task import TaskRef

        ref = TaskRef(task_name="up", args=(1,))
        # 混合路径：所有 args 和 kwargs 都是 TaskRef → b""
        assert build_payload((ref,), {"r": ref}) == b""

    def test_args_only_with_kwargs_empty_dict(self):
        """has_kwargs 为 False（kwargs={}），走 args-only 路径。"""
        from actant._serialization import _dispatch_task, build_payload, loads

        def fn(a, b):
            return a + b

        payload = build_payload((1, 2), {})
        assert loads(_dispatch_task(fn, payload)) == 3


class TestCloudpickleWarnOnce:
    """P1-S3：cloudpickle 安全警告每个进程只触发一次。"""

    def test_cloudpickle_warns_once(self):
        """进程级标志（sys 属性）确保多次 reload 只产生一次 UserWarning。"""
        import importlib
        import sys
        import warnings

        import actant._serialization as ser_mod

        # 重置进程级标志以模拟首次 import
        key = ser_mod._CLOUDPICKLE_WARN_KEY
        was_warned = getattr(sys, key, False)
        try:
            setattr(sys, key, False)
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always", UserWarning)
                importlib.reload(ser_mod)
                first_count = len([w for w in caught if "cloudpickle" in str(w.message)])

                # 第二次 reload：进程级标志已设置，不应再警告
                importlib.reload(ser_mod)
                total_count = len([w for w in caught if "cloudpickle" in str(w.message)])

            second_count = total_count - first_count
            assert first_count == 1, f"expected 1 warning on first reload, got {first_count}"
            assert second_count == 0, f"expected 0 new warnings on second reload, got {second_count}"
        finally:
            setattr(sys, key, was_warned)

    def test_env_var_suppresses_warning(self):
        """ACTANT_NO_CLOUDPICKLE_WARN=1 禁用 cloudpickle 警告。"""
        import importlib
        import os
        import sys
        import warnings

        import actant._serialization as ser_mod

        key = ser_mod._CLOUDPICKLE_WARN_KEY
        was_warned = getattr(sys, key, False)
        os.environ.get("ACTANT_NO_CLOUDPICKLE_WARN")
        try:
            setattr(sys, key, False)
            os.environ["ACTANT_NO_CLOUDPICKLE_WARN"] = "1"
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always", UserWarning)
                importlib.reload(ser_mod)
            warn_count = len([w for w in caught if "cloudpickle" in str(w.message)])
            assert warn_count == 0, f"expected 0 warnings with env var, got {warn_count}"
        finally:
            setattr(sys, key, was_warned)


class TestPayloadTooLarge:
    """P1-D4：序列化载荷超限时抛 PayloadTooLargeError。"""

    def test_dumps_raises_payload_too_large(self):
        """序列化超过 max_size 的对象抛 PayloadTooLargeError。"""
        from actant._serialization import dumps
        from actant.exceptions import PayloadTooLargeError

        # 构造一个略大于 1 MiB 的对象
        data = b"x" * (1024 * 1024 + 1)  # 1 MiB + 1 byte
        with pytest.raises(PayloadTooLargeError) as exc_info:
            dumps(data, max_size=1024 * 1024)
        assert exc_info.value.actual > 1024 * 1024
        assert exc_info.value.limit == 1024 * 1024

    def test_dumps_within_limit_succeeds(self):
        """序列化不超限的对象正常返回。"""
        from actant._serialization import dumps

        data = b"x" * 100
        result = dumps(data, max_size=1024)
        assert isinstance(result, bytes)
        assert len(result) > 100  # cloudpickle 有 overhead

    def test_dumps_no_limit_always_succeeds(self):
        """max_size=None（默认）不校验大小。"""
        from actant._serialization import dumps

        data = b"x" * (16 * 1024 * 1024 + 1)  # 16 MiB + 1
        result = dumps(data)  # no max_size
        assert isinstance(result, bytes)
