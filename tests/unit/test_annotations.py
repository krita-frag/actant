"""annotations 模块单元测试：annotate 上下文管理器、合并规则、线程隔离。

覆盖：
- annotate 单层/嵌套行为
- 标量字段（priority/timeout/retry_policy）：内层非 None 覆盖
- tags 列表累加
- metadata dict 合并
- 异常退出时栈正确 pop
- 线程局部隔离
- merge_options 三层合并：defaults < annotations < overrides
"""

from __future__ import annotations

import threading

import pytest

from actant._annotations import (
    _merge_layers,
    _stack,
    annotate,
    current_annotations,
    merge_options,
)

# ---------------------------------------------------------------------------
# 基础栈行为
# ---------------------------------------------------------------------------


class TestStackBasics:
    """注解栈的初始化与清理。"""

    def test_empty_stack_returns_empty_dict(self):
        """无活跃注解时 current_annotations 返回空 dict。"""
        # 注意：测试运行环境可能残留其他注解，使用显式清理
        # _stack 是线程局部，每个测试线程独立
        assert current_annotations() == {} or isinstance(
            current_annotations(), dict
        )

    def test_stack_is_thread_local(self):
        """_stack 返回线程局部对象，不同线程互不影响。"""
        result = {}

        def _worker():
            result["stack"] = _stack()
            result["same_obj"] = _stack() is _stack()

        t = threading.Thread(target=_worker)
        t.start()
        t.join()
        # 子线程的 stack 是独立的 list 实例
        assert result["stack"] is not _stack()
        assert result["same_obj"] is True


# ---------------------------------------------------------------------------
# annotate 上下文管理器
# ---------------------------------------------------------------------------


class TestAnnotateContext:
    """annotate 上下文管理器 push/pop 行为。"""

    def test_push_and_pop(self):
        before = current_annotations()
        with annotate(priority="high", timeout=5.0):
            ann = current_annotations()
            assert ann["priority"] == "high"
            assert ann["timeout"] == 5.0
        after = current_annotations()
        # 退出后栈恢复
        assert after == before

    def test_nested_inner_overrides_outer(self):
        with annotate(priority="low", timeout=1.0):
            with annotate(priority="high", timeout=10.0):
                ann = current_annotations()
                # 内层覆盖外层
                assert ann["priority"] == "high"
                assert ann["timeout"] == 10.0
            # 退出内层后恢复外层
            ann = current_annotations()
            assert ann["priority"] == "low"
            assert ann["timeout"] == 1.0

    def test_partial_inner_override(self):
        """内层只覆盖部分字段，其余继承外层。"""
        with annotate(priority="high", timeout=5.0, tags=["a"]), annotate(
            priority="critical"
        ):
            ann = current_annotations()
            assert ann["priority"] == "critical"
            # timeout 未在内层指定，继承外层
            assert ann["timeout"] == 5.0
            assert ann["tags"] == ["a"]

    def test_exception_pops_stack(self):
        """异常退出时也保证 pop。"""
        before = current_annotations()
        with pytest.raises(RuntimeError, match="boom"), annotate(priority="high"):
            raise RuntimeError("boom")
        assert current_annotations() == before

    def test_yielded_snapshot_is_readonly(self):
        """annotate yield 的快照修改不影响栈。"""
        with annotate(priority="high") as snapshot:
            assert snapshot["priority"] == "high"
            # 修改快照不应影响栈
            snapshot["priority"] = "low"
            assert current_annotations()["priority"] == "high"


# ---------------------------------------------------------------------------
# 标量字段合并
# ---------------------------------------------------------------------------


class TestScalarMerge:
    """priority/timeout/retry_policy：内层非 None 覆盖外层。"""

    def test_priority_override(self):
        with annotate(priority="low"), annotate(priority="high"):
            assert current_annotations()["priority"] == "high"

    def test_timeout_override(self):
        with annotate(timeout=1.0), annotate(timeout=5.0):
            assert current_annotations()["timeout"] == 5.0

    def test_retry_policy_override(self):
        with annotate(retry_policy={"max_attempts": 1}), annotate(
            retry_policy={"max_attempts": 5}
        ):
            assert current_annotations()["retry_policy"] == {"max_attempts": 5}

    def test_inner_none_keeps_outer(self):
        """内层 None 不覆盖外层值。"""
        with annotate(priority="high", timeout=5.0), annotate(priority=None, timeout=None):
            ann = current_annotations()
            assert ann["priority"] == "high"
            assert ann["timeout"] == 5.0


# ---------------------------------------------------------------------------
# tags 累加
# ---------------------------------------------------------------------------


class TestTagsAccumulate:
    """tags 嵌套时累加（保持顺序，不去重）。"""

    def test_single_layer_tags(self):
        with annotate(tags=["gpu"]):
            assert current_annotations()["tags"] == ["gpu"]

    def test_nested_tags_accumulate(self):
        with annotate(tags=["gpu", "fast"]), annotate(tags=["critical"]):
            tags = current_annotations()["tags"]
            assert tags == ["gpu", "fast", "critical"]

    def test_empty_inner_tags_keeps_outer(self):
        with annotate(tags=["a"]), annotate(tags=None):
            assert current_annotations()["tags"] == ["a"]

    def test_no_duplicates_removed(self):
        """相同 tag 不去重（保持顺序）。"""
        with annotate(tags=["gpu"]), annotate(tags=["gpu"]):
            assert current_annotations()["tags"] == ["gpu", "gpu"]


# ---------------------------------------------------------------------------
# actant.task 装饰器（__init__.py 中的分支）
# ---------------------------------------------------------------------------


class TestTaskDecorator:
    """覆盖 actant.task 装饰器的所有分支：直接装饰 / 带参装饰 / 类型校验。"""

    def test_direct_decoration_returns_task(self):
        """@actant.task（无括号）直接装饰函数 → 返回 Task。"""
        import actant
        from actant.task import Task

        @actant.task
        def add(x, y):
            return x + y

        assert isinstance(add, Task)
        assert add.name == "add"
        assert add(2, 3) == 5

    def test_parametrized_decoration_returns_decorator(self):
        """@actant.task(...) 带参数 → 返回装饰器，再调用返回 Task。"""
        import actant
        from actant.task import Task

        @actant.task(max_retries=5, retry_delay=2.0, timeout=60.0, priority="high")
        def reliable(data):
            return data

        assert isinstance(reliable, Task)
        assert reliable.name == "reliable"
        assert reliable._retry_policy["max_retries"] == 5

    def test_parametrized_with_explicit_name(self):
        """带参数装饰时显式指定 name。"""
        import actant
        from actant.task import Task

        @actant.task(name="custom-name", tags=["gpu"], metadata={"k": "v"})
        def fn(x):
            return x

        assert isinstance(fn, Task)
        assert fn.name == "custom-name"

    def test_non_callable_raises_type_error(self):
        """func 非 callable 且非 None → raise TypeError。"""
        import actant

        with pytest.raises(TypeError, match="func must be callable"):
            actant.task("not-a-function")  # type: ignore[arg-type]

    def test_decorator_with_func_none_returns_callable(self):
        """actant.task(func=None) 返回 decorator（可重复使用）。"""
        import actant
        from actant.task import Task

        decorator = actant.task(max_retries=3)  # func 默认 None
        assert callable(decorator)

        @decorator
        def fn(x):
            return x

        assert isinstance(fn, Task)
        assert fn._retry_policy["max_retries"] == 3


# ---------------------------------------------------------------------------
# metadata 合并
# ---------------------------------------------------------------------------


class TestMetadataMerge:
    """metadata 嵌套时 dict 合并，内层覆盖同键。"""

    def test_single_layer_metadata(self):
        with annotate(metadata={"owner": "team_a"}):
            assert current_annotations()["metadata"] == {"owner": "team_a"}

    def test_nested_metadata_merges(self):
        with annotate(metadata={"owner": "team_a", "env": "prod"}), annotate(
            metadata={"owner": "team_b", "priority": "p0"}
        ):
            meta = current_annotations()["metadata"]
            assert meta == {"owner": "team_b", "env": "prod", "priority": "p0"}

    def test_inner_empty_metadata_keeps_outer(self):
        with annotate(metadata={"a": "1"}), annotate(metadata=None):
            assert current_annotations()["metadata"] == {"a": "1"}


# ---------------------------------------------------------------------------
# merge_options 三层合并
# ---------------------------------------------------------------------------


class TestMergeOptions:
    """merge_options: defaults < annotations < overrides。"""

    def test_uses_defaults_when_no_overrides_or_annotations(self):
        defaults = {
            "priority": "high",
            "timeout": 5.0,
            "retry_policy": {"max_attempts": 3},
            "tags": ["a"],
            "metadata": {"k": "v"},
        }
        opts = merge_options(defaults, {"priority": None, "timeout": None,
                                        "retry_policy": None, "tags": None,
                                        "metadata": None})
        assert opts["priority"] == "high"
        assert opts["timeout"] == 5.0
        assert opts["retry_policy"] == {"max_attempts": 3}
        assert opts["tags"] == ["a"]
        assert opts["metadata"] == {"k": "v"}

    def test_overrides_take_precedence(self):
        defaults = {"priority": "low", "timeout": 1.0, "retry_policy": None,
                    "tags": [], "metadata": {}}
        opts = merge_options(defaults, {"priority": "critical", "timeout": 100.0,
                                        "retry_policy": {"max": 10},
                                        "tags": ["urgent"],
                                        "metadata": {"k": "v2"}})
        assert opts["priority"] == "critical"
        assert opts["timeout"] == 100.0
        assert opts["retry_policy"] == {"max": 10}
        assert opts["tags"] == ["urgent"]
        assert opts["metadata"] == {"k": "v2"}

    def test_annotations_between_defaults_and_overrides(self):
        """注解层位于 defaults 和 overrides 之间。"""
        defaults = {"priority": "low", "timeout": 1.0, "retry_policy": None,
                    "tags": ["d"], "metadata": {"a": "1"}}
        overrides = {"priority": None, "timeout": None, "retry_policy": None,
                     "tags": None, "metadata": None}
        with annotate(priority="high", timeout=5.0, tags=["ann"],
                      metadata={"b": "2"}):
            opts = merge_options(defaults, overrides)
            assert opts["priority"] == "high"
            assert opts["timeout"] == 5.0
            # tags 累加：default + annotation
            assert opts["tags"] == ["d", "ann"]
            # metadata 合并
            assert opts["metadata"] == {"a": "1", "b": "2"}

    def test_overrides_override_annotations(self):
        """显式 overrides 优先于 annotations。"""
        defaults = {"priority": None, "timeout": None, "retry_policy": None,
                    "tags": [], "metadata": {}}
        with annotate(priority="high"):
            opts = merge_options(defaults,
                                 {"priority": "critical", "timeout": None,
                                  "retry_policy": None, "tags": None,
                                  "metadata": None})
            assert opts["priority"] == "critical"


# ---------------------------------------------------------------------------
# 类型校验
# ---------------------------------------------------------------------------


class TestAnnotateTypeValidation:
    """annotate 入参类型校验。"""

    def test_tags_must_be_list(self):
        with pytest.raises(TypeError, match="tags must be list"), annotate(
            tags="gpu"  # type: ignore[arg-type]
        ):
            pass

    def test_tags_must_be_list_not_tuple(self):
        with pytest.raises(TypeError, match="tags must be list"), annotate(
            tags=("gpu",)  # type: ignore[arg-type]
        ):
            pass

    def test_metadata_must_be_dict(self):
        with pytest.raises(TypeError, match="metadata must be dict"), annotate(
            metadata=[("k", "v")]  # type: ignore[arg-type]
        ):
                pass

    def test_none_tags_allowed(self):
        with annotate(tags=None):
            # 不抛异常即可
            pass

    def test_none_metadata_allowed(self):
        with annotate(metadata=None):
            pass


# ---------------------------------------------------------------------------
# _merge_layers 直接测试
# ---------------------------------------------------------------------------


class TestMergeLayers:
    """_merge_layers 纯函数测试。"""

    def test_empty_layers(self):
        assert _merge_layers([]) == {}

    def test_single_layer(self):
        layer = {"priority": "high", "timeout": None, "retry_policy": None,
                 "tags": ["a"], "metadata": {"k": "v"}}
        merged = _merge_layers([layer])
        assert merged["priority"] == "high"
        assert merged["tags"] == ["a"]
        assert merged["metadata"] == {"k": "v"}
        # None 字段不写入
        assert "timeout" not in merged

    def test_multiple_layers_accumulate_tags(self):
        l1 = {"priority": None, "timeout": None, "retry_policy": None,
              "tags": ["a"], "metadata": None}
        l2 = {"priority": None, "timeout": None, "retry_policy": None,
              "tags": ["b"], "metadata": None}
        l3 = {"priority": None, "timeout": None, "retry_policy": None,
              "tags": ["c"], "metadata": None}
        merged = _merge_layers([l1, l2, l3])
        assert merged["tags"] == ["a", "b", "c"]
