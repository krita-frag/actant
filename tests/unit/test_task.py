"""task 模块单元测试：Task、TaskRef、注册表、选项合并。

覆盖：
- Task 调用行为（flow 内/外）
- TaskRef 属性与不可 await 语义
- Task.map / Task.reduce chord 语义
- 全局任务注册表线程安全
- _merge_task_options 优先级合并
- _task_origin / inline_func 处理
"""

from __future__ import annotations

import threading

import pytest

from actant.exceptions import InvalidStateError
from actant.task import (
    Task,
    TaskRef,
    _merge_task_options,
    _task_origin,
    clear_global_tasks,
    get_global_task,
    get_global_tasks,
    register_global_task,
)

# ---------------------------------------------------------------------------
# TaskRef
# ---------------------------------------------------------------------------


class TestTaskRef:
    """TaskRef 不可变数据容器。"""

    def test_default_construction(self):
        ref = TaskRef("my_task")
        assert ref.task_name == "my_task"
        assert ref.args == ()
        assert ref.kwargs is None
        assert ref.retry_policy is None
        assert ref.timeout is None
        assert ref.priority is None
        assert ref.tags == []
        assert ref.metadata == {}
        assert ref.inline_func is None
        # id 自动生成且唯一
        assert isinstance(ref.id, str)
        assert len(ref.id) > 0

    def test_unique_ids(self):
        """每个 TaskRef 有独立 id。"""
        r1 = TaskRef("a")
        r2 = TaskRef("a")
        assert r1.id != r2.id

    def test_priority_default_is_none(self):
        """P1-V1：不传 priority 时默认 None（而非 0/IntEnum 混淆值）。

        normalize_priority(None) 应返回 NORMAL(0)，但 TaskRef.priority
        本身保留 None 以区分"未指定"与"显式指定 NORMAL"。
        """
        from actant.config import TaskPriority, normalize_priority

        ref = TaskRef("t")
        assert ref.priority is None, "default priority must be None, not 0"
        assert normalize_priority(None) == int(TaskPriority.NORMAL)

    def test_repr(self):
        ref = TaskRef("sum", args=(1, 2))
        repr_str = repr(ref)
        assert "TaskRef" in repr_str
        assert "sum" in repr_str
        assert "1" in repr_str

    def test_cannot_await(self):
        """TaskRef 不可 await — DAG 编译模型。"""
        ref = TaskRef("t")

        async def _try_await():
            await ref  # type: ignore[misc]

        coro = _try_await()
        with pytest.raises(TypeError, match="cannot await TaskRef"):
            coro.send(None)
        coro.close()

    def test_kwargs_default_none(self):
        """kwargs 默认 None 而非 {}。"""
        assert TaskRef("t").kwargs is None

    def test_kwargs_explicit(self):
        ref = TaskRef("t", kwargs={"x": 1})
        assert ref.kwargs == {"x": 1}


# ---------------------------------------------------------------------------
# Task 调用行为
# ---------------------------------------------------------------------------


class TestTaskCall:
    """Task.__call__ 在 flow 内/外行为不同。"""

    def test_outside_flow_directly_executes(self, reset_flow_context_local):
        """flow 上下文外直接执行返回结果。"""
        t = Task("add", func=lambda a, b: a + b)
        assert t(1, 2) == 3

    def test_outside_flow_kwargs(self, reset_flow_context_local):
        t = Task("greet", func=lambda name, greeting: f"{greeting}, {name}")
        assert t("world", greeting="hi") == "hi, world"

    def test_outside_flow_no_func_raises(self, reset_flow_context_local):
        """Task 无 func 时 apply 应失败。"""
        t = Task("noop", func=None)
        with pytest.raises(InvalidStateError):
            t()

    def test_inside_flow_returns_taskref(
        self, reset_flow_context_local, fresh_flow_context
    ):
        """flow 上下文内返回 TaskRef 并自动 track。"""
        from actant.flow import _context_local

        _context_local.flow_context = fresh_flow_context
        try:
            t = Task("add", func=lambda a, b: a + b)
            ref = t(1, 2)
            assert isinstance(ref, TaskRef)
            assert ref.task_name == "add"
            assert ref.args == (1, 2)
            # 自动 track 到上下文
            assert fresh_flow_context.ref_count == 1
        finally:
            _context_local.flow_context = None

    def test_inside_flow_inline_func_included(
        self, reset_flow_context_local, fresh_flow_context
    ):
        """inline=True 将 func 内联到 TaskRef。"""
        from actant.flow import _context_local

        _context_local.flow_context = fresh_flow_context
        try:
            fn = lambda x: x * 2  # noqa: E731
            t = Task("double", func=fn)
            ref = t(5, inline=True)
            assert ref.inline_func is fn
        finally:
            _context_local.flow_context = None

    def test_inside_flow_default_no_inline(
        self, reset_flow_context_local, fresh_flow_context
    ):
        """默认 inline=False，inline_func 为 None。"""
        from actant.flow import _context_local

        _context_local.flow_context = fresh_flow_context
        try:
            t = Task("double", func=lambda x: x * 2)
            ref = t(5)
            assert ref.inline_func is None
        finally:
            _context_local.flow_context = None


# ---------------------------------------------------------------------------
# Task 选项合并
# ---------------------------------------------------------------------------


class TestMergeTaskOptions:
    """_merge_task_options: 调用参数 > Task 默认。"""

    def test_uses_task_defaults(self, reset_flow_context_local):
        t = Task(
            "t",
            func=lambda: None,
            _priority="high",
            _timeout=5.0,
            _tags=["gpu"],
            _metadata={"owner": "team"},
        )
        opts = _merge_task_options(t, None, None, None, None, None)
        assert opts["priority"] == "high"
        assert opts["timeout"] == 5.0
        assert opts["tags"] == ["gpu"]
        assert opts["metadata"] == {"owner": "team"}

    def test_call_overrides_task_default(self, reset_flow_context_local):
        t = Task("t", func=lambda: None, _priority="low", _timeout=1.0)
        opts = _merge_task_options(t, "critical", 10.0, None, None, None)
        assert opts["priority"] == "critical"
        assert opts["timeout"] == 10.0

    def test_partial_override_keeps_other_defaults(self, reset_flow_context_local):
        """仅覆盖部分选项，其余保持 Task 默认。"""
        t = Task("t", func=lambda: None, _priority="low", _timeout=1.0, _tags=["a"])
        opts = _merge_task_options(t, "high", None, None, None, None)
        assert opts["priority"] == "high"
        assert opts["timeout"] == 1.0
        assert opts["tags"] == ["a"]

    def test_tags_override_accumulates(self, reset_flow_context_local):
        """tags 是累加：默认 + 注解 + 显式（与 metadata 三层合并一致）。"""
        t = Task("t", func=lambda: None, _tags=["a", "b"])
        opts = _merge_task_options(t, None, None, None, ["c"], None)
        assert opts["tags"] == ["a", "b", "c"]

    def test_metadata_override_merges(self, reset_flow_context_local):
        """metadata 是合并：默认 < 注解 < 显式。"""
        t = Task("t", func=lambda: None, _metadata={"a": "1"})
        opts = _merge_task_options(t, None, None, None, None, {"b": "2"})
        assert opts["metadata"] == {"a": "1", "b": "2"}

    def test_metadata_override_replaces_value(self, reset_flow_context_local):
        """metadata 同 key 时显式值覆盖默认值。"""
        t = Task("t", func=lambda: None, _metadata={"a": "1"})
        opts = _merge_task_options(t, None, None, None, None, {"a": "2"})
        assert opts["metadata"] == {"a": "2"}

    def test_retry_policy_override(self, reset_flow_context_local):
        t = Task("t", func=lambda: None, _retry_policy={"max_attempts": 3})
        opts = _merge_task_options(t, None, None, {"max_attempts": 5}, None, None)
        assert opts["retry_policy"] == {"max_attempts": 5}


# ---------------------------------------------------------------------------
# Task.map
# ---------------------------------------------------------------------------


class TestTaskMap:
    """Task.map 在 flow 内创建多个 TaskRef，flow 外直接执行。"""

    def test_outside_flow_executes_each(self, reset_flow_context_local):
        t = Task("double", func=lambda x: x * 2)
        results = t.map([1, 2, 3])
        assert results == [2, 4, 6]

    def test_inside_flow_returns_taskrefs(
        self, reset_flow_context_local, fresh_flow_context
    ):
        from actant.flow import _context_local

        _context_local.flow_context = fresh_flow_context
        try:
            t = Task("double", func=lambda x: x * 2)
            refs = t.map([1, 2, 3])
            assert len(refs) == 3
            assert all(isinstance(r, TaskRef) for r in refs)
            assert fresh_flow_context.ref_count == 3
            # 每个 ref 都有独立的 item
            assert refs[0].args == (1,)
            assert refs[1].args == (2,)
            assert refs[2].args == (3,)
        finally:
            _context_local.flow_context = None

    def test_empty_items_returns_empty(
        self, reset_flow_context_local, fresh_flow_context
    ):
        from actant.flow import _context_local

        _context_local.flow_context = fresh_flow_context
        try:
            t = Task("double", func=lambda x: x * 2)
            refs = t.map([])
            assert refs == []
        finally:
            _context_local.flow_context = None

    def test_options_propagated(
        self, reset_flow_context_local, fresh_flow_context
    ):
        from actant.flow import _context_local

        _context_local.flow_context = fresh_flow_context
        try:
            t = Task("double", func=lambda x: x * 2)
            refs = t.map([1, 2], priority="high", timeout=10.0, tags=["gpu"])
            for r in refs:
                assert r.priority == "high"
                assert r.timeout == 10.0
                assert r.tags == ["gpu"]
        finally:
            _context_local.flow_context = None


# ---------------------------------------------------------------------------
# Task.reduce
# ---------------------------------------------------------------------------


class TestTaskReduce:
    """Task.reduce chord 语义。"""

    def test_outside_flow_calls_aggregator(self, reset_flow_context_local):
        t = Task("sum", func=lambda items: sum(items))
        # flow 外：refs 是实际值
        assert t.reduce([1, 2, 3, 4]) == 10

    def test_inside_flow_creates_chord(
        self, reset_flow_context_local, fresh_flow_context
    ):
        """flow 内：reduce 创建 callback TaskRef，args=[refs]。"""
        from actant.flow import _context_local

        _context_local.flow_context = fresh_flow_context
        try:
            t = Task("sum", func=lambda items: sum(items))
            # 先创建上游 refs
            producer = Task("double", func=lambda x: x * 2)
            upstream = producer.map([1, 2, 3])
            assert fresh_flow_context.ref_count == 3

            # reduce 创建 chord callback
            callback = t.reduce(upstream)
            assert isinstance(callback, TaskRef)
            assert callback.task_name == "sum"
            # args 是 [refs]，chord 模式
            assert isinstance(callback.args, tuple)
            assert len(callback.args) == 1
            assert isinstance(callback.args[0], list)
            assert callback.args[0] == list(upstream)
            # 4 个 ref：3 个上游 + 1 个 callback
            assert fresh_flow_context.ref_count == 4
        finally:
            _context_local.flow_context = None

    def test_options_propagated_to_callback(
        self, reset_flow_context_local, fresh_flow_context
    ):
        from actant.flow import _context_local

        _context_local.flow_context = fresh_flow_context
        try:
            t = Task("sum", func=lambda items: sum(items))
            producer = Task("double", func=lambda x: x * 2)
            upstream = producer.map([1, 2])
            callback = t.reduce(
                upstream, priority="critical", timeout=30.0, tags=["aggregator"]
            )
            assert callback.priority == "critical"
            assert callback.timeout == 30.0
            assert callback.tags == ["aggregator"]
        finally:
            _context_local.flow_context = None


# ---------------------------------------------------------------------------
# 全局任务注册表
# ---------------------------------------------------------------------------


class TestGlobalTaskRegistry:
    """全局任务注册表：register / get / clear / 线程安全。"""

    def test_register_and_get(self, reset_global_task_registry):
        t = Task("my_task", func=lambda: None)
        register_global_task(t)
        assert get_global_task("my_task") is t
        assert "my_task" in get_global_tasks()

    def test_get_nonexistent_returns_none(self, reset_global_task_registry):
        assert get_global_task("nonexistent") is None

    def test_clear(self, reset_global_task_registry):
        t = Task("t", func=lambda: None)
        register_global_task(t)
        assert "t" in get_global_tasks()
        clear_global_tasks()
        assert get_global_tasks() == {}

    def test_register_overwrites(self, reset_global_task_registry):
        """重复注册同名 task 会覆盖。"""
        t1 = Task("t", func=lambda: 1)
        t2 = Task("t", func=lambda: 2)
        register_global_task(t1)
        register_global_task(t2)
        assert get_global_task("t") is t2

    def test_concurrent_register_safe(self, reset_global_task_registry):
        """多线程并发注册不同 task 不丢失。"""
        tasks = [Task(f"t_{i}", func=lambda i=i: i) for i in range(20)]

        def _register_batch(batch):
            for t in batch:
                register_global_task(t)

        threads = []
        for i in range(4):
            batch = tasks[i * 5 : (i + 1) * 5]
            threads.append(threading.Thread(target=_register_batch, args=(batch,)))
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        registered = get_global_tasks()
        assert len(registered) == 20
        for i in range(20):
            assert f"t_{i}" in registered


# ---------------------------------------------------------------------------
# _task_origin
# ---------------------------------------------------------------------------


class TestTaskOrigin:
    """_task_origin 返回 task func 的来源描述（用于错误诊断）。"""

    def test_returns_string(self, reset_flow_context_local):
        t = Task("my_task", func=lambda: None)
        origin = _task_origin(t)
        assert isinstance(origin, str)
        assert len(origin) > 0

    def test_reflects_call_site(self, reset_flow_context_local):
        """_task_origin 反映 func 的定义位置，可用于诊断。"""
        t = Task("my_task", func=lambda: None)
        origin = _task_origin(t)
        # lambda 的 origin 通常是所在测试函数路径
        assert "test_task" in origin or "lambda" in origin


# ---------------------------------------------------------------------------
# Task.apply
# ---------------------------------------------------------------------------


class TestTaskApply:
    """Task.apply 本地直接执行。"""

    def test_basic_execution(self, reset_flow_context_local):
        t = Task("add", func=lambda a, b: a + b)
        assert t.apply(1, 2) == 3

    def test_kwargs(self, reset_flow_context_local):
        t = Task("greet", func=lambda name, greeting: f"{greeting}, {name}")
        assert t.apply("world", greeting="hi") == "hi, world"

    def test_no_func_raises(self, reset_flow_context_local):
        t = Task("noop", func=None)
        with pytest.raises(InvalidStateError):
            t.apply()


# ---------------------------------------------------------------------------
# _run_sync_or_async（async 分支覆盖）
# ---------------------------------------------------------------------------


class TestRunSyncOrAsync:
    """_run_sync_or_async 处理同步/async 函数的三种场景。"""

    def test_sync_function_direct_call(self):
        """同步函数直接调用。"""
        from actant.task import _run_sync_or_async

        def add(a, b):
            return a + b

        assert _run_sync_or_async(add, 1, 2) == 3

    def test_async_function_without_running_loop(self):
        """async 函数，无运行中的事件循环 → asyncio.run()。"""
        from actant.task import _run_sync_or_async

        async def fetch():
            return "async-result"

        # 在主线程无运行 loop 时调用
        assert _run_sync_or_async(fetch) == "async-result"

    def test_async_function_with_running_loop(self):
        """async 函数，已有运行中的事件循环 → 独立线程 asyncio.run()。"""
        import asyncio

        from actant.task import _run_sync_or_async

        async def fetch():
            return "in-thread-result"

        async def driver():
            # 在已有运行 loop 的上下文中调用 _run_sync_or_async
            return _run_sync_or_async(fetch)

        result = asyncio.run(driver())
        assert result == "in-thread-result"


# ---------------------------------------------------------------------------
# Task.__repr__
# ---------------------------------------------------------------------------


class TestTaskRepr:
    """Task.__repr__ 应返回可读字符串。"""

    def test_repr_format(self, reset_flow_context_local):
        t = Task("my_task", func=lambda: None)
        repr_str = repr(t)
        assert "Task" in repr_str
        assert "my_task" in repr_str


# ---------------------------------------------------------------------------
# _task_origin 的 None func 分支
# ---------------------------------------------------------------------------


class TestTaskOriginNoFunc:
    """_task_origin 当 func 为 None 时返回 '<no-func>'。"""

    def test_no_func_returns_placeholder(self, reset_flow_context_local):
        t = Task("noop", func=None)
        origin = _task_origin(t)
        assert origin == "<no-func>"


# ---------------------------------------------------------------------------
# 重复注册同名 task 的警告
# ---------------------------------------------------------------------------


class TestDuplicateRegistrationWarning:
    """不同 func 同名注册应发出 UserWarning。"""

    def test_warns_on_duplicate_name_different_func(
        self, reset_global_task_registry, recwarn
    ):
        import warnings

        # 第一个 task — 用命名函数以获得唯一 qualname
        def first_impl():
            return 1

        t1 = Task("dup_name", func=first_impl)
        register_global_task(t1)

        # 第二个同名 task（不同命名函数，不同 origin）
        def second_impl():
            return 2

        t2 = Task("dup_name", func=second_impl)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            register_global_task(t2)

        # 应触发警告
        assert any(
            "already registered" in str(warning.message) for warning in w
        ), f"expected duplicate registration warning, got: {[str(x.message) for x in w]}"

    def test_same_func_reimport_no_warning(
        self, reset_global_task_registry, recwarn
    ):
        """同 origin 重复注册不触发警告。"""
        import warnings

        def fn():
            return None
        t1 = Task("same_name", func=fn)
        register_global_task(t1)
        t2 = Task("same_name", func=fn)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            register_global_task(t2)
        # 同 origin 不警告
        assert not any(
            "already registered" in str(warning.message) for warning in w
        )


# ---------------------------------------------------------------------------
# _make_task 的 register=False 分支
# ---------------------------------------------------------------------------


class TestMakeTaskNoRegister:
    """_make_task(register=False) 不注册到全局表。"""

    def test_register_false_skips_global_registry(
        self, reset_global_task_registry, reset_flow_context_local
    ):
        from actant.task import _make_task, get_global_tasks

        def my_fn():
            return "result"

        # register=False 不应注册
        t = _make_task(my_fn, name="no_reg_task", register=False)
        assert t.name == "no_reg_task"
        assert "no_reg_task" not in get_global_tasks()

    def test_register_true_default_registers(
        self, reset_global_task_registry, reset_flow_context_local
    ):
        from actant.task import _make_task, get_global_task

        def my_fn2():
            return "result"

        t = _make_task(my_fn2, name="reg_task")
        assert get_global_task("reg_task") is t
