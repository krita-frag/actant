"""``actant._internal.tasks.worker`` 运行时部件单元测试（进程内驱动，不 spawn 子进程）。

覆盖 worker 进程除帧编解码以外的全部运行时职责：序列化复用器、stderr 边带
（指标 / 任务日志 / stdout 文本层）、取消桥接、日志捕获作用域、dispatch 执行与
结果打包、stdin 读取线程、进程状态恢复，以及 ``main()`` 主循环。

帧编解码（``_read_frame`` / ``_write_frame`` / ``_writev_all``）见
``test_worker_frames.py``；真实子进程路径见 e2e 用例。
"""
from __future__ import annotations

import contextlib
import io
import logging
import os
import queue
import sys
import threading
from collections.abc import Iterator
from typing import Any

import pytest

from actant._internal.tasks import worker as w
from actant._internal.tasks.helpers import _build_v2_envelope
from actant.exceptions import ActantError, TaskCancelledError

# ───────────────────────── 辅助 ─────────────────────────


@contextlib.contextmanager
def _sideband() -> Iterator[io.StringIO]:
    """把 ``sys.stderr`` 临时换成内存流，用于断言 stderr 边带内容。

    必须在用例体内进入，不能做成 fixture：pytest 的 fd 级捕获会在 fixture
    setup 阶段把 ``sys.stderr`` 重新绑定为 ``EncodedFile``，覆盖 fixture 里
    打的补丁，结果写入照旧落到真实 fd 2。
    """
    buf = io.StringIO()
    original = sys.stderr
    sys.stderr = buf
    try:
        yield buf
    finally:
        sys.stderr = original


def _frame(msg_type: int, body: bytes = b"") -> bytes:
    """按 worker 帧格式编码一帧（``[4B 长度][1B 类型][正文]``）。"""
    return w._FRAME_HEADER.pack(1 + len(body), msg_type) + body


def _dispatch_body(
    func: Any,
    args: tuple[Any, ...] = (),
    kwargs: dict[str, Any] | None = None,
    *,
    task_id: str = "task-1",
    workflow_id: str = "wf-1",
    retries: int = 0,
    retry_delay_ms: int = 0,
) -> bytes:
    """用生产侧的 envelope 构造器生成 v2 Dispatch 正文。"""
    return _build_v2_envelope(
        func,
        args,
        dict(kwargs or {}),
        {
            "task_id": task_id,
            "workflow_id": workflow_id,
            "retries": retries,
            "retry_delay_ms": retry_delay_ms,
        },
        task_id,
    )


def _parse_frames(data: bytes) -> list[tuple[int, bytes]]:
    """把 worker 输出流解析回 ``(类型, 正文)`` 列表。"""
    frames: list[tuple[int, bytes]] = []
    pos = 0
    while pos < len(data):
        length, msg_type = w._FRAME_HEADER.unpack_from(data, pos)
        pos += w._FRAME_HEADER.size
        frames.append((msg_type, data[pos : pos + length - 1]))
        pos += length - 1
    return frames


class _FakeStd:
    """替身标准流：只暴露 ``.buffer``，供 ``main()`` 取 raw 二进制层。"""

    def __init__(self, stream: Any) -> None:
        self.buffer = stream


class _FakeTextStream:
    """替身文本流：记录写入与 flush 次数，用于验证代理的属性透传。"""

    encoding = "utf-8"

    def __init__(self) -> None:
        self.text = ""
        self.flushes = 0

    def write(self, s: str) -> int:
        self.text += s
        return len(s)

    def flush(self) -> None:
        self.flushes += 1


# 跨进程载荷经 cloudpickle 按值序列化，闭包计数器在反序列化副本里不可见；
# 用模块级函数 + 模块级计数器，cloudpickle 按引用序列化后与原对象共享状态。
_RETRY_CALLS: list[int] = []


def _flaky_until_three_calls() -> str:
    _RETRY_CALLS.append(1)
    if len(_RETRY_CALLS) < 3:
        raise ValueError("not yet")
    return "ok"


# ───────────────────────── 序列化复用器 ─────────────────────────


def test_reuse_pack_unpack_roundtrip() -> None:
    payload = {"a": [1, 2, 3], "b": "文本"}
    assert w._reuse_unpack(w._reuse_pack(payload)) == payload


def test_reuse_pack_clears_memo_between_calls() -> None:
    """复用 Pickler 时 memo 必须每轮清空，否则跨任务的对象 id 会串味。"""
    a = w._reuse_pack({"k": 1})
    b = w._reuse_pack({"k": 2})
    assert w._reuse_unpack(a) == {"k": 1}
    assert w._reuse_unpack(b) == {"k": 2}
    # 同一输入重复序列化结果一致，说明输出不依赖上一轮残留状态。
    assert w._reuse_pack({"k": 1}) == a


def test_reuse_unpack_handles_empty_buffer_state() -> None:
    """连续反序列化不得残留上一轮读取位置。"""
    first = w._reuse_pack(("x", 1))
    second = w._reuse_pack(("y", 2))
    assert w._reuse_unpack(first) == ("x", 1)
    assert w._reuse_unpack(second) == ("y", 2)


# ───────────────────────── stderr 边带 ─────────────────────────


def test_emit_metric_writes_prefixed_line() -> None:
    with _sideband() as err:
        w._emit_metric("task.handler_ms", 12)
    assert err.getvalue() == "actant_metric: task.handler_ms=12\n"


def test_elapsed_ms_truncates_to_integer() -> None:
    """毫秒口径与 Rust 侧 ``as_millis()`` 一致：截断而非四舍五入。"""
    assert w._elapsed_ms(0.1239) == 123
    assert w._elapsed_ms(0.0) == 0


def test_emit_task_log_splits_multiline() -> None:
    with _sideband() as err:
        w._emit_task_log("t-1", "STDOUT", "line1\nline2")
    lines = err.getvalue().splitlines()
    assert len(lines) == 2
    assert all(line.startswith("actant_log: ") for line in lines)
    assert lines[0].endswith("STDOUT t-1 line1")
    assert lines[1].endswith("STDOUT t-1 line2")


def test_emit_task_log_timestamp_is_ms_epoch() -> None:
    with _sideband() as err:
        w._emit_task_log("t-1", "WARNING", "m")
    ts = err.getvalue().split()[1]
    assert ts.isdigit()
    assert len(ts) == 13  # 毫秒时间戳，不是秒


def test_emit_task_log_empty_message_still_emits_one_line() -> None:
    with _sideband() as err:
        w._emit_task_log("t-1", "STDOUT", "")
    assert len(err.getvalue().splitlines()) == 1


# ───────────────────────── 取消桥接 ─────────────────────────


def test_cancel_holder_sets_current_event() -> None:
    holder = w._CancelHolder()
    evt = threading.Event()
    holder.set_current(evt)
    holder.cancel()
    assert evt.is_set()


def test_cancel_holder_defers_cancel_arriving_before_dispatch() -> None:
    """Rust 派发后立刻取消、而 worker 尚未取帧：预取消必须落到该任务上。"""
    holder = w._CancelHolder()
    holder.cancel()
    evt = threading.Event()
    holder.set_current(evt)
    assert evt.is_set()


def test_cancel_holder_does_not_touch_settled_task() -> None:
    """任务结算后到达的 Cancel 不得回写已完成任务的取消事件。"""
    holder = w._CancelHolder()
    evt = threading.Event()
    holder.set_current(evt)
    holder.clear_current()
    holder.cancel()
    assert not evt.is_set()


def test_worker_cancel_token_reflects_event() -> None:
    evt = threading.Event()
    token = w._WorkerCancelToken(evt)
    assert token.is_cancelled() is False
    evt.set()
    assert token.is_cancelled() is True


# ───────────────────────── stdout 文本层代理 ─────────────────────────


def test_stdout_proxy_emits_only_complete_lines() -> None:
    with _sideband() as err:
        proxy = w._StdoutLogProxy(_FakeTextStream(), "t-1")
        assert proxy.write("a\nb") == 3
        assert [line.rsplit(" ", 1)[-1] for line in err.getvalue().splitlines()] == ["a"]
        proxy.write("c\n")
        assert [line.rsplit(" ", 1)[-1] for line in err.getvalue().splitlines()] == [
            "a",
            "bc",
        ]


def test_stdout_proxy_flush_emits_partial_line() -> None:
    with _sideband() as err:
        proxy = w._StdoutLogProxy(_FakeTextStream(), "t-1")
        proxy.write("tail")
        assert err.getvalue() == ""
        proxy.flush()
    assert err.getvalue().endswith("STDOUT t-1 tail\n")


def test_stdout_proxy_flush_without_buffer_is_noop() -> None:
    real = _FakeTextStream()
    with _sideband() as err:
        proxy = w._StdoutLogProxy(real, "t-1")
        proxy.flush()
    assert err.getvalue() == ""
    assert real.flushes == 1


def test_stdout_proxy_flush_with_empty_fragment_is_noop() -> None:
    """只写下空串时缓冲里是空片段：flush 不得发出一条空日志行。"""
    real = _FakeTextStream()
    with _sideband() as err:
        proxy = w._StdoutLogProxy(real, "t-1")
        proxy.write("")
        proxy.flush()
    assert err.getvalue() == ""
    assert real.flushes == 1


def test_stdout_proxy_passes_through_unknown_attributes() -> None:
    proxy = w._StdoutLogProxy(_FakeTextStream(), "t-1")
    assert proxy.encoding == "utf-8"


# ───────────────────────── 日志捕获作用域 ─────────────────────────


def test_task_log_scope_captures_warning() -> None:
    before = sys.stdout
    with _sideband() as err, w._TaskLogScope("t-9"):
        logging.getLogger("actant.test").warning("boom")
        assert sys.stdout is not before  # 任务期间 stdout 被代理接管
    assert sys.stdout is before
    assert "WARNING t-9 actant.test: boom" in err.getvalue()


def test_task_log_scope_ignores_info_level(monkeypatch: pytest.MonkeyPatch) -> None:
    """handler 级别取 WARNING：INFO 级框架日志不进边带，避免任务粒度不可控。"""
    monkeypatch.setattr(logging.getLogger(), "level", logging.DEBUG)
    with _sideband() as err, w._TaskLogScope("t-10"):
        logging.getLogger("actant.test").info("quiet")
    assert err.getvalue() == ""


def test_task_log_scope_removes_handler_on_exit() -> None:
    root = logging.getLogger()
    before = list(root.handlers)
    with _sideband(), w._TaskLogScope("t-11"):
        assert len(root.handlers) == len(before) + 1
    assert root.handlers == before


def test_task_log_scope_flushes_partial_stdout_on_exit() -> None:
    """退出时先 flush：任务末尾无换行的输出不能随代理一起被丢弃。"""
    with _sideband() as err, w._TaskLogScope("t-12"):
        sys.stdout.write("no-newline")
    assert "STDOUT t-12 no-newline" in err.getvalue()


def test_task_log_scope_falls_back_when_formatter_raises() -> None:
    """记录格式化失败时退化为原始 message，不能因此丢掉整条日志。"""

    class _BoomFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            raise RuntimeError("bad format")

    with _sideband() as err:
        scope = w._TaskLogScope("t-15")
        with scope:
            assert scope._handler is not None
            scope._handler.setFormatter(_BoomFormatter())
            logging.getLogger("actant.test").warning("raw message")
    assert "WARNING t-15 raw message" in err.getvalue()


def test_task_log_scope_exit_tolerates_replaced_stdout() -> None:
    """任务代码自己换掉 ``sys.stdout`` 时，退出不得因「已不是代理」而崩溃。"""
    scope = w._TaskLogScope("t-16")
    real = scope.__enter__()
    try:
        sys.stdout = io.StringIO()  # 模拟任务内替换
    finally:
        real.__exit__()
    assert not isinstance(sys.stdout, w._StdoutLogProxy)


# ───────────────────────── dispatch 执行 ─────────────────────────


def test_run_dispatch_packs_success_result() -> None:
    with _sideband():
        result = w._run_dispatch(_dispatch_body(lambda: 41 + 1), threading.Event())
    assert w._reuse_unpack(result) == (True, 42)


def test_run_dispatch_passes_args_and_kwargs() -> None:
    with _sideband():
        result = w._run_dispatch(
            _dispatch_body(lambda a, b=0: a * b, (6,), {"b": 7}), threading.Event()
        )
    assert w._reuse_unpack(result) == (True, 42)


def test_run_dispatch_reports_deserialization_failure() -> None:
    """头解析失败必须转成可序列化的 serialization 错误，而不是抛出中断协议循环。"""
    with _sideband():
        result = w._run_dispatch(b"\x02\x00\x00", threading.Event())
    success, payload = w._reuse_unpack(result)
    assert success is False
    assert isinstance(payload, ActantError)
    assert payload.kind == "serialization"
    assert "failed to deserialize payload" in str(payload)


def test_run_dispatch_rejects_unknown_payload_version() -> None:
    body = bytearray(_dispatch_body(lambda: None))
    body[0] = 0x7F  # 破坏 version 字节
    with _sideband():
        result = w._run_dispatch(bytes(body), threading.Event())
    success, payload = w._reuse_unpack(result)
    assert success is False
    assert isinstance(payload, ActantError)


def test_run_dispatch_honours_pre_cancel() -> None:
    evt = threading.Event()
    evt.set()
    called: list[int] = []
    body = _dispatch_body(lambda: called.append(1))
    with _sideband():
        result = w._run_dispatch(body, evt)
    success, payload = w._reuse_unpack(result)
    assert success is False
    assert isinstance(payload, TaskCancelledError)
    assert called == []


def test_run_dispatch_retries_before_succeeding() -> None:
    _RETRY_CALLS.clear()
    with _sideband():
        result = w._run_dispatch(
            _dispatch_body(_flaky_until_three_calls, retries=2), threading.Event()
        )
    assert w._reuse_unpack(result) == (True, "ok")
    assert len(_RETRY_CALLS) == 3


def test_run_dispatch_returns_last_error_after_retries_exhausted() -> None:
    def always_fails() -> None:
        raise ValueError("always")

    with _sideband():
        result = w._run_dispatch(_dispatch_body(always_fails, retries=1), threading.Event())
    success, payload = w._reuse_unpack(result)
    assert success is False
    assert isinstance(payload, ValueError)


def test_run_dispatch_emits_handler_metric() -> None:
    with _sideband() as err:
        w._run_dispatch(_dispatch_body(lambda: None), threading.Event())
    assert "actant_metric: task.handler_ms=" in err.getvalue()


def test_run_dispatch_routes_task_stdout_to_sideband() -> None:
    """任务里的 print 不能污染帧通道，必须走 stderr 边带。"""

    def noisy() -> None:
        print("hello from task")

    with _sideband() as err:
        w._run_dispatch(_dispatch_body(noisy), threading.Event())
    assert "STDOUT" in err.getvalue()
    assert "hello from task" in err.getvalue()


# ───────────────────────── 结果打包 ─────────────────────────


def test_pack_result_roundtrips_plain_payload() -> None:
    assert w._reuse_unpack(w._pack_result(True, {"n": 1})) == (True, {"n": 1})


def test_pack_result_degrades_when_payload_unserializable() -> None:
    """结果不可序列化时退化为 RuntimeError，而不是让结果帧写不出去。"""
    success, payload = w._reuse_unpack(w._pack_result(True, threading.Lock()))
    assert success is False
    assert isinstance(payload, RuntimeError)
    assert "unserializable result" in str(payload)


def test_ensure_exception_keeps_picklable_instance() -> None:
    exc = ValueError("v")
    assert w._ensure_exception(exc) is exc


def test_ensure_exception_wraps_unpicklable_instance() -> None:
    exc = ValueError("v")
    exc.attr = threading.Lock()  # type: ignore[attr-defined]  # 不可序列化的附加属性
    wrapped = w._ensure_exception(exc)
    assert isinstance(wrapped, RuntimeError)
    assert "ValueError: v" in str(wrapped)


# ───────────────────────── stdin 读取线程 ─────────────────────────


def _run_reader(
    stdin: io.BytesIO, holder: w._CancelHolder | None = None
) -> tuple[queue.SimpleQueue[bytes | None], threading.Event]:
    """同步跑一遍读取循环，返回其队列与关停信号。"""
    q: queue.SimpleQueue[bytes | None] = queue.SimpleQueue()
    shutdown = threading.Event()
    w._reader_loop(stdin, q, holder or w._CancelHolder(), shutdown)
    return q, shutdown


def test_reader_loop_queues_dispatch_then_stops_on_shutdown() -> None:
    stdin = io.BytesIO(_frame(w.FRAME_DISPATCH, b"body") + _frame(w.FRAME_SHUTDOWN))
    q, shutdown = _run_reader(stdin)
    assert q.get() == b"body"
    assert q.get() is None
    assert shutdown.is_set()


def test_reader_loop_routes_cancel_frame_to_current_task() -> None:
    evt = threading.Event()
    holder = w._CancelHolder()
    holder.set_current(evt)
    stdin = io.BytesIO(_frame(w.FRAME_CANCEL) + _frame(w.FRAME_SHUTDOWN))
    _run_reader(stdin, holder)
    assert evt.is_set()


def test_reader_loop_treats_eof_as_shutdown() -> None:
    q, shutdown = _run_reader(io.BytesIO(b""))
    assert shutdown.is_set()
    assert q.get() is None


def test_reader_loop_skips_unknown_frame_type() -> None:
    """未知类型既不改变状态也不终止循环：只有三种已知帧参与状态机。"""
    stdin = io.BytesIO(
        _frame(0x7F, b"ignored")
        + _frame(w.FRAME_DISPATCH, b"body")
        + _frame(w.FRAME_SHUTDOWN)
    )
    q, shutdown = _run_reader(stdin)
    assert q.get() == b"body"  # 未知帧没有入队
    assert q.get() is None
    assert shutdown.is_set()


def test_reader_loop_reports_corrupt_frame_length() -> None:
    stdin = io.BytesIO(w._FRAME_HEADER.pack(0, w.FRAME_DISPATCH))
    with _sideband() as err:
        q, shutdown = _run_reader(stdin)
    assert "frame read failed" in err.getvalue()
    assert "invalid frame length" in err.getvalue()
    assert shutdown.is_set()
    assert q.get() is None


def test_reader_loop_reports_truncated_header() -> None:
    with _sideband() as err:
        _, shutdown = _run_reader(io.BytesIO(b"\x01\x02"))
    assert "truncated frame header" in err.getvalue()
    assert shutdown.is_set()


# ───────────────────────── 进程状态恢复 ─────────────────────────


def test_restore_process_state_resets_cwd(tmp_path: Any) -> None:
    original = os.getcwd()
    try:
        os.chdir(str(tmp_path))
        assert os.getcwd() == str(tmp_path)
        w._restore_process_state(original, dict(os.environ))
        assert os.getcwd() == original
    finally:
        os.chdir(original)


def test_restore_process_state_drops_added_and_restores_changed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ACTANT_EXISTING_KEY", "origin")
    baseline = dict(os.environ)
    try:
        os.environ["ACTANT_LEAKED_KEY"] = "leak"
        os.environ["ACTANT_EXISTING_KEY"] = "mutated"
        w._restore_process_state(os.getcwd(), baseline)
        assert "ACTANT_LEAKED_KEY" not in os.environ
        assert os.environ["ACTANT_EXISTING_KEY"] == "origin"
    finally:
        os.environ.pop("ACTANT_LEAKED_KEY", None)


def test_restore_process_state_reports_chdir_failure() -> None:
    with _sideband() as err:
        w._restore_process_state("/nonexistent/actant/restore-target", dict(os.environ))
    assert "failed to restore cwd" in err.getvalue()


# ───────────────────────── main() 主循环 ─────────────────────────


def _run_main(
    monkeypatch: pytest.MonkeyPatch, frames: bytes
) -> tuple[int, bytes, str]:
    """用内存流跑一遍 ``main()``，返回 ``(退出码, stdout 原始字节, stderr 文本)``。"""
    out = io.BytesIO()
    err = io.StringIO()
    monkeypatch.setattr(sys, "stdin", _FakeStd(io.BytesIO(frames)))
    monkeypatch.setattr(sys, "stdout", _FakeStd(out))
    monkeypatch.setattr(sys, "stderr", err)
    code = w.main()
    return code, out.getvalue(), err.getvalue()


def test_main_writes_one_result_frame_per_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frames = _frame(w.FRAME_DISPATCH, _dispatch_body(lambda: 7)) + _frame(
        w.FRAME_SHUTDOWN
    )
    code, out, _ = _run_main(monkeypatch, frames)
    assert code == 0
    parsed = _parse_frames(out)
    assert len(parsed) == 1
    assert parsed[0][0] == w.FRAME_RESULT
    assert w._reuse_unpack(parsed[0][1]) == (True, 7)


def test_main_handles_several_dispatches_in_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frames = (
        b"".join(
            _frame(w.FRAME_DISPATCH, _dispatch_body(lambda n=n: n, (n,)))
            for n in range(3)
        )
        + _frame(w.FRAME_SHUTDOWN)
    )
    code, out, _ = _run_main(monkeypatch, frames)
    assert code == 0
    results = [w._reuse_unpack(body) for _, body in _parse_frames(out)]
    assert results == [(True, 0), (True, 1), (True, 2)]


def test_main_exits_zero_on_immediate_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    code, out, _ = _run_main(monkeypatch, _frame(w.FRAME_SHUTDOWN))
    assert code == 0
    assert out == b""


def test_main_converts_unexpected_exception_to_failure_frame(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """执行路径抛出的任何异常都必须变成失败结果帧，不能让协议循环断掉。"""

    def boom(payload: bytes, cancel_event: threading.Event) -> bytes:
        raise RuntimeError("kaboom")

    monkeypatch.setattr(w, "_run_dispatch", boom)
    frames = _frame(w.FRAME_DISPATCH, b"x") + _frame(w.FRAME_SHUTDOWN)
    code, out, _ = _run_main(monkeypatch, frames)
    assert code == 0
    parsed = _parse_frames(out)
    success, payload = w._reuse_unpack(parsed[0][1])
    assert success is False
    assert isinstance(payload, RuntimeError)
    assert "kaboom" in str(payload)


def test_main_returns_one_when_result_write_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """stdout 已断开（父进程停止 / 强杀）：上报无门，直接以退出码 1 结束。"""

    def broken(stream: Any, msg_type: int, body: bytes = b"") -> None:
        raise BrokenPipeError(32, "Broken pipe")

    monkeypatch.setattr(w, "_write_frame", broken)
    frames = _frame(w.FRAME_DISPATCH, _dispatch_body(lambda: None))
    code, _, err = _run_main(monkeypatch, frames)
    assert code == 1
    assert "stdout write failed" in err


def test_main_restores_cwd_after_task_changes_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """同一 worker 进程承接多个任务：前一个任务的 chdir 不得污染后一个。"""
    before = os.getcwd()
    target = str(tmp_path)

    def sneaky() -> str:
        os.chdir(target)
        return os.getcwd()

    frames = (
        _frame(w.FRAME_DISPATCH, _dispatch_body(sneaky))
        + _frame(w.FRAME_DISPATCH, _dispatch_body(os.getcwd))
        + _frame(w.FRAME_SHUTDOWN)
    )
    try:
        code, out, _ = _run_main(monkeypatch, frames)
    finally:
        os.chdir(before)
    assert code == 0
    first, second = [w._reuse_unpack(body) for _, body in _parse_frames(out)]
    assert first == (True, target)
    assert second == (True, before)
