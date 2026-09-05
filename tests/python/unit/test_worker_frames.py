"""``actant.task._worker`` stdio 帧协议单元测试（不依赖 Runtime/子进程）。

覆盖：pipe 帧读写往返、帧长上限防线（协议损坏判定）、v2 dispatch 载荷截断
严格检查、writev 部分写失败回退语义。跨进程路径由 Python 集成测试与 e2e 派发
测试覆盖。
"""
from __future__ import annotations

import io
import os
import sys

import pytest

from actant.task._worker import (
    _FRAME_HEADER,
    _PAYLOAD_VERSION,
    _V2_HEADER_STRUCT,
    MAX_FRAME_BYTES,
    _parse_dispatch_payload,
    _read_frame,
    _write_frame,
    _writev_all,
    _WriteVError,
)

# Windows 下 ``os.writev`` 仅 POSIX 可用（生产 ``_write_frame`` 已有无 writev 回退）。
# 依赖 writev 原语的用例在 Windows 跳过，其余用例作为 Windows CI 冒烟。
_windows_skip = pytest.mark.skipif(
    sys.platform == "win32",
    reason="依赖 POSIX os.writev 语义",
)


def _readn(f: io.FileIO, n: int) -> bytes:
    """从 raw fd 流精确读取 ``n`` 字节（FileIO.read 对管道可能短读）。"""
    buf = bytearray(n)
    view = memoryview(buf)
    got = 0
    while got < n:
        r = f.readinto(view[got:])
        if not r:
            raise EOFError("pipe closed before frame complete")
        got += r
    return bytes(buf)


# ───────────────────────── 帧语义（stdio pipe） ─────────────────────────


@_windows_skip
def test_dispatch_frame_pipe_roundtrip() -> None:
    """父端经 pipe 写 Dispatch 帧；worker 端 `_read_frame` 完整读出类型与正文。"""
    p_r, p_w = os.pipe()
    parent_side = io.FileIO(p_w, "wb")
    child_side = io.FileIO(p_r, "rb")
    try:
        body = b"dispatch-body-" * 8
        _writev_all(
            parent_side.fileno(),
            _FRAME_HEADER.pack(1 + len(body), 0x01),
            body,
        )
        parent_side.flush()

        frame = _read_frame(child_side)
        assert frame is not None
        assert frame[0] == 0x01
        assert frame[1:] == body
    finally:
        child_side.close()
        parent_side.close()


@_windows_skip
def test_write_frame_roundtrip_via_pipe() -> None:
    """`_write_frame` 写出的帧（帧头 + 正文）可被对端完整读回。"""
    p_r, p_w = os.pipe()
    child_out = io.FileIO(p_w, "wb")
    parent_in = io.FileIO(p_r, "rb")
    try:
        result = b"result-body-" * 8
        _write_frame(child_out, 0x02, result)

        hdr = _readn(parent_in, _FRAME_HEADER.size)
        length, msg_type = _FRAME_HEADER.unpack(hdr)
        assert msg_type == 0x02
        assert length == 1 + len(result)
        assert _readn(parent_in, length - 1) == result
    finally:
        child_out.close()
        parent_in.close()


@_windows_skip
def test_write_frame_empty_body() -> None:
    """空正文帧（Cancel）：只写 5 字节帧头。"""
    p_r, p_w = os.pipe()
    child_out = io.FileIO(p_w, "wb")
    parent_in = io.FileIO(p_r, "rb")
    try:
        _write_frame(child_out, 0x02, b"")
        hdr = _readn(parent_in, _FRAME_HEADER.size)
        length, msg_type = _FRAME_HEADER.unpack(hdr)
        assert msg_type == 0x02
        assert length == 1
    finally:
        child_out.close()
        parent_in.close()


# ───────────────────────── 帧长度上限（协议损坏判定） ─────────────────────────


@_windows_skip
def test_read_frame_rejects_length_over_limit() -> None:
    """帧头长度超过 MAX_FRAME_BYTES 视为协议损坏，抛 ValueError 而非巨量分配。"""
    p_r, p_w = os.pipe()
    child_side = io.FileIO(p_r, "rb")
    parent_side = io.FileIO(p_w, "wb")
    try:
        parent_side.write(_FRAME_HEADER.pack(MAX_FRAME_BYTES + 1, 0x01))
        parent_side.flush()
        with pytest.raises(ValueError, match="exceeds protocol limit"):
            _read_frame(child_side)
    finally:
        child_side.close()
        parent_side.close()


@_windows_skip
def test_read_frame_accepts_length_under_limit() -> None:
    """长度在 MAX_FRAME_BYTES 内的正常帧不受上限检查影响。"""
    p_r, p_w = os.pipe()
    child_side = io.FileIO(p_r, "rb")
    parent_side = io.FileIO(p_w, "wb")
    try:
        body = b"ok"
        parent_side.write(_FRAME_HEADER.pack(1 + len(body), 0x01))
        parent_side.write(body)
        parent_side.flush()
        frame = _read_frame(child_side)
        assert frame is not None
        assert frame[0] == 0x01
        assert frame[1:] == body
    finally:
        child_side.close()
        parent_side.close()


# ───────────────────────── dispatch 载荷截断严格检查 ─────────────────────────


def _v2_body(task_id: bytes, workflow_id: bytes, func_payload: bytes) -> bytes:
    return (
        _V2_HEADER_STRUCT.pack(_PAYLOAD_VERSION, 0, 0, len(task_id))
        + task_id
        + len(workflow_id).to_bytes(2, "little")
        + workflow_id
        + func_payload
    )


def test_parse_dispatch_payload_rejects_truncated_task_id() -> None:
    """task_id 长度前缀超界：抛 ValueError 而非把后续字段字节当 task_id 解码。"""
    body = _v2_body(b"task-1", b"wf-1", b"payload")
    # 截到 fixed header + 2 字节（不足以容纳声明的 task_id 与 wid_len 头）。
    with pytest.raises(ValueError, match="task_id"):
        _parse_dispatch_payload(body[: _V2_HEADER_STRUCT.size + 2])


def test_parse_dispatch_payload_rejects_truncated_workflow_id() -> None:
    """workflow_id 长度前缀超界：抛 ValueError 而非静默截短。"""
    body = _v2_body(b"task-1", b"wf-1", b"payload")
    # 截到 workflow_id 字段中途（保住 task_id 与 wid_len 头，砍掉 wid 一部分）。
    tid_len = len(b"task-1")
    cut = _V2_HEADER_STRUCT.size + tid_len + 2 + 1
    with pytest.raises(ValueError, match="workflow_id"):
        _parse_dispatch_payload(body[:cut])


def test_parse_dispatch_payload_valid_body_unchanged() -> None:
    """完整载荷解析结果与字段一一对应（截断检查不改变正常路径行为）。"""
    body = _v2_body(b"task-1", b"wf-1", b"func-bytes")
    retries, delay_ms, task_id, workflow_id, rest = _parse_dispatch_payload(body)
    assert (retries, delay_ms) == (0, 0)
    assert task_id == "task-1"
    assert workflow_id == "wf-1"
    assert rest == b"func-bytes"


# ───────────────────────── writev 部分写失败回退语义 ─────────────────────────


@_windows_skip
def test_write_frame_falls_back_when_nothing_written(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """writev 首次调用即失败（未写出任何字节）：安全回退拼接重写，帧完整。"""
    p_r, p_w = os.pipe()
    child_out = io.FileIO(p_w, "wb")
    parent_in = io.FileIO(p_r, "rb")

    def _boom(fileno: int, bufs: list[bytes]) -> int:
        raise BrokenPipeError(32, "Broken pipe")

    monkeypatch.setattr(os, "writev", _boom)
    try:
        body = b"fallback-body"
        _write_frame(child_out, 0x02, body)
        hdr = _readn(parent_in, _FRAME_HEADER.size)
        length, msg_type = _FRAME_HEADER.unpack(hdr)
        assert msg_type == 0x02
        assert _readn(parent_in, length - 1) == body
    finally:
        child_out.close()
        parent_in.close()


@_windows_skip
def test_write_frame_raises_when_partial_bytes_written(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """writev 已写出部分字节后失败：不得回退重写（会重复字节），直接上抛。"""
    p_r, p_w = os.pipe()
    child_out = io.FileIO(p_w, "wb")
    parent_in = io.FileIO(p_r, "rb")

    big_body = b"x" * 200_000  # 超过管道容量，writev 必然经历多轮短写

    call_count = [0]

    def _partial_then_fail(fileno: int, bufs: list[bytes]) -> int:
        call_count[0] += 1
        if call_count[0] == 1:
            return 64 * 1024  # 第一次调用短写成功（部分字节已进 pipe）
        raise BrokenPipeError(32, "Broken pipe")

    monkeypatch.setattr(os, "writev", _partial_then_fail)
    try:
        with pytest.raises(_WriteVError) as excinfo:
            _write_frame(child_out, 0x02, big_body)
        # written 为失败前累计的短写字节数；_write_frame 不得回退重写。
        assert excinfo.value.written == 64 * 1024
        # writev 被 mock，pipe 实际为空：证明未发生全量重写。
        os.set_blocking(parent_in.fileno(), False)
        assert parent_in.read() is None
    finally:
        child_out.close()
        parent_in.close()
