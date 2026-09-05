"""worker 子进程循环（进程池执行后端）。

入口：``python -m actant.task._worker``。由 Rust `ProcessTaskDispatcher`
通过长度前缀二进制帧 IPC 驱动。子进程是纯 Python 解释器，不持有 Rust 核心、
不依赖 iroh / PyO3，职责收敛为「读帧 → 反序列化 → 执行 → 回传结果」。

帧协议（双向，Rust ↔ Python）：

- 传输层：stdio pipe，每帧为 ``[4 字节小端长度][1 字节类型][正文]`` 连续写入
  管道（与 Rust `ProcessTaskDispatcher` 一致）。
- Rust → Python：
  - ``Dispatch`` (0x01)：正文 = v2 载荷 = 紧凑控制头部 + ``cloudpickle(func, args,
    kwargs)``。头部布局见 ``_parse_dispatch_payload``；控制元数据（retries /
    retry_delay_ms / task_id / workflow_id）内联为二进制头部，仅函数载荷走 cloudpickle。
  - ``Cancel`` (0x02)：正文空。请求协作取消当前任务。
  - ``Shutdown`` (0x03)：正文空。优雅退出循环。
- Python → Rust：
  - ``Result`` (0x02)：正文 = ``cloudpickle.dumps((success, payload_obj))``，
    与线程池后端 generic handler 的返回格式完全一致，调用方无缝兼容。

序列化器复用：worker 单任务单线程模型（同一时刻至多执行一个任务），模块级复用
cloudpickle ``Pickler`` 绑定的 ``BytesIO`` 与读取用 ``BytesIO``，避免每任务新建序列化
器/缓冲。``Pickler`` 每次 dump 前 ``clear_memo()`` 防 memo 串扰；Unpickler 实例不复用
（跨 pickle 状态不安全），仅复用输入缓冲区经 ``cloudpickle.load`` 读入。

指标边带：子进程自身计时（如任务执行耗时）通过 stderr 单行上报，格式
``actant_metric: <name>=<value_ms>``。Rust 侧 ``drain_stderr`` 识别该前缀并
汇入对应 OTel histogram；其余 stderr 行原样转发为日志。任务得/失败由 Rust
在父进程依据 Result 帧与超时判定计入 counters，无需子进程上报。

取消模型：线程读取 stdin 期间持续处理 ``Cancel`` 帧；``Cancel`` 到达时设置
当前任务（或待分配的下一个任务）的取消事件。执行线程在 ``_execute_with_retries``
的尝试间检查点 / ``_interruptible_sleep`` 段内协作退出。Rust 侧负责硬超时后的
强杀回收，本进程只做协作配合。
"""

from __future__ import annotations

import io
import os
import queue
import struct
import sys
import threading
import time
from typing import Any

import cloudpickle

from actant.task._context import _DispatchTaskContext, _task_context_scope
from actant.task._helpers import (
    _PAYLOAD_VERSION,
    MAX_FRAME_BYTES,
    _execute_with_retries,
)

# 帧类型常量（与 Rust `ProcessTaskDispatcher` 保持一致）。
FRAME_DISPATCH = 0x01
FRAME_CANCEL = 0x02
FRAME_RESULT = 0x02
FRAME_SHUTDOWN = 0x03

# 帧头 = 4 字节小端长度 + 1 字节类型。
_FRAME_HEADER = struct.Struct("<IB")

# 帧长度上限（帧头长度字段为 u32，理论最大 4GiB-1）。Rust 端
# ``ProcessTaskDispatcher`` 对长度仅校验 >= 1、不设上界；此处选取 256MB 作为
# worker 帧协议上限（提交侧 `_safe_serialize` 共用，见 `MAX_FRAME_BYTES`）。


# 预分配的头缓冲（worker 主线程串行使用，读写循环共用）。
_HEADER_BUF = bytearray(_FRAME_HEADER.size)
_HEADER_MV = memoryview(_HEADER_BUF)


def _read_frame(stream: Any) -> bytes | None:
    """从 stdio 读一帧（``[4B 长度][1B 类型][正文]``）。到达 EOF 返回 ``None``。

    使用 ``readinto`` + ``memoryview``，避免 ``stream.read(n)`` 为每帧分配一个
    新 ``bytes`` 对象做 header、也省掉 `_read_exact` 的 chunk list + 最终 join
    拼接复制。正文大小 < 64KB 时走同一块栈上 ``bytearray``，否则按需重新分配。
    """
    n = _read_into(stream, _HEADER_MV)
    if n == 0:
        return None
    if n != _FRAME_HEADER.size:
        raise EOFError("truncated frame header")
    length, msg_type = _FRAME_HEADER.unpack(_HEADER_BUF)
    if length < 1:
        raise ValueError(f"invalid frame length: {length}")
    if length > MAX_FRAME_BYTES:
        # 长度字段超限 = 协议损坏（见 MAX_FRAME_BYTES 注释），按损坏帧处理：
        # 异常交由 _reader_loop 记 stderr 日志并置 shutdown_event 退出。
        raise ValueError(
            f"frame length {length} exceeds protocol limit {MAX_FRAME_BYTES}"
        )
    body = _read_exact(stream, length - 1)
    return bytes([msg_type]) + body


def _read_into(stream: Any, mv: memoryview) -> int:
    """填满 ``mv``，返回实际读入字节数（EOF = 0，截断 = EOFError）。

    与 ``_read_exact`` 不同：对固定小容量缓冲（如 frame header）按 memoryview 切片
    复用，避免每次都 ``read`` 产生一次性小 bytes 对象。
    """
    remaining = len(mv)
    while remaining:
        got = stream.readinto(mv[-remaining:])
        if got == 0:
            break
        remaining -= got
    return len(mv) - remaining


def _read_exact(stream: Any, n: int) -> bytes:
    """从流中读取精确 ``n`` 字节，用 ``bytearray`` + ``readinto`` 单次分配。

    去掉了旧的 chunk list + `b''.join` 拼接（每 chunk 一个中间 bytes 分配 +
    末尾一次 memcpy）。对于常见小载荷（< 4KB 任务元数据与结果）零额外分配。
    """
    buf = bytearray(n)
    mv = memoryview(buf)
    got_total = 0
    while got_total < n:
        got = stream.readinto(mv[got_total:])
        if not got:
            raise EOFError("unexpected EOF while reading frame body")
        got_total += got
    return bytes(buf)


class _WriteVError(OSError):
    """``os.writev`` 失败，携带失败前经 syscall 已写出的字节总数。

    ``written == 0``：未写出任何字节，调用方整体重写安全；
    ``written > 0``：已有部分字节进入 pipe，重写会重复这些字节腐蚀帧流，
    调用方必须放弃重写、直接上抛。
    """

    def __init__(self, written: int, reason: BaseException) -> None:
        super().__init__(f"writev failed after writing {written} byte(s): {reason!r}")
        self.written = written
        self.reason = reason


def _writev_all(fileno: int, *bufs: bytes) -> None:
    """把多个 buffer 全部写入 ``fileno``，处理 ``os.writev`` 的短写。

    管道容量（默认 64KB）小于大正文时，单次 ``os.writev`` 只写入前段；循环推进
    已写入偏移直到全部落盘，保证帧字节序列完整——与 Rust ``write_vectored`` 的
    完成语义一致。小帧（≤ PIPE_BUF）仍是一次 syscall。

    写失败时抛 ``_WriteVError``（``written`` 为失败前已写出的累计字节数）。
    """
    written_total = 0
    remaining = list(bufs)
    while remaining:
        try:
            written = os.writev(fileno, remaining)
        except OSError as exc:
            raise _WriteVError(written_total, exc) from exc
        if written == 0:
            raise _WriteVError(written_total, OSError("pipe write returned 0 bytes"))
        written_total += written
        # 推进已写入偏移：先耗尽前部完整写出的 buffer，再截断首个未写完的 buffer。
        while remaining and written >= len(remaining[0]):
            written -= len(remaining[0])
            remaining.pop(0)
        if remaining and written:
            remaining[0] = remaining[0][written:]


def _write_frame(stream: Any, msg_type: int, body: bytes = b"") -> None:
    """向 stream 写一帧：``[4B 长度][1B 类型][正文]`` 连续写入。

    可用时走 ``os.writev`` 避免中间 bytes 拼接：把 ``[帧头含类型][body]`` 两段 iovec
    直接交给内核，跳过 ``bytes([msg_type]) + body`` 的分配 + memcpy，
    与 Rust dispatcher 侧的 ``write_vectored`` 对称。如果 ``os.writev`` 不可用或 stream
    不是带 fileno() 的 raw fd（测试内存流），回退到原拼接写法。
    """
    # 帧头含类型字节：``[4 字节长度][1 字节类型]``，与读端 ``_read_frame`` 一致。
    header = _FRAME_HEADER.pack(1 + len(body), msg_type)
    try:
        fileno = stream.fileno()
    except (AttributeError, OSError, io.UnsupportedOperation):
        # 内存流（BytesIO / tests）或不可转 fd → 兼容路径。
        stream.write(header + body)
        stream.flush()
        return
    try:
        _writev_all(fileno, header, body)
    except AttributeError:
        # 平台无 os.writev：未写出任何字节，拼接重写安全。
        stream.write(header + body)
        stream.flush()
        return
    except _WriteVError as exc:
        if exc.written != 0:
            # 部分字节已写出（如 pipe 断连前的短写成功）：全量重写会重复这些
            # 字节、腐蚀后续帧流，不可回退，直接上抛交由调用方按断连处理。
            raise
        # written == 0：仅此场景（典型为首次 writev 即遇 pipe 断连）回退拼接
        # 重写是安全的——未写出任何字节，重写不会产生重复帧字节。
        stream.write(header + body)
        stream.flush()
        return
    stream.flush()


# v2 Dispatch 头部固定前缀（小端）：version u8 + retries u32 + retry_delay_ms u32 + task_id_len u16。
# 其后跟随 task_id 字节、workflow_id_len u16、workflow_id 字节，剩余为 cloudpickle(func,args,kwargs)。
_V2_HEADER_STRUCT = struct.Struct("<BIIH")


def _parse_dispatch_payload(
    body: bytes,
) -> tuple[int, int, str, str, bytes]:
    """解析 v2 Dispatch 正文，返回 ``(retries, retry_delay_ms, task_id, workflow_id, func_payload)``。

    头部布局与 ``actant/task/_helpers.py::_build_v2_envelope`` 保持一致（小端）：:

        u8   version   = ``_PAYLOAD_VERSION`` (0x02)
        u32  retries
        u32  retry_delay_ms
        u16  task_id_len ; N 字节 task_id (utf-8)
        u16  workflow_id_len ; N 字节 workflow_id (utf-8)
        其余 = ``cloudpickle(func, args, kwargs)``

    版本字节非 ``_PAYLOAD_VERSION`` 或数据截断时抛 ``ValueError``，由调用方作
    协议损坏走防御分支（不影响协议循环）。
    """
    if len(body) < _V2_HEADER_STRUCT.size:
        raise ValueError("dispatch payload too short for v2 header")
    version, retries, retry_delay_ms, tid_len = _V2_HEADER_STRUCT.unpack_from(body)
    if version != _PAYLOAD_VERSION:
        raise ValueError(f"unsupported dispatch payload version: {version}")
    offset = _V2_HEADER_STRUCT.size
    # 逐字段严格检查截断：切片静默截短会把残缺字节解码成错误值（如把下一字段
    # 的字节当 task_id 尾部），必须显式拒绝。
    # task_id 之后还需 workflow_id_len 的 2 字节头。
    if offset + tid_len + 2 > len(body):
        raise ValueError(
            f"truncated dispatch payload: task_id ({tid_len} bytes) overruns "
            f"body of {len(body)} bytes at offset {offset}"
        )
    task_id = body[offset : offset + tid_len].decode("utf-8")
    offset += tid_len
    wid_len = int.from_bytes(body[offset : offset + 2], "little")
    offset += 2
    if offset + wid_len > len(body):
        raise ValueError(
            f"truncated dispatch payload: workflow_id ({wid_len} bytes) overruns "
            f"body of {len(body)} bytes at offset {offset}"
        )
    workflow_id = body[offset : offset + wid_len].decode("utf-8")
    offset += wid_len
    return retries, retry_delay_ms, task_id, workflow_id, body[offset:]


# ───────────────────────── 复用序列化器 ─────────────────────────
# 前提：worker 单任务单线程（同一时刻至多执行一个任务），模块级对象无共享竞态。
_dump_buffer = io.BytesIO()
_dump_pickler = cloudpickle.Pickler(_dump_buffer)
_load_buffer = io.BytesIO()


def _reuse_pack(obj: Any) -> bytes:
    """用复用的 Pickler 序列化 ``obj`` 并返回字节（每次 dump 前清空 memo）。"""
    _dump_buffer.seek(0)
    _dump_buffer.truncate(0)
    _dump_pickler.clear_memo()
    _dump_pickler.dump(obj)
    return _dump_buffer.getvalue()


def _reuse_unpack(data: bytes) -> Any:
    """经复用的输入缓冲区反序列化 ``data``。

    仅复用 ``BytesIO`` 而非 Unpickler 实例——Unpickler 跨 pickle 保留读取状态，复用
    不安全；此处省去每次新建输入缓冲的分配，解析本身仍由 ``cloudpickle.load`` 完成。
    """
    _load_buffer.seek(0)
    _load_buffer.truncate(0)
    _load_buffer.write(data)
    _load_buffer.seek(0)
    return cloudpickle.load(_load_buffer)


# 指标边带行前缀：父进程 `drain_stderr` 据此识别行内容是从属指标而非日志。
_METRIC_LINE_PREFIX = "actant_metric: "

# stderr 由主线发射指标与读取线程打印协议错误并存，须经互斥串行化避免行交错。
_metric_lock = threading.Lock()


def _emit_metric(name: str, value_ms: int) -> None:
    """通过 stderr 单行上报一个从属计时指标（毫秒级整数直方图采样）。

    父进程通过该进程的 stdout 读取 Result 帧，stderr 专用于日志与指标边带。
    用户脚本在任务函数内对 stderr 的打印走独立 fd.write，与本行不交错——
    但为保险起见，本函数输出仍加锁，防止多次调用间行交错。
    """
    with _metric_lock:
        sys.stderr.write(f"{_METRIC_LINE_PREFIX}{name}={value_ms}\n")


def _elapsed_ms(elapsed_s: float) -> int:
    """浮点秒耗时转为毫秒整数（与 Rust 侧 `elapsed().as_millis()` 口径一致）。"""
    return int(elapsed_s * 1000)


class _CancelHolder:
    """把 stdin 读取线程的 ``Cancel`` 帧与执行线程的任务取消事件关联起来。

    - ``set_current(evt)`` 在开始执行新任务前调用，记录当前任务取消事件。
    - ``cancel()`` 由读取线程在收到 ``Cancel`` 帧时调用。
    - 若 ``cancel()`` 先于 ``set_current``（Rust 在任务派发后立即取消，但取帧
      尚未完成），标记 ``pending``，下一次 ``set_current`` 立即生效——避免预取消遗漏。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._current: threading.Event | None = None
        self._pending = False

    def set_current(self, evt: threading.Event) -> None:
        with self._lock:
            self._current = evt
            if self._pending:
                evt.set()
                self._pending = False

    def clear_current(self) -> None:
        with self._lock:
            self._current = None

    def cancel(self) -> None:
        with self._lock:
            if self._current is not None:
                self._current.set()
            else:
                self._pending = True


class _WorkerCancelToken:
    """桥接 stdio ``Cancel`` 帧到 ``_execute_with_retries`` 的协作取消检查。

    提供 ``is_cancelled()`` 以便复用现有重试 / 可中断 sleep 逻辑；取消事件一
    经触发即保持（``threading.Event``），并在 ``_DispatchTaskContext`` 首次
    检测到时传播到 ``on_cancel`` 清理钩子。
    """

    def __init__(self, event: threading.Event) -> None:
        self._event = event

    def is_cancelled(self) -> bool:
        return self._event.is_set()


def _run_dispatch(payload: bytes, cancel_event: threading.Event) -> bytes:
    """解析 v2 任务载荷并执行，返回与 generic handler 兼容的结果字节。

    Rust 已校验载荷完整性，此处直接解析。控制元数据来自紧凑头部，函数载荷经
    ``cloudpickle`` 反序列化；异常与成功统一编码为 ``cloudpickle.dumps((success,
    payload_obj))``，跨进程回传。
    """
    try:
        retries, retry_delay_ms, task_id, workflow_id, func_payload = _parse_dispatch_payload(
            payload
        )
        func, args, kwargs = _reuse_unpack(func_payload)
    except Exception as exc:  # 头解析 / 反序列化失败：返回可序列化的错误
        from actant.exceptions import ActantError

        safe = _ensure_exception(ActantError(
            f"failed to deserialize payload: {exc}", kind="serialization",
        ))
        return _pack_result(False, safe)

    # timeout_ms 为死参数（硬超时由 Rust 进程池强杀负责），固定传 0。
    token = _WorkerCancelToken(cancel_event)
    ctx = _DispatchTaskContext(task_id, workflow_id, token)
    # 无 Runtime：事件（started/completed/failed...）由 Rust Worker 在父进程侧
    # 发布；子进程内 silent=True 抑制 TaskLifecycle emit，避免依赖 parent runtime。
    with _task_context_scope(ctx):
        t0 = time.monotonic()
        success, payload_obj = _execute_with_retries(
            func, args, kwargs, 0, retries, retry_delay_ms,
            task_id, workflow_id, token, silent=True,
        )
    # 任务实际执行时段内（含重试的全部尝试）耗时上报到 stderr 边带，由父进程
    # drain_stderr 汇入 python.handler_ms 直方图——保持进程池隔离后可观测性不丢失。
    _emit_metric("python.handler_ms", _elapsed_ms(time.monotonic() - t0))
    return _pack_result(success, payload_obj)


def _pack_result(success: bool, payload_obj: Any) -> bytes:
    try:
        return _reuse_pack((success, payload_obj))
    except Exception:
        # 结果不可序列化：退化为携带类型与消息的 RuntimeError，
        # 与线程池后端 `_ensure_picklable` 的行为一致。
        return _reuse_pack(
            (False, RuntimeError(f"unserializable result: {payload_obj!r}"))
        )


def _ensure_exception(exc: BaseException) -> BaseException:
    try:
        cloudpickle.dumps(exc)
        return exc
    except Exception:
        return RuntimeError(f"{type(exc).__name__}: {exc}")


def _reader_loop(
    stdin: Any,
    dispatch_queue: Any,
    cancel_holder: _CancelHolder,
    shutdown_event: threading.Event,
) -> None:
    """持续读取 stdin 的读取线程循环。处理 Dispatch / Cancel / Shutdown 帧。"""
    while True:
        try:
            frame = _read_frame(stdin)
        except Exception as exc:
            # 帧损坏 / EOF：worker 无法继续协议，记一行 stderr 诊断（父进程
            # drain_stderr 转发为日志）后通知主线程退出。
            with _metric_lock:
                print(f"actant worker: frame read failed: {exc}", file=sys.stderr)
            shutdown_event.set()
            dispatch_queue.put_nowait(None)
            return
        if frame is None:
            shutdown_event.set()
            dispatch_queue.put(None)
            return
        msg_type = frame[0]
        body = frame[1:]
        if msg_type == FRAME_DISPATCH:
            dispatch_queue.put(body)
        elif msg_type == FRAME_CANCEL:
            cancel_holder.cancel()
        elif msg_type == FRAME_SHUTDOWN:
            shutdown_event.set()
            dispatch_queue.put(None)
            return


def main() -> int:
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
    stderr = sys.stderr

    # 无界任务队列：同一时刻至多一个任务在执行（进程池每进程一任务），
    # 队列仅缓冲「Dispatch 已读取、主线程尚未启动」的窗口，不会堆积。
    dispatch_queue: queue.SimpleQueue[bytes | None] = queue.SimpleQueue()

    cancel_holder = _CancelHolder()
    shutdown_event = threading.Event()

    reader = threading.Thread(
        target=_reader_loop,
        args=(stdin, dispatch_queue, cancel_holder, shutdown_event),
        name="actant-worker-reader",
        daemon=True,
    )
    reader.start()

    while True:
        payload = dispatch_queue.get()
        if payload is None:
            break
        cancel_event = threading.Event()
        cancel_holder.set_current(cancel_event)
        try:
            result = _run_dispatch(payload, cancel_event)
        except BaseException as exc:  # 防御：任何异常都转成结果帧，保协议不中断
            result = _pack_result(False, _ensure_exception(exc))
        finally:
            cancel_holder.clear_current()
        try:
            _write_frame(stdout, FRAME_RESULT, result)
        except Exception as exc:
            # stdio 已断开（父进程停止 / 强杀）：无法上报，退出。
            print(f"actant worker: stdout write failed: {exc}", file=stderr)
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
