"""Runtime 的持久化协作者。

``Runtime`` 保留编排与句柄管理，Store 上的三类内部数据拆到这里：

- :class:`TerminalRecordStore`：独立任务终态记录与 workflow 返回值
  （合并提交、解码、删除）；
- :class:`IdempotencyLedger`：幂等键 → ``task_id`` 映射与并发提交占位协议。

两者只依赖窄协议（``_StoreBackend`` / ``_RecordBackend``，由 Runtime 实现），
不感知 Runtime 的其余部分，也不导入 ``actant._internal.runtime``——避免循环依赖，同时
让持久化逻辑可以脱离 Runtime 单测。
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any, Generic, Protocol, TypeVar

from actant.exceptions import NotFoundError, TaskCancelledError, reconstruct_error

_logger = logging.getLogger("actant.runtime")

# Store 中所有框架内部键的保留前缀。用户的业务键不应以此前缀开头；
# 前缀独立于任何业务命名空间，使清理/审计内部记录时不会误伤用户数据。
INTERNAL_KEY_PREFIX = "__actant__:"

# 幂等键的保留前缀。值是该键首次提交的 ``task_id``（UTF-8）。
# 键作用域是本节点的 data_dir（同一 Store），与 Runtime 实例无关。
IDEMPOTENCY_KEY_PREFIX = f"{INTERNAL_KEY_PREFIX}idem:"

# 独立任务终态记录（结果帧 / 错误）与 workflow 返回值的保留前缀。
TASK_RESULT_KEY_PREFIX = f"{INTERNAL_KEY_PREFIX}task-result:"
FLOW_RESULT_KEY_PREFIX = f"{INTERNAL_KEY_PREFIX}flow-result:"

_T = TypeVar("_T", bound="_SubmitHandle")


class _SubmitHandle(Protocol):
    """幂等提交返回的句柄（由 Runtime 的 ``AsyncResult`` 满足）。"""

    task_id: str


class _StoreBackend(Protocol):
    """Store KV 原语（由 Runtime 实现）。"""

    def _store_get(self, key: str) -> bytes | None: ...

    def _store_put(self, key: str, value: bytes) -> None: ...

    def _store_put_batch(self, entries: list[tuple[str, bytes]]) -> None: ...

    def _store_delete(self, key: str) -> None: ...


class _RecordBackend(_StoreBackend, Protocol):
    """终态记录额外需要的 blob 存取能力（由 Runtime 实现）。"""

    def _blob_store(self, payload: bytes) -> bytes: ...

    def _blob_fetch(self, ref: Any) -> bytes: ...


class _RecordWriter:
    """合并提交终态记录。

    默认 ``SyncMode::Sync`` 下每次 ``Store.put`` 是独立事务（commit 含 fsync，
    毫秒级）。结果泵逐任务触发终态，逐条写会把 fsync 延迟直接乘到任务吞吐上。
    本类把时间窗内的多条记录合并为一次 ``put_batch``（单事务、一次 commit），
    在不改变"进程正常退出前记录已持久化"语义的前提下消除逐任务 fsync。

    写入失败只记 warning：结果持久化是查询能力，不参与任务终态判定。
    """

    #: 无新记录时的最长合并等待时间；决定单条记录的持久化延迟上界。
    _FLUSH_INTERVAL_SECS = 0.02
    #: 单次批量上限，防止突发流量下单个事务过大。
    _MAX_BATCH = 256

    def __init__(self, backend: _StoreBackend) -> None:
        self._backend = backend
        self._cond = threading.Condition()
        self._pending: dict[str, bytes] = {}
        self._inflight = 0
        self._closing = False
        self._thread = threading.Thread(
            target=self._run, name="actant-terminal-records", daemon=True
        )
        self._thread.start()

    def enqueue(self, key: str, payload: bytes) -> None:
        """入队一条待写记录；同 key 后写覆盖先写。"""
        with self._cond:
            if not self._closing:
                self._pending[key] = payload
                if len(self._pending) >= self._MAX_BATCH:
                    self._cond.notify()
                return
        # 已进入关闭流程：不再合并，直接同步写，避免记录静默丢失。
        try:
            self._backend._store_put(key, payload)
        except Exception:
            _logger.warning("failed to persist terminal record %r", key, exc_info=True)

    def flush(self, timeout: float = 5.0) -> bool:
        """等待已入队记录全部提交；超时返回 ``False``。"""
        deadline = time.monotonic() + timeout
        with self._cond:
            self._cond.notify()
            while self._pending or self._inflight:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(remaining)
        return True

    def close(self, timeout: float = 5.0) -> None:
        """刷出剩余记录并停止后台线程。"""
        if not self.flush(timeout):
            _logger.warning("terminal record writer: flush timed out; records may be lost")
        with self._cond:
            self._closing = True
            self._cond.notify_all()
        self._thread.join(timeout)
        if self._thread.is_alive():
            _logger.warning("terminal record writer: thread did not exit within %.1fs", timeout)

    def _run(self) -> None:
        while True:
            with self._cond:
                if not self._pending and not self._closing:
                    self._cond.wait(self._FLUSH_INTERVAL_SECS)
                if not self._pending:
                    if self._closing:
                        return
                    continue
                batch = list(self._pending.items())
                self._pending.clear()
                self._inflight += 1
            try:
                self._backend._store_put_batch(batch)
            except Exception:
                _logger.warning(
                    "failed to persist %d terminal record(s)", len(batch), exc_info=True
                )
            finally:
                with self._cond:
                    self._inflight -= 1
                    self._cond.notify_all()


class TerminalRecordStore:
    """独立任务终态与 workflow 返回值的持久化记录。

    记录写入失败不得翻转任务终态或阻塞结果回传，故写路径只 warning；读取路径
    （``task_result`` / ``workflow_result``）在记录缺失时抛 ``NotFoundError``，
    并把持久化的失败异常原样重抛。
    """

    def __init__(self, backend: _RecordBackend) -> None:
        self._backend = backend
        self._writer = _RecordWriter(backend)

    # -- 写入 ---------------------------------------------------------------

    def put_task_result(
        self, task_id: str, frame: bytes, ref_bytes: bytes | None
    ) -> None:
        """持久化成功任务的结果帧：大结果存 Ref，小结果内联帧字节。

        两态与 ``AsyncResult`` 一致：帧是 ``cloudpickle((success, payload))``，
        大帧已在 blob store 中，记录只保存 ``Ref`` 编码，避免 LMDB 里复制一份。
        """
        if ref_bytes is not None:
            self.put_task_record(task_id, {"state": "Completed", "ref": ref_bytes})
        else:
            self.put_task_record(task_id, {"state": "Completed", "frame": frame})

    def put_task_failure(
        self,
        task_id: str,
        state: str,
        error_text: str,
        exc: BaseException | None = None,
    ) -> None:
        """持久化失败/取消/跳过终态（没有结果帧，只有错误文本）。

        ``exc`` 存在时一并编码：读取侧重抛原异常对象，使同进程
        ``AsyncResult.result()`` 与 :meth:`task_result` 的异常类型一致，而不是
        退化为按文本重建的泛化异常。
        """
        record: dict[str, Any] = {"state": state, "error": error_text}
        if exc is not None:
            import cloudpickle

            try:
                record["exc"] = cloudpickle.dumps(exc)
            except Exception:
                # 不可序列化的异常对象吞掉后仍保留文本，读取侧回退 rebuild。
                _logger.debug(
                    "task %s: exception is not picklable; persisting text only",
                    task_id,
                    exc_info=True,
                )
        self.put_task_record(task_id, record)

    def put_task_record(self, task_id: str, record: dict[str, Any]) -> None:
        """把独立任务的终态记录编码后入队合并提交。

        结果持久化是**查询能力**，不是任务语义的一部分：编码或写入失败不得
        翻转任务终态，故只记录 warning。
        """
        import cloudpickle

        try:
            payload = cloudpickle.dumps(record)
        except Exception:
            _logger.warning(
                "task %s: terminal record is not picklable; result not persisted",
                task_id,
                exc_info=True,
            )
            return
        self._writer.enqueue(f"{TASK_RESULT_KEY_PREFIX}{task_id}", payload)

    def put_flow_result(self, workflow_id: str, result: Any) -> None:
        """把 workflow 函数体的返回值写入持久化 Store。

        大结果先经 ``ValueStore`` 落 blob，记录只保存 Ref；写失败只记 warning——
        返回值查询不得反向影响已经成功的编排。
        """
        import cloudpickle

        from actant._internal.tasks.ref import REF_INLINE_THRESHOLD

        record: dict[str, Any] = {}
        try:
            payload = cloudpickle.dumps(result)
        except Exception:
            _logger.warning(
                "workflow %s: return value is not picklable; result not persisted",
                workflow_id,
                exc_info=True,
            )
            return
        if len(payload) > REF_INLINE_THRESHOLD:
            try:
                record["ref"] = self._backend._blob_store(payload)
            except Exception:
                _logger.warning(
                    "workflow %s: failed to offload return value; storing inline",
                    workflow_id,
                    exc_info=True,
                )
                record["value"] = result
        else:
            record["value"] = result
        try:
            encoded = cloudpickle.dumps(record)
        except Exception:
            _logger.warning(
                "workflow %s: return value record is not picklable; result not persisted",
                workflow_id,
                exc_info=True,
            )
            return
        # workflow 返回值低频（每工作流一次），同步写即可；它还需要在
        # completed 事件广播前可见（事件处理器会立即查询返回值）。
        try:
            self._backend._store_put(f"{FLOW_RESULT_KEY_PREFIX}{workflow_id}", encoded)
        except Exception:
            _logger.warning(
                "workflow %s: failed to persist return value", workflow_id, exc_info=True
            )

    # -- 读取 ---------------------------------------------------------------

    def task_result(self, task_id: str) -> Any:
        """读取独立任务的持久化终态；未记录抛 ``NotFoundError``。"""
        self.flush()
        raw = self._backend._store_get(f"{TASK_RESULT_KEY_PREFIX}{task_id}")
        if raw is None:
            raise NotFoundError(
                f"no persisted result for task {task_id!r}: only independent tasks "
                "submitted on this data_dir are recorded"
            )
        return self._decode_task_record(task_id, raw)

    def workflow_result(
        self,
        workflow_id: str,
        successor: Callable[[str], bytes | None],
    ) -> Any:
        """沿 ``continue_as_new`` 链读取 workflow 返回值。

        ``successor`` 返回该代的后继 id（无后继返回 ``None``）；链上任意一代
        都能查到最终代的返回值。
        """
        import cloudpickle

        current = workflow_id
        visited: set[str] = set()
        while True:
            raw = self._backend._store_get(f"{FLOW_RESULT_KEY_PREFIX}{current}")
            if raw is not None:
                record = cloudpickle.loads(raw)
                if "ref" in record:
                    return cloudpickle.loads(self._backend._blob_fetch(record["ref"]))
                return record["value"]
            if current in visited:
                raise NotFoundError(
                    f"continue_as_new chain from workflow {workflow_id!r} is cyclic"
                )
            visited.add(current)
            nxt = successor(current)
            if nxt is None:
                raise NotFoundError(
                    f"no persisted return value for workflow {workflow_id!r}"
                )
            current = bytes(nxt).decode()

    def _decode_task_record(self, task_id: str, raw: bytes) -> Any:
        import cloudpickle

        record = cloudpickle.loads(raw)
        state = record.get("state")
        if state == "Completed":
            frame = (
                self._backend._blob_fetch(record["ref"])
                if "ref" in record
                else record["frame"]
            )
            # 帧约定与结果路径一致：cloudpickle((success, payload))。
            return cloudpickle.loads(frame)[1]
        if state == "Cancelled":
            raise TaskCancelledError(f"task {task_id!r} was cancelled")
        if "exc" in record:
            try:
                exc = cloudpickle.loads(record["exc"])
            except Exception:
                # 记录来自不同解释器/依赖变更时可能无法反序列化；回退文本重建。
                _logger.warning(
                    "task %s: persisted exception is not loadable; falling back to text",
                    task_id,
                    exc_info=True,
                )
                exc = None
            if isinstance(exc, BaseException):
                raise exc
        raise reconstruct_error(record.get("error") or f"task {task_id!r} failed")

    # -- 清理 ---------------------------------------------------------------

    def forget_task(self, task_id: str) -> None:
        """删除独立任务的终态记录（幂等）。"""
        # 先刷出待写记录，避免删除后又被合并提交写回。
        self.flush()
        self._backend._store_delete(f"{TASK_RESULT_KEY_PREFIX}{task_id}")

    def forget_workflow_result(self, workflow_id: str) -> None:
        """删除 workflow 返回值的记录（幂等）。"""
        self._backend._store_delete(f"{FLOW_RESULT_KEY_PREFIX}{workflow_id}")

    # -- 生命周期 -----------------------------------------------------------

    def flush(self) -> None:
        """等待合并提交器写完全部待写记录。"""
        if not self._writer.flush():
            _logger.warning(
                "terminal records not flushed within timeout; a subsequent read "
                "may miss the most recent record"
            )

    def close(self, timeout: float = 5.0) -> None:
        """刷出剩余记录并停止后台线程。"""
        self._writer.close(timeout)


class IdempotencyLedger(Generic[_T]):
    """幂等键 → ``task_id`` 映射与并发提交占位协议。

    锁只保护"占位 / 发布"这类短临界区；``submit`` 本身在锁外执行，避免依赖
    解析阻塞时串行化全部幂等提交。
    """

    def __init__(self, backend: _StoreBackend) -> None:
        self._backend = backend
        self._handles: dict[str, _T] = {}
        # 正在入队的幂等键占位：key -> 完成事件。
        self._inflight: dict[str, threading.Event] = {}
        self._lock = threading.Lock()

    def lookup(self, key: str) -> str | None:
        """查询持久化 Store 中该键已登记的任务 ID。

        存储错误原样传播——读失败时宁可拒绝提交，也不能在未确认去重状态的情况
        下重复执行。
        """
        raw = self._backend._store_get(f"{IDEMPOTENCY_KEY_PREFIX}{key}")
        if raw is None:
            return None
        return raw.decode("utf-8")

    def submit_once(self, key: str, submit: Callable[[], _T]) -> _T:
        """按 ``key`` 去重提交：首次调用 ``submit`` 并登记，其后返回同一句柄。

        语义边界：

        - 进程内命中已发布句柄时直接返回（同一 Runtime 实例）；
        - 进程内未命中但持久化 Store 已有该键（重启/另一进程）：抛
          ``AlreadyExistsError`` 并带上首次提交的 ``task_id``，**不会**再次入队；
        - 并发同键提交只有一个线程成为 owner 入队，其余等待占位事件后复用同一
          句柄；owner 失败不登记，等待方重新竞争；
        - 已取消/已失败的句柄同样会被返回：取消是对"这次提交"的后续决定，
          重试提交不应把它复活；
        - 不比对参数：复用一个 ``key`` 表示调用方声明"这是同一次提交"，
          首次提交的参数即事实。
        """
        from actant.exceptions import AlreadyExistsError

        while True:
            owner = False
            with self._lock:
                handle = self._handles.get(key)
                if handle is not None:
                    return handle
                # inflight 检查必须早于持久化查找：owner 在发布句柄前已写入
                # 幂等映射，此时并发的同键提交应等待而非误判为"重启后重复"。
                inflight = self._inflight.get(key)
                if inflight is None:
                    existing_task_id = self.lookup(key)
                    if existing_task_id is not None:
                        raise AlreadyExistsError(
                            f"idempotency_key {key!r} was already submitted on this "
                            f"data_dir (task_id={existing_task_id!r}); refusing to "
                            "submit a second time"
                        )
                    inflight = threading.Event()
                    self._inflight[key] = inflight
                    owner = True
            if not owner:
                # 另一线程已在入队同一 key：不持锁等待，命中则返回同一句柄；
                # 首次提交失败（未登记）时回到循环重试判定。
                inflight.wait()
                continue
            try:
                handle = submit()
            except BaseException:
                with self._lock:
                    self._inflight.pop(key, None)
                inflight.set()
                raise
            # 先落幂等映射再发布句柄：崩溃窗口内不会出现"句柄可见但键未登记"。
            self._record(key, handle.task_id)
            with self._lock:
                self._handles[key] = handle
                self._inflight.pop(key, None)
            inflight.set()
            return handle

    def forget(self, key: str) -> None:
        """删除内存与持久层登记，使该键可重新提交（幂等）。"""
        with self._lock:
            self._handles.pop(key, None)
        self._backend._store_delete(f"{IDEMPOTENCY_KEY_PREFIX}{key}")

    def _record(self, key: str, task_id: str) -> None:
        """把幂等键与首次提交的 ``task_id`` 写入持久化 Store。

        写入失败只记录 warning：任务**已经入队**，此时抛错会误导调用方重试并
        产生第二次执行；进程内映射仍然去重，代价仅是本次键的去重无法跨重启。
        """
        try:
            self._backend._store_put(
                f"{IDEMPOTENCY_KEY_PREFIX}{key}", task_id.encode()
            )
        except Exception:
            _logger.warning(
                "idempotency_key %r: failed to persist mapping to task %r; "
                "dedup is now process-local for this key",
                key,
                task_id,
                exc_info=True,
            )
