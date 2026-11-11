"""服务协议 v1：HTTP/JSON + SSE。

这是 L2 服务层的传输形态，只做「协议 ↔ Runtime 调用」的映射，不引入新的
执行语义。端点为：

- ``POST /v1/workflows``：按 :class:`DefinitionRef` 提交 flow；
- ``GET  /v1/workflows``：列出工作流；
- ``GET  /v1/workflows/{id}``：查询执行状态；
- ``GET  /v1/workflows/{id}/result``：读取持久化返回值；
- ``POST /v1/workflows/{id}/cancel``：取消工作流；
- ``POST /v1/workflows/{id}/signal``：递交等待点信号；
- ``GET  /v1/workflows/{id}/journal``：读取稳定 journal；
- ``GET  /v1/workflows/{id}/events``：SSE 订阅 journal 增量。

载荷编码：提交参数用 ``cloudpickle`` + base64（不透明字节，与核心一致）；
状态/结果/事件中的 ``bytes`` 编码为 ``{"$bytes": "<base64>"}``，JSON 无法
表达的 Python 对象编码为 ``{"$pickle": "<base64>"}``。

.. warning::
   协议 v1 **没有鉴权**，且会 ``cloudpickle.loads`` 客户端提交的载荷（等价于
    执行任意代码）。只允许在可信网络内暴露；鉴权、租户隔离与审计属于第 3 层。
"""

from __future__ import annotations

import base64
import ipaddress
import json
import logging
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any, NoReturn, cast
from urllib.parse import parse_qs, quote, unquote, urlparse

import cloudpickle

from act_ext_service.definitions import (
    DefinitionError,
    DefinitionHashMismatchError,
    DefinitionNotFoundError,
    DefinitionRef,
    DefinitionResolver,
    DefinitionVersionMismatchError,
)
from actant.exceptions import (
    ActantTimeoutError,
    AlreadyExistsError,
    InternalError,
    InvalidStateError,
    NetworkError,
    NotFoundError,
    SerializationError,
    raise_for_kind,
)

if TYPE_CHECKING:
    from actant import Runtime

__all__ = ["PROTOCOL_VERSION", "ServiceClient", "ServiceServer"]

_logger = logging.getLogger("act_ext_service")

PROTOCOL_VERSION = "v1"

_PREFIX = f"/{PROTOCOL_VERSION}"
_TERMINAL_STATES = frozenset({"Completed", "Failed", "Cancelled"})
_SSE_HEARTBEAT_S = 5.0
_MAX_BODY_BYTES = 64 * 1024 * 1024


def _is_loopback(host: str) -> bool:
    """判断监听地址是否仅本机可达。"""
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


_DEFINITION_ERRORS: dict[str, type[DefinitionError]] = {
    "definition_not_found": DefinitionNotFoundError,
    "definition_version_mismatch": DefinitionVersionMismatchError,
    "definition_hash_mismatch": DefinitionHashMismatchError,
}


class _HTTPError(Exception):
    """协议层直接回写的 HTTP 错误。"""

    def __init__(self, status: int, kind: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.kind = kind


@dataclass(frozen=True)
class _ServiceContext:
    runtime: Runtime
    resolver: DefinitionResolver
    poll_interval_s: float
    stop_event: threading.Event


class _ServiceHTTPServer(ThreadingHTTPServer):
    """携带服务上下文的 HTTP 服务器。"""

    daemon_threads = True
    block_on_close = False
    context: _ServiceContext


class _RequestHandler(BaseHTTPRequestHandler):
    """把 HTTP 请求转成 Runtime 调用。"""

    server_version = "ActantService/1"
    protocol_version = "HTTP/1.1"

    _streaming = False

    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")

    def log_message(self, format: str, *args: Any) -> None:
        _logger.debug("service %s - %s", self.address_string(), format % args)

    # ------------------------------------------------------------------
    # 路由与错误边界
    # ------------------------------------------------------------------

    def _dispatch(self, method: str) -> None:
        self._streaming = False
        try:
            parsed = urlparse(self.path)
            parts = [unquote(part) for part in parsed.path.strip("/").split("/") if part]
            query = parse_qs(parsed.query)
            self._route(method, parts, query)
        except _HTTPError as exc:
            self._safe_send_json(
                exc.status, {"error": {"kind": exc.kind, "message": str(exc)}}
            )
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        except Exception as exc:  # 边界处统一转为协议错误
            status, kind = _status_for(exc)
            if status >= 500:
                _logger.exception("service: unhandled error for %s %s", method, self.path)
            else:
                _logger.debug("service: %s %s -> %s: %s", method, self.path, kind, exc)
            self._safe_send_json(
                status, {"error": {"kind": kind, "message": str(exc)}}
            )

    def _route(
        self, method: str, parts: list[str], query: dict[str, list[str]]
    ) -> None:
        if parts[:2] != [PROTOCOL_VERSION, "workflows"]:
            raise _HTTPError(404, "not_found", f"unknown endpoint {self.path!r}")

        if len(parts) == 2:
            if method == "GET":
                self._handle_list(query)
                return
            if method == "POST":
                self._handle_submit()
                return
            raise _HTTPError(405, "method_not_allowed", f"{method} not allowed")

        workflow_id = parts[2]
        if len(parts) == 3:
            if method == "GET":
                self._handle_state(workflow_id)
                return
            raise _HTTPError(405, "method_not_allowed", f"{method} not allowed")

        action = parts[3]
        if action == "result" and method == "GET":
            self._handle_result(workflow_id)
        elif action == "cancel" and method == "POST":
            self._handle_cancel(workflow_id)
        elif action == "signal" and method == "POST":
            self._handle_signal(workflow_id)
        elif action == "journal" and method == "GET":
            self._handle_journal(workflow_id)
        elif action == "events" and method == "GET":
            self._handle_events(workflow_id, query)
        elif action in {"result", "cancel", "signal", "journal", "events"}:
            raise _HTTPError(405, "method_not_allowed", f"{method} not allowed")
        else:
            raise _HTTPError(404, "not_found", f"unknown workflow action {action!r}")

    # ------------------------------------------------------------------
    # 端点实现
    # ------------------------------------------------------------------

    def _handle_list(self, query: dict[str, list[str]]) -> None:
        runtime = self._context().runtime
        workflow_ids = runtime.list_workflows(
            include_archived=_query_flag(query, "include_archived")
        )
        self._send_json(200, {"workflow_ids": workflow_ids})

    def _handle_submit(self) -> None:
        body = self._read_json()
        raw_ref = body.get("definition")
        if not isinstance(raw_ref, dict):
            raise ValueError("field 'definition' must be an object")
        ref = DefinitionRef.from_dict(raw_ref)
        payload = body.get("payload")
        if not isinstance(payload, str):
            raise ValueError("field 'payload' must be a base64 string")
        args, kwargs = _decode_invocation(payload)
        timeout_ms = body.get("timeout_ms")
        if timeout_ms is not None and (
            not isinstance(timeout_ms, int) or isinstance(timeout_ms, bool)
        ):
            raise ValueError("field 'timeout_ms' must be an integer")
        failure_strategy = body.get("failure_strategy")
        if failure_strategy is not None and not isinstance(failure_strategy, str):
            raise ValueError("field 'failure_strategy' must be a string")

        context = self._context()
        context.resolver.resolve(ref)
        handle = context.runtime.start_flow(
            ref.name,
            *args,
            version=ref.version,
            timeout_ms=timeout_ms,
            failure_strategy=failure_strategy,
            **kwargs,
        )
        self._send_json(200, {"workflow_id": handle.workflow_id})

    def _handle_state(self, workflow_id: str) -> None:
        state = self._context().runtime.get_workflow_state(workflow_id)
        self._send_json(200, {"state": _jsonable(state)})

    def _handle_result(self, workflow_id: str) -> None:
        result = self._context().runtime.get_workflow_result(workflow_id)
        self._send_json(200, {"result": _jsonable(result)})

    def _handle_cancel(self, workflow_id: str) -> None:
        self._context().runtime.cancel_workflow(workflow_id)
        self._send_json(200, {"cancelled": True})

    def _handle_signal(self, workflow_id: str) -> None:
        body = self._read_json()
        key = body.get("key")
        if not isinstance(key, str) or not key:
            raise ValueError("field 'key' must be a non-empty string")
        raw_payload = body.get("payload", "")
        if not isinstance(raw_payload, str):
            raise ValueError("field 'payload' must be a base64 string")
        delivered = self._context().runtime.signal_wait_point(
            workflow_id, key, _decode_bytes(raw_payload)
        )
        if delivered is None:
            self._send_json(200, {"delivered": False, "payload": None})
        else:
            self._send_json(
                200, {"delivered": True, "payload": _encode_bytes(delivered)}
            )

    def _handle_journal(self, workflow_id: str) -> None:
        events = self._context().runtime.get_workflow_journal(workflow_id)
        self._send_json(200, {"events": [_jsonable(event) for event in events]})

    def _handle_events(
        self, workflow_id: str, query: dict[str, list[str]]
    ) -> None:
        context = self._context()
        if context.runtime.get_workflow_state(workflow_id) is None:
            raise _HTTPError(404, "not_found", f"workflow {workflow_id!r} not found")
        after_seq = _after_seq(query, self.headers.get("Last-Event-ID"))

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        self._streaming = True
        self.close_connection = True

        last_seq = after_seq
        last_write = time.monotonic()
        try:
            while not context.stop_event.is_set():
                events = context.runtime.get_workflow_journal(workflow_id)
                last_seq, emitted = self._emit_events(events, last_seq)
                if emitted:
                    last_write = time.monotonic()

                state = context.runtime.get_workflow_state(workflow_id)
                if state is None or state.get("state") in _TERMINAL_STATES:
                    # 终态可能和最后一条 journal 事件同一批写入，再读一次确认。
                    last_seq, emitted = self._emit_events(
                        context.runtime.get_workflow_journal(workflow_id), last_seq
                    )
                    if emitted:
                        last_write = time.monotonic()
                        continue
                    terminal_state = None if state is None else state.get("state")
                    data = json.dumps({"state": terminal_state}, ensure_ascii=False)
                    self.wfile.write(f"event: end\ndata: {data}\n\n".encode())
                    self.wfile.flush()
                    return

                if time.monotonic() - last_write >= _SSE_HEARTBEAT_S:
                    self.wfile.write(b": keep-alive\n\n")
                    self.wfile.flush()
                    last_write = time.monotonic()
                time.sleep(context.poll_interval_s)
        except (BrokenPipeError, ConnectionResetError):
            _logger.debug("service: client disconnected from events for %s", workflow_id)

    def _emit_events(
        self, events: list[dict[str, Any]], last_seq: int
    ) -> tuple[int, bool]:
        emitted = False
        for event in events:
            seq = _event_seq(event)
            if seq <= last_seq:
                continue
            last_seq = seq
            data = json.dumps(_jsonable(event), ensure_ascii=False, separators=(",", ":"))
            self.wfile.write(f"id: {seq}\ndata: {data}\n\n".encode())
            emitted = True
        if emitted:
            self.wfile.flush()
        return last_seq, emitted

    # ------------------------------------------------------------------
    # 请求/响应辅助
    # ------------------------------------------------------------------

    def _context(self) -> _ServiceContext:
        return cast("_ServiceHTTPServer", self.server).context

    def _read_json(self) -> dict[str, Any]:
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            raise ValueError("request body required (Content-Length missing)")
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise ValueError(f"invalid Content-Length {raw_length!r}") from exc
        if length <= 0:
            raise ValueError("request body required")
        if length > _MAX_BODY_BYTES:
            raise _HTTPError(413, "payload_too_large", "request body too large")
        try:
            body = json.loads(self.rfile.read(length))
        except ValueError as exc:
            raise ValueError(f"invalid JSON body: {exc}") from exc
        if not isinstance(body, dict):
            raise ValueError("request body must be a JSON object")
        return body

    def _send_json(self, status: int, body: Mapping[str, Any]) -> None:
        data = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
        self.wfile.flush()

    def _safe_send_json(self, status: int, body: Mapping[str, Any]) -> None:
        if self._streaming:
            self.close_connection = True
            return
        try:
            self._send_json(status, body)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True


class ServiceServer:
    """在已启动的 Runtime 上暴露服务协议 v1。"""

    def __init__(
        self,
        runtime: Runtime,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        resolver: DefinitionResolver | None = None,
        poll_interval_ms: int = 200,
        allow_insecure: bool = False,
    ) -> None:
        """
        Args:
            runtime: 已启动的 Runtime；服务不接管其生命周期。
            host: 监听地址。默认仅本机；非 loopback 地址需要
                ``allow_insecure=True`` 显式确认。
            port: 监听端口；``0`` 表示由内核分配。
            resolver: 定义解析器；默认使用进程级 ``FLOW_REGISTRY``。
            poll_interval_ms: SSE 轮询和事件检查间隔。
            allow_insecure: 允许监听非 loopback 地址。协议 v1 无鉴权且会
                ``cloudpickle.loads`` 客户端载荷，只应在可信网络内开启。

        Raises:
            ValueError: ``poll_interval_ms`` 非正，或非 loopback 监听未显式确认。
        """
        if poll_interval_ms <= 0:
            raise ValueError("poll_interval_ms must be positive")
        loopback = _is_loopback(host)
        if not loopback and not allow_insecure:
            raise ValueError(
                "ServiceServer 无鉴权且会反序列化客户端载荷；"
                "非 loopback 监听需显式 allow_insecure=True"
            )
        if not loopback:
            _logger.warning(
                "service: 监听非 loopback 地址 %s；协议 v1 无鉴权，"
                "必须置于可信反向代理或网络隔离之后",
                host,
            )
        self._context = _ServiceContext(
            runtime=runtime,
            resolver=DefinitionResolver() if resolver is None else resolver,
            poll_interval_s=poll_interval_ms / 1000.0,
            stop_event=threading.Event(),
        )
        self._httpd = _ServiceHTTPServer((host, port), _RequestHandler)
        self._httpd.context = self._context
        self._thread: threading.Thread | None = None

    @property
    def address(self) -> tuple[str, int]:
        """实际监听地址（端口为 0 时返回内核分配的端口）。"""
        return cast("tuple[str, int]", self._httpd.server_address)

    @property
    def base_url(self) -> str:
        """客户端可用的基础 URL。"""
        host, port = self.address
        return f"http://{host}:{port}"

    def start(self) -> None:
        """在后台线程开始服务。

        Raises:
            InvalidStateError: 重复启动。
        """
        if self._thread is not None:
            raise InvalidStateError("ServiceServer is already started")
        thread = threading.Thread(
            target=self._httpd.serve_forever,
            name="actant-service",
            daemon=True,
        )
        self._thread = thread
        thread.start()

    def stop(self) -> None:
        """停止服务；可重复调用。"""
        if self._thread is None:
            return
        self._context.stop_event.set()
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5.0)
        self._thread = None

    def __enter__(self) -> ServiceServer:
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.stop()


class ServiceClient:
    """调用服务协议 v1 的客户端（无第三方依赖）。"""

    def __init__(self, base_url: str, *, timeout_s: float = 30.0) -> None:
        if not base_url:
            raise ValueError("base_url must not be empty")
        self._base = base_url.rstrip("/")
        self._timeout_s = timeout_s

    def submit(
        self,
        definition: DefinitionRef,
        *args: Any,
        timeout_ms: int | None = None,
        failure_strategy: str | None = None,
        **kwargs: Any,
    ) -> str:
        """提交一次 flow 调用，返回 ``workflow_id``。"""
        body: dict[str, Any] = {
            "definition": definition.to_dict(),
            "payload": _encode_invocation(args, kwargs),
        }
        if timeout_ms is not None:
            body["timeout_ms"] = timeout_ms
        if failure_strategy is not None:
            body["failure_strategy"] = failure_strategy
        response = self._request("POST", f"{_PREFIX}/workflows", body)
        workflow_id = response.get("workflow_id")
        if not isinstance(workflow_id, str) or not workflow_id:
            raise InternalError("service response missed 'workflow_id'")
        return workflow_id

    def list_workflows(self, *, include_archived: bool = False) -> list[str]:
        """列出工作流 id。"""
        path = f"{_PREFIX}/workflows"
        if include_archived:
            path += "?include_archived=1"
        response = self._request("GET", path)
        workflow_ids = response.get("workflow_ids")
        if not isinstance(workflow_ids, list) or not all(
            isinstance(item, str) for item in workflow_ids
        ):
            raise InternalError("service response has invalid 'workflow_ids'")
        return cast("list[str]", workflow_ids)

    def state(self, workflow_id: str) -> dict[str, Any] | None:
        """查询工作流状态；不存在时返回 ``None``。"""
        response = self._request(
            "GET", f"{_PREFIX}/workflows/{_path_id(workflow_id)}"
        )
        state = _from_jsonable(response.get("state"))
        if state is None:
            return None
        if not isinstance(state, dict):
            raise InternalError("service response has invalid 'state'")
        return cast("dict[str, Any]", state)

    def result(self, workflow_id: str) -> Any:
        """读取工作流持久化返回值。"""
        response = self._request(
            "GET", f"{_PREFIX}/workflows/{_path_id(workflow_id)}/result"
        )
        return _from_jsonable(response.get("result"))

    def cancel(self, workflow_id: str) -> None:
        """取消工作流。"""
        self._request(
            "POST", f"{_PREFIX}/workflows/{_path_id(workflow_id)}/cancel", {}
        )

    def signal(
        self, workflow_id: str, key: str, payload: bytes = b""
    ) -> bytes | None:
        """递交等待点信号。

        Returns:
            被唤醒等待点记录的 payload；``None`` 表示信号已入缓冲。
        """
        response = self._request(
            "POST",
            f"{_PREFIX}/workflows/{_path_id(workflow_id)}/signal",
            {"key": key, "payload": _encode_bytes(payload)},
        )
        delivered = response.get("delivered")
        if not isinstance(delivered, bool):
            raise InternalError("service response has invalid 'delivered'")
        if not delivered:
            return None
        raw = response.get("payload")
        if not isinstance(raw, str):
            raise InternalError("service response has invalid 'payload'")
        return _decode_bytes(raw)

    def journal(self, workflow_id: str) -> list[dict[str, Any]]:
        """读取工作流稳定 journal。"""
        response = self._request(
            "GET", f"{_PREFIX}/workflows/{_path_id(workflow_id)}/journal"
        )
        return _decode_events(response.get("events"))

    def subscribe(
        self, workflow_id: str, *, after_seq: int = 0
    ) -> Iterator[dict[str, Any]]:
        """订阅 journal 增量，工作流终态后迭代结束。"""
        path = (
            f"{_PREFIX}/workflows/{_path_id(workflow_id)}/events"
            f"?after_seq={after_seq}"
        )
        request = urllib.request.Request(
            self._base + path, headers={"Accept": "text/event-stream"}
        )
        try:
            response = urllib.request.urlopen(request, timeout=self._timeout_s)
        except urllib.error.HTTPError as exc:
            self._raise_http_error(exc)
        except urllib.error.URLError as exc:
            raise NetworkError(f"service subscribe failed: {exc.reason}") from exc

        try:
            event_type = "message"
            data_lines: list[str] = []
            for raw_line in response:
                line = raw_line.decode("utf-8").rstrip("\r\n")
                if not line:
                    if data_lines:
                        event = _decode_sse_event(event_type, data_lines)
                        if event is not None:
                            yield event
                    event_type = "message"
                    data_lines = []
                elif line.startswith(":"):
                    continue
                elif line.startswith("event:"):
                    event_type = line[len("event:") :].strip()
                elif line.startswith("data:"):
                    data_lines.append(line[len("data:") :].lstrip())
        finally:
            response.close()

    # ------------------------------------------------------------------
    # HTTP 辅助
    # ------------------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        body: Mapping[str, Any] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> dict[str, Any]:
        data = None
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers = {"Accept": "application/json"}
        if data is not None:
            headers["Content-Type"] = "application/json; charset=utf-8"
        request = urllib.request.Request(
            self._base + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(
                request, timeout=self._timeout_s if timeout_s is None else timeout_s
            ) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            self._raise_http_error(exc)
        except urllib.error.URLError as exc:
            raise NetworkError(f"service request to {path} failed: {exc.reason}") from exc

        try:
            parsed = json.loads(raw)
        except ValueError as exc:
            raise InternalError(f"service response is not JSON: {raw[:200]!r}") from exc
        if not isinstance(parsed, dict):
            raise InternalError("service response must be a JSON object")
        return parsed

    def _raise_http_error(self, exc: urllib.error.HTTPError) -> NoReturn:
        raw = exc.read()
        kind = "internal"
        message = f"service request failed with HTTP {exc.code}"
        try:
            parsed = json.loads(raw)
        except ValueError:
            parsed = None
        if isinstance(parsed, dict):
            error = parsed.get("error")
            if isinstance(error, dict):
                raw_kind = error.get("kind")
                raw_message = error.get("message")
                if isinstance(raw_kind, str):
                    kind = raw_kind
                if isinstance(raw_message, str):
                    message = raw_message
        _raise_service_error(kind, message)


def _raise_service_error(kind: str, message: str) -> NoReturn:
    definition_error = _DEFINITION_ERRORS.get(kind)
    if definition_error is not None:
        raise definition_error(message)
    raise_for_kind(kind, message)
    raise InternalError(f"raise_for_kind returned for unknown kind {kind!r}")


def _status_for(exc: Exception) -> tuple[int, str]:
    kind = cast("str", getattr(exc, "kind", "internal"))
    if isinstance(exc, (DefinitionNotFoundError, NotFoundError)):
        return 404, kind
    if isinstance(
        exc, (DefinitionVersionMismatchError, DefinitionHashMismatchError, ValueError, TypeError)
    ):
        return 400, kind
    if isinstance(exc, (AlreadyExistsError, InvalidStateError)):
        return 409, kind
    if isinstance(exc, ActantTimeoutError):
        return 504, kind
    return 500, kind


def _path_id(workflow_id: str) -> str:
    if not workflow_id:
        raise ValueError("workflow_id must not be empty")
    return quote(workflow_id, safe="")


def _query_flag(query: dict[str, list[str]], key: str) -> bool:
    values = query.get(key)
    if not values:
        return False
    return values[0].lower() in {"1", "true", "yes", "on"}


def _after_seq(query: dict[str, list[str]], header: str | None) -> int:
    values = query.get("after_seq")
    raw = values[0] if values else header
    if raw is None or raw == "":
        return 0
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"invalid after_seq {raw!r}") from exc
    if value < 0:
        raise ValueError("after_seq must be >= 0")
    return value


def _event_seq(event: Mapping[str, Any]) -> int:
    value = event.get("seq")
    return value if isinstance(value, int) else 0


def _encode_invocation(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    try:
        raw = cloudpickle.dumps((args, kwargs))
    except Exception as exc:  # 序列化边界
        raise SerializationError(f"failed to serialize flow arguments: {exc}") from exc
    return _encode_bytes(raw)


def _decode_invocation(payload: str) -> tuple[tuple[Any, ...], dict[str, Any]]:
    raw = _decode_bytes(payload)
    try:
        value = cloudpickle.loads(raw)
    except Exception as exc:  # 反序列化边界
        raise SerializationError(f"failed to deserialize flow arguments: {exc}") from exc
    if (
        not isinstance(value, tuple)
        or len(value) != 2
        or not isinstance(value[0], tuple)
        or not isinstance(value[1], dict)
    ):
        raise ValueError("payload must encode (args, kwargs)")
    return value[0], cast("dict[str, Any]", value[1])


def _encode_bytes(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _decode_bytes(data: str) -> bytes:
    try:
        return base64.b64decode(data.encode("ascii"), validate=True)
    except (ValueError, UnicodeEncodeError) as exc:
        raise ValueError("invalid base64 payload") from exc


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"$bytes": _encode_bytes(bytes(value))}
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    try:
        return {"$pickle": _encode_bytes(cloudpickle.dumps(value))}
    except Exception as exc:  # 序列化边界
        raise SerializationError(f"failed to serialize protocol value: {exc}") from exc


def _from_jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        if set(value) == {"$bytes"}:
            raw = value["$bytes"]
            if not isinstance(raw, str):
                raise InternalError("invalid $bytes payload")
            return _decode_bytes(raw)
        if set(value) == {"$pickle"}:
            raw = value["$pickle"]
            if not isinstance(raw, str):
                raise InternalError("invalid $pickle payload")
            try:
                return cloudpickle.loads(_decode_bytes(raw))
            except Exception as exc:  # 反序列化边界
                raise SerializationError(
                    f"failed to deserialize protocol value: {exc}"
                ) from exc
        return {key: _from_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_from_jsonable(item) for item in value]
    return value


def _decode_events(raw_events: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_events, list):
        raise InternalError("service response has invalid 'events'")
    events: list[dict[str, Any]] = []
    for item in raw_events:
        event = _from_jsonable(item)
        if not isinstance(event, dict):
            raise InternalError("service response has invalid journal event")
        events.append(cast("dict[str, Any]", event))
    return events


def _decode_sse_event(event_type: str, data_lines: list[str]) -> dict[str, Any] | None:
    if event_type == "end":
        return None
    try:
        payload = json.loads("\n".join(data_lines))
    except ValueError as exc:
        raise InternalError(f"invalid SSE payload: {exc}") from exc
    event = _from_jsonable(payload)
    if not isinstance(event, dict):
        raise InternalError("SSE event must be a JSON object")
    return cast("dict[str, Any]", event)
