"""服务协议 v1：跨进程提交、查询、信号、取消与订阅。

服务端与客户端只通过 HTTP 交换数据；客户端不共享服务端的 ``data_dir``，
也不要求本进程导入 flow 定义（引用由 :class:`DefinitionResolver` 在客户端
构造，或由部署清单提供）。
"""

from __future__ import annotations

import http.client
import json
import threading
from pathlib import Path

import pytest
from act_ext_service import (
    DefinitionHashMismatchError,
    DefinitionNotFoundError,
    DefinitionRef,
    DefinitionResolver,
    DefinitionVersionMismatchError,
    ServiceClient,
    ServiceServer,
)

from actant import Runtime, flow, task, use_runtime
from actant.workflow import wait_signal

_PARKED = threading.Event()


@task(name="service-inc")
def _inc(value: int) -> int:
    return value + 1


@flow(name="service-echo", version=2, timeout_ms=30_000)
def _echo(value: int) -> int:
    return _inc.submit(value).result()


@flow(name="service-wait", version=1)
def _wait_for_go() -> bytes:
    _PARKED.set()
    return wait_signal("go", timeout_ms=10_000) or b""


def _serve(data_dir: Path) -> tuple[Runtime, ServiceServer, ServiceClient]:
    runtime = Runtime(name="service-node", data_dir=str(data_dir))
    runtime.start()
    server = ServiceServer(runtime)
    server.start()
    return runtime, server, ServiceClient(server.base_url)


def _host(runtime: Runtime) -> None:
    # 重放体经 effect 广播生命周期事件，需要线程内的 Runtime 上下文。
    with use_runtime(runtime):
        runtime.host_flows()


def test_submit_query_result_journal_and_subscribe(tmp_path: Path) -> None:
    runtime, server, client = _serve(tmp_path)
    try:
        ref = DefinitionResolver().reference("service-echo")
        workflow_id = client.submit(ref, 41)

        assert workflow_id in client.list_workflows()
        pending = client.state(workflow_id)
        assert pending is not None
        assert pending["state"] != "Completed"

        _host(runtime)

        completed = client.state(workflow_id)
        assert completed is not None and completed["state"] == "Completed"
        assert client.result(workflow_id) == 42

        events = client.journal(workflow_id)
        assert events[0]["kind"] == "submitted"
        assert any(event["kind"] == "task_completed" for event in events)

        kinds = [event["kind"] for event in client.subscribe(workflow_id)]
        assert "completed" in kinds
    finally:
        server.stop()
        runtime.stop()


def test_signal_wakes_parked_flow(tmp_path: Path) -> None:
    _PARKED.clear()
    runtime, server, client = _serve(tmp_path)
    host = threading.Thread(target=_host, args=(runtime,), daemon=True)
    try:
        workflow_id = client.submit(
            DefinitionResolver().reference("service-wait")
        )
        host.start()
        assert _PARKED.wait(5.0), "flow body was not hosted"

        # 等待点可能尚未注册：此时信号入缓冲，flow 注册后立即命中。
        delivered = client.signal(workflow_id, "go", b"hello")
        assert delivered is None or delivered == b"hello"

        host.join(timeout=15.0)
        assert not host.is_alive(), "flow did not converge after signal"
        assert client.result(workflow_id) == b"hello"
    finally:
        server.stop()
        runtime.stop()
        host.join(timeout=5.0)


def test_cancel_unhosted_flow(tmp_path: Path) -> None:
    runtime, server, client = _serve(tmp_path)
    try:
        workflow_id = client.submit(
            DefinitionResolver().reference("service-echo"), 1
        )

        client.cancel(workflow_id)

        state = client.state(workflow_id)
        assert state is not None and state["state"] == "Cancelled"
    finally:
        server.stop()
        runtime.stop()


def test_definition_drift_returns_stable_errors(tmp_path: Path) -> None:
    runtime, server, client = _serve(tmp_path)
    try:
        ref = DefinitionResolver().reference("service-echo")

        with pytest.raises(DefinitionNotFoundError):
            client.submit(DefinitionRef("service-missing", 1, "0" * 64), 1)
        with pytest.raises(DefinitionVersionMismatchError):
            client.submit(DefinitionRef(ref.name, ref.version + 1, ref.hash), 1)
        with pytest.raises(DefinitionHashMismatchError):
            client.submit(DefinitionRef(ref.name, ref.version, "0" * 64), 1)
    finally:
        server.stop()
        runtime.stop()


def test_non_loopback_requires_explicit_opt_in(tmp_path: Path) -> None:
    runtime = Runtime(name="service-node", data_dir=str(tmp_path))
    runtime.start()
    try:
        with pytest.raises(ValueError, match="allow_insecure"):
            ServiceServer(runtime, host="0.0.0.0")
    finally:
        runtime.stop()


def test_payload_too_large_returns_413(tmp_path: Path) -> None:
    runtime, server, _client = _serve(tmp_path)
    host, port = server.address
    conn = http.client.HTTPConnection(host, port, timeout=5.0)
    try:
        conn.putrequest("POST", "/v1/workflows")
        conn.putheader("Content-Length", str(65 * 1024 * 1024))
        conn.endheaders()
        response = conn.getresponse()
        assert response.status == 413
        payload = json.loads(response.read())
        assert payload["error"]["kind"] == "payload_too_large"
    finally:
        conn.close()
        server.stop()
        runtime.stop()
