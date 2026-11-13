"""``actant.ext`` 通用扩展契约单元测试。"""

from __future__ import annotations

import json
import threading
from typing import Any

import pytest

from actant.capabilities import TaskEvent, WorkflowEvent
from actant.ext import (
    ClusterView,
    EventAdapter,
    EventEnvelope,
    NodeView,
    PlacementContext,
    PlacementDecision,
    bind_placement_policy,
    compose,
    runtime_config,
)


def test_node_view_from_peer_and_candidate() -> None:
    peer = {
        "node_id": "n1",
        "labels": {"pool": "a"},
        "available_slots": 3,
        "max_slots": 8,
        "active_workflows": ["w1"],
        "platform": "linux",
        "last_heartbeat_ms": 123,
        "endpoint": "abc",
    }
    view = NodeView.from_peer(peer)
    assert view.node_id == "n1"
    assert view.labels["pool"] == "a"
    assert view.available_slots == 3
    assert view.max_slots == 8
    assert view.active_workflows == ("w1",)
    assert view.platform == "linux"
    assert view.endpoint == "abc"

    candidate = {
        "node_id": "n2",
        "labels": {"pool": "b"},
        "available_slots": 2,
        "max_slots": 4,
        "role": "worker",
        "draining": True,
        "endpoint_addr": "def",
        "available_quotas": {"slot": 2},
    }
    candidate_view = NodeView.from_route_candidate(candidate)
    assert candidate_view.role == "worker"
    assert candidate_view.draining is True
    assert candidate_view.endpoint == "def"
    assert candidate_view.available_quotas["slot"] == 2


def test_placement_context_from_route_context() -> None:
    ctx = PlacementContext.from_route_context(
        {
            "local_role": "worker",
            "local_draining": False,
            "local_available_slots": 2,
            "heartbeat_ttl_ms": 5000,
            "local_labels": {"pool": "a"},
            "local_available_quotas": {"slot": 1},
            "required_tags": ["pool=a", "arch=x86_64"],
            "required_quota": "slot",
        }
    )
    assert ctx.local_role == "worker"
    assert ctx.local_available_slots == 2
    assert ctx.required_tags == ("pool=a", "arch=x86_64")
    assert ctx.required_quota == "slot"


def test_placement_decision_replies() -> None:
    assert PlacementDecision.auto().to_route_reply() is None
    assert PlacementDecision.local().to_route_reply() == "local"
    assert PlacementDecision.remote("n1").to_route_reply() == ("remote", "n1")
    assert PlacementDecision.defer("busy").to_route_reply() == ("defer", "busy")
    with pytest.raises(ValueError):
        PlacementDecision("remote")
    with pytest.raises(ValueError):
        PlacementDecision("bogus")  # type: ignore[arg-type]


def test_bind_placement_policy_translates_context_and_decision() -> None:
    seen: dict[str, Any] = {}

    def policy(ctx: PlacementContext, nodes: list[NodeView]) -> PlacementDecision:
        seen["ctx"] = ctx
        seen["nodes"] = nodes
        return PlacementDecision.remote(nodes[0].node_id)

    route = bind_placement_policy(policy)
    reply = route(
        {"local_role": "worker", "required_tags": ["pool=a"]},
        [{"node_id": "n1", "labels": {"pool": "a"}, "available_slots": 2}],
    )
    assert reply == ("remote", "n1")
    assert seen["ctx"].required_tags == ("pool=a",)
    assert seen["nodes"][0].labels["pool"] == "a"

    with pytest.raises(TypeError):
        bind_placement_policy(lambda ctx, nodes: "not-a-decision")({}, [{"node_id": "n1"}])
    with pytest.raises(TypeError):
        bind_placement_policy("not-callable")  # type: ignore[arg-type]


def test_event_envelope_task_is_deterministic_and_json_safe() -> None:
    event = TaskEvent(
        kind="completed",
        task_id="t1",
        workflow_id="w1",
        result_payload=b"ok",
        attempt=1,
    )
    first = EventEnvelope.from_task(event, source="n1")
    second = EventEnvelope.from_task(event, source="n1")
    assert first.kind == "task.completed"
    assert first.source == "n1"
    assert first.event_id == second.event_id
    assert first.payload["result_payload"] == {"__bytes__": "b2s="}
    json.dumps(first.to_dict())


def test_event_envelope_workflow() -> None:
    envelope = EventEnvelope.from_workflow(
        WorkflowEvent(kind="failed", workflow_id="w1", error="boom")
    )
    assert envelope.kind == "workflow.failed"
    assert envelope.payload["error"] == "boom"


class _FakeLayer:
    def __init__(self) -> None:
        self.handlers: list[Any] = []

    def chain(self, handler: Any) -> _FakeLayer:
        self.handlers.append(handler)
        return self

    def remove(self, handler: Any) -> bool:
        if handler in self.handlers:
            self.handlers.remove(handler)
            return True
        return False


class _FakeRuntime:
    def __init__(self, peers: list[dict[str, Any]]) -> None:
        self._peers = peers
        self.layers = {
            "TaskLifecycle": _FakeLayer(),
            "WorkflowLifecycle": _FakeLayer(),
        }

    def layer(self, name: str) -> _FakeLayer:
        return self.layers[name]

    def peers(self) -> list[dict[str, Any]]:
        return list(self._peers)


def test_event_adapter_subscribe_and_close() -> None:
    runtime = _FakeRuntime([])
    received: list[EventEnvelope] = []
    subscription = EventAdapter(source="n1").subscribe(runtime, received.append)

    runtime.layer("TaskLifecycle").handlers[0](TaskEvent(kind="completed", task_id="t1"))
    runtime.layer("WorkflowLifecycle").handlers[0](WorkflowEvent(kind="started", workflow_id="w1"))
    assert [envelope.kind for envelope in received] == [
        "task.completed",
        "workflow.started",
    ]

    subscription.close()
    subscription.close()
    assert runtime.layer("TaskLifecycle").handlers == []


def test_cluster_view_nodes_and_watch() -> None:
    runtime = _FakeRuntime(
        [
            {
                "node_id": "n2",
                "labels": {"pool": "b"},
                "available_slots": 1,
                "max_slots": 2,
            },
            {"node_id": "n1", "labels": {}, "available_slots": 0, "max_slots": 1},
        ]
    )
    view = ClusterView(runtime)
    assert [node.node_id for node in view.nodes()] == ["n1", "n2"]
    assert view.snapshot()["n2"].labels["pool"] == "b"

    stop = threading.Event()
    result: list[Any] = []

    def _next_change() -> None:
        result.append(next(view.watch(interval_s=0.005, stop_event=stop)))

    thread = threading.Thread(target=_next_change, daemon=True)
    thread.start()
    runtime._peers = [
        {"node_id": "n2", "labels": {"pool": "b"}, "available_slots": 1, "max_slots": 2},
        {"node_id": "n3", "labels": {}, "available_slots": 0, "max_slots": 1},
    ]
    thread.join(timeout=2)
    stop.set()
    assert result
    assert [node.node_id for node in result[0].joined] == ["n3"]
    assert [node.node_id for node in result[0].left] == ["n1"]


def test_compose_starts_in_order_and_stops_in_reverse() -> None:
    events: list[str] = []

    class Component:
        def __init__(self, name: str) -> None:
            self.name = name

        def start(self, runtime: Any) -> None:
            events.append(f"start:{self.name}")

        def stop(self) -> None:
            events.append(f"stop:{self.name}")

    composition = compose(object(), Component("a"), Component("b"))
    composition.stop()
    composition.stop()
    assert events == ["start:a", "start:b", "stop:b", "stop:a"]


def test_compose_rolls_back_on_start_failure() -> None:
    events: list[str] = []

    class Good:
        def start(self, runtime: Any) -> None:
            events.append("start:good")

        def stop(self) -> None:
            events.append("stop:good")

    class Bad:
        def start(self, runtime: Any) -> None:
            raise RuntimeError("boom")

        def stop(self) -> None:
            events.append("stop:bad")

    with pytest.raises(RuntimeError, match="boom"):
        compose(object(), Good(), Bad())
    assert events == ["start:good", "stop:good"]


def test_runtime_config_builder_builds_network_and_role() -> None:
    config = (
        runtime_config()
        .role("worker")
        .labels({"pool": "a"})
        .quotas({"slot": 2})
        .network(preset="none", listen_ip="127.0.0.1", listen_port=41000)
        .worker(max_concurrent_tasks=2)
        .build()
    )
    assert config.role == "worker"
    assert config.node_labels == {"pool": "a"}
    assert config.node_quotas == {"slot": 2}
    assert config.network.preset == "none"
    assert config.network.listen_ip == "127.0.0.1"
    assert config.network.listen_port == 41000
    assert config.max_concurrent_tasks == 2


def test_runtime_config_builder_validation() -> None:
    with pytest.raises(ValueError):
        runtime_config().role("bogus")
    with pytest.raises(ValueError):
        runtime_config().scheduler("bogus")
    with pytest.raises(ValueError):
        runtime_config().quotas({"slot": -1})
    with pytest.raises(ValueError):
        runtime_config().signing("", required=True).build()
    assert runtime_config().signing("secret", required=True).build().payload_signing_key
