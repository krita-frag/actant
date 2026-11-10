"""拓扑模型单元测试（无 Runtime）。"""

from __future__ import annotations

import pytest
from act_ext_testkit.topology import Topology, diff_topology


def test_mesh_is_bidirectional_and_connected() -> None:
    topo = Topology.mesh(["a", "b", "c"])
    assert topo.neighbours("a") == {"b", "c"}
    assert topo.neighbours("b") == {"a", "c"}
    assert topo.is_connected()
    assert len(topo.edges) == 6


def test_ring_neighbours() -> None:
    topo = Topology.ring(["a", "b", "c", "d"])
    assert topo.neighbours("a") == {"b", "d"}
    assert topo.neighbours("c") == {"b", "d"}
    assert topo.is_connected()


def test_line_has_two_components_when_edge_removed() -> None:
    topo = Topology.line(["a", "b", "c"])
    assert topo.neighbours("a") == {"b"}
    assert topo.neighbours("b") == {"a", "c"}
    split = Topology.custom(["a", "b", "c"], {("a", "b")}, bidirectional=True)
    assert not split.is_connected()
    assert split.components() == [("a", "b"), ("c",)]


def test_star_center_connects_all_leaves() -> None:
    topo = Topology.star("hub", ["x", "y", "z"])
    assert topo.neighbours("hub") == {"x", "y", "z"}
    assert topo.neighbours("x") == {"hub"}
    assert topo.is_connected()


def test_custom_directed_edges_are_not_mirrored() -> None:
    topo = Topology.custom(["a", "b"], {("a", "b")})
    assert topo.neighbours("a") == {"b"}
    assert topo.neighbours("b") == set()


def test_validation_rejects_unknown_nodes_and_self_edges() -> None:
    with pytest.raises(ValueError, match="unknown node"):
        Topology(("a",), frozenset({("a", "ghost")}))
    with pytest.raises(ValueError, match="self edge"):
        Topology(("a",), frozenset({("a", "a")}))


def test_diff_topology_reports_added_and_removed() -> None:
    before = Topology.line(["a", "b", "c"])
    after = Topology.ring(["a", "b", "c"])
    change = diff_topology(before, after)
    assert ("a", "c") in change.added_edges
    assert ("c", "a") in change.added_edges
    assert not change.removed_edges


def test_from_name_star_uses_first_node_as_center() -> None:
    topo = Topology.from_name("star", ["hub", "a", "b"])
    assert topo.neighbours("hub") == {"a", "b"}
