"""``actant.ext.discovery`` 的单元测试。

纯逻辑用例用 FakeRuntime（不触网）；beacon 用例用真实 UDP 组播回环
（单机两实例互发现，验证 socket 装配与载荷解析全链路）。
"""

from __future__ import annotations

import time

import pytest

from actant.ext.discovery import Dialer, PeerBeacon, parse_announcement


class _FakeRuntime:
    """最小 Runtime 面：listen_addresses / dial / discover_peers。"""

    def __init__(self, endpoint_id: str, endpoint_addr: str, *, dial_failures: int = 0):
        self._endpoint_id = endpoint_id
        self._endpoint_addr = endpoint_addr
        self._dial_failures = dial_failures
        self.dial_addrs: list[str] = []
        self._gossip_known: list[str] = []
        self.name = ""

    def listen_addresses(self) -> dict[str, object]:
        return {
            "endpoint_id": self._endpoint_id,
            "endpoint_addr": self._endpoint_addr,
            "relay_url": None,
            "direct_addrs": [],
        }

    def dial(self, addr: str) -> None:
        if self._dial_failures > 0:
            self._dial_failures -= 1
            raise RuntimeError("dial boom")
        self.dial_addrs.append(addr)

    def discover_peers(self) -> list[str]:
        return list(self._gossip_known)


def _wait_until(pred, *, timeout_s: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if pred():
            return
        time.sleep(0.05)
    raise AssertionError("condition not met within timeout")


# ---- parse_announcement ----


def test_parse_announcement_accepts_valid_payload() -> None:
    payload = (
        b'{"v":1,"endpoint_id":"abc","endpoint_addr":"deadbeef","name":"node-1"}'
    )
    assert parse_announcement(payload) == {
        "endpoint_id": "abc",
        "endpoint_addr": "deadbeef",
        "name": "node-1",
    }


def test_parse_announcement_rejects_invalid_payloads() -> None:
    assert parse_announcement(b"not json") is None
    assert parse_announcement(b'{"v":2,"endpoint_id":"a","endpoint_addr":"b"}') is None
    assert parse_announcement(b'{"v":1,"endpoint_addr":"b"}') is None
    assert parse_announcement(b'{"v":1,"endpoint_id":"a"}') is None
    assert parse_announcement(b'{"v":1,"endpoint_id":"","endpoint_addr":"b"}') is None
    assert parse_announcement(b'{"v":1,"endpoint_id":1,"endpoint_addr":"b"}') is None


# ---- Dialer ----


def test_dialer_dedups_same_endpoint_and_dials_once() -> None:
    rt = _FakeRuntime("self", "self-addr")
    dialer = Dialer(rt, poll_interval_s=0.05)
    dialer.start()
    try:
        assert dialer.announce("peer-1", "peer-1-addr") is True
        assert dialer.announce("peer-1", "peer-1-addr") is False

        _wait_until(lambda: rt.dial_addrs == ["peer-1-addr"])
        # 已连接且地址未变的重复 announce 不触发重拨。
        dialer.announce("peer-1", "peer-1-addr")
        time.sleep(0.3)
        assert rt.dial_addrs == ["peer-1-addr"]

        snapshot = dialer.known_peers()
        assert snapshot["peer-1"]["state"] == "connected"
        assert snapshot["peer-1"]["endpoint_addr"] == "peer-1-addr"
    finally:
        dialer.stop()


def test_dialer_address_change_reactivates_dial() -> None:
    rt = _FakeRuntime("self", "self-addr")
    dialer = Dialer(rt, poll_interval_s=0.05)
    dialer.start()
    try:
        dialer.announce("peer-1", "old-addr")
        _wait_until(lambda: rt.dial_addrs == ["old-addr"])
        dialer.announce("peer-1", "new-addr")
        _wait_until(lambda: rt.dial_addrs == ["old-addr", "new-addr"])
    finally:
        dialer.stop()


def test_dialer_retries_up_to_max_then_reports_error() -> None:
    rt = _FakeRuntime("self", "self-addr", dial_failures=10)
    errors: list[tuple[str, str, Exception]] = []
    dialer = Dialer(
        rt,
        max_dial_attempts=2,
        retry_interval_s=0.05,
        poll_interval_s=0.05,
        on_error=lambda eid, addr, exc: errors.append((eid, addr, exc)),
    )
    dialer.start()
    try:
        dialer.announce("peer-1", "peer-1-addr")
        _wait_until(lambda: len(errors) >= 1)
        assert errors[0][0] == "peer-1"
        assert errors[0][1] == "peer-1-addr"
        assert dialer.known_peers()["peer-1"]["state"] == "failed"

        # failed 条目不再自动重试：等待远超重试窗口后计数不变。
        time.sleep(0.4)
        assert len(rt.dial_addrs) == 0  # 全部失败，从未成功

        # 新的 announce 重新激活。
        rt._dial_failures = 0
        dialer.announce("peer-1", "peer-1-addr")
        _wait_until(lambda: rt.dial_addrs == ["peer-1-addr"])
        assert dialer.known_peers()["peer-1"]["state"] == "connected"
    finally:
        dialer.stop()


def test_dialer_reconnect_redials_missing_peer() -> None:
    rt = _FakeRuntime("self", "self-addr")
    dialer = Dialer(
        rt,
        retry_interval_s=0.05,
        reconnect=True,
        reconnect_interval_s=0.3,
        poll_interval_s=0.05,
    )
    dialer.start()
    try:
        dialer.announce("peer-1", "peer-1-addr")
        _wait_until(lambda: rt.dial_addrs == ["peer-1-addr"])

        # dial 成功后 FakeRuntime 不自动把对端放进 gossip 视图：探测必然
        # miss（rt._gossip_known 一直为空），每个探测周期重拨一次。
        _wait_until(lambda: len(rt.dial_addrs) >= 2, timeout_s=5.0)
        assert dialer.known_peers()["peer-1"]["state"] in ("connected", "stale")
    finally:
        dialer.stop()


def test_dialer_reconnect_skips_never_connected_entries() -> None:
    rt = _FakeRuntime("self", "self-addr", dial_failures=1)
    dialer = Dialer(
        rt,
        max_dial_attempts=1,
        retry_interval_s=0.05,
        reconnect=True,
        reconnect_interval_s=0.2,
        poll_interval_s=0.05,
    )
    dialer.start()
    try:
        dialer.announce("peer-1", "peer-1-addr")
        _wait_until(lambda: dialer.known_peers().get("peer-1", {}).get("state") == "failed")
        time.sleep(0.5)
        # 初始 dial 失败的条目不参与重连探测（从未成功建立）。
        assert rt.dial_addrs == []
    finally:
        dialer.stop()


def test_dialer_validates_parameters() -> None:
    rt = _FakeRuntime("self", "self-addr")
    with pytest.raises(ValueError):
        Dialer(rt, max_dial_attempts=0)
    with pytest.raises(ValueError):
        Dialer(rt, retry_interval_s=0)
    d = Dialer(rt)
    with pytest.raises(ValueError):
        d.announce("", "addr")
    with pytest.raises(ValueError):
        d.announce("id", "")


# ---- PeerBeacon（真实组播回环）----


def test_beacons_discover_each_other_on_loopback() -> None:
    rt_a = _FakeRuntime("node-a", "addr-a")
    rt_b = _FakeRuntime("node-b", "addr-b")
    beacon_a = PeerBeacon(rt_a, interval_s=0.15, interface="127.0.0.1")
    beacon_b = PeerBeacon(rt_b, interval_s=0.15, interface="127.0.0.1")
    try:
        beacon_a.start()
        beacon_b.start()

        # 双向互发现并拨号。
        _wait_until(lambda: rt_a.dial_addrs == ["addr-b"], timeout_s=10.0)
        _wait_until(lambda: rt_b.dial_addrs == ["addr-a"], timeout_s=10.0)

        # 周期性重发不造成重复拨号（dedup 生效）。
        time.sleep(0.6)
        assert rt_a.dial_addrs == ["addr-b"]
        assert rt_b.dial_addrs == ["addr-a"]
    finally:
        beacon_a.stop()
        beacon_b.stop()


def test_beacon_ignores_own_announcement() -> None:
    rt = _FakeRuntime("solo", "solo-addr")
    beacon = PeerBeacon(rt, interval_s=0.1, interface="127.0.0.1")
    try:
        beacon.start()
        time.sleep(0.5)
        # 本机回环收到的自身信标被过滤，地址簿为空。
        assert beacon.dialer.known_peers() == {}
        assert rt.dial_addrs == []
    finally:
        beacon.stop()


def test_beacon_requires_started_runtime() -> None:
    class _StoppedRuntime:
        def listen_addresses(self) -> dict[str, object]:
            raise RuntimeError("Runtime 未启动")

    beacon = PeerBeacon(_StoppedRuntime())
    with pytest.raises(RuntimeError):
        beacon.start()


def test_beacon_validates_parameters() -> None:
    rt = _FakeRuntime("x", "y")
    with pytest.raises(ValueError):
        PeerBeacon(rt, interval_s=0)
    with pytest.raises(ValueError):
        PeerBeacon(rt, port=70000)
    with pytest.raises(ValueError):
        PeerBeacon(rt, group="127.0.0.1")  # 非组播地址
    with pytest.raises(ValueError):
        PeerBeacon(rt, interface="not-an-ip")


def test_beacon_shares_external_dialer() -> None:
    rt = _FakeRuntime("shared", "shared-addr")
    dialer = Dialer(rt, poll_interval_s=0.05)
    beacon = PeerBeacon(rt, interval_s=0.15, interface="127.0.0.1", dialer=dialer)
    try:
        beacon.start()
        assert beacon.dialer is dialer
        dialer.start()
        # 自建信标只收到自身载荷（被过滤），外部投喂的条目正常拨号。
        dialer.announce("peer-x", "peer-x-addr")
        _wait_until(lambda: rt.dial_addrs == ["peer-x-addr"])
    finally:
        beacon.stop()
        dialer.stop()


def test_dialer_stop_is_idempotent() -> None:
    rt = _FakeRuntime("self", "self-addr")
    dialer = Dialer(rt)
    dialer.start()
    dialer.stop()
    dialer.stop()
    assert dialer._thread is None
