"""NAT 配置门禁与信任域/对抗场景单元测试。"""

from __future__ import annotations

import pytest
from act_ext_testkit import (
    AdversaryError,
    NatError,
    NatMode,
    NatTopology,
    NatUnavailable,
    TrustDomain,
    assert_rejected,
    direct,
    generate_signing_key,
    hole_punch,
    key_fingerprint,
    mismatched_domains,
    nat_enabled,
    relay_only,
    restricted_domain,
)
from act_ext_testkit.nat import NAT_ENV

from actant.actant import _ActantConfig, _NetworkConfig


def test_relay_only_requires_endpoints() -> None:
    with pytest.raises(NatError, match="relay endpoint"):
        NatTopology(NatMode.RELAY_ONLY)
    with pytest.raises(NatError):
        hole_punch([])


def test_external_nat_is_gated_by_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(NAT_ENV, raising=False)
    assert nat_enabled() is False
    topology = relay_only(["https://relay.example"])
    assert topology.requires_external_network
    with pytest.raises(NatUnavailable, match=NAT_ENV):
        topology.ensure_available()


def test_direct_nat_works_without_external_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(NAT_ENV, raising=False)
    topology = direct()
    assert not topology.requires_external_network
    config = topology.network_config()
    assert isinstance(config, _NetworkConfig)


def test_relay_config_built_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(NAT_ENV, "1")
    topology = relay_only(["https://relay.example"], network_overrides={"preset": "none"})
    assert nat_enabled() is True
    config = topology.network_config()
    assert isinstance(config, _NetworkConfig)
    assert topology.describe()["mode"] == "relay_only"


def test_trust_domain_builds_signed_config() -> None:
    key = generate_signing_key()
    domain = TrustDomain(key, allowed_peer_ids=("peer-a",))
    config = domain.build_config(data_dir="/tmp/not-used", labels={"role": "x"})
    assert isinstance(config, _ActantConfig)
    assert key_fingerprint(key) in str(domain.describe())
    assert len(key_fingerprint(key)) == 16


def test_trust_domain_requires_signing_key() -> None:
    with pytest.raises(AdversaryError, match="signing_key"):
        TrustDomain("").build_config(data_dir="/tmp/not-used")


def test_mismatched_domains_use_distinct_keys() -> None:
    left, right = mismatched_domains()
    assert left.signing_key != right.signing_key
    assert key_fingerprint(left.signing_key) != key_fingerprint(right.signing_key)


def test_restricted_domain_requires_members() -> None:
    with pytest.raises(AdversaryError):
        restricted_domain([])
    domain = restricted_domain(["peer-1", "peer-2"])
    assert domain.allowed_peer_ids == ("peer-1", "peer-2")


def test_assert_rejected_accepts_exception() -> None:
    error = assert_rejected(
        lambda: (_ for _ in ()).throw(ValueError("denied")),
        description="handshake",
    )
    assert isinstance(error, ValueError)


def test_assert_rejected_fails_on_success() -> None:
    with pytest.raises(AssertionError, match="expected to be rejected"):
        assert_rejected(lambda: None, description="silent success")
