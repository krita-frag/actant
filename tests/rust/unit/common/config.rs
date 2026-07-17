//! Unit tests extracted from `src/common/config.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;

#[test]
fn validate_accepts_empty_signing_key() {
    let config = ActantConfig {
        payload_signing_key: Vec::new(),
        ..Default::default()
    };
    assert!(
        config.validate().is_ok(),
        "empty signing key should disable signing"
    );
}

#[test]
fn validate_accepts_non_empty_signing_key() {
    let config = ActantConfig {
        payload_signing_key: b"shared-secret".to_vec(),
        ..Default::default()
    };
    assert!(config.validate().is_ok());
}

#[test]
fn failover_validate_accepts_default() {
    assert!(FailoverConfig::default().validate().is_ok());
}

#[test]
fn failover_validate_rejects_zero_heartbeat() {
    let cfg = FailoverConfig {
        heartbeat_interval_ms: 0,
        ..FailoverConfig::default()
    };
    let err = cfg.validate().unwrap_err();
    assert!(format!("{err}").contains("heartbeat_interval_ms"));
}

#[test]
fn failover_validate_rejects_failure_le_heartbeat() {
    let default = FailoverConfig::default();
    let cfg = FailoverConfig {
        failure_timeout_ms: default.heartbeat_interval_ms,
        ..default
    };
    let err = cfg.validate().unwrap_err();
    assert!(format!("{err}").contains("failure_timeout_ms"));
}

#[test]
fn failover_validate_rejects_lease_le_failure() {
    let default = FailoverConfig::default();
    let cfg = FailoverConfig {
        lease_duration_ms: default.failure_timeout_ms,
        ..default
    };
    let err = cfg.validate().unwrap_err();
    assert!(format!("{err}").contains("split-brain"));
}

#[test]
fn failover_validate_rejects_zero_check_interval() {
    let cfg = FailoverConfig {
        lease_expiry_check_interval_secs: 0,
        ..FailoverConfig::default()
    };
    let err = cfg.validate().unwrap_err();
    assert!(format!("{err}").contains("lease_expiry_check_interval_secs"));
}
