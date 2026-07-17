//! Unit tests extracted from `src/common/error.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;

#[test]
fn display_variants() {
    assert_eq!(
        ActantError::Storage("bad".into()).to_string(),
        "storage error: bad"
    );
    assert_eq!(
        ActantError::Network("down".into()).to_string(),
        "network error: down"
    );
    assert_eq!(
        ActantError::NotFound("x".into()).to_string(),
        "not found: x"
    );
}

#[test]
fn from_io_error() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "oops");
    let err: ActantError = io_err.into();
    assert!(matches!(err, ActantError::StorageIo(_)));
}

#[test]
fn from_arc_io_error_unique() {
    let io_err = std::io::Error::other("arc");
    let arc = Arc::new(io_err);
    let err: ActantError = arc.into();
    assert!(matches!(err, ActantError::StorageIo(_)));
}

#[test]
fn from_arc_io_error_shared() {
    let io_err = std::io::Error::other("shared");
    let arc = Arc::new(io_err);
    let clone = Arc::clone(&arc);
    let _kept = clone;
    let err: ActantError = arc.into();
    assert!(matches!(err, ActantError::StorageIo(_)));
}
