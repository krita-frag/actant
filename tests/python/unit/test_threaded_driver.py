"""``ThreadedDriver`` 的并发去重、容量上限与错误传播。"""

from __future__ import annotations

import threading

import pytest

from actant.driver import ThreadedDriver
from actant.exceptions import ActantTimeoutError, InvalidStateError


def test_rejects_invalid_max_workers() -> None:
    with pytest.raises(ValueError, match="max_workers"):
        ThreadedDriver(max_workers=0)


def test_deduplicates_in_flight_workflow() -> None:
    started = threading.Event()
    release = threading.Event()

    def _run() -> None:
        started.set()
        release.wait(5)

    driver = ThreadedDriver(max_workers=2)
    try:
        assert driver.spawn("wf-1", _run) is True
        assert started.wait(5)
        assert driver.spawn("wf-1", _run) is False
    finally:
        release.set()
        driver.join()


def test_rejects_spawn_over_capacity() -> None:
    release = threading.Event()
    driver = ThreadedDriver(max_workers=1)
    try:
        assert driver.spawn("wf-1", lambda: release.wait(5)) is True
        with pytest.raises(InvalidStateError, match="max_workers"):
            driver.spawn("wf-2", lambda: None)
    finally:
        release.set()
        driver.join()


def test_join_reraises_replay_error_once() -> None:
    def _boom() -> None:
        raise RuntimeError("boom")

    driver = ThreadedDriver()
    assert driver.spawn("wf-1", _boom) is True

    with pytest.raises(RuntimeError, match="boom"):
        driver.join()
    driver.join()


def test_deadline_timeout_is_not_a_join_error() -> None:
    def _timeout() -> None:
        raise ActantTimeoutError("workflow deadline exceeded")

    driver = ThreadedDriver()
    assert driver.spawn("wf-1", _timeout) is True
    driver.join()
