"""Release-test safety rails for timeouts and accidental network access."""

from __future__ import annotations

import os
import signal
import socket
import threading
from collections.abc import Generator
from contextlib import contextmanager

import pytest

_EXTERNAL_MARKERS = ("external", "integration")
_DEFAULT_TEST_TIMEOUT_SECONDS = 30

# Root conftest is imported before test modules. Keep collection and unmarked
# tests fully local: setup.py otherwise restores DBFS state during import, and
# later catalog lookups can reuse that live SDK client outside fixture control.
os.environ.setdefault("COST_OBS_RELEASE_TESTING", "1")
os.environ.setdefault("COST_OBS_CATALOG", "cost_obs_test_catalog")
os.environ.setdefault("COST_OBS_SCHEMA", "cost_obs_test_schema")


def _is_external_test(item: pytest.Item) -> bool:
    return any(item.get_closest_marker(marker) is not None for marker in _EXTERNAL_MARKERS)


@pytest.fixture(autouse=True)
def block_unmarked_network(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Make live network use fail fast unless the test is explicitly opt-in."""
    if _is_external_test(request.node):
        return

    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex
    original_thread_start = threading.Thread.start

    def test_origin() -> str:
        return getattr(threading.current_thread(), "_cost_obs_test_origin", request.node.nodeid)

    def tagged_thread_start(thread: threading.Thread, *args: object, **kwargs: object) -> object:
        thread._cost_obs_test_origin = request.node.nodeid  # type: ignore[attr-defined]
        return original_thread_start(thread, *args, **kwargs)

    def reject_inet_connect(sock: socket.socket, address: object) -> object:
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            pytest.fail(
                f"Unmarked test {test_origin()} attempted external network access; "
                "mark it external/integration and run the opt-in suite."
            )
        return original_connect(sock, address)

    def reject_inet_connect_ex(sock: socket.socket, address: object) -> int:
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            pytest.fail(
                f"Unmarked test {test_origin()} attempted external network access; "
                "mark it external/integration and run the opt-in suite."
            )
        return original_connect_ex(sock, address)

    def reject_create_connection(*_args: object, **_kwargs: object) -> socket.socket:
        pytest.fail(
            f"Unmarked test {test_origin()} attempted external network access; "
            "mark it external/integration and run the opt-in suite."
        )

    monkeypatch.setattr(threading.Thread, "start", tagged_thread_start)
    monkeypatch.setattr(socket.socket, "connect", reject_inet_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", reject_inet_connect_ex)
    monkeypatch.setattr(socket, "create_connection", reject_create_connection)


@contextmanager
def _phase_timeout(item: pytest.Item, phase: str) -> Generator[None, None, None]:
    """Bound one pytest setup/call/teardown phase on POSIX."""
    timeout_seconds = int(
        os.environ.get("COST_OBS_TEST_TIMEOUT_SECONDS", _DEFAULT_TEST_TIMEOUT_SECONDS)
    )
    if timeout_seconds <= 0 or not hasattr(signal, "SIGALRM"):
        yield
        return

    def fail_on_timeout(_signum: int, _frame: object) -> None:
        pytest.fail(
            f"{item.nodeid} {phase} exceeded the "
            f"{timeout_seconds}s release-gate timeout"
        )

    previous_handler = signal.signal(signal.SIGALRM, fail_on_timeout)
    previous_timer = signal.getitimer(signal.ITIMER_REAL)
    signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer[0] > 0:
            signal.setitimer(signal.ITIMER_REAL, *previous_timer)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_setup(item: pytest.Item) -> Generator[None, None, None]:
    with _phase_timeout(item, "setup"):
        yield


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item: pytest.Item) -> Generator[None, None, None]:
    with _phase_timeout(item, "call"):
        yield


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_teardown(
    item: pytest.Item,
    nextitem: pytest.Item | None,
) -> Generator[None, None, None]:
    del nextitem
    with _phase_timeout(item, "teardown"):
        yield
