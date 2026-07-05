"""Pytest session config (v1.24.12).

v1.24.12 made forward-auth FAIL CLOSED: with `trust_forward_auth` on but an
empty `forward_auth_allowed_ips`, motif no longer trusts the X-Authentik-Username
header (it was an admin-bypass footgun on a directly-reachable port). The test
suite (162 files) authenticates admin requests via that header with trust on, so
without an allowlist every such request would now be rejected.

Two shims make the suite represent a CORRECTLY-configured forward-auth deploy:
  1. set a default allowlist (127.0.0.1) for the session, and
  2. default every TestClient's peer to 127.0.0.1 — Starlette's default is the
     non-IP host "testclient", which `ipaddress` can't parse, so it could never
     match an IP allowlist.

Tests that exercise the allowlist directly are unaffected: v1.17.23's unit tests
use a MagicMock request, and v1.21.16 asserts static boot-warning text. A test
that needs the EMPTY-allowlist (fail-closed) path overrides the env per-test via
`monkeypatch.setenv("MOTIF_FORWARD_AUTH_ALLOWED_IPS", "")`.
"""
from __future__ import annotations

import os

import pytest
from starlette.testclient import TestClient


@pytest.fixture(autouse=True)
def _reset_login_rate_limit_state():
    """v0.51.81: isolate the login-failure counters between tests.

    The per-IP bucket AND the new global brute-force ceiling live as
    process-level state in app.core.auth; the global window (900s) does not age
    out within a single suite run, so failed logins in one test would otherwise
    pile up across the ~4-min suite and start 429-ing unrelated later tests.
    Reset both before every test so each starts from a clean slate."""
    from app.core.auth import _reset_login_failures_for_test
    _reset_login_failures_for_test()
    yield

# 1. Allowlist the test client's IP so forward-auth (fail-closed since v1.24.12)
#    admits the X-Authentik-Username header the suite uses to authenticate.
#    setdefault → a real env value (or a per-test monkeypatch) still wins.
os.environ.setdefault("MOTIF_FORWARD_AUTH_ALLOWED_IPS", "127.0.0.1")

# 2. Present a real, allowlisted peer. Starlette defaults TestClient's client to
#    ("testclient", 50000) — a non-IP host. Force 127.0.0.1 unless a test passes
#    its own `client=`. Idempotent (conftest can be imported once per session,
#    but guard anyway).
_orig_testclient_init = TestClient.__init__
if not getattr(_orig_testclient_init, "_motif_client_patched", False):
    def _patched_testclient_init(self, *args, **kwargs):
        kwargs.setdefault("client", ("127.0.0.1", 50000))
        return _orig_testclient_init(self, *args, **kwargs)
    _patched_testclient_init._motif_client_patched = True
    TestClient.__init__ = _patched_testclient_init
