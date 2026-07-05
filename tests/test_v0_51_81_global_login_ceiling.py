"""v0.51.81 — CSS+login audit: a GLOBAL brute-force ceiling on /login.

The v1.21.18 per-IP throttle is defeated by an attacker rotating source IPs —
trivial, since request.client.host is spoofable while uvicorn runs
forwarded_allow_ips="*" (the deferred forward-auth item). motif has exactly one
admin account, so a high failure volume across ALL IPs is unambiguously an
attack. A second in-memory rolling-window counter caps total failures across
every IP: once LOGIN_GLOBAL_MAX_FAILURES is reached the endpoint refuses
everyone (429, no password check) until the failures age out.

Behavioral tests against the real counters (the autouse conftest fixture resets
the process-level buckets before each test, so these start from a clean slate).
"""
from __future__ import annotations

import pytest

from app.core.auth import (
    LOGIN_GLOBAL_MAX_FAILURES,
    LOGIN_MAX_FAILURES,
    _reset_login_failures_for_test,
    clear_login_failures,
    login_rate_limited,
    record_login_failure,
)


def test_global_ceiling_blocks_a_fresh_ip():
    """Once the global ceiling is reached via many DISTINCT IPs (each below its
    own per-IP limit), a brand-new IP with zero personal failures is refused —
    that's the IP-rotation defense the per-IP counter can't provide."""
    _reset_login_failures_for_test()
    # spread failures across distinct IPs, each staying UNDER its own per-IP
    # limit, so no single IP is throttled on its own account — only the global
    # bucket fills.
    per_ip = LOGIN_MAX_FAILURES - 1
    ips_needed = (LOGIN_GLOBAL_MAX_FAILURES // per_ip) + 1
    for n in range(ips_needed):
        for _ in range(per_ip):
            record_login_failure(f"198.51.100.{n}")
    # a brand-new IP has ZERO personal failures, so its per-IP bucket is empty;
    # the only thing that can refuse it is the global ceiling.
    fresh = "203.0.113.99"
    assert login_rate_limited(fresh), (
        "the global ceiling must refuse even an IP with no personal failures")


def test_global_ceiling_counts_empty_ip_failures():
    """A missing client IP still counts toward the global ceiling — an attacker
    sending a blank host can't dodge it."""
    _reset_login_failures_for_test()
    for _ in range(LOGIN_GLOBAL_MAX_FAILURES):
        record_login_failure("")
    assert login_rate_limited("203.0.113.5"), (
        "blank-IP failures must still trip the global ceiling for everyone")


def test_success_does_not_clear_the_global_ceiling():
    """clear_login_failures (called on a successful login) resets only the
    per-IP bucket. Wiping the global counter on the admin's own success would
    hand an interleaving attacker their attempts back."""
    _reset_login_failures_for_test()
    for n in range(LOGIN_GLOBAL_MAX_FAILURES):
        record_login_failure(f"198.51.100.{n}")
    # a (hypothetical) success on one IP clears THAT IP only.
    clear_login_failures("198.51.100.0")
    assert login_rate_limited("203.0.113.7"), (
        "the global ceiling must survive a per-IP clear")


def test_below_global_ceiling_a_fresh_ip_is_allowed():
    """Guard the other direction: distributed failures BELOW the ceiling must
    not lock out an innocent fresh IP (no false-positive lockout)."""
    _reset_login_failures_for_test()
    for n in range(LOGIN_GLOBAL_MAX_FAILURES - 5):
        record_login_failure(f"10.0.{n // 256}.{n % 256}")  # distinct keys
    assert not login_rate_limited("203.0.113.20"), (
        "under the ceiling, a fresh IP must still be allowed to try")


def test_global_ceiling_is_higher_than_per_ip():
    """Sanity: the global backstop sits well above the per-IP limit so ordinary
    single-admin fat-fingering can never trip the global lockout."""
    assert LOGIN_GLOBAL_MAX_FAILURES > LOGIN_MAX_FAILURES
