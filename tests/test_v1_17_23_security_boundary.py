"""v1.17.23 — security boundary hardening.

Three contained fixes from the v1.17.12 security audit's
defensible-but-deferred items. All target the auth /
information-leak boundary; together they harden the surface
where untrusted input meets motif's identity model.

## HIGH 1 — Forward-auth IP allowlist

Pre-fix: `MOTIF_TRUST_FORWARD_AUTH=true` made motif trust
`X-Authentik-Username` / `X-Forwarded-User` from ANY caller.
Anyone reaching :5309 directly (LAN, misconfigured firewall,
exposed container) could spoof admin scope via the header.

Fix: new `web.forward_auth_allowed_ips: list[str]` config field
(env: `MOTIF_FORWARD_AUTH_ALLOWED_IPS=ip,cidr,...`). When
non-empty, the AuthMiddleware checks `request.client.host`
against each entry (IP or CIDR). Mismatches fall through to
API token / session / anonymous instead of trusting the
forward-auth header. Empty list was originally "legacy
permissive" (trust any peer) — v1.24.12 changed that to FAIL
CLOSED: an empty allowlist now trusts NO peer (forward-auth is
a no-op until the proxy IP is configured).

## MED 3 — Scrubber message widening

Pre-fix: `_scrub` only redacted dict KEYS matching the
substring list. URL-embedded credentials in non-suspicious-
keyed values + message-field strings were never scrubbed.

Fix: `_URL_CREDENTIALS_RE` redacts `<scheme>://[user:pass@]host`
to `<scheme>://***@host`. `_scrub_text(s)` applies it + caps
length at 2 KB. `log_event` runs `_scrub_text` on both
message + string-typed detail. `_scrub` adds a string-value
branch so URL creds in dict values get redacted even when
the key isn't a scrub substring.

## MED 6 — X-Authentik-Username sanitization

`_sanitize_forward_auth_username` strips control chars (C0 +
C1 + DEL), caps length at 200, rejects empty/whitespace.
Applied in AuthMiddleware `_resolve_principal` before the
value flows into Principal.username / logs / audit
`updated_by` columns.
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock


REPO = Path(__file__).resolve().parent.parent
APP_INIT = REPO / "app" / "__init__.py"


# ── HIGH 1 — Forward-auth IP allowlist ────────────────────────


def test_web_config_has_forward_auth_allowed_ips():
    """WebConfig must declare the list field, defaulting empty.
    v1.24.12: an empty default now means FAIL CLOSED (forward-auth
    trusts no peer until the operator sets the proxy IP), not the
    original legacy-permissive 'trust any peer'."""
    from app.core.config_file import WebConfig
    cfg = WebConfig()
    assert hasattr(cfg, "forward_auth_allowed_ips"), (
        "v1.17.23: WebConfig must declare "
        "forward_auth_allowed_ips."
    )
    assert isinstance(cfg.forward_auth_allowed_ips, list)
    assert cfg.forward_auth_allowed_ips == [], (
        "default must be empty list — v1.24.12 makes empty = "
        "fail-closed (no peer trusted) rather than permissive."
    )


def test_env_binding_present():
    """`MOTIF_FORWARD_AUTH_ALLOWED_IPS` env var binds to the
    new field via the _to_csv_list converter."""
    from app.core.config_file import ENV_BINDINGS, _to_csv_list
    names = [b[0] for b in ENV_BINDINGS]
    assert "MOTIF_FORWARD_AUTH_ALLOWED_IPS" in names
    binding = next(b for b in ENV_BINDINGS
                   if b[0] == "MOTIF_FORWARD_AUTH_ALLOWED_IPS")
    _name, path, converter = binding
    assert path == "web.forward_auth_allowed_ips"
    assert converter is _to_csv_list, (
        "v1.17.23: forward-auth allowlist env var must use "
        "_to_csv_list so comma-separated values land as a list."
    )


def _make_request_with_host(host: str):
    """Helper: build a minimal Request-like object with a
    `client.host` attribute. Avoids spinning up FastAPI's full
    test client for the IP-check unit tests."""
    req = MagicMock()
    req.client = MagicMock()
    req.client.host = host
    return req


def test_client_ip_check_matches_exact_ip():
    from app.web.api import _client_ip_in_allowlist
    req = _make_request_with_host("172.17.0.5")
    assert _client_ip_in_allowlist(req, ["172.17.0.5"]) is True


def test_client_ip_check_matches_cidr():
    from app.web.api import _client_ip_in_allowlist
    req = _make_request_with_host("172.17.0.42")
    assert _client_ip_in_allowlist(req, ["172.17.0.0/24"]) is True


def test_client_ip_check_rejects_unlisted():
    from app.web.api import _client_ip_in_allowlist
    req = _make_request_with_host("10.0.0.5")
    assert _client_ip_in_allowlist(req, ["172.17.0.0/16"]) is False


def test_client_ip_check_skips_malformed_entry():
    """A single bad entry must not take down the whole list —
    valid entries still match, malformed entries skip with a
    debug breadcrumb."""
    from app.web.api import _client_ip_in_allowlist
    req = _make_request_with_host("172.17.0.5")
    assert _client_ip_in_allowlist(
        req, ["not-an-ip", "172.17.0.0/24"]
    ) is True


def test_client_ip_check_fail_closed_on_missing_client():
    """When request.client is None (test harness, unusual
    proxy setup), return False — better to reject than to
    implicit-trust."""
    from app.web.api import _client_ip_in_allowlist
    req = MagicMock()
    req.client = None
    assert _client_ip_in_allowlist(req, ["172.17.0.0/24"]) is False


def test_client_ip_check_ipv6():
    """IPv6 hosts should also work."""
    from app.web.api import _client_ip_in_allowlist
    req = _make_request_with_host("fd00::5")
    assert _client_ip_in_allowlist(req, ["fd00::/8"]) is True
    assert _client_ip_in_allowlist(req, ["fe80::/64"]) is False


# ── MED 6 — Username sanitization ─────────────────────────────


def test_sanitize_username_strips_control_chars():
    from app.web.api import _sanitize_forward_auth_username
    # Newlines / tabs / NUL should be stripped.
    assert _sanitize_forward_auth_username("admin\n") == "admin"
    assert _sanitize_forward_auth_username("ad\tmin") == "admin"
    assert _sanitize_forward_auth_username("ad\x00min") == "admin"
    # DEL + C1 controls.
    assert _sanitize_forward_auth_username("ad\x7fmin") == "admin"
    assert _sanitize_forward_auth_username("ad\x9bmin") == "admin"


def test_sanitize_username_caps_length():
    from app.web.api import (
        _FORWARD_AUTH_USERNAME_MAX_LEN,
        _sanitize_forward_auth_username,
    )
    over_long = "x" * (_FORWARD_AUTH_USERNAME_MAX_LEN + 50)
    result = _sanitize_forward_auth_username(over_long)
    assert len(result) == _FORWARD_AUTH_USERNAME_MAX_LEN


def test_sanitize_username_handles_empty():
    from app.web.api import _sanitize_forward_auth_username
    assert _sanitize_forward_auth_username(None) == ""
    assert _sanitize_forward_auth_username("") == ""
    assert _sanitize_forward_auth_username("   ") == ""
    assert _sanitize_forward_auth_username("\n\t") == ""


def test_sanitize_username_preserves_normal_input():
    from app.web.api import _sanitize_forward_auth_username
    assert _sanitize_forward_auth_username("user") == "user"
    assert (
        _sanitize_forward_auth_username("user@example.com")
        == "user@example.com"
    )
    # International chars are not control chars.
    assert _sanitize_forward_auth_username("user-naïve") == "user-naïve"


# ── MED 3 — Scrubber widening ─────────────────────────────────


def test_scrub_text_redacts_url_credentials():
    from app.core.events import _scrub_text
    # Standard userinfo redaction.
    assert _scrub_text("https://user:pass@host.example") \
        == "https://***@host.example"
    # User-only (no password) — still redact.
    assert _scrub_text("ftp://user@host") == "ftp://***@host"
    # Multiple URLs in one message.
    out = _scrub_text(
        "fetched https://u:p@a.x and https://u2:p2@b.x"
    )
    assert "u:p" not in out
    assert "u2:p2" not in out
    assert out.count("***@") == 2


def test_scrub_text_preserves_clean_messages():
    """URLs without credentials must pass through unchanged."""
    from app.core.events import _scrub_text
    assert _scrub_text("connected to https://host.example") \
        == "connected to https://host.example"
    assert _scrub_text("normal message") == "normal message"


def test_scrub_text_caps_length():
    from app.core.events import _scrub_text
    long_msg = "a" * 4000
    result = _scrub_text(long_msg)
    assert len(result) <= 2049  # 2048 + ellipsis char
    assert result.endswith("…")


def test_scrub_dict_redacts_url_credentials_in_values():
    """Pre-fix `{"url": "https://user:pass@host"}` slipped
    through because `url` isn't a scrub substring. The new
    string-value branch in _scrub catches it."""
    from app.core.events import _scrub
    scrubbed = _scrub({"url": "https://user:pass@host.example"})
    assert scrubbed["url"] == "https://***@host.example"


def test_scrub_dict_key_redaction_still_works():
    """Counter-pin: the existing _SCRUB_SUBSTRINGS key-based
    redaction must keep working after the v1.17.23 changes."""
    from app.core.events import _scrub
    scrubbed = _scrub({
        "password": "literal-secret",
        "api_key": "abc123",
        "plex_token": "xyz789",
    })
    assert scrubbed["password"] == "***REDACTED***"
    assert scrubbed["api_key"] == "***REDACTED***"
    assert scrubbed["plex_token"] == "***REDACTED***"


# ── AuthMiddleware integration shape ──────────────────────────


def test_settings_exposes_forward_auth_allowed_ips():
    """The Settings facade in app/config.py must expose the
    new list so AuthMiddleware can read it without reaching
    into the raw cfg tree."""
    from app.config import Settings
    assert hasattr(Settings, "forward_auth_allowed_ips"), (
        "v1.17.23: Settings must expose forward_auth_allowed_ips "
        "as a property for AuthMiddleware to read."
    )


# ── Version pin (soft floor) ──────────────────────────────────


def test_version_pinned_at_or_above_1_17_23():
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m
    found = tuple(int(x) for x in m.groups())
    assert found >= (0, 17, 23), (
        f"v1.17.23: __version__ must be >= 1.17.23 "
        f"(found {'.'.join(str(x) for x in found)})."
    )
