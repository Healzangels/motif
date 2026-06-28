"""v1.24.12 — security-review remediation.

Three items from the full security review:

1. **Forward-auth FAIL CLOSED.** _resolve_principal used to trust the
   X-Authentik-Username header from ANY peer when forward_auth_allowed_ips was
   empty ("legacy permissive"). On a directly-reachable port that's an admin
   bypass (any LAN host/container sends the header → admin). Now the header is
   trusted ONLY when the allowlist is non-empty AND the client IP matches; an
   empty allowlist (or a non-matching peer) falls through to token/session/
   anonymous. Not live in the deployed config (trust_forward_auth=False), but a
   fail-open footgun if enabled without the allowlist — and the boot warning
   already screamed about it.

2. **section_id digit-guard** (defense-in-depth): the /collections section
   filter interpolates section_id into SQL. It's route-pinned to `^[0-9]*$`, so
   not injectable today, but _library_main_query now guards it in-function so a
   future direct caller can't smuggle SQL.

3. **Webhook-path scrubber** (defense-in-depth): the events scrubber now redacts
   `…/webhooks/<id>/<token>` (Discord/Slack), whose secret lives in the path —
   the userinfo + query-param regexes missed it.

The fail-closed behavioral tests rely on tests/conftest.py, which sets a default
allowlist (127.0.0.1) + defaults the TestClient peer to 127.0.0.1.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.db import init_db
from app.core.auth import init_auth_schema, create_admin

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
MAIN_PY = (REPO / "app" / "main.py").read_text()


# ── 1. Forward-auth fail-closed — source shape ───────────────

def test_resolve_principal_trusts_header_only_with_matching_allowlist():
    # The fail-closed gate: trust only when allowlist set AND IP matches.
    assert "if allowed and _client_ip_in_allowlist(request, allowed):" in API_PY
    # The old fail-OPEN shape must be gone (empty allowlist → else → trust).
    assert "if allowed and not _client_ip_in_allowlist(request, allowed):" not in API_PY


def test_boot_warning_describes_fail_closed():
    assert "FAILS CLOSED" in MAIN_PY
    assert "forward-auth disabled (fail-closed) until" in MAIN_PY


# ── 1b. Forward-auth fail-closed — behavioral ────────────────

def _app(tmp_path, monkeypatch, allowlist: str) -> TestClient:
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_FORWARD_AUTH_ALLOWED_IPS", allowlist)
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    from app.web.api import create_app
    return TestClient(create_app(settings))


_HDR = {"X-Authentik-Username": "someadmin"}


def test_empty_allowlist_rejects_forward_auth_header(tmp_path, monkeypatch):
    """Fail closed: trust on + EMPTY allowlist → the header is NOT trusted,
    so an admin endpoint is unreachable via it."""
    client = _app(tmp_path, monkeypatch, "")  # empty → fail closed
    r = client.get("/api/config", headers=_HDR, follow_redirects=False)
    assert r.status_code != 200, (
        "empty allowlist must NOT grant admin via X-Authentik-Username")
    assert r.status_code in (401, 403) or 300 <= r.status_code < 400


def test_allowlisted_peer_admits_forward_auth_header(tmp_path, monkeypatch):
    """Control: trust on + allowlist matches the (conftest) 127.0.0.1 peer →
    the header authenticates as admin."""
    client = _app(tmp_path, monkeypatch, "127.0.0.1")
    r = client.get("/api/config", headers=_HDR)
    assert r.status_code == 200, (
        "an allowlisted peer's X-Authentik-Username must still authenticate")


# ── 2. section_id digit-guard ────────────────────────────────

def test_section_id_digit_guard_present():
    assert "if not section_id.isdigit():" in API_PY
    assert 'raise ValueError(f"section_id must be digits' in API_PY


# ── 3. Webhook-path scrubber ─────────────────────────────────

def test_scrubber_redacts_webhook_path_token():
    from app.core.events import _redact_url_credentials
    raw = "POST https://discord.com/api/webhooks/123456789/SuperSecretTokenABC failed"
    out = _redact_url_credentials(raw)
    assert "SuperSecretTokenABC" not in out
    assert "/webhooks/123456789/***" in out
    # the id stays for diagnostics; the host/scheme stay too.
    assert "discord.com" in out


def test_scrubber_still_redacts_userinfo_and_query():
    from app.core.events import _redact_url_credentials
    assert _redact_url_credentials("https://u:p@host/x") == "https://***@host/x"
    assert "***" in _redact_url_credentials("https://h/a?X-Plex-Token=abc123")
