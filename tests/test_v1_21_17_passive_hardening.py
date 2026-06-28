"""v1.21.17 — passive security hardening (ground-up review).

the user asked for a holistic, best-practices security pass. The token/
secret exposure surfaces all came back clean (Plex token is header-only,
never logged/templated/returned; cookies.txt contents never egress; no
secrets in repo/history). This tag ships the no-behavior-tradeoff
hardening that pass surfaced:

  - baseline security response headers (nosniff / Referrer-Policy /
    Permissions-Policy / HSTS-when-https). No CSP/X-Frame-Options here —
    those are deferred (can't live-test + dashboard-embed tradeoff).
  - sync.git_url / database_url userinfo masking in GET /api/config (the
    last two credential-capable fields returned in cleartext) + a
    masked-marker PATCH-preserve so a round-trip can't corrupt a stored PAT.
  - events scrubber: redact ?token=/?X-Plex-Token=/?api_key= query-param
    secrets (only ://user:pass@ was covered before). Latent gap, no live
    trigger, but the scrubber is the last line of defense.
  - web.cookie_secure config knob ("auto"|"on"|"off") so an HTTPS-only
    deploy can stop depending on the spoofable X-Forwarded-Proto header.

Behavioral where it counts (headers/masking/cookie via TestClient),
unit for the scrubber, source-pin for the static hygiene (.gitignore,
requirements floor).
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db


REPO = Path(__file__).resolve().parent.parent
FWD_HDR = {"X-Authentik-Username": "testadmin"}


def _make_app(tmp_path, monkeypatch, **env):
    """Fresh Settings-driven app at a tmp DB. env kwargs become MOTIF_*
    overrides set BEFORE Settings construction. Forward-auth on so the
    X-Authentik-Username header satisfies _require_admin."""
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")

    from app.web.api import create_app
    client = TestClient(create_app(settings))
    return client, settings


# ── security response headers ────────────────────────────────

def test_security_headers_present_on_every_response(tmp_path, monkeypatch):
    client, _ = _make_app(tmp_path, monkeypatch)
    r = client.get("/healthz")
    assert r.headers.get("X-Content-Type-Options") == "nosniff"
    assert r.headers.get("Referrer-Policy") == "no-referrer"
    assert "geolocation=()" in r.headers.get("Permissions-Policy", "")
    # plain HTTP (TestClient) → no HSTS (don't pin a LAN box to https)
    assert "Strict-Transport-Security" not in r.headers


def test_hsts_only_when_https(tmp_path, monkeypatch):
    client, _ = _make_app(tmp_path, monkeypatch)
    r = client.get("/healthz", headers={"X-Forwarded-Proto": "https"})
    assert "max-age=" in r.headers.get("Strict-Transport-Security", "")


# ── git_url / database_url masking + PATCH-preserve ──────────

def test_credentialed_git_url_masked_in_get(tmp_path, monkeypatch):
    client, settings = _make_app(tmp_path, monkeypatch)
    cred = "https://ci:ghp_secrettoken@git.example.com/mirror/ThemerrDB.git"
    r = client.patch("/api/config", json={"sync": {"git_url": cred}},
                     headers=FWD_HDR)
    assert r.status_code == 200, r.text
    # On disk: the REAL credential is stored (it's the live config).
    assert settings.cfg.sync.git_url == cred
    # Over the wire: userinfo redacted, host+path still visible.
    got = client.get("/api/config", headers=FWD_HDR).json()
    assert got["config"]["sync"]["git_url"] == \
        "https://***@git.example.com/mirror/ThemerrDB.git"
    assert "ghp_secrettoken" not in client.get(
        "/api/config", headers=FWD_HDR).text


def test_masked_git_url_patch_preserves_stored_credential(tmp_path, monkeypatch):
    client, settings = _make_app(tmp_path, monkeypatch)
    cred = "https://ci:ghp_secrettoken@git.example.com/mirror/x.git"
    client.patch("/api/config", json={"sync": {"git_url": cred}},
                 headers=FWD_HDR)
    # Round-trip the MASKED form back (what the UI would PATCH unchanged).
    masked = "https://***@git.example.com/mirror/x.git"
    r = client.patch("/api/config", json={"sync": {"git_url": masked}},
                     headers=FWD_HDR)
    assert r.status_code == 200, r.text
    # The stored credential must NOT be corrupted to the masked string.
    assert settings.cfg.sync.git_url == cred


def test_credential_free_url_passes_through_unmasked(tmp_path, monkeypatch):
    client, _ = _make_app(tmp_path, monkeypatch)
    got = client.get("/api/config", headers=FWD_HDR).json()
    # Default git_url is the public ThemerrDB URL — no userinfo, shown full.
    assert "@" not in got["config"]["sync"]["git_url"]
    assert got["config"]["sync"]["git_url"].startswith("https://")
    assert "***" not in got["config"]["sync"]["git_url"]


def test_database_url_also_masked(tmp_path, monkeypatch):
    client, settings = _make_app(tmp_path, monkeypatch)
    cred = "https://user:tok@db.example.com/snap.tar.gz"
    client.patch("/api/config", json={"sync": {"database_url": cred}},
                 headers=FWD_HDR)
    got = client.get("/api/config", headers=FWD_HDR).json()
    assert got["config"]["sync"]["database_url"] == \
        "https://***@db.example.com/snap.tar.gz"
    assert settings.cfg.sync.database_url == cred


# ── cookie_secure knob ───────────────────────────────────────

def _login_setcookie(client):
    r = client.post("/login",
                    data={"username": "testadmin", "password": "testpassword"},
                    follow_redirects=False)
    return r.headers.get("set-cookie", "")


def test_cookie_secure_on_forces_secure_flag(tmp_path, monkeypatch):
    client, _ = _make_app(tmp_path, monkeypatch, MOTIF_COOKIE_SECURE="on")
    sc = _login_setcookie(client)
    assert "secure" in sc.lower(), sc


def test_cookie_secure_auto_over_http_has_no_secure(tmp_path, monkeypatch):
    client, _ = _make_app(tmp_path, monkeypatch, MOTIF_COOKIE_SECURE="auto")
    sc = _login_setcookie(client)
    assert "secure" not in sc.lower(), sc


def test_validate_rejects_bad_cookie_secure():
    from app.core.config_file import MotifConfig, validate
    cfg = MotifConfig()
    cfg.web.cookie_secure = "bogus"
    errs = validate(cfg, require_themes_dir=False)
    assert any("cookie_secure" in e for e in errs), errs


# ── events scrubber: query-param secrets ─────────────────────

def test_scrubber_redacts_query_param_token():
    from app.core import events
    out = events._redact_url_credentials(
        "yt error fetching http://plex.lan/lib?X-Plex-Token=ABC123&p=1")
    assert "ABC123" not in out
    assert "X-Plex-Token=***" in out


def test_scrubber_still_redacts_userinfo():
    from app.core import events
    out = events._redact_url_credentials("clone https://u:ghp_x@github.com/r")
    assert "ghp_x" not in out and "u:" not in out
    assert "https://***@github.com/r" in out


# ── static hygiene (source pins) ─────────────────────────────

def test_gitignore_names_secret_files():
    gi = (REPO / ".gitignore").read_text()
    for name in ("cookies.txt", "motif.yaml", "*.db", "*.pem", "*.key"):
        assert name in gi, name


def test_multipart_floor_pinned():
    reqs = (REPO / "requirements.txt").read_text()
    # v1.24.42 (security audit): floor bumped 0.0.18 → 0.0.31 (CVE-2026-53538/
    # 53539/53540). Still a security floor, just a higher minimum.
    assert "python-multipart>=0.0.31" in reqs


def test_version_bumped():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
