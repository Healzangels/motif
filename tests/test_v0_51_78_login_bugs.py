"""v0.51.78 — two real login bugs from the CSS+login audit.

BUG 1 (auth.py): the no-admin timing-equalizer ran
bcrypt.checkpw(b"x", b"$2b$12$" + b"x"*53) — a MALFORMED 60-byte salt → ValueError
in microseconds. So the timing defense was inert AND inverted (no-admin returned
FASTER), and the unhandled ValueError propagated to an HTTP 500 on POST /login during
the fresh-install / wiped-admin window (which also exempted that state from the rate
limit). Fixed: verify against a real precomputed rounds=12 hash via verify_password
(which swallows bcrypt errors) so timing matches and nothing can raise.

BUG 2 (api.py login_get): the page redirected away on `if settings.trust_forward_auth`
ALONE — but AuthMiddleware only trusts the forward-auth header when
forward_auth_allowed_ips is set AND the IP matches (v1.24.12 fail-closed). Flipping
trust on WITHOUT the allowlist → anonymous → GET /login → '/' → (anonymous on '/') →
/login: an infinite loop locking the operator out of the local-login fallback. Fixed
by relying on the existing is_authenticated check (the middleware always resolves the
principal), so a genuinely forward-authed visitor is redirected home and everyone else
sees the login form.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


def _make_app(tmp_path, monkeypatch, *, create_admin_user: bool, trust_fa: bool):
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true" if trust_fa else "false")
    # deliberately DO NOT set MOTIF_FORWARD_AUTH_ALLOWED_IPS → empty allowlist.
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    if create_admin_user:
        create_admin(s.db_path, username="admin", password="password123")
    return s, TestClient(create_app(s))


# ── BUG 1: no-admin login must not 500 (and the dummy must not raise) ──

def test_authenticate_password_no_admin_returns_false_without_raising(tmp_path, monkeypatch):
    s, _ = _make_app(tmp_path, monkeypatch, create_admin_user=False, trust_fa=False)
    from app.core.auth import authenticate_password
    # Pre-fix this raised ValueError('Invalid salt') from the malformed dummy.
    assert authenticate_password(s.db_path, username="whoever", password="x") is False


def test_post_login_on_fresh_install_does_not_500(tmp_path, monkeypatch):
    _s, client = _make_app(tmp_path, monkeypatch, create_admin_user=False, trust_fa=False)
    r = client.post("/login", data={"username": "x", "password": "y", "next": "/"},
                    follow_redirects=False)
    assert r.status_code != 500, r.text  # pre-fix: 500 from the ValueError


# ── BUG 2: forward-auth ON + empty allowlist must not redirect-loop ──

def test_login_page_renders_when_forward_auth_on_but_allowlist_empty(tmp_path, monkeypatch):
    _s, client = _make_app(tmp_path, monkeypatch, create_admin_user=True, trust_fa=True)
    # Not logged in, forward-auth trusted but NO allowlist → middleware fails closed to
    # anonymous. GET /login must render the form (200), NOT redirect (302 → loop).
    r = client.get("/login", follow_redirects=False)
    assert r.status_code == 200, f"expected the login form, got {r.status_code} (redirect loop)"
    assert "password" in r.text.lower()
