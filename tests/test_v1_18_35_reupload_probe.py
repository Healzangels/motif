"""v1.18.35 — Probe the re-upload trick (the only viable LPS path).

v1.18.34's POST/PUT singular `/theme?url=...` probe results:
  - POST returned 404 (no route handler)
  - PUT returned 500 (route exists but errors on the URI we send)

The `?url=...` parameter is for "fetch a NEW theme from this
remote URL" semantics, not for selecting an existing entry by
its internal Plex key. **Plex has no native "select existing
theme" API.** python-plexapi's setTheme() raising
NotImplementedError makes sense — there's no primitive to wrap.

## What still works (proven in v1.18.31-34)

  - GET /library/metadata/{rk}/themes — enumerate
  - POST /library/metadata/{rk}/themes — upload audio bytes,
    auto-selects, content-dedupes by SHA-1
  - DELETE /library/metadata/{rk}/theme (singular) — clear
    selection (entries in /themes survive)

## The re-upload trick — v1.18.35's investigation

Plex content-dedupes uploads (same SHA-1 → same entry, marked
selected). So:

  1. GET the audio bytes of an existing theme entry from
     /library/metadata/{rk}/file?url=<theme-uri>
  2. POST those bytes back to /library/metadata/{rk}/themes
  3. Plex sees the same hash → re-uses the existing entry
     → marks it `selected: true`

This is the ONLY remaining viable path for LET PLEX SERVE to
leave a row serving Plex's pre-motif theme without user
intervention. Bandwidth: ~1MB round-trip per LPS click.

v1.18.35 adds the probe. If it works on the user's 12 Monkeys
row (rk=124233, single themerr-plex `upload://` entry), v1.18.36
wires it into LET PLEX SERVE alongside the URL change for
delete + SWITCH bug fixes.

If it doesn't work, v1.18.36's LPS would have to fall back to
"goes themeless, log + notify the user."
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db


def _stub_plex_client(get_response: MagicMock,
                       upload_ok: bool = True):
    from app.core import plex as plex_mod
    from app.core.plex import PlexConfig
    cfg = PlexConfig(
        url="http://plex.test:32400", token="testtoken",
        movie_section="1", tv_section="2", enabled=True,
    )
    client = plex_mod.PlexClient(cfg)
    fake_httpx = MagicMock()
    fake_httpx.get = MagicMock(return_value=get_response)
    client._client = fake_httpx
    # Stub upload_collection_theme directly so the multipart
    # path doesn't need separate mocking.
    # v1.18.68: upload_collection_theme returns (ok, status_code,
    # body_snip) — the test fixture must match the new shape so
    # the LPS reupload destructuring works.
    _status = 200 if upload_ok else 500
    _body = "(empty)" if upload_ok else "server error"
    client.upload_collection_theme = MagicMock(
        return_value=(upload_ok, _status, _body)
    )
    return client, fake_httpx


# ── PlexClient.try_reupload_existing_theme ───────────────────


def test_try_reupload_happy_path():
    """Successful fetch + successful upload → ok=True, both
    sub-results populated, bytes count reflects what was fetched."""
    audio = b"\x00\x01\x02" * 1000  # 3000 bytes mock audio
    get_resp = MagicMock()
    get_resp.status_code = 200
    get_resp.content = audio
    client, httpx_mock = _stub_plex_client(get_resp, upload_ok=True)
    out = client.try_reupload_existing_theme(
        item_rating_key="124233",
        theme_rating_key="upload://themes/d02ec955",
    )
    assert out["ok"] is True
    assert out["step_failed"] is None
    assert out["fetch"]["http_status"] == 200
    assert out["fetch"]["bytes"] == 3000
    # The fetch URL uses /file (not /themes) with the encoded
    # theme rk as the url query param.
    assert "/library/metadata/124233/file" in out["fetch"]["url"]
    assert "url=upload%3A%2F%2Fthemes%2Fd02ec955" in out["fetch"]["url"]
    assert out["upload"]["ok"] is True
    # Verify upload_collection_theme was called with the fetched bytes.
    client.upload_collection_theme.assert_called_once_with(
        rating_key="124233", audio_bytes=audio,
    )


def test_try_reupload_fetch_failure():
    """4xx/5xx on the fetch step → ok=False, step_failed='fetch',
    upload not attempted."""
    get_resp = MagicMock()
    get_resp.status_code = 404
    get_resp.text = "not found"
    client, httpx_mock = _stub_plex_client(get_resp)
    out = client.try_reupload_existing_theme(
        item_rating_key="124233",
        theme_rating_key="upload://themes/missing",
    )
    assert out["ok"] is False
    assert out["step_failed"] == "fetch"
    assert out["fetch"]["http_status"] == 404
    assert out["fetch"]["bytes"] == 0
    assert out["upload"] is None
    # Upload must NOT have been called when fetch failed.
    client.upload_collection_theme.assert_not_called()


def test_try_reupload_transport_exception_on_fetch():
    """httpx exception on fetch → ok=False with error message,
    no upload attempted."""
    from app.core import plex as plex_mod
    from app.core.plex import PlexConfig
    cfg = PlexConfig(
        url="http://plex.test:32400", token="testtoken",
        movie_section="1", tv_section="2", enabled=True,
    )
    client = plex_mod.PlexClient(cfg)
    fake_httpx = MagicMock()
    fake_httpx.get = MagicMock(side_effect=RuntimeError("boom"))
    client._client = fake_httpx
    client.upload_collection_theme = MagicMock()
    out = client.try_reupload_existing_theme(
        item_rating_key="124233",
        theme_rating_key="upload://themes/abc",
    )
    assert out["ok"] is False
    assert out["step_failed"] == "fetch"
    assert "RuntimeError" in out["fetch"]["error"]
    client.upload_collection_theme.assert_not_called()


def test_try_reupload_upload_failure():
    """Fetch succeeds, upload returns False → ok=False,
    step_failed='upload'."""
    audio = b"x" * 100
    get_resp = MagicMock()
    get_resp.status_code = 200
    get_resp.content = audio
    client, _ = _stub_plex_client(get_resp, upload_ok=False)
    out = client.try_reupload_existing_theme(
        item_rating_key="124233",
        theme_rating_key="upload://themes/abc",
    )
    assert out["ok"] is False
    assert out["step_failed"] == "upload"
    assert out["fetch"]["bytes"] == 100
    assert out["upload"]["ok"] is False


# ── Admin endpoint ───────────────────────────────────────────


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_PLEX_ENABLED", "true")
    monkeypatch.setenv("MOTIF_PLEX_URL", "http://plex.test:32400")
    monkeypatch.setenv("MOTIF_PLEX_TOKEN", "testtoken")
    from app.config import Settings
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    app = create_app(settings)
    client = TestClient(app)
    return client


AUTH = {"X-Authentik-Username": "testadmin"}


def test_endpoint_requires_auth(admin_client):
    r = admin_client.post(
        "/api/admin/probe-reupload-theme"
        "?item_rk=124233&theme_rk=upload://themes/abc",
    )
    assert r.status_code in (401, 403)


def test_endpoint_post_only(admin_client):
    r = admin_client.get(
        "/api/admin/probe-reupload-theme"
        "?item_rk=124233&theme_rk=upload://themes/abc",
        headers=AUTH,
    )
    assert r.status_code == 405


def test_endpoint_dispatches_pre_attempt_post(
    admin_client, monkeypatch,
):
    call_log: list[str] = []

    class FakePlex:
        def __init__(self, *a, **kw):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *exc):
            pass
        def get_themes(self, *, rating_key):
            call_log.append(f"get:{rating_key}")
            return {"ok": True, "http_status": 200,
                    "error": None, "body": {"stage": "snapshot"}}
        def try_reupload_existing_theme(
            self, *, item_rating_key, theme_rating_key,
        ):
            call_log.append(
                f"reupload:{item_rating_key}:{theme_rating_key}"
            )
            return {"ok": True, "step_failed": None,
                    "fetch": {"bytes": 1234},
                    "upload": {"ok": True}}

    import app.core.plex as plex_mod
    monkeypatch.setattr(plex_mod, "PlexClient", FakePlex)

    r = admin_client.post(
        "/api/admin/probe-reupload-theme"
        "?item_rk=124233&theme_rk=upload://themes/d02ec955",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["item_rk"] == "124233"
    assert data["theme_rk"] == "upload://themes/d02ec955"
    # Pre + reupload + post, in order.
    assert call_log == [
        "get:124233",
        "reupload:124233:upload://themes/d02ec955",
        "get:124233",
    ]
    assert data["attempt"]["ok"] is True
    assert data["attempt"]["fetch"]["bytes"] == 1234


def test_endpoint_503_when_plex_disabled(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_PLEX_ENABLED", "false")
    from app.config import Settings
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    app = create_app(settings)
    client = TestClient(app)
    r = client.post(
        "/api/admin/probe-reupload-theme"
        "?item_rk=124233&theme_rk=upload://themes/abc",
        headers=AUTH,
    )
    assert r.status_code == 503


# ── No production change pin ─────────────────────────────────


def test_v1_18_35_does_not_alter_production_paths():
    """Pin: v1.18.35 still investigation-only. Production
    delete + upload paths unchanged until v1.18.36 lands."""
    plex_py = (
        Path(__file__).resolve().parent.parent
        / "app" / "core" / "plex.py"
    ).read_text()
    # delete_collection_theme still uses plural (v1.18.36 swap).
    assert (
        'url = f"/library/metadata/{rating_key}/themes"' in plex_py
    )
