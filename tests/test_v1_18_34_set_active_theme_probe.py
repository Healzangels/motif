"""v1.18.34 — Probe Plex's POST/PUT singular `/theme?url=...` shape.

v1.18.33's singular DELETE probe confirmed:
  - DELETE /library/metadata/{rk}/theme works (HTTP 200)
  - Entries in /themes collection survive
  - Selection cleared (all `selected: false`)
  - Plex does NOT auto-pick a replacement
  - Motif's subsequent POST to plural /themes auto-overrides
    the "locked" state and re-selects

the user surfaced a follow-up via a screenshot of Plex Web's item
edit dialog (General/Tags/Labels/Poster/Background/Logo/Square
Art/Advanced) — there's no theme picker UI. So the LET PLEX
SERVE design that depended on "user manually selects a fallback
theme via Plex Web" is a dead end.

Path forward: motif directly selects a non-motif theme entry
via POST/PUT singular `/theme?url=<theme-rk>`. OpenAPI docs:

  POST /library/metadata/{ids}/{element}
    summary: Set an item's artwork, theme, etc
    param: url (query) - The url of the new asset.

python-plexapi's setTheme() raises NotImplementedError so they
never wrapped this — the HTTP API supports it per Plex's spec
but no library has confirmed it works in practice. v1.18.34's
probe gives us the confirmation.

If POST select works on the user's install, v1.18.35's LET PLEX
SERVE flow becomes:
  1. GET /themes — see what's in the collection
  2. Filter for non-motif entries (prefer metadata://, fall
     back to upload:// that isn't motif's hash)
  3. DELETE singular — clear motif's selection
  4. POST select on chosen fallback
  5. Row continues to serve a theme. No user intervention.

If POST select doesn't work either, v1.18.35 would need to
test metadata refresh + unlock as a fallback. v1.18.34 stays
investigation-only either way.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db


def _fake_httpx_response(status: int, body_json: dict | None = None,
                          body_text: str = "") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    if body_json is not None:
        resp.json = MagicMock(return_value=body_json)
        resp.text = ""
    else:
        resp.json = MagicMock(side_effect=ValueError("not JSON"))
        resp.text = body_text
    return resp


def _stub_plex_client(response: MagicMock):
    from app.core import plex as plex_mod
    from app.core.plex import PlexConfig
    cfg = PlexConfig(
        url="http://plex.test:32400", token="testtoken",
        movie_section="1", tv_section="2", enabled=True,
    )
    client = plex_mod.PlexClient(cfg)
    fake_httpx = MagicMock()
    fake_httpx.post = MagicMock(return_value=response)
    fake_httpx.put = MagicMock(return_value=response)
    fake_httpx.get = MagicMock(return_value=response)
    client._client = fake_httpx
    return client, fake_httpx


# ── PlexClient.try_set_active_theme ──────────────────────────


def test_try_set_active_theme_post_method():
    """method='post' issues POST to singular /theme with the
    encoded theme rk as the url query parameter."""
    response = _fake_httpx_response(200, body_json={"ok": True})
    client, httpx_mock = _stub_plex_client(response)
    out = client.try_set_active_theme(
        item_rating_key="628269",
        theme_rating_key="metadata://themes/31c360d3",
        method="post",
    )
    assert out["ok"] is True
    assert out["http_status"] == 200
    assert out["attempted_method"] == "POST"
    assert "/library/metadata/628269/theme" in out["attempted_url"]
    assert "url=metadata%3A%2F%2Fthemes%2F31c360d3" in out["attempted_url"]
    httpx_mock.post.assert_called_once()
    httpx_mock.put.assert_not_called()


def test_try_set_active_theme_put_method():
    """method='put' issues PUT (alternate per OpenAPI)."""
    response = _fake_httpx_response(200, body_json={"ok": True})
    client, httpx_mock = _stub_plex_client(response)
    out = client.try_set_active_theme(
        item_rating_key="628269",
        theme_rating_key="metadata://themes/31c360d3",
        method="put",
    )
    assert out["ok"] is True
    assert out["attempted_method"] == "PUT"
    httpx_mock.put.assert_called_once()
    httpx_mock.post.assert_not_called()


def test_try_set_active_theme_unknown_method_returns_error():
    client, httpx_mock = _stub_plex_client(MagicMock())
    out = client.try_set_active_theme(
        item_rating_key="628269",
        theme_rating_key="metadata://themes/31c360d3",
        method="bogus",
    )
    assert out["ok"] is False
    assert "unknown method" in out["error"]
    httpx_mock.post.assert_not_called()
    httpx_mock.put.assert_not_called()


def test_try_set_active_theme_transport_exception():
    from app.core import plex as plex_mod
    from app.core.plex import PlexConfig
    cfg = PlexConfig(
        url="http://plex.test:32400", token="testtoken",
        movie_section="1", tv_section="2", enabled=True,
    )
    client = plex_mod.PlexClient(cfg)
    fake_httpx = MagicMock()
    fake_httpx.post = MagicMock(side_effect=RuntimeError("boom"))
    client._client = fake_httpx
    out = client.try_set_active_theme(
        item_rating_key="628269",
        theme_rating_key="metadata://themes/31c360d3",
        method="post",
    )
    assert out["ok"] is False
    assert "RuntimeError" in out["error"]


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
        "/api/admin/probe-set-theme"
        "?item_rk=628269&theme_rk=metadata://themes/abc",
    )
    assert r.status_code in (401, 403)


def test_endpoint_post_only(admin_client):
    r = admin_client.get(
        "/api/admin/probe-set-theme"
        "?item_rk=628269&theme_rk=metadata://themes/abc",
        headers=AUTH,
    )
    assert r.status_code == 405


def test_endpoint_rejects_unknown_method(admin_client):
    r = admin_client.post(
        "/api/admin/probe-set-theme"
        "?item_rk=628269&theme_rk=metadata://themes/abc&method=bogus",
        headers=AUTH,
    )
    assert r.status_code == 400
    assert "method" in r.text.lower()


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
        def try_set_active_theme(
            self, *, item_rating_key, theme_rating_key, method,
        ):
            call_log.append(
                f"set:{item_rating_key}:{theme_rating_key}:{method}"
            )
            return {"ok": True, "http_status": 200,
                    "error": None,
                    "attempted_url": "/library/metadata/628269/theme?url=...",
                    "attempted_method": method.upper(),
                    "body": {"selected": True}}

    import app.core.plex as plex_mod
    monkeypatch.setattr(plex_mod, "PlexClient", FakePlex)

    r = admin_client.post(
        "/api/admin/probe-set-theme"
        "?item_rk=628269&theme_rk=metadata://themes/abc&method=post",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["item_rk"] == "628269"
    assert data["theme_rk"] == "metadata://themes/abc"
    assert data["method"] == "post"
    assert call_log == [
        "get:628269",
        "set:628269:metadata://themes/abc:post",
        "get:628269",
    ]


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
        "/api/admin/probe-set-theme"
        "?item_rk=628269&theme_rk=metadata://themes/abc",
        headers=AUTH,
    )
    assert r.status_code == 503


# ── No production change ─────────────────────────────────────


def test_v1_18_34_does_not_alter_delete_or_upload_paths():
    """Pin: v1.18.34 is investigation-only for the production
    code paths. `delete_collection_theme` still uses plural
    `/themes` (we know it's broken but the singular swap waits
    on v1.18.35 once we've confirmed POST select works for the
    full LPS flow). No production behavior change yet."""
    plex_py = (
        Path(__file__).resolve().parent.parent
        / "app" / "core" / "plex.py"
    ).read_text()
    assert (
        'url = f"/library/metadata/{rating_key}/themes"' in plex_py
    ), (
        "v1.18.34: production delete URL must STILL be plural — "
        "the singular swap lands in v1.18.35 alongside the POST "
        "select for the LPS fallback"
    )
