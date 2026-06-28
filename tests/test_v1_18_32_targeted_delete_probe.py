"""v1.18.32 — Probe Plex's targeted-DELETE shape for a theme entry.

v1.18.31's GET probe of `/library/metadata/{rk}/themes` revealed
that each Theme entry carries its own ratingKey (`upload://
themes/<hash>` for uploaded themes, `metadata://themes/<hash>`
for Plex's agent themes). the user's probe data on his live install
also showed items can have 2-3 coexisting theme entries:

  rk=530614 (M*A*S*H): 1 metadata + 2 upload entries, size=3
  rk=533875 (*batteries): 2 upload entries, size=2
  rk=628269: 1 metadata + 1 upload, size=2

Some `upload://` entries are motif's; some are leftovers from
the now-defunct themerr-plex plugin or earlier motif SWITCH
operations that orphaned the prior placement. the user confirmed
the provenance is mixed.

Motif's current `delete_theme` hits `DELETE /library/metadata/
{rk}/themes` with no selectivity — likely wipes EVERY theme on
the item. Pre-v1.18.32, themerr-plex's leftovers (and Plex's
own metadata theme) survive only because motif hasn't been
deleting recently. The moment LET PLEX SERVE fires, everything
goes.

v1.18.32 adds a probe to characterise Plex's targeted-DELETE
shape. Three candidates we try, in order of likeliness:

  - `query`        — DELETE .../themes?url=<encoded-theme_rk>
  - `subpath`      — DELETE .../themes/<encoded-theme_rk>
  - `put_unselect` — PUT .../themes?url=<encoded>&unset=1

The endpoint runs (pre-probe) → (delete attempt) → (post-probe)
so the operator can see the resulting state delta in one
response.

Investigation-only for the LET PLEX SERVE direction; v1.18.33
will wire the surviving shape into the production delete path.
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
    fake_httpx.delete = MagicMock(return_value=response)
    fake_httpx.put = MagicMock(return_value=response)
    fake_httpx.get = MagicMock(return_value=response)
    client._client = fake_httpx
    return client, fake_httpx


# ── PlexClient.try_targeted_delete_theme — URL shapes ────────


def test_try_targeted_delete_query_shape():
    """`shape='query'` issues DELETE with ?url=<encoded>."""
    response = _fake_httpx_response(200, body_json={"ok": True})
    client, httpx_mock = _stub_plex_client(response)
    out = client.try_targeted_delete_theme(
        item_rating_key="530614",
        theme_rating_key="upload://themes/abc123",
        shape="query",
    )
    assert out["ok"] is True
    assert out["http_status"] == 200
    assert out["attempted_method"] == "DELETE"
    # URL must include the percent-encoded theme rk as a query param.
    assert "/library/metadata/530614/themes" in out["attempted_url"]
    assert "url=upload%3A%2F%2Fthemes%2Fabc123" in out["attempted_url"]
    httpx_mock.delete.assert_called_once()


def test_try_targeted_delete_subpath_shape():
    """`shape='subpath'` issues DELETE with the encoded theme rk
    as a path segment after /themes/."""
    response = _fake_httpx_response(200, body_json={"ok": True})
    client, httpx_mock = _stub_plex_client(response)
    out = client.try_targeted_delete_theme(
        item_rating_key="530614",
        theme_rating_key="upload://themes/abc123",
        shape="subpath",
    )
    assert out["attempted_method"] == "DELETE"
    assert (
        "/library/metadata/530614/themes/upload%3A%2F%2Fthemes%2Fabc123"
        in out["attempted_url"]
    )
    httpx_mock.delete.assert_called_once()


def test_try_targeted_delete_singular_delete_shape():
    """v1.18.33: `shape='singular_delete'` issues DELETE against
    the SINGULAR `/library/metadata/{rk}/theme` endpoint
    (python-plexapi's deleteTheme uses this URL). The
    theme_rating_key argument is ignored — Plex doesn't accept
    a per-entry selector at this endpoint; it removes the
    currently-serving theme association."""
    response = _fake_httpx_response(200, body_json={"ok": True})
    client, httpx_mock = _stub_plex_client(response)
    out = client.try_targeted_delete_theme(
        item_rating_key="530614",
        theme_rating_key="upload://themes/abc123",
        shape="singular_delete",
    )
    assert out["ok"] is True
    assert out["attempted_method"] == "DELETE"
    # SINGULAR `/theme` — no `s`, no query param, no theme_rk.
    assert out["attempted_url"] == "/library/metadata/530614/theme"
    httpx_mock.delete.assert_called_once()


def test_try_targeted_delete_put_unselect_shape():
    """`shape='put_unselect'` issues PUT (not DELETE) with
    ?url=<encoded>&unset=1 — a fallback for Plex APIs that
    model unselection as a PUT operation."""
    response = _fake_httpx_response(200, body_json={"ok": True})
    client, httpx_mock = _stub_plex_client(response)
    out = client.try_targeted_delete_theme(
        item_rating_key="530614",
        theme_rating_key="upload://themes/abc123",
        shape="put_unselect",
    )
    assert out["attempted_method"] == "PUT"
    assert "unset=1" in out["attempted_url"]
    httpx_mock.put.assert_called_once()
    httpx_mock.delete.assert_not_called()


def test_try_targeted_delete_unknown_shape_returns_error():
    """Unknown shape → error dict, no HTTP call. Defensive guard."""
    client, httpx_mock = _stub_plex_client(MagicMock())
    out = client.try_targeted_delete_theme(
        item_rating_key="530614",
        theme_rating_key="upload://themes/abc123",
        shape="bogus",
    )
    assert out["ok"] is False
    assert "unknown shape" in out["error"]
    httpx_mock.delete.assert_not_called()
    httpx_mock.put.assert_not_called()


def test_try_targeted_delete_transport_exception_returns_error():
    """httpx exceptions land in the error field, don't propagate."""
    from app.core import plex as plex_mod
    from app.core.plex import PlexConfig
    cfg = PlexConfig(
        url="http://plex.test:32400", token="testtoken",
        movie_section="1", tv_section="2", enabled=True,
    )
    client = plex_mod.PlexClient(cfg)
    fake_httpx = MagicMock()
    fake_httpx.delete = MagicMock(side_effect=RuntimeError("boom"))
    client._client = fake_httpx
    out = client.try_targeted_delete_theme(
        item_rating_key="530614",
        theme_rating_key="upload://themes/abc123",
        shape="query",
    )
    assert out["ok"] is False
    assert "RuntimeError" in out["error"]


# ── Endpoint fixture ────────────────────────────────────────


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


# ── Endpoint behavior ────────────────────────────────────────


def test_endpoint_requires_auth(admin_client):
    """No auth → 401/403. Targeted DELETE without admin would be
    a destructive footgun."""
    r = admin_client.post(
        "/api/admin/probe-targeted-delete"
        "?item_rk=530614&theme_rk=upload://themes/abc",
    )
    assert r.status_code in (401, 403)


def test_endpoint_only_post(admin_client):
    """GET must NOT trigger the probe — endpoint is POST-only so
    a stray bookmark can't accidentally delete."""
    r = admin_client.get(
        "/api/admin/probe-targeted-delete"
        "?item_rk=530614&theme_rk=upload://themes/abc",
        headers=AUTH,
    )
    # FastAPI returns 405 Method Not Allowed for unrouted methods.
    assert r.status_code == 405


def test_endpoint_rejects_unknown_shape(admin_client):
    """shape outside {query, subpath, put_unselect,
    singular_delete} → 400."""
    r = admin_client.post(
        "/api/admin/probe-targeted-delete"
        "?item_rk=530614&theme_rk=upload://themes/abc&shape=bogus",
        headers=AUTH,
    )
    assert r.status_code == 400
    assert "shape" in r.text.lower()


def test_endpoint_accepts_singular_delete_shape(
    admin_client, monkeypatch,
):
    """v1.18.33: singular_delete must be on the whitelist."""
    class FakePlex:
        def __init__(self, *a, **kw):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *exc):
            pass
        def get_themes(self, *, rating_key):
            return {"ok": True, "http_status": 200, "error": None,
                    "body": {}}
        def try_targeted_delete_theme(self, *, item_rating_key,
                                       theme_rating_key, shape):
            return {"ok": True, "http_status": 200, "error": None,
                    "attempted_url": "/library/metadata/530614/theme",
                    "attempted_method": "DELETE", "body": {}}

    import app.core.plex as plex_mod
    monkeypatch.setattr(plex_mod, "PlexClient", FakePlex)

    r = admin_client.post(
        "/api/admin/probe-targeted-delete"
        "?item_rk=530614&theme_rk=upload://themes/abc"
        "&shape=singular_delete",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text


def test_endpoint_runs_pre_attempt_post_sequence(
    admin_client, monkeypatch,
):
    """Endpoint must (a) GET themes pre-attempt, (b) try the
    targeted delete, (c) GET themes post-attempt. The response
    shape lets the operator see the state delta in one call."""
    call_log: list[str] = []

    class FakePlex:
        def __init__(self, *a, **kw):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *exc):
            pass
        def get_themes(self, *, rating_key: str) -> dict:
            call_log.append(f"get:{rating_key}")
            stage = "pre" if "get:530614" not in call_log[:-1] else "post"
            return {
                "ok": True, "http_status": 200, "error": None,
                "body": {"stage": stage, "rk": rating_key},
            }
        def try_targeted_delete_theme(
            self, *, item_rating_key, theme_rating_key, shape,
        ) -> dict:
            call_log.append(
                f"del:{item_rating_key}:{theme_rating_key}:{shape}",
            )
            return {
                "ok": True, "http_status": 200, "error": None,
                "attempted_url": "fake://url",
                "attempted_method": "DELETE",
                "body": {"deleted": True},
            }

    import app.core.plex as plex_mod
    monkeypatch.setattr(plex_mod, "PlexClient", FakePlex)

    r = admin_client.post(
        "/api/admin/probe-targeted-delete"
        "?item_rk=530614&theme_rk=upload://themes/abc&shape=query",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["ok"] is True
    assert data["item_rk"] == "530614"
    assert data["theme_rk"] == "upload://themes/abc"
    assert data["shape"] == "query"
    # The three call points must have fired in the right order.
    assert call_log == [
        "get:530614",
        "del:530614:upload://themes/abc:query",
        "get:530614",
    ]
    # And the response includes the three result blocks.
    assert data["pre_themes"]["body"]["stage"] == "pre"
    assert data["attempt"]["body"] == {"deleted": True}
    assert data["post_themes"]["body"]["stage"] == "post"


def test_endpoint_503_when_plex_disabled(tmp_path, monkeypatch):
    """Plex disabled → 503 instead of crashing inside the client
    construction."""
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
        "/api/admin/probe-targeted-delete"
        "?item_rk=530614&theme_rk=upload://themes/abc",
        headers=AUTH,
    )
    assert r.status_code == 503


# ── No behavior change in production paths ───────────────────


def test_v1_18_32_does_not_alter_delete_theme_path():
    """Pin: v1.18.32 is still investigation-only for the
    production code paths. `delete_theme` /
    `delete_collection_theme` still use the collection-wide
    DELETE; the targeted shape lives only in the probe helper.
    v1.18.33 is when this assertion flips."""
    plex_py = (
        Path(__file__).resolve().parent.parent
        / "app" / "core" / "plex.py"
    ).read_text()
    # delete_collection_theme still uses no-selector DELETE.
    assert (
        'url = f"/library/metadata/{rating_key}/themes"' in plex_py
    ), (
        "v1.18.32: production delete URL must still be the "
        "collection-wide DELETE — targeted shape only lives "
        "in try_targeted_delete_theme for now"
    )
