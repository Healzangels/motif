"""v1.18.31 — Read-only probe of Plex's per-item theme entries.

the user's safety question on the LET PLEX SERVE path for T+P rows
whose +P stamp came from the now-defunct themerr-plex Plex
Media Server plugin: does motif's `DELETE
/library/metadata/{rk}/themes` (no theme-id selectivity) wipe
the old plugin's persisted theme alongside motif's own upload?

themerr-plex used `item.uploadTheme()` → the SAME POST endpoint
motif uses, so its themes are persisted in Plex's Uploads/themes
directory and survive the plugin's removal. The plugin's own
remove path was a `shutil.rmtree` of that directory — strongly
suggesting Plex's HTTP DELETE has no theme-id selectivity. If
that hunch holds, LET PLEX SERVE on a T+P row would destroy
both motif's upload AND the underlying plugin theme.

This tag adds a read-only probe so we can characterise the
situation empirically on the user's live install before changing
any behavior:

  GET /api/admin/probe-themes?rk=12345&rk=67890

Returns Plex's raw `/library/metadata/{rk}/themes` response per
rk so we can see how many entries exist, who owns them, and
whether the JSON includes a theme-id we could later target with
a selective DELETE.

Diagnostic-only. v1.18.31 ships NO behavior change to LET PLEX
SERVE — the v1.18.32+ direction (warn / backup / targeted-delete
/ leave-as-is) waits on real probe data.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db


# ── PlexClient.get_themes unit shape ─────────────────────────


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


def _stub_plex_client(monkeypatch, response: MagicMock):
    """Build a PlexClient whose underlying httpx.Client returns
    the supplied response for any GET. Skips the real httpx
    construction by patching the class."""
    from app.core import plex as plex_mod
    from app.core.plex import PlexConfig

    cfg = PlexConfig(
        url="http://plex.test:32400", token="testtoken",
        movie_section="1", tv_section="2", enabled=True,
    )
    client = plex_mod.PlexClient(cfg)
    fake_httpx = MagicMock()
    fake_httpx.get = MagicMock(return_value=response)
    client._client = fake_httpx
    return client


def test_get_themes_success_returns_parsed_json(monkeypatch):
    """Happy path: Plex returns 200 + JSON. The helper unwraps
    `r.json()` into the `body` field and reports ok=True."""
    body = {
        "MediaContainer": {
            "size": 2,
            "Theme": [
                {"id": "t-001", "provider": "themerr-plex"},
                {"id": "t-002", "provider": "local://upload"},
            ],
        },
    }
    response = _fake_httpx_response(200, body_json=body)
    client = _stub_plex_client(monkeypatch, response)
    out = client.get_themes(rating_key="12345")
    assert out["ok"] is True
    assert out["http_status"] == 200
    assert out["error"] is None
    assert out["body"] == body


def test_get_themes_404_returns_ok_false():
    """Plex 404 (item lacks themes or rk unknown) — helper
    surfaces the status code; ok is False."""
    response = _fake_httpx_response(404, body_json={"error": "not found"})
    client = _stub_plex_client(None, response)
    out = client.get_themes(rating_key="12345")
    assert out["ok"] is False
    assert out["http_status"] == 404
    assert out["body"] == {"error": "not found"}


def test_get_themes_xml_response_falls_back_to_text():
    """When Plex returns XML even with Accept: application/json
    (some legacy endpoints do), the helper captures the raw
    text so the operator sees the shape rather than getting an
    empty body."""
    response = _fake_httpx_response(
        200, body_text="<MediaContainer size='1'><Theme id='abc'/></MediaContainer>",
    )
    client = _stub_plex_client(None, response)
    out = client.get_themes(rating_key="12345")
    assert out["ok"] is True
    assert "<MediaContainer" in out["body"]


def test_get_themes_transport_exception_returns_error():
    """httpx transport errors land in the `error` field rather
    than bubbling up — the probe endpoint can keep iterating."""
    fake_httpx = MagicMock()
    fake_httpx.get = MagicMock(side_effect=RuntimeError("boom"))
    from app.core import plex as plex_mod
    from app.core.plex import PlexConfig
    cfg = PlexConfig(
        url="http://plex.test:32400", token="testtoken",
        movie_section="1", tv_section="2", enabled=True,
    )
    client = plex_mod.PlexClient(cfg)
    client._client = fake_httpx
    out = client.get_themes(rating_key="12345")
    assert out["ok"] is False
    assert out["http_status"] is None
    assert "RuntimeError" in out["error"]
    assert out["body"] is None


# ── Endpoint fixture (admin-gated, Plex stubbed) ─────────────


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


def test_probe_endpoint_requires_auth(admin_client):
    """No auth headers → 401/403 (whatever _require_admin
    raises). The endpoint must NOT serve probe data to
    anonymous callers."""
    r = admin_client.get("/api/admin/probe-themes?rk=1")
    assert r.status_code in (401, 403)


def test_probe_endpoint_400_without_rk(admin_client):
    """Empty rk list → 400 with a helpful message."""
    r = admin_client.get("/api/admin/probe-themes", headers=AUTH)
    assert r.status_code == 400
    assert "rk" in r.text.lower()


def test_probe_endpoint_400_when_rk_count_exceeds_cap(admin_client):
    """DoS guard: > 10 rks per call → 400. Prevents a malformed
    bookmark from hammering Plex with arbitrary fan-out."""
    qs = "&".join(f"rk={i}" for i in range(11))
    r = admin_client.get(
        f"/api/admin/probe-themes?{qs}", headers=AUTH,
    )
    assert r.status_code == 400
    assert "10" in r.text


def test_probe_endpoint_dispatches_to_get_themes(
    admin_client, monkeypatch,
):
    """The endpoint must call `plex.get_themes(rating_key=rk)`
    for each rk and return the results keyed by rk. Stub the
    PlexClient class so no real HTTP fires."""
    call_log: list[str] = []

    class FakePlex:
        def __init__(self, *a, **kw):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *exc):
            pass
        def get_themes(self, *, rating_key: str) -> dict:
            call_log.append(rating_key)
            return {
                "ok": True, "http_status": 200,
                "error": None,
                "body": {"rk": rating_key, "size": 1},
            }

    # Swap the import target inside the route. The route does a
    # late `from ..core.plex import PlexClient`, so patching the
    # module attribute works.
    import app.core.plex as plex_mod
    monkeypatch.setattr(plex_mod, "PlexClient", FakePlex)

    r = admin_client.get(
        "/api/admin/probe-themes?rk=12345&rk=67890",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["ok"] is True
    assert data["endpoint"] == "/library/metadata/{rk}/themes (GET)"
    assert set(data["results"].keys()) == {"12345", "67890"}
    assert data["results"]["12345"]["body"]["rk"] == "12345"
    assert data["results"]["67890"]["body"]["rk"] == "67890"
    # Both rks must have hit the underlying client.
    assert call_log == ["12345", "67890"]


def test_probe_endpoint_503_when_plex_disabled(
    tmp_path, monkeypatch,
):
    """If Plex isn't configured, the probe can't probe — surface
    503 instead of crashing inside PlexClient construction."""
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
    r = client.get(
        "/api/admin/probe-themes?rk=12345", headers=AUTH,
    )
    assert r.status_code == 503
    assert "plex" in r.text.lower()


# ── No behavior-change pin ───────────────────────────────────


def test_v1_18_31_does_not_alter_delete_theme_path():
    """Pin: v1.18.31 is investigation-only. The `delete_theme`
    + `delete_collection_theme` paths are unchanged — same
    URL, same HTTP method, no theme-id selectivity yet. The
    v1.18.32+ tag will decide based on probe results whether
    to add selectivity, a confirm dialog, or a backup step."""
    plex_py = (
        Path(__file__).resolve().parent.parent
        / "app" / "core" / "plex.py"
    ).read_text()
    # The DELETE shape stays the same — no theme-id in the URL.
    assert (
        'url = f"/library/metadata/{rating_key}/themes"' in plex_py
    ), (
        "v1.18.31 must NOT modify the delete URL — that's a "
        "v1.18.32+ decision after probe data lands"
    )
