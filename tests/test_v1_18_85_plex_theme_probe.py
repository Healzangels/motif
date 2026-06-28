"""v1.18.85 — diagnostic probe for Plex's theme API responses.

Goal: empirically characterise what `/library/metadata/{rk}/themes`
+ `/library/metadata/{rk}/file?url=<entry-rk>` return across the
four P sub-flavors (themerr-plex embed / user upload / Plex Pass
cloud / sidecar) so we can plan a cloud-themes backup feature.

The only sub-flavor where motif has NO recovery path is Plex Pass
cloud themes — themerr-plex has the TDB URL, uploads came from the
user, sidecars are on disk. Cloud themes are served from Plex's
infrastructure and disappear with Plex Pass.

the user's v1.18.31-40 cadence: probe first, characterise, design
later. This tag ships the probe surface only. Production cloud-
backup feature is a later tag, gated on the probe results.

## What's pinned

- `PlexClient.probe_theme_entry_bytes(item_rk, entry_uri,
  range_bytes=4096)` exists, captures status + headers +
  content_type + body_preview_hex.
- `POST /api/admin/probe-plex-themes` endpoint:
  - Admin-gated
  - Accepts `{"titles": [str, ...]}` with cap 8
  - Resolves titles to rating_keys via plex_items LIKE match
    (exact match preferred via ORDER BY)
  - Returns structured per-entry probe results
- Settings UI block under DIAGNOSTICS panel with textarea +
  Run button + output pre-block.
- JS handler wires textarea → POST → render JSON in pre-block.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
PLEX_PY = REPO / "app" / "core" / "plex.py"
API_PY = REPO / "app" / "web" / "api.py"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── PlexClient method ────────────────────────────────────────


def test_plexclient_has_probe_theme_entry_bytes():
    """PlexClient must expose probe_theme_entry_bytes for the
    admin probe to call. Signature pins: item_rating_key,
    entry_uri, range_bytes (default 4096 per the user's spec)."""
    src = PLEX_PY.read_text()
    assert "def probe_theme_entry_bytes(" in src
    # Find the function body.
    idx = src.index("def probe_theme_entry_bytes(")
    fn_end = src.find("\n    def ", idx + 1)
    body = src[idx:fn_end if fn_end != -1 else len(src)]
    # Required kwargs.
    assert "item_rating_key" in body
    assert "entry_uri" in body
    assert "range_bytes" in body
    # Hits the /file?url= endpoint.
    assert "/file" in body
    assert "url=" in body
    # Captures status + headers + content_type + body_preview_hex.
    assert "http_status" in body
    assert "headers" in body
    assert "content_type" in body
    assert "body_preview_hex" in body


def test_probe_uses_range_header_to_cap_download():
    """The Range header must be sent so a misconfigured entry
    can't accidentally stream a huge file. Specifically
    `bytes=0-(range_bytes - 1)`."""
    src = PLEX_PY.read_text()
    idx = src.index("def probe_theme_entry_bytes(")
    fn_end = src.find("\n    def ", idx + 1)
    body = src[idx:fn_end if fn_end != -1 else len(src)]
    assert "Range" in body
    assert "bytes=0-" in body


def test_probe_method_never_raises():
    """Mirror v1.18.31's get_themes: every failure path lands
    in the return dict so the caller can keep iterating. The
    function body must have try/except wrapping the HTTP call."""
    src = PLEX_PY.read_text()
    idx = src.index("def probe_theme_entry_bytes(")
    fn_end = src.find("\n    def ", idx + 1)
    body = src[idx:fn_end if fn_end != -1 else len(src)]
    assert "try:" in body
    assert "except Exception" in body


# ── Admin endpoint ───────────────────────────────────────────


def test_endpoint_registered():
    """`POST /api/admin/probe-plex-themes` must be registered."""
    src = API_PY.read_text()
    assert '@app.post("/api/admin/probe-plex-themes")' in src
    assert "async def api_admin_probe_plex_themes(" in src


def test_endpoint_is_admin_gated():
    """The endpoint must require admin auth — same gate as the
    sibling v1.18.31 probe endpoints."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_admin_probe_plex_themes(")
    fn_end = src.index("@app.", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "_require_admin(request)" in body


def test_endpoint_caps_titles_per_call():
    """DoS guard: cap titles per call (sibling probes use 10
    rks; this one uses 8 — fewer because each title can fan
    out to multiple entries on the per-rk side)."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_admin_probe_plex_themes(")
    fn_end = src.index("@app.", fn_idx + 1)
    body = src[fn_idx:fn_end]
    # Look for "cap is 8 titles" or similar guard
    assert "len(titles) > 8" in body or "len(titles)>=9" in body
    assert "cap is 8" in body or "(DoS guard)" in body


def test_endpoint_resolves_titles_via_plex_items():
    """Title fragments resolve to rating_keys via the local
    plex_items table — NOT Plex's search API. Deterministic
    + offline-friendly."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_admin_probe_plex_themes(")
    fn_end = src.index("@app.", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "FROM plex_items" in body
    assert "title LIKE" in body
    # Exact match should win — pinned via the ORDER BY.
    assert "LOWER(title) = LOWER" in body


def test_endpoint_returns_status_503_when_plex_disabled():
    """If plex_enabled=False or plex_token empty, the endpoint
    must 503 — same pattern as the v1.18.31 probe siblings."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_admin_probe_plex_themes(")
    fn_end = src.index("@app.", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "503" in body
    assert "plex_enabled" in body
    assert "plex_token" in body


def test_endpoint_walks_entries_and_probes_bytes():
    """For each resolved rk, the endpoint must call
    `plex.get_themes` AND then walk the entries calling
    `probe_theme_entry_bytes` for each one's URI."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_admin_probe_plex_themes(")
    fn_end = src.index("@app.", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "plex.get_themes(" in body
    assert "probe_theme_entry_bytes(" in body
    # The scheme extraction lets the operator group entries by
    # URI scheme (upload/metadata/cloud/...) without re-parsing.
    assert "scheme" in body


# ── Settings UI block ────────────────────────────────────────


def test_settings_has_probe_block_under_diagnostics():
    """The settings page must render the PROBE PLEX THEME API
    block under the DIAGNOSTICS tab panel."""
    html = SETTINGS_HTML.read_text()
    assert 'id="probe-plex-themes-titles"' in html
    assert 'id="probe-plex-themes-btn"' in html
    assert 'id="probe-plex-themes-status"' in html
    assert 'id="probe-plex-themes-output"' in html
    # Block lives under data-panel="diagnostics".
    block_idx = html.index('id="probe-plex-themes-btn"')
    # Walk backwards to find the nearest enclosing <section> tag.
    section_open = html.rfind("<section", 0, block_idx)
    section_chunk = html[section_open:block_idx]
    assert 'data-panel="diagnostics"' in section_chunk, (
        "v1.18.85: PROBE block must be under the DIAGNOSTICS "
        "panel — not the PLEX or DOWNLOADS tabs"
    )


def test_settings_block_title_and_intro_present():
    """Title + intro paragraph must mention what the probe does
    so a future operator opening this block sees the purpose
    inline."""
    html = SETTINGS_HTML.read_text()
    assert "PROBE PLEX THEME API" in html
    # The intro explains the 4-flavor framing.
    assert (
        "cloud" in html.lower()
        and "themerr-plex" in html.lower()
    ), (
        "v1.18.85: intro must explain the cloud-vs-themerr-plex "
        "distinction so the purpose of the probe is obvious"
    )


# ── JS handler ───────────────────────────────────────────────


def test_js_has_bind_probe_plex_themes():
    """The bindProbePlexThemes() function must exist + be
    invoked alongside the other settings binders."""
    src = APP_JS.read_text()
    assert "function bindProbePlexThemes()" in src
    # Must be called from the settings-init block alongside
    # the existing binders.
    assert "bindProbePlexThemes()" in src


def test_js_handler_posts_titles_and_renders_output():
    """The handler must POST {titles: [...]} to the endpoint
    + render the JSON response in the output pre-block."""
    src = APP_JS.read_text()
    fn_idx = src.index("function bindProbePlexThemes()")
    fn_end = src.find("\n  function ", fn_idx + 1)
    body = src[fn_idx:fn_end if fn_end != -1 else len(src)]
    assert "/api/admin/probe-plex-themes" in body
    assert "titles" in body
    # Splits textarea by line.
    assert "split" in body
    # Renders JSON to the pre-block.
    assert "JSON.stringify" in body
    assert "probe-plex-themes-output" in body


def test_js_enforces_client_side_caps():
    """Client-side guard: cap titles at 8 BEFORE the POST so
    a stray paste doesn't trigger a 400 from the backend.
    Also bail on empty input."""
    src = APP_JS.read_text()
    fn_idx = src.index("function bindProbePlexThemes()")
    fn_end = src.find("\n  function ", fn_idx + 1)
    body = src[fn_idx:fn_end if fn_end != -1 else len(src)]
    # Cap matches backend (8).
    assert "8" in body
    # Empty-input guard.
    assert (
        "!titles.length" in body
        or "titles.length === 0" in body
        or "titles.length == 0" in body
    )


# ── End-to-end: endpoint behavior with stub Plex ─────────────


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    """Bootstrap a fresh app pointing at a tmp DB + forward-auth
    on. plex_enabled=False so we can hit the 503-no-token branch
    without needing a real Plex stub (sibling tests for the Plex-
    enabled path require live integration)."""
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path,
                        data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), db


AUTH = {"X-Authentik-Username": "testadmin"}


def test_endpoint_400s_on_empty_titles(app_client):
    """Empty or missing `titles` must 400."""
    client, _ = app_client
    r = client.post(
        "/api/admin/probe-plex-themes",
        headers=AUTH, json={"titles": []},
    )
    assert r.status_code == 400


def test_endpoint_400s_when_titles_missing_key(app_client):
    """No `titles` key in body → 400."""
    client, _ = app_client
    r = client.post(
        "/api/admin/probe-plex-themes",
        headers=AUTH, json={},
    )
    assert r.status_code == 400


def test_endpoint_400s_when_titles_exceeds_cap(app_client):
    """>8 titles → 400 (DoS guard)."""
    client, _ = app_client
    r = client.post(
        "/api/admin/probe-plex-themes",
        headers=AUTH,
        json={"titles": [f"t{i}" for i in range(10)]},
    )
    assert r.status_code == 400


def test_endpoint_503s_when_plex_disabled(app_client):
    """plex_enabled=False (default for the test fixture) →
    503. Even if titles validate, no Plex means no work."""
    client, _ = app_client
    r = client.post(
        "/api/admin/probe-plex-themes",
        headers=AUTH, json={"titles": ["Inception"]},
    )
    assert r.status_code == 503
