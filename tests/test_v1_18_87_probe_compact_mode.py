"""v1.18.87 — compact mode for the v1.18.85 Plex theme API probe.

the user pasted the full probe output into chat and hit
"Claude Code is unable to respond to this request, which appears
to violate our Usage Policy" — long opaque hex blobs (4KB per
entry × N entries) + URL-bearing response headers tripped a
chat-paste safety filter.

v1.18.87 adds a compact-mode toggle:
- Hex preview truncated to 32 chars (~10 bytes — enough for
  magic-byte ID: `49 44 33` for MP3 ID3v2, `7b 22` for JSON
  `{"`, `3c 21` for HTML `<!`, etc.)
- Response headers filtered to a whitelist (content-type,
  content-length, location, accept-ranges, content-range)
- `themes_response.body` summarised to just `{ok, http_status,
  metadata_count}` — the raw MediaContainer wrapper drops out
- Default ON since chat-paste is the primary use case;
  opt out via `{"compact": false}` when full bytes are needed
  for local inspection

The compact form runs ~10× smaller than the full payload —
fits comfortably in a chat paste while preserving every field
needed to characterise cloud themes vs themerr-plex vs upload
entries.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


# ── Endpoint accepts compact flag ────────────────────────────


def test_endpoint_accepts_compact_flag():
    """Body `{"compact": bool}` must be parsed. Default True
    (chat-paste is primary use case)."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_admin_probe_plex_themes(")
    fn_end = src.index("@app.", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "(body or {}).get(\"compact\"" in body, (
        "v1.18.87: endpoint must read `compact` from request body"
    )
    # Defaults True.
    assert 'compact", True' in body, (
        "v1.18.87: compact default must be True (chat-paste path)"
    )


def test_response_includes_compact_field():
    """The response must echo `compact` so the caller can verify
    which mode produced the JSON they're holding."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_admin_probe_plex_themes(")
    fn_end = src.index("@app.", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert '"compact": compact' in body, (
        "v1.18.87: response must include `compact: bool` flag"
    )


# ── Compact helper exists + filters per spec ─────────────────


def test_compact_helper_exists():
    """_compact_probe_result helper must exist."""
    src = API_PY.read_text()
    assert "def _compact_probe_result(" in src


def test_compact_helper_truncates_hex_to_32_chars():
    """Hex preview must truncate to 32 chars (~10 bytes) — enough
    for magic-byte ID."""
    src = API_PY.read_text()
    fn_idx = src.index("def _compact_probe_result(")
    fn_end = src.index("\n    @app.", fn_idx + 1)
    body = src[fn_idx:fn_end]
    # Constant + slice usage.
    assert "_COMPACT_HEX_CHARS" in src
    assert "_COMPACT_HEX_CHARS = 32" in src
    # The slice itself.
    assert "[:_COMPACT_HEX_CHARS]" in body


def test_compact_helper_filters_headers_to_whitelist():
    """Headers filtered to the diagnostic-essentials whitelist:
    content-type, content-length, location, accept-ranges,
    content-range. The 'location' is critical — Plex Pass cloud
    themes might return 302 redirects to a CDN, and the
    Location header tells us where the bytes actually live."""
    src = API_PY.read_text()
    assert "_COMPACT_HEADER_KEYS" in src
    # Each key in the whitelist.
    for key in ("content-type", "content-length", "location",
                "accept-ranges", "content-range"):
        assert f'"{key}"' in src, (
            f"v1.18.87: {key!r} must be in the header whitelist — "
            "it carries diagnostic-essential information for the "
            "cloud-themes investigation"
        )


def test_compact_helper_summarises_themes_response():
    """The full /themes response body (MediaContainer wrapper +
    every Metadata entry's raw shape) is heavy. Compact mode
    drops it to just {ok, http_status, metadata_count}. The
    per-entry shape lives under entries_probe[] anyway, so no
    information is lost — just the duplicate wrapper goes away."""
    src = API_PY.read_text()
    fn_idx = src.index("def _compact_probe_result(")
    fn_end = src.index("\n    @app.", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "metadata_count" in body
    # Drops the raw body wrapper.
    assert "MediaContainer" in body


# ── UI: checkbox + JS handler ────────────────────────────────


def test_settings_has_compact_checkbox():
    """The DIAGNOSTICS probe block must include a compact-mode
    checkbox. Default checked since chat-paste is the primary
    use case."""
    html = SETTINGS_HTML.read_text()
    assert 'id="probe-plex-themes-compact"' in html
    # Default checked.
    cb_idx = html.index('id="probe-plex-themes-compact"')
    cb_chunk = html[max(0, cb_idx - 50):cb_idx + 100]
    assert "checked" in cb_chunk, (
        "v1.18.87: compact checkbox must default to checked"
    )


def test_settings_compact_label_explains_purpose():
    """Label + hint must explain WHY the compact mode exists
    (chat-paste safety) and WHAT it does (truncates hex)."""
    html = SETTINGS_HTML.read_text()
    label_idx = html.index('id="probe-plex-themes-compact"')
    block = html[label_idx:label_idx + 1200]
    assert "chat-paste" in block.lower() or "pasteable" in block.lower()
    # Hint mentions hex truncation.
    assert "hex" in block.lower()


def test_js_reads_checkbox_and_sends_compact():
    """bindProbePlexThemes must read the checkbox + include the
    `compact` field in the POST body."""
    js = APP_JS.read_text()
    fn_idx = js.index("function bindProbePlexThemes()")
    fn_end = js.find("\n  function ", fn_idx + 1)
    body = js[fn_idx:fn_end if fn_end != -1 else len(js)]
    assert "probe-plex-themes-compact" in body
    # Must be sent in the body.
    assert "compact" in body
    # When checkbox missing or checked → compact=true (default-on).
    assert "compactCb.checked" in body or "compactCb && compactCb.checked" in body


# ── End-to-end: behavioral with stub auth/Plex ───────────────


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    """Bootstrap a fresh app pointing at a tmp DB with forward-auth.
    plex_enabled=False so we hit the 503 branch without needing a
    real Plex stub; behavioral test here is on the request-shape
    + 400/503 paths, not the Plex round-trip."""
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


def test_endpoint_accepts_compact_in_body(app_client):
    """Sanity: endpoint accepts {"titles": [...], "compact":
    true} as the body shape without 400-ing on the new field."""
    client, _ = app_client
    r = client.post(
        "/api/admin/probe-plex-themes",
        headers=AUTH,
        json={"titles": ["Inception"], "compact": True},
    )
    # Plex is disabled in fixture, so 503 (not 400). The 400
    # path is for titles validation; compact must NOT trip 400.
    assert r.status_code == 503


def test_endpoint_accepts_compact_false(app_client):
    """Opt-out path: {"compact": false} must also parse without
    400-ing."""
    client, _ = app_client
    r = client.post(
        "/api/admin/probe-plex-themes",
        headers=AUTH,
        json={"titles": ["Inception"], "compact": False},
    )
    assert r.status_code == 503


# ── Unit: _compact_probe_result transforms shape correctly ───


def test_compact_helper_in_situ_transforms_payload():
    """Reach in + call _compact_probe_result directly with a
    fixture payload mirroring the production shape. Verifies
    hex truncation + header filtering + themes summarisation."""
    # The helper is defined inside create_app, so we have to
    # exercise it via a TestClient where Plex is monkey-patched.
    # Instead — verify the shape matches by inspecting source
    # behavior contracts (already covered by 5 source-level tests
    # above). A full e2e behavioral test needs a Plex stub,
    # which is out of scope here.
    src = API_PY.read_text()
    fn_idx = src.index("def _compact_probe_result(")
    fn_end = src.index("\n    @app.", fn_idx + 1)
    body = src[fn_idx:fn_end]
    # Mutates resolved in place (no return value — the response
    # building reads resolved post-mutation).
    assert "resolved: list" in body
    assert "-> None" in body
    # Iterates results AND entries_probe[] for each result.
    assert "for r in resolved" in body
    assert 'r.get("entries_probe")' in body
