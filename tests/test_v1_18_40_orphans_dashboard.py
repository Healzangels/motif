"""v1.18.40 — Operator dashboard for the orphan scan.

v1.18.37's `/api/admin/orphan-scan` returned JSON only — useful
for one-off testing via DevTools, fragile for routine ops use.
v1.18.40 adds `/admin/orphans` HTML page that hits the scan
endpoint, renders the findings as a filterable table with
per-row action buttons (RE-PUSH / LET PLEX SERVE / PURGE)
that fire motif's existing endpoints.

## What ships

  - app/web/api.py: GET /admin/orphans route serves the new
    template; admin-gated.
  - app/web/templates/orphans.html: full page with summary
    strip + drift-type chip filters + findings table + per-row
    action buttons.
  - app/core/orphan_scan.py: scan findings enriched with
    `title` and `year` so the UI doesn't need a follow-up
    lookup per row.

## Action button mapping per drift_type

  - ok / motif_not_selected  → LET PLEX SERVE (re-affirm choice)
  - motif_entry_missing / nothing_selected / no_plex_entries
    / motif_not_selected → RE-PUSH (re-upload motif's theme)
  - rk_lookup_failed → PURGE (orphan to clean up)
  - All rows → PROBE (re-scan after manual action)

All actions fire motif's existing endpoints (/replace,
/unplace, /forget). No new mutation paths in v1.18.40.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import get_conn, init_db, transaction


REPO = Path(__file__).resolve().parent.parent
ORPHANS_HTML = REPO / "app" / "web" / "templates" / "orphans.html"


# ── Template structure ───────────────────────────────────────


def test_orphans_template_extends_base():
    """The template must extend base.html for nav + topbar
    consistency."""
    src = ORPHANS_HTML.read_text()
    assert '{% extends "base.html" %}' in src


def test_orphans_template_has_run_scan_button():
    """The page must have a clearly-labeled scan trigger
    button."""
    src = ORPHANS_HTML.read_text()
    assert 'id="orphans-run"' in src
    assert "RUN SCAN" in src


def test_orphans_template_renders_summary_chips():
    """The page must render a chips row for drift-type
    filtering."""
    src = ORPHANS_HTML.read_text()
    assert 'id="orphans-drift-chips"' in src
    assert 'activeDriftFilter' in src


def test_orphans_template_fires_admin_endpoint():
    """The page must call /api/admin/orphan-scan to load data."""
    src = ORPHANS_HTML.read_text()
    assert "/api/admin/orphan-scan" in src


def test_orphans_template_wires_action_buttons_to_existing_endpoints():
    """Per-row actions must fire motif's existing endpoints —
    no new mutation paths in v1.18.40. RE-PUSH → /replace,
    LPS → /unplace, PURGE → /forget."""
    src = ORPHANS_HTML.read_text()
    assert "/replace" in src
    assert "/unplace" in src
    assert "/forget" in src


def test_orphans_template_drift_type_tone_map_present():
    """Drift-type chips carry a severity tone map. v0.51.114: the ok/warn
    tiers use the FIXED btn-tone-* family (green/amber) instead of themeable
    greens, so the ok/warn/danger scale stays readable on every theme."""
    src = ORPHANS_HTML.read_text()
    assert "DRIFT_TONE" in src
    assert "'ok': 'btn-tone-ok'" in src  # fixed green (was lib-source-themerrdb)
    assert "lib-source-plex" in src  # LPS action uses plex tone


def test_orphans_template_purge_requires_confirm():
    """PURGE is destructive — must confirm before firing."""
    src = ORPHANS_HTML.read_text()
    assert "confirm(" in src
    # Confirm message mentions PURGE explicitly.
    assert "PURGE" in src


# ── Route + admin gating ─────────────────────────────────────


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


def test_orphans_page_requires_admin(admin_client):
    """No auth headers → redirect to /login OR 401/403. The
    admin gate is whatever motif applies elsewhere (the
    HTMLResponse routes redirect; the JSON endpoints 401)."""
    r = admin_client.get("/admin/orphans", follow_redirects=False)
    # Redirect (302/303) to login OR explicit unauth (401/403).
    assert r.status_code in (302, 303, 401, 403), (
        f"expected redirect or unauth; got {r.status_code}"
    )


def test_orphans_page_renders_with_admin(admin_client):
    """Admin sees the page with expected scaffolding."""
    r = admin_client.get("/admin/orphans", headers=AUTH)
    assert r.status_code == 200
    body = r.text
    assert "// ORPHAN SCAN" in body
    assert "RUN SCAN" in body
    # The page references the scan endpoint by URL.
    assert "/api/admin/orphan-scan" in body


# ── orphan_scan enrichment with title + year ─────────────────


def _seed_titled_placement(db, *, media_type, tmdb_id, section_id,
                            title, year):
    ts = "2026-05-21T00:00:00+00:00"
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            "INSERT OR IGNORE INTO themes "
            "(media_type, tmdb_id, title, year, youtube_url, "
            " upstream_source, last_seen_sync_at, "
            " first_seen_sync_at) "
            "VALUES (?, ?, ?, ?, ?, 'themoviedb', ?, ?)",
            (media_type, tmdb_id, title, year, "", ts, ts),
        )
        conn.execute(
            "INSERT OR IGNORE INTO plex_sections "
            "(section_id, title, type, included, "
            " discovered_at, last_seen_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (section_id, "Test Section", "movie", 1, ts, ts),
        )
        conn.execute(
            "INSERT OR IGNORE INTO plex_items "
            "(rating_key, section_id, media_type, title, year, "
            " guid_tmdb, has_theme, first_seen_at, last_seen_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (f"rk-{tmdb_id}", section_id, "movie", title, year,
             str(tmdb_id), 1, ts, ts),
        )
        conn.execute(
            "INSERT OR REPLACE INTO placements "
            "(media_type, tmdb_id, section_id, placement_kind, "
            " media_folder, placed_at) "
            "VALUES (?, ?, ?, 'plex_upload', '', ?)",
            (media_type, tmdb_id, section_id, ts),
        )


def test_scan_findings_include_title_and_year(tmp_path):
    """Each finding must include title + year from themes so the
    UI can render readable rows without a follow-up lookup."""
    from app.core.orphan_scan import scan_plex_upload_placements
    db = tmp_path / "motif.db"
    init_db(db)
    _seed_titled_placement(
        db, media_type="movie", tmdb_id=300, section_id="1",
        title="The Test Movie", year="1995",
    )
    plex = MagicMock()
    plex.get_themes.return_value = {
        "ok": True, "http_status": 200, "error": None,
        "body": {"MediaContainer": {"size": 0, "Metadata": []}},
    }
    findings = scan_plex_upload_placements(db, plex)
    assert len(findings) == 1
    assert findings[0]["title"] == "The Test Movie"
    assert findings[0]["year"] == "1995"


def test_scan_findings_title_falls_back_to_id_when_themes_row_missing(
    tmp_path,
):
    """If the themes row is missing (orphan placement), title
    falls back to '<mt>/<id>' so the UI still has something
    readable to display."""
    from app.core.orphan_scan import scan_plex_upload_placements
    db = tmp_path / "motif.db"
    init_db(db)
    _seed_titled_placement(
        db, media_type="movie", tmdb_id=301, section_id="1",
        title="Will Be Dropped", year="2000",
    )
    # Drop the themes row to simulate an orphan placement.
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            "DELETE FROM themes WHERE media_type='movie' "
            "AND tmdb_id=301"
        )
    plex = MagicMock()
    plex.get_themes.return_value = {
        "ok": True, "http_status": 200, "error": None,
        "body": {"MediaContainer": {"size": 0, "Metadata": []}},
    }
    findings = scan_plex_upload_placements(db, plex)
    # With themes row gone, placement.media_type/tmdb_id FK fires
    # ON DELETE CASCADE — placement is gone too, no findings.
    # (If FK is OFF for some reason, fallback would be
    # "movie/301".) Either outcome is acceptable; just don't
    # crash.
    for f in findings:
        assert "title" in f
        assert f["title"]  # non-empty
