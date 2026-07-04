"""v0.51.50 — RECAPTURE FROM PLEX for a row serving a motif upload.

the user (post-UNMANAGE bare-P row): "you get the download plex backup option
when it's in that state but it never seems to download backup — confusing."

Root: after UNMANAGE, Plex keeps serving the theme motif ITSELF uploaded (an
`upload://` entry), not a Plex-Pass `metadata://` cloud theme. DOWNLOAD PLEX
BACKUP's strict cloud-backup walker only captures cloud themes, so it finds
nothing (0 backed up) and the flow drops into a confusing "found nothing →
capture anyway?" confirm. A real capture path (the force variant) existed but
was buried behind that negative-leading confirm.

v0.51.50 exposes plex_items.plex_theme_uri in the /api/library payload so the
SOURCE menu can tell the two apart, and for an `upload://` serving theme offers
a single honest RECAPTURE FROM PLEX action that goes STRAIGHT to the one-step
force-capture (positive-leading confirm), skipping the no-op strict run. A
`metadata://` (or not-yet-enumerated NULL) uri keeps DOWNLOAD PLEX BACKUP.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}


# ── source guards ────────────────────────────────────────────

def test_library_payload_exposes_plex_theme_uri():
    # both /api/library SELECT branches carry the field (plex-first + the
    # TDB-only branch) so the frontend key is consistent across row kinds.
    assert "pi.plex_theme_uri," in API_PY
    assert "NULL AS plex_theme_uri," in API_PY


def test_js_recapture_branch_gates_on_upload_uri():
    i = APP_JS.index("const servingIsMotifUpload =")
    body = APP_JS[i:i + 400]
    assert "(it.plex_theme_uri || '').indexOf('upload://') === 0" in body
    # the upload:// branch offers RECAPTURE, not DOWNLOAD PLEX BACKUP.
    assert "'RECAPTURE FROM PLEX'" in APP_JS
    assert "recapture: '1'" in APP_JS


def test_js_menuitem_registers_data_recapture():
    assert 'data-recapture="${extras.recapture}"' in APP_JS


def test_js_click_handler_recapture_skips_strict_run():
    i = APP_JS.index("if (btn.dataset.recapture === '1')")
    body = APP_JS[i:i + 160]
    # goes straight to the one-step force-capture, then returns before the
    # strict cloud-backup run.
    assert "cloudBackupForceCapture(rk, hasTdb, true)" in body
    assert "return;" in body


def test_js_force_capture_direct_lead_is_positive():
    i = APP_JS.index("function cloudBackupForceCapture(rk, hasTdb, direct)")
    body = APP_JS[i:i + 2200]
    assert "const lead = direct" in body
    # the direct (RECAPTURE) lead leads with the capture, not the negative
    # strict-run "found no cloud theme" framing.
    assert "Capture those exact bytes back into motif" in body


# ── behavioral: plex_theme_uri reaches the /api/library payload ──

@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(settings))


def _seed(tmp_path):
    from app.config import Settings
    db = Settings(config_dir=tmp_path, data_dir=tmp_path / "data").db_path
    now = "2026-07-04T00:00:00"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime, "
            "  is_4k, themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (now, now))
        # A: Plex serving a motif upload:// theme (post-UNMANAGE, no local file).
        # B: Plex serving a metadata:// Plex-Pass cloud theme.
        rows = (
            (301, "Recapture Row", "upload://themes/aaaaaaaa"),
            (302, "Cloud Row", "metadata://themes/bbbbbbbb"),
        )
        for tid, title, uri in rows:
            conn.execute(
                "INSERT INTO themes (id, media_type, tmdb_id, title, "
                "  upstream_source, last_seen_sync_at, first_seen_sync_at, "
                "  youtube_url) VALUES (?,?,?,?,'imdb',?,?, "
                "  'https://www.youtube.com/watch?v=X')",
                (tid, "movie", tid, title, now, now))
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type, "
                "  theme_id, guid_tmdb, title, year, has_theme, local_theme_file, "
                "  folder_path, plex_theme_uri, plex_independent_theme, "
                "  plex_theme_verified_ok, first_seen_at, last_seen_at) "
                "VALUES (?, '1','movie',?,?,?,2020,1,0,'/data/movies/x',?,0,1,?,?)",
                (f"r{tid}", tid, tid, title, uri, now, now))
        conn.commit()


def _rows_by_title(client):
    r = client.get("/api/library?tab=movies&per_page=50", headers=AUTH)
    assert r.status_code == 200, r.text
    return {(it.get("plex_title") or it.get("theme_title")): it
            for it in r.json()["items"]}


def test_plex_theme_uri_in_library_payload(admin_client, tmp_path):
    _seed(tmp_path)
    rows = _rows_by_title(admin_client)
    assert "Recapture Row" in rows and "Cloud Row" in rows
    # the discriminator the RECAPTURE gate reads is present + correct per row.
    assert rows["Recapture Row"]["plex_theme_uri"] == "upload://themes/aaaaaaaa"
    assert rows["Cloud Row"]["plex_theme_uri"] == "metadata://themes/bbbbbbbb"
