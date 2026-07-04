"""v0.51.37 — INFO card explains a Plex-serving / no-motif-theme row.

the user (after UNMANAGE of a plex_upload movie): the row lands at SRC=P (Plex
keeps serving the theme motif uploaded — untracked, by design since v1.20.57) but
the INFO card said "(none — row has no theme staged)", which reads as "motif lost
the theme" — "left in a very confusing state".

Root: the card's Plex-serving branch only fired on plex_independent_theme=1, which
plex_enum sets on its NEXT cycle — so in the window right after UNMANAGE the flag
is still 0 and the card fell through to "(none)". v0.51.37 also surfaces
plex_items.has_theme from api_item so the card names the real state + the recovery
paths (RE-DOWNLOAD TDB / PURGE) in that window too.

Also: the DOWNLOAD PLEX BACKUP force-capture confirm now leads with WHY the strict
run found nothing (Plex serving a motif upload, not a cloud theme) so it doesn't
read as "the action did nothing".
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}


# ── source guards ────────────────────────────────────────────

def test_js_playback_label_has_plex_serving_branch():
    assert "data.plex_has_theme === 1" in APP_JS
    assert "Plex is serving a theme motif no longer manages" in APP_JS
    # the new branch must sit AFTER the plex_independent_theme branch and BEFORE
    # the "(none)" fallback.
    indep = APP_JS.index("data.plex_independent_theme === 1")
    has = APP_JS.index("data.plex_has_theme === 1")
    none = APP_JS.index("'(none — row has no theme staged)'")
    assert indep < has < none


def test_force_capture_confirm_explains_why():
    # v0.51.51 superseded the v0.51.37 strict-run "found no cloud theme" confirm:
    # the per-row backup now goes straight to capture (no confusing two-step),
    # so that negative-leading copy is gone. The only remaining confirm is the
    # destructive swap case (replacing an existing local file).
    assert "DOWNLOAD PLEX BACKUP found no Plex Pass cloud theme" not in APP_JS
    assert "REPLACE it with whatever Plex is currently serving" in APP_JS


# ── behavioral: api_item surfaces plex_has_theme ─────────────

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


def test_api_item_reports_plex_has_theme_for_plex_serving_row(admin_client, tmp_path):
    from app.config import Settings
    db = Settings(config_dir=tmp_path, data_dir=tmp_path / "data").db_path
    now = "2026-07-03T00:00:00"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, "
            "  themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (now, now))
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, "
            "  last_seen_sync_at, first_seen_sync_at, youtube_url) "
            "VALUES (1,'movie',4951,'Plex Serving Row','imdb',?,?, "
            "  'https://www.youtube.com/watch?v=X')", (now, now))
        # Plex serves a theme (has_theme=1) but motif has NO local_files + NO
        # placement + the independent flag not set yet (the post-UNMANAGE window).
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, "
            "  guid_tmdb, title, year, has_theme, local_theme_file, folder_path, "
            "  plex_independent_theme, plex_theme_verified_ok, first_seen_at, "
            "  last_seen_at) VALUES ('r1','1','movie',1,4951,'Plex Serving Row',"
            "  1999,1,0,'/data/movies/x',0,1,?,?)", (now, now))
        conn.commit()
    r = admin_client.get("/api/items/movie/4951?rating_key=r1", headers=AUTH)
    assert r.status_code == 200, r.text
    data = r.json()
    # the card now knows Plex is serving even though the independent flag is 0.
    assert data.get("plex_has_theme") == 1
    assert data.get("local_file") is None
