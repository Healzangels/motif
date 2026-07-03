"""v0.51.36 — a backup-only (TB) row is NOT "awaiting placement".

the user (after DOWNLOAD TDB BACKUP on a movie): "it leaves it an amber state PL
row thinking that it needs to be placed when it was just downloading the backup."

Root cause = contract-drift (CLAUDE.md bug class 9): the v1.19.21 `backup_only`
terminal state was added to the WRITE side (worker stamps
last_place_attempt_reason='backup_only') + the retry-sweep skip-list, but the
READ-side "awaiting placement" predicates were never taught to exclude it. So a
finished backup-only row still painted amber PL + the amber "!" glyph (JS
awaitingApproval) and matched the AWAIT / attention filters (SQL).

v0.51.36 excludes backup_only at all FOUR await sites (mirror-drift):
  - JS `awaitingApproval` (app.js) — the visible amber PL dot + "!" glyph
  - `_LIB_AWAIT_SQL` (api.py) — the attn_pills=await filter + attention count
  - the `pl_pills=await` SQL branch (api.py)
  - `_row_matches_attn` await branch (api.py)
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


# ── mirror-drift guard: backup_only excluded at all four await sites ──

def test_js_awaitingApproval_excludes_backup_only():
    i = APP_JS.index("const awaitingApproval =")
    body = APP_JS[i:i + 260]
    assert "it.last_place_attempt_reason !== 'backup_only'" in body


def test_backend_await_predicates_exclude_backup_only():
    # _LIB_AWAIT_SQL (attn filter)
    i = API_PY.index("_LIB_AWAIT_SQL = (")
    assert "IS NOT 'backup_only'" in API_PY[i:i + 1100]
    # the pl_pills=await SQL branch
    j = API_PY.index('elif p == "await":\n                # v0.51.36')
    assert "IS NOT 'backup_only'" in API_PY[j:j + 800]
    # _row_matches_attn await branch (Python)
    k = API_PY.index('it.get("last_place_attempt_reason") != "backup_only"')
    assert k > 0


# ── behavioral: the backup-only row drops out of the await filters ──

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
    now = "2026-07-03T00:00:00"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime, "
            "  is_4k, themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (now, now))
        # Two movies: A = backup-only (TB, terminal); B = genuinely awaiting place.
        for tid, title in ((101, "Backup Only Row"), (102, "Awaiting Place Row")):
            conn.execute(
                "INSERT INTO themes (id, media_type, tmdb_id, title, "
                "  upstream_source, last_seen_sync_at, first_seen_sync_at, "
                "  youtube_url) VALUES (?,?,?,?,'imdb',?,?, "
                "  'https://www.youtube.com/watch?v=X')",
                (tid, "movie", tid, title, now, now))
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type, "
                "  theme_id, guid_tmdb, title, year, has_theme, local_theme_file, "
                "  folder_path, plex_independent_theme, plex_theme_verified_ok, "
                "  first_seen_at, last_seen_at) VALUES (?, '1','movie',?,?,?,2020,"
                "  1,1,'/data/movies/x',0,1,?,?)",
                (f"r{tid}", tid, tid, title, now, now))
        # A: themerrdb backup-only canonical, NO placement → the TB terminal state.
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, theme_id, "
            "  file_path, downloaded_at, source_video_id, provenance, source_kind, "
            "  last_place_attempt_reason) VALUES ('movie',101,'1',101,"
            "  'movies/a/theme.mp3',?,'X','auto','themerrdb','backup_only')", (now,))
        # B: downloaded, NOT placed, reason NULL → a genuine "awaiting placement".
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, theme_id, "
            "  file_path, downloaded_at, source_video_id, provenance, source_kind, "
            "  last_place_attempt_reason) VALUES ('movie',102,'1',102,"
            "  'movies/b/theme.mp3',?,'X','auto','themerrdb',NULL)", (now,))
        conn.commit()


def _titles(client, query):
    r = client.get(f"/api/library?tab=movies&per_page=50&{query}", headers=AUTH)
    assert r.status_code == 200, r.text
    return [(it.get("plex_title") or it.get("theme_title"))
            for it in r.json()["items"]]


def test_backup_only_row_excluded_from_await_filters(admin_client, tmp_path):
    _seed(tmp_path)
    # PL=await filter: the genuine await row matches; the backup-only one does NOT.
    pl_await = _titles(admin_client, "pl_pills=await")
    assert "Awaiting Place Row" in pl_await, "a genuine await row must still match"
    assert "Backup Only Row" not in pl_await, (
        "v0.51.36: a backup_only (TB) row is terminal, not awaiting placement")
    # attn=await filter (the NEEDS WORK / attention axis) — same exclusion.
    attn_await = _titles(admin_client, "attn_pills=await")
    assert "Awaiting Place Row" in attn_await
    assert "Backup Only Row" not in attn_await


def test_backup_only_row_still_visible_unfiltered(admin_client, tmp_path):
    # sanity: excluding it from AWAIT must not hide the row entirely — it still
    # lists (as a TB backup) with no filter.
    _seed(tmp_path)
    all_titles = _titles(admin_client, "")
    assert "Backup Only Row" in all_titles and "Awaiting Place Row" in all_titles
