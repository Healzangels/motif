"""v1.23.11 — restore ATTN pill narrows to the RESTORE-labeled subset.

Round 2 of the user's repro ("see 4 last results in anime still under
this filter"): themed T/P rows whose user-kind snapshot came from
ACCEPT UPDATE ("user URL captured for revert") matched the v1.23.10
gate — correctly mirroring the SOURCE menu, which DOES offer REVERT
there. But on a healthy themed row REVERT is a plain undo; the ↩
pill means "theme destroyed but recoverable". The JS draws exactly
that line with the button label (renderLibraryRow:
`isRestore = srcLetter '-' or 'M'` → RESTORE vs REVERT), so the
pill now requires src '-'/'M' on top of the menu gate.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
LIB_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    return TestClient(create_app(settings)), db


_H = {"X-Authentik-Username": "testadmin"}


def _seed(db: Path) -> None:
    now = now_iso()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, "
            "  included, is_anime, is_4k, themes_subdir, "
            "  discovered_at, last_seen_at) "
            "VALUES ('3', 'Anime', 'show', 1, 1, 0, 'anime', ?, ?)",
            (now, now),
        )
        # the user's round-2 shape (Am I Actually the Strongest?):
        # healthy T-row, ACCEPT UPDATE captured the old user URL →
        # menu shows REVERT (undo), pill must NOT match.
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, title_norm, "
            "  year, upstream_source, youtube_url, "
            "  last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('tv', 8201, 'Undo Show', 'undo show', '2023', "
            "  'themoviedb', 'https://www.youtube.com/watch?v=TDBCURRENT1', "
            "  ?, ?)",
            (now, now),
        )
        tid = cur.lastrowid
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "  title, title_norm, guid_tmdb, theme_id, has_theme, "
            "  first_seen_at, last_seen_at) "
            "VALUES ('821', '3', 'show', 'Undo Show', 'undo show', "
            "  8201, ?, 0, ?, ?)",
            (tid, now, now),
        )
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, "
            "  theme_id, file_path, downloaded_at, source_video_id, "
            "  provenance, source_kind, edition_key) "
            "VALUES ('tv', 8201, '3', ?, 'anime/Undo Show/theme.mp3', "
            "  ?, 'TDBCURRENT1', 'auto', 'themerrdb', '')",
            (tid, now),
        )
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, "
            "  theme_id, media_folder, placed_at, placement_kind, "
            "  provenance, edition_key) "
            "VALUES ('tv', 8201, '3', ?, '', ?, 'plex_upload', 'auto', '')",
            (tid, now),
        )
        conn.execute(
            "INSERT INTO previous_urls (media_type, tmdb_id, section_id, "
            "  youtube_url, kind, captured_at) "
            "VALUES ('tv', 8201, '3', "
            "  'https://www.youtube.com/watch?v=USERPREV001', 'user', ?)",
            (now,),
        )
        # The designed target: post-PURGE zombie ('-' row, user-kind
        # snapshot) — RESTORE-labeled, must keep matching.
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, title_norm, "
            "  year, upstream_source, youtube_url, youtube_video_id, "
            "  last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('tv', -902, 'Purged Show', 'purged show', '2024', "
            "  'plex_orphan', NULL, NULL, ?, ?)",
            (now, now),
        )
        ztid = cur.lastrowid
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "  title, title_norm, guid_tmdb, theme_id, has_theme, "
            "  first_seen_at, last_seen_at) "
            "VALUES ('822', '3', 'show', 'Purged Show', 'purged show', "
            "  -902, ?, 0, ?, ?)",
            (ztid, now, now),
        )
        conn.execute(
            "INSERT INTO previous_urls (media_type, tmdb_id, section_id, "
            "  youtube_url, kind, captured_at) "
            "VALUES ('tv', -902, '3', "
            "  'https://www.youtube.com/watch?v=USERPREV002', 'user', ?)",
            (now,),
        )
        conn.commit()


def test_revert_undo_rows_excluded(app_client):
    client, db = app_client
    _seed(db)
    r = client.get("/api/library?tab=anime&attn_pills=restore", headers=_H)
    assert r.status_code == 200
    titles = [it["plex_title"] for it in r.json()["items"]]
    assert titles == ["Purged Show"], (
        "the themed row's user-kind snapshot is a REVERT undo, not a "
        "restorable state — only the '-' row may match"
    )


def test_unfiltered_view_still_shows_both(app_client):
    client, db = app_client
    _seed(db)
    r = client.get("/api/library?tab=anime", headers=_H)
    assert r.status_code == 200
    titles = sorted(it["plex_title"] for it in r.json()["items"])
    assert titles == ["Purged Show", "Undo Show"]


def test_user_kind_standalone_eligibility_retired():
    """Source pin: the v1.23.10 `kind = 'user' OR` alternative must
    be gone from the restore branch — user-kind snapshots only count
    when the row is '-' or 'M' (the RESTORE label condition)."""
    assert ("AND (COALESCE(pv_sec.kind, pv_global.kind) = 'user'"
            not in API_PY)
    assert 'f" AND ({_LIB_SRC_LETTER_SQL}) IN (\'-\', \'M\'))"' in API_PY


def test_tooltip_describes_restore_not_revert():
    assert "RESTORE in the SOURCE menu brings it back" in LIB_HTML
