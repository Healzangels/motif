"""v1.23.10 — restore ATTN pill mirrors the FULL SOURCE-menu gate.

the user on v1.23.7: "seeing a lot of results for restorable under
anime and 2 results on movies ... these rows aren't in the state
that this filter should be showing results for." The v1.23.7 branch
hand-copied only the has_previous_url shape; the SOURCE menu gates
REVERT/RESTORE on THREE conditions (app.js renderLibraryRow):
has_previous_url && !revert_redundant && (previous kind 'user' OR
src letter '-'/'M' — the v1.12.65 kind gate). His repro rows were
U-rows whose snapshot is a themerrdb-kind capture (SET URL stashed
the old TDB URL) — REVERT suppressed, pill matched anyway.

The fix extracts _PREV_URL_DIFFERS_SQL / _REVERT_REDUNDANT_SQL as
shared constants interpolated by BOTH the library SELECT and the
filter branch, so the pill can't drift from the menu again.
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

_TDB_URL = "https://www.youtube.com/watch?v=TDBTDBTDB01"
_USER_URL = "https://www.youtube.com/watch?v=USERUSER001"


def _seed_theme(conn, tmdb_id, title, youtube_url, rk):
    now = now_iso()
    cur = conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, title_norm, "
        "  year, upstream_source, youtube_url, "
        "  last_seen_sync_at, first_seen_sync_at) "
        "VALUES ('tv', ?, ?, ?, '2023', 'imdb', ?, ?, ?)",
        (tmdb_id, title, title.lower(), youtube_url, now, now),
    )
    tid = cur.lastrowid
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, "
        "  title, title_norm, guid_tmdb, theme_id, has_theme, "
        "  first_seen_at, last_seen_at) "
        "VALUES (?, '2', 'show', ?, ?, ?, ?, 0, ?, ?)",
        (rk, title, title.lower(), tmdb_id, tid, now, now),
    )
    return tid


def _seed(db: Path) -> None:
    now = now_iso()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, "
            "  included, is_anime, is_4k, themes_subdir, "
            "  discovered_at, last_seen_at) "
            "VALUES ('2', 'TV Shows', 'show', 1, 0, 0, 'tv', ?, ?)",
            (now, now),
        )
        # A — the user's repro shape: U-row whose snapshot is the old
        # TDB URL (SET URL capture, kind=themerrdb). REVERT hidden
        # by the v1.12.65 kind gate → must NOT match.
        tid = _seed_theme(conn, 8101, "Set Url Show", _TDB_URL, "801")
        conn.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, "
            "  section_id, youtube_url, set_at) "
            "VALUES ('tv', 8101, '2', ?, ?)", (_USER_URL, now),
        )
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, "
            "  theme_id, file_path, downloaded_at, source_video_id, "
            "  provenance, source_kind, edition_key) "
            "VALUES ('tv', 8101, '2', ?, 'tv/Set Url Show/theme.mp3', "
            "  ?, 'USERUSER001', 'manual', 'url', '')",
            (tid, now),
        )
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, "
            "  theme_id, media_folder, placed_at, placement_kind, "
            "  provenance, edition_key) "
            "VALUES ('tv', 8101, '2', ?, '', ?, 'plex_upload', "
            "  'manual', '')",
            (tid, now),
        )
        conn.execute(
            "INSERT INTO previous_urls (media_type, tmdb_id, section_id, "
            "  youtube_url, kind, captured_at) "
            "VALUES ('tv', 8101, '2', ?, 'themerrdb', ?)",
            (_TDB_URL, now),
        )
        # B — failed-download '-' row where the snapshot equals the
        # current TDB URL: the v1.12.103 revert_redundant branch
        # suppresses RESTORE (DOWNLOAD TDB is identical) → must NOT
        # match even though src '-' passes the kind gate.
        _seed_theme(conn, 8102, "Redundant Show", _TDB_URL, "802")
        conn.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, "
            "  section_id, youtube_url, set_at) "
            "VALUES ('tv', 8102, '2', ?, ?)", (_USER_URL, now),
        )
        conn.execute(
            "INSERT INTO previous_urls (media_type, tmdb_id, section_id, "
            "  youtube_url, kind, captured_at) "
            "VALUES ('tv', 8102, '2', ?, 'themerrdb', ?)",
            (_TDB_URL, now),
        )
        # C — TDB rolled the URL since the capture: snapshot locks to
        # the OLD TDB URL, meaningfully different from DOWNLOAD TDB →
        # RESTORE shows (v1.12.101 '-' relaxation) → MUST match.
        _seed_theme(conn, 8103, "Rolled Show",
                    "https://www.youtube.com/watch?v=NEWTDBURL01", "803")
        conn.execute(
            "INSERT INTO previous_urls (media_type, tmdb_id, section_id, "
            "  youtube_url, kind, captured_at) "
            "VALUES ('tv', 8103, '2', "
            "  'https://www.youtube.com/watch?v=OLDTDBURL01', "
            "  'themerrdb', ?)",
            (now,),
        )
        conn.commit()


def test_pill_excludes_menu_suppressed_rows(app_client):
    client, db = app_client
    _seed(db)
    r = client.get("/api/library?tab=tv&attn_pills=restore", headers=_H)
    assert r.status_code == 200
    titles = [it["plex_title"] for it in r.json()["items"]]
    assert titles == ["Rolled Show"], (
        "only the row whose SOURCE menu actually offers RESTORE may "
        "match; the kind-gated U-row and the revert_redundant row "
        "were the user's false positives"
    )


def test_unfiltered_view_still_shows_all(app_client):
    client, db = app_client
    _seed(db)
    r = client.get("/api/library?tab=tv", headers=_H)
    assert r.status_code == 200
    titles = sorted(it["plex_title"] for it in r.json()["items"])
    assert titles == ["Redundant Show", "Rolled Show", "Set Url Show"]


def test_count_agrees_with_rows(app_client):
    """attn_pills routes through the full FROM on the count path
    (v1.13.85) — the header count must agree with the row list."""
    client, db = app_client
    _seed(db)
    r = client.get("/api/library?tab=tv&attn_pills=restore", headers=_H)
    assert r.status_code == 200
    assert r.json()["total"] == 1


def test_shared_constants_interpolated_at_both_sites():
    """Mirror-drift guard: the SELECT CASEs and the filter branch
    must reference the SAME constants — a hand-copied predicate is
    exactly how v1.23.7 drifted."""
    assert "(CASE WHEN {_PREV_URL_DIFFERS_SQL}" in API_PY
    assert "(CASE WHEN {_REVERT_REDUNDANT_SQL}" in API_PY
    assert 'f"(({_PREV_URL_DIFFERS_SQL})\\n"' in API_PY
    assert 'f" AND NOT ({_REVERT_REDUNDANT_SQL})\\n"' in API_PY
    assert "({_LIB_SRC_LETTER_SQL}) IN ('-', 'M')" in API_PY
