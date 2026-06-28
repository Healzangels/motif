"""v0.50.34 — the topbar AWAIT badge is removed; the await FILTER + state stay.

The v1.24.43 topbar AWAIT badge flickered in during the download→place handoff (a
row is transiently downloaded-but-not-placed mid-place) and duplicated RE-PUSH, so
the user asked to drop it. The surviving surfaces — the attn_pills=await FILTER
(library.html ATTN chip) + the PL=await row state, both off the shared
_LIB_AWAIT_SQL predicate — are kept and guarded here, plus a pin that the badge +
its count machinery (_AWAIT_COUNT_SQL / stats.awaiting) are gone.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.db import get_conn, init_db

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime,"
                  " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path


def _theme(c, tid, tmdb, title):
    c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, "
              " upstream_source, last_seen_sync_at, first_seen_sync_at) "
              "VALUES (?, 'movie', ?, ?, '2001', 'imdb', ?, ?)",
              (tid, tmdb, title, NOW, NOW))


def _canonical(c, tid, tmdb):
    c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, "
              " edition_key, theme_id, file_path, source_kind, source_video_id, "
              " downloaded_at, canonical_present) "
              "VALUES ('movie', ?, '1', '', ?, 'f.mp3','themerrdb','v',?,1)",
              (tmdb, tid, NOW))


def _plex_item(c, *, tid, tmdb, rk, title, lps=0):
    c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, "
              " theme_id, guid_tmdb, title, edition_key, plex_independent_theme, "
              " has_theme, first_seen_at, last_seen_at) "
              "VALUES (?, '1','movie',?,?,?,'',?,0,?,?)",
              (rk, tid, tmdb, title, lps, NOW, NOW))


def _placement(c, *, tid, tmdb, media_folder):
    c.execute("INSERT INTO placements (theme_id, media_type, tmdb_id, section_id,"
              " edition_key, media_folder, placed_at, placement_kind, "
              " plex_refreshed, theme_present) "
              "VALUES (?, 'movie', ?, '1', '', ?, ?, 'hardlink', 1, 1)",
              (tid, tmdb, media_folder, NOW))


def _filter_rks(client):
    c, _ = client
    r = c.get("/api/library?tab=movies&attn_pills=await", headers=AUTH)
    assert r.status_code == 200, r.text
    return {it["rating_key"] for it in r.json()["items"]}


# ── the surviving attn_pills=await FILTER renders exactly the !P rows ─────────

def test_await_filter_renders_only_unplaced(client):
    c, db = client
    with get_conn(db) as conn:
        # await: canonical + no placement + not LPS
        _theme(conn, 10, -1, "Awaiting"); _canonical(conn, 10, -1)
        _plex_item(conn, tid=10, tmdb=-1, rk="rk-await", title="Awaiting")
        # placed: has a placement media_folder → NOT await
        _theme(conn, 11, -2, "Placed"); _canonical(conn, 11, -2)
        _plex_item(conn, tid=11, tmdb=-2, rk="rk-placed", title="Placed")
        _placement(conn, tid=11, tmdb=-2, media_folder="/data/m/placed")
        # LPS: Plex serves its own → NOT await
        _theme(conn, 12, -3, "LPS"); _canonical(conn, 12, -3)
        _plex_item(conn, tid=12, tmdb=-3, rk="rk-lps", title="LPS", lps=1)
        conn.commit()
    assert _filter_rks(client) == {"rk-await"}, "filter renders exactly the !P row"


def test_await_filter_empty_when_nothing_unplaced(client):
    assert _filter_rks(client) == set()


# ── surface pins: filter kept, badge + count machinery gone ──────────────────

def test_filter_uses_shared_predicate():
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "_LIB_AWAIT_SQL" in src
    assert "attn_branches.append(_LIB_AWAIT_SQL)" in src


def test_badge_and_count_machinery_removed():
    base = (REPO / "app" / "web" / "templates" / "base.html").read_text()
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    src = (REPO / "app" / "web" / "api.py").read_text()
    # the topbar badge + its glossary chip are gone
    assert "topbar-await-badge" not in base and "topbar-await-badge" not in js
    assert "gc-await" not in base
    # the badge-only count machinery is gone (the row predicate stays)
    assert "_AWAIT_COUNT_SQL" not in src
    assert "_AWAIT_TAB_BREAKDOWN_SQL" not in src
    assert "stats.awaiting" not in js
    # the row-level ATTN chip that drives the filter survives
    lib = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    assert 'data-attn-pill="await"' in lib
