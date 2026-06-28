"""v1.24.43 — AWAIT topbar badge surfaces `!P` rows without filtering.

the user: the downloaded-but-not-placed (`!P`) rows — e.g. the v1.24.34 edition-
coverage backups — could only be found by manually filtering to ATTN=!P. This
adds a topbar AWAIT badge (mirroring RE-PUSH / FAIL): a count + tab_hint that
routes to the owning tab, hidden at 0. The count (_AWAIT_COUNT_SQL) and the
attn_pills=await FILTER share one predicate (_LIB_AWAIT_SQL), so the badge can't
drift from the page it links to (the v1.24.41 count-vs-render lesson).
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


def _count(db):
    from app.web.api import _AWAIT_COUNT_SQL
    with get_conn(db) as c:
        return c.execute(_AWAIT_COUNT_SQL).fetchone()[0]


def _filter_rks(client):
    c, _ = client
    r = c.get("/api/library?tab=movies&attn_pills=await", headers=AUTH)
    assert r.status_code == 200, r.text
    return {it["rating_key"] for it in r.json()["items"]}


# ── the badge counts the same rows the !P filter renders ─────────────────

def test_count_matches_the_await_filter(client):
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
    assert _count(db) == 1, "only the genuine await row counts"
    assert _filter_rks(client) == {"rk-await"}, "filter renders exactly that row"


def test_badge_total_and_tab_hint_in_stats(client):
    c, db = client
    with get_conn(db) as conn:
        _theme(conn, 20, -7, "Solo Await"); _canonical(conn, 20, -7)
        _plex_item(conn, tid=20, tmdb=-7, rk="rk-solo", title="Solo Await")
        conn.commit()
    r = c.get("/api/stats", headers=AUTH)
    assert r.status_code == 200
    awaiting = r.json()["awaiting"]
    assert awaiting["total"] == 1
    assert awaiting["tab_hint"] == "movies"


def test_zero_when_nothing_unplaced(client):
    c, db = client
    r = c.get("/api/stats", headers=AUTH)
    assert r.json()["awaiting"]["total"] == 0  # badge hides at 0


# ── source / surface pins ────────────────────────────────────────────────

def test_filter_and_count_share_one_predicate():
    src = (Path(__file__).resolve().parent.parent / "app" / "web" / "api.py").read_text()
    assert "_LIB_AWAIT_SQL" in src
    # the attn_pills=await branch appends the shared constant (no inline copy)
    assert "attn_branches.append(_LIB_AWAIT_SQL)" in src
    assert "_AWAIT_COUNT_SQL = f\"SELECT COUNT(*) {_AWAIT_COUNT_FROM} WHERE {_LIB_AWAIT_SQL}\"" in src


def test_badge_wired_in_template_and_js():
    base = (Path(__file__).resolve().parent.parent / "app" / "web" / "templates" / "base.html").read_text()
    js = (Path(__file__).resolve().parent.parent / "app" / "web" / "static" / "app.js").read_text()
    assert 'id="topbar-await-badge"' in base and "attn_pills=await" in base
    assert "topbar-await-badge" in js and "stats.awaiting" in js
