"""v1.24.41 — RE-PUSH count/render parity + sort + NULL-rk (code-review fixes).

The v1.24.40 review found the badge count (_REPUSH_COUNT_SQL) diverged from what
the library renders (_LIB_STALE_PU_SQL):

  #1 count was `FROM placements` while the render is anchored `FROM plex_items` —
     a stale plex_upload whose Plex item was REMOVED (no live plex_items row) was
     counted but never rendered (un-clickable badge). Fix: count is now a
     mini-render (plex_items-anchored, WHERE _LIB_STALE_PU_SQL).
  #2 the badge linked to a static /movies; collections (the likely RP source)
     were unreachable. Fix: a tab_hint routes the href to the owning tab.
  #3 the attention/NEEDS WORK sort bucketed bare theme_present=0 as most-urgent
     with no rk-liveness gate — a live-rk stale plex_upload (rendered placed)
     topped NEEDS WORK looking fine. Fix: the bucket reuses _LIB_STALE_PU_SQL.
  #4 _LIB_STALE_PU_SQL lacked the `plex_rating_key IS NOT NULL` guard the count
     had — a NULL-rk row's vacuous NOT EXISTS read RP. Fix: guard added to both.
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
        # a collections section (collections always use plex_upload)
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime,"
                  " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('9','Collections','movie',0,0,'coll',1,?,?)", (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path


def _theme(c, tid, tmdb, title):
    c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, "
              " upstream_source, last_seen_sync_at, first_seen_sync_at) "
              "VALUES (?, ?, ?, ?, '2002', 'imdb', ?, ?)",
              (tid, 'collection' if tmdb < -900000 else 'movie', tmdb, title, NOW, NOW))


def _placement(c, *, tmdb, tid, section, kind, theme_present, rk, media_folder=""):
    mt = 'collection' if tmdb < -900000 else 'movie'
    c.execute("INSERT INTO placements (theme_id, media_type, tmdb_id, section_id,"
              " edition_key, media_folder, placed_at, placement_kind, "
              " plex_rating_key, plex_refreshed, theme_present) "
              "VALUES (?, ?, ?, ?, '', ?, ?, ?, ?, 1, ?)",
              (tid, mt, tmdb, section, media_folder, NOW, kind, rk, theme_present))


def _plex_item(c, *, tmdb, tid, section, rk, title, media_type='movie'):
    c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, "
              " theme_id, guid_tmdb, title, edition_key, has_theme, "
              " first_seen_at, last_seen_at) "
              "VALUES (?, ?, ?, ?, ?, ?, '', 1, ?, ?)",
              (rk, section, media_type, tid, tmdb, title, NOW, NOW))


def _count(db):
    from app.web.api import _REPUSH_COUNT_SQL
    with get_conn(db) as c:
        return c.execute(_REPUSH_COUNT_SQL).fetchone()[0]


def _lib(c, qs=""):
    r = c.get(f"/api/library?tab=movies{qs}", headers=AUTH)
    assert r.status_code == 200, r.text
    return {it["rating_key"]: it for it in r.json()["items"]}


# ── #1: orphaned placement (no live plex_items) is NOT counted ───────────

def test_orphaned_plex_upload_not_counted_or_rendered(client):
    # Plex removed the item: a stale plex_upload placement survives but there's
    # NO plex_items row. The badge must not count it (it can't be rendered).
    c, db = client
    with get_conn(db) as conn:
        _theme(conn, 10, -1, "Gone")
        _placement(conn, tmdb=-1, tid=10, section='1', kind='plex_upload',
                   theme_present=0, rk='dead')           # no plex_item seeded
        conn.commit()
    assert _count(db) == 0, "an unrenderable orphan must not inflate the badge"
    assert _lib(c) == {}, "and the library renders nothing for it"


# ── #1b: a genuine re-add (live plex_items, dead placement rk) IS counted ─

def test_genuine_readd_is_counted_and_rendered(client):
    c, db = client
    with get_conn(db) as conn:
        _theme(conn, 11, -2, "Avenue Q")
        _placement(conn, tmdb=-2, tid=11, section='1', kind='plex_upload',
                   theme_present=0, rk='old-dead')
        _plex_item(conn, tmdb=-2, tid=11, section='1', rk='new-live',
                   title="Avenue Q")
        conn.commit()
    assert _count(db) == 1
    assert _lib(c)["new-live"]["needs_repush"] == 1


# ── #2: tab_hint routes the badge to the owning tab (collections) ────────

def test_tab_hint_points_at_the_collection_tab(client):
    c, db = client
    with get_conn(db) as conn:
        _theme(conn, 12, -999001, "Marvel Collection")
        _placement(conn, tmdb=-999001, tid=12, section='9', kind='plex_upload',
                   theme_present=0, rk='coll-dead')
        _plex_item(conn, tmdb=-999001, tid=12, section='9', rk='coll-live',
                   title="Marvel Collection", media_type='collection')
        conn.commit()
    r = c.get("/api/stats", headers=AUTH)
    assert r.status_code == 200
    repush = r.json()["repush"]
    assert repush["total"] == 1
    assert repush["tab_hint"] == "collections", "RP collection routes to /collections, not /movies"


# ── #3: attention sort — live-rk stale PU does NOT top NEEDS WORK ────────

def test_attention_sort_excludes_live_rk_stale_pu(client):
    # Adversarial titles: a genuinely-broken SIDECAR 'ZZZ' (bucket 0) and a
    # live-rk stale plex_upload 'AAA'. Pre-fix both sat in bucket 0 → title
    # tiebreak put AAA first. Post-fix the live-rk PU drops out of bucket 0, so
    # the broken sidecar must precede it.
    c, db = client
    with get_conn(db) as conn:
        _theme(conn, 13, -3, "ZZZ Broken Sidecar")
        _placement(conn, tmdb=-3, tid=13, section='1', kind='hardlink',
                   theme_present=0, rk=None, media_folder="/data/m/zzz")
        _plex_item(conn, tmdb=-3, tid=13, section='1', rk='zzz', title="ZZZ Broken Sidecar")
        _theme(conn, 14, -4, "AAA Live PU")
        _placement(conn, tmdb=-4, tid=14, section='1', kind='plex_upload',
                   theme_present=0, rk='aaa-live')        # rk IS live below
        _plex_item(conn, tmdb=-4, tid=14, section='1', rk='aaa-live', title="AAA Live PU")
        conn.commit()
    r = c.get("/api/library?tab=movies&sort=attention&sort_dir=asc", headers=AUTH)
    order = [it["rating_key"] for it in r.json()["items"]]
    assert order.index("zzz") < order.index("aaa-live"), (
        "a broken sidecar must out-rank a live-rk stale plex_upload in NEEDS WORK")


# ── #4: NULL-rk plex_upload is NOT RP (render) and NOT counted ───────────

def test_null_rk_plex_upload_not_rp(client):
    c, db = client
    with get_conn(db) as conn:
        _theme(conn, 15, -5, "Legacy NULLrk")
        _placement(conn, tmdb=-5, tid=15, section='1', kind='plex_upload',
                   theme_present=0, rk=None)              # NULL rating_key
        _plex_item(conn, tmdb=-5, tid=15, section='1', rk='live', title="Legacy NULLrk")
        conn.commit()
    assert _lib(c)["live"]["needs_repush"] == 0, "a NULL-rk row can't be re-pushed → not RP"
    assert _count(db) == 0, "and is not counted (render == count)"


# ── source pins ─────────────────────────────────────────────────────────

def test_count_is_plex_items_anchored():
    src = (Path(__file__).resolve().parent.parent / "app" / "web" / "api.py").read_text()
    assert "_REPUSH_COUNT_FROM" in src
    assert "FROM plex_items pi " in src and "_REPUSH_COUNT_SQL = f\"SELECT COUNT(*) {_REPUSH_COUNT_FROM} WHERE {_LIB_STALE_PU_SQL}\"" in src
    # v1.24.47: the RE-PUSH badge's owning-tab hint is now derived from the
    # breakdown's first row (_breakdown_tab_hint), not a separate LIMIT-1 query.
    assert "_breakdown_tab_hint(repush_tab_breakdown_rows)" in src
    assert "COALESCE(p_e.plex_rating_key, p_g.plex_rating_key) IS NOT NULL" in src
