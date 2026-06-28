"""v1.24.44 — topbar badges cycle through EVERY impacted tab (incl. collections).

the user: the "7 AWAIT" badge had 6 movies + 1 collection, but clicking it always
landed on /movies — the collection AWAIT was unreachable. The RE-PUSH + AWAIT
badges only carried a single tab_hint (LIMIT 1); FAIL/UPD already cycle through a
per-tab breakdown. This gives RE-PUSH / AWAIT (and DROP) the same `tabs`
breakdown + the shared bindBadgeCycle, and widens the cycle regex to include
/collections. Each badge now routes to every section that has a match.
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


def _await_row(c, *, tid, tmdb, rk, title, media_type):
    c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, "
              " upstream_source, last_seen_sync_at, first_seen_sync_at) "
              "VALUES (?, ?, ?, ?, '2001', 'imdb', ?, ?)",
              (tid, media_type, tmdb, title, NOW, NOW))
    c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, "
              " edition_key, theme_id, file_path, source_kind, source_video_id, "
              " downloaded_at, canonical_present) "
              "VALUES (?, ?, '1', '', ?, 'f.mp3','themerrdb','v',?,1)",
              (media_type, tmdb, tid, NOW))
    # plex_items media_type='collection' for the collection case; NO placement
    # → await (canonical present, no placement, Plex not self-serving).
    c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, "
              " theme_id, guid_tmdb, title, edition_key, plex_independent_theme, "
              " has_theme, first_seen_at, last_seen_at) "
              "VALUES (?, '1', ?, ?, ?, ?, '', 0, 0, ?, ?)",
              (rk, media_type, tid, tmdb, title, NOW, NOW))


def _tabs(client, key):
    c, _ = client
    r = c.get("/api/stats", headers=AUTH)
    assert r.status_code == 200, r.text
    return {t["tab"] for t in r.json()[key]["tabs"]}


# ── the bug: a collection AWAIT must appear in the cycle, not just movies ─

def test_await_breakdown_includes_collections(client):
    c, db = client
    with get_conn(db) as conn:
        _await_row(conn, tid=10, tmdb=-1, rk="m1", title="A Movie", media_type="movie")
        _await_row(conn, tid=11, tmdb=-900001, rk="c1", title="A Collection",
                   media_type="collection")
        conn.commit()
    tabs = _tabs(client, "awaiting")
    assert tabs == {"movies", "collections"}, (
        "the AWAIT cycle must reach BOTH the movies and the collections tab")


def test_stats_total_counts_both(client):
    c, db = client
    with get_conn(db) as conn:
        _await_row(conn, tid=10, tmdb=-1, rk="m1", title="A Movie", media_type="movie")
        _await_row(conn, tid=11, tmdb=-900001, rk="c1", title="A Collection",
                   media_type="collection")
        conn.commit()
    r = c.get("/api/stats", headers=AUTH)
    assert r.json()["awaiting"]["total"] == 2


# ── source / wiring pins ─────────────────────────────────────────────────

def test_breakdown_queries_and_cycle_wired():
    api = (Path(__file__).resolve().parent.parent / "app" / "web" / "api.py").read_text()
    js = (Path(__file__).resolve().parent.parent / "app" / "web" / "static" / "app.js").read_text()
    # API: per-tab breakdown queries + the tabs arrays in the responses
    assert "_REPUSH_TAB_BREAKDOWN_SQL" in api and "_AWAIT_TAB_BREAKDOWN_SQL" in api
    # v1.24.47: failures/updates keep the inline comprehension; the cycle family
    # (drops/repush/await) routes through the shared, deduped _breakdown_tabs().
    assert api.count('"tabs": [') >= 2  # failures, updates (inline)
    assert api.count('"tabs": _breakdown_tabs(') == 3  # drops, repush, awaiting
    # JS: shared cycle binder + the three new badges wired + collections in regex
    assert "function bindBadgeCycle(" in js
    assert "bindBadgeCycle('topbar-repush-badge', 'repushTabs', 'attn_pills=repush')" in js
    assert "bindBadgeCycle('topbar-await-badge', 'awaitTabs', 'attn_pills=await')" in js
    assert "bindBadgeCycle('topbar-drops-badge', 'dropTabs', 'tdb_pills=dropped')" in js
    assert js.count("movies|tv|anime|collections") == 1  # v1.24.48: one shared binder


def test_badges_stash_their_breakdown_dataset():
    js = (Path(__file__).resolve().parent.parent / "app" / "web" / "static" / "app.js").read_text()
    assert "repushBadge.dataset.repushTabs = JSON.stringify(breakdown)" in js
    assert "awaitBadge.dataset.awaitTabs = JSON.stringify(breakdown)" in js
    assert "dropBadge.dataset.dropTabs = JSON.stringify(dropBreakdown)" in js
