"""v0.51.238 — the PU filter must select exactly the rows that render PU.

v1.24.28 split stale plex_uploads out of PU into the RP chip, writing the PU
filter as `theme_present IS NOT 0` — which was the whole definition of stale at
the time. v1.24.40 then added rk-liveness and v1.24.41 a non-NULL-rk guard, both
ONLY to _LIB_STALE_PU_SQL (which drives the render via needs_repush, and the
sort). The filter never caught up, so two shapes rendered PU yet matched NEITHER
LINK filter — filtering by PU hid rows that visibly paint PU:

  * theme_present=0 but the stored rk is LIVE (the v1.24.40 self-correction,
    the operator's Two Towers case)
  * a legacy plex_upload with a NULL stored rk (v1.24.41)

Fix derives the PU filter from the negation of the same constant, so PU and RP
are an exact partition of plex_upload and cannot drift apart again. The tests
below assert THAT PROPERTY rather than pinning SQL text, so they keep holding if
the staleness definition is refined a fourth time.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

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


def _seed(conn, *, tmdb, tid, rk, theme_present, placement_rk, title):
    conn.execute("INSERT INTO themes (id, media_type, tmdb_id, title, "
                 " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                 "VALUES (?, 'movie', ?, ?, 'imdb', ?, ?)",
                 (tid, tmdb, title, NOW, NOW))
    conn.execute("INSERT INTO plex_items (rating_key, section_id, media_type, "
                 " theme_id, guid_tmdb, title, edition_key, has_theme, "
                 " first_seen_at, last_seen_at) "
                 "VALUES (?, '1', 'movie', ?, ?, ?, '', 1, ?, ?)",
                 (rk, tid, tmdb, title, NOW, NOW))
    conn.execute("INSERT INTO placements (theme_id, media_type, tmdb_id, "
                 " section_id, edition_key, media_folder, placed_at, "
                 " placement_kind, plex_rating_key, plex_refreshed, theme_present) "
                 "VALUES (?, 'movie', ?, '1', '', '', ?, 'plex_upload', ?, 1, ?)",
                 (tid, tmdb, NOW, placement_rk, theme_present))


def _seed_all_four(db):
    """The four plex_upload shapes the staleness definition distinguishes."""
    with get_conn(db) as conn:
        # healthy: theme_present=1, rk live
        _seed(conn, tmdb=1, tid=1, rk="rk1", theme_present=1,
              placement_rk="rk1", title="Healthy")
        # v1.24.40: theme_present=0 but the rk is LIVE — read-time self-correction
        _seed(conn, tmdb=2, tid=2, rk="rk2", theme_present=0,
              placement_rk="rk2", title="LiveRk")
        # v1.24.41: legacy plex_upload that never stored a rating_key
        _seed(conn, tmdb=3, tid=3, rk="rk3", theme_present=0,
              placement_rk=None, title="NullRk")
        # genuinely stale: theme_present=0 and the stored rk is DEAD
        _seed(conn, tmdb=4, tid=4, rk="rk4", theme_present=0,
              placement_rk="dead", title="Stale")
        conn.commit()


def _rows(c, qs=""):
    r = c.get(f"/api/library?tab=movies{qs}", headers=AUTH)
    assert r.status_code == 200, r.text
    return r.json()["items"]


def _renders_pu(it):
    """What renderLibraryRow paints: the needs_repush branch is FIRST, so a row
    only reaches the plex_upload/PU branch when needs_repush is falsey."""
    return not it.get("needs_repush") and it.get("placement_kind") == "plex_upload"


def test_every_row_that_renders_pu_is_selected_by_the_pu_filter(client):
    """The regression. Pre-fix LiveRk and NullRk painted PU but the filter
    dropped them."""
    c, db = client
    _seed_all_four(db)
    rendered = {it["rating_key"] for it in _rows(c) if _renders_pu(it)}
    filtered = {it["rating_key"] for it in _rows(c, "&link_pills=pu")}
    assert rendered == filtered, (
        f"rows painting PU but missing from link_pills=pu: {rendered - filtered}; "
        f"rows in the filter that do not paint PU: {filtered - rendered}")
    assert rendered == {"rk1", "rk2", "rk3"}


def test_only_the_genuinely_stale_row_is_rp(client):
    c, db = client
    _seed_all_four(db)
    rp = {it["rating_key"] for it in _rows(c, "&link_pills=rp")}
    assert rp == {"rk4"}, "only a dead stored rk is a re-push candidate"


def test_pu_and_rp_partition_every_plex_upload(client):
    """The invariant that makes this drift-proof: each plex_upload placement
    lands in exactly ONE of the two chips. Disjoint AND covering — if the
    staleness definition is refined again, whichever side moves, both filters
    move with it because they derive from the same constant."""
    c, db = client
    _seed_all_four(db)
    pu = {it["rating_key"] for it in _rows(c, "&link_pills=pu")}
    rp = {it["rating_key"] for it in _rows(c, "&link_pills=rp")}
    every_upload = {"rk1", "rk2", "rk3", "rk4"}
    assert not (pu & rp), f"a row cannot be both PU and RP: {pu & rp}"
    assert pu | rp == every_upload, f"unclassified plex_upload(s): {every_upload - (pu | rp)}"


def test_the_filter_derives_from_the_shared_staleness_constant(client):
    """Guards the mechanism, not just today's outcome: a hand-rolled staleness
    test in the filter is what drifted for three tags."""
    import inspect
    from app.web import api
    src = inspect.getsource(api.build_library_query) if hasattr(
        api, "build_library_query") else inspect.getsource(api)
    i = src.index('elif p == "pu":')
    j = src.index('elif p == "rp":', i)
    # CODE lines only — the rationale comment above the branch names the
    # constant too, so a comment-inclusive check passes even when the SQL has
    # been reverted to the hand-rolled test (verified: it did).
    code = "\n".join(ln for ln in src[i:j].splitlines()
                     if not ln.lstrip().startswith("#"))
    assert "_LIB_STALE_PU_SQL" in code, (
        "the PU branch must negate the shared constant, not re-implement "
        "staleness — that divergence is the bug")
    assert "theme_present" not in code, (
        "a hand-rolled theme_present test in the PU branch is exactly what "
        "drifted from _LIB_STALE_PU_SQL for three tags")
