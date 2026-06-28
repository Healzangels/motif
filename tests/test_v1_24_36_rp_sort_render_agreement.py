"""v1.24.36 — RP rows sort by the chip they render (review finding #4).

A stale plex_upload (RP) renders PL='await' (amber) and the orange RP LINK badge
(media_folder/placement_kind nulled by _LIB_EFF_MEDIA_FOLDER). But the PL/LINK
ORDER-BY CASEs read the RAW placement_kind/theme_present, so pre-fix an RP row
sorted as red-'broken' (PL) / PU (LINK) — contradicting the dot + badge it paints
(the v1.23.24/.25 "rank by the chip you paint" invariant). Fixed by pinning the
stale-plex_upload state (_LIB_STALE_PU_SQL) to the await rank (PL=3) and a
dedicated first rank (LINK=-1).

Titles are chosen adversarially: if RP shared a bucket with the comparison row
(the pre-fix bug), the alphabetical tiebreak would order them the OTHER way — so
the assertions only pass once RP is in its own correct bucket.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
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


def _row(c, *, tmdb, tid, rk, title, kind, theme_present,
         media_folder="", canonical=True, placement_rk=None):
    # v1.24.40: placement_rk points the placement at a DEAD rk (absent from
    # plex_items) so a stale plex_upload reads as genuinely RP under the
    # rk-liveness-tightened _LIB_STALE_PU_SQL. Defaults to rk (live).
    _place_rk = placement_rk if placement_rk is not None else rk
    c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, "
              " upstream_source, last_seen_sync_at, first_seen_sync_at) "
              "VALUES (?, 'movie', ?, ?, '2001', 'imdb', ?, ?)",
              (tid, tmdb, title, NOW, NOW))
    if canonical:
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, "
                  " edition_key, theme_id, file_path, source_kind, "
                  " source_video_id, downloaded_at, canonical_present) "
                  "VALUES ('movie', ?, '1', '', ?, 'f.mp3','themerrdb','v',?,1)",
                  (tmdb, tid, NOW))
    c.execute("INSERT INTO placements (theme_id, media_type, tmdb_id, section_id,"
              " edition_key, media_folder, placed_at, placement_kind, "
              " plex_rating_key, plex_refreshed, theme_present) "
              "VALUES (?, 'movie', ?, '1', '', ?, ?, ?, ?, 1, ?)",
              (tid, tmdb, media_folder, NOW, kind,
               (_place_rk if kind == 'plex_upload' else None), theme_present))
    c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, "
              " theme_id, guid_tmdb, title, edition_key, folder_path, has_theme, "
              " first_seen_at, last_seen_at) "
              "VALUES (?, '1','movie',?,?,?,'',?,0,?,?)",
              (rk, tid, tmdb, title, media_folder, NOW, NOW))


def _order(client, sort):
    c, _ = client
    r = c.get(f"/api/library?tab=movies&sort={sort}&sort_dir=asc", headers=AUTH)
    assert r.status_code == 200, r.text
    return [it["rating_key"] for it in r.json()["items"]]


# ── LINK sort: RP sorts first (its own rank), not lumped with PU ─────────

def test_link_sort_rp_before_valid_pu(client):
    c, db = client
    with sqlite3.connect(db) as conn:
        # RP titled 'ZZZ' (would sort LAST on the title tiebreak if same bucket);
        # a VALID plex_upload titled 'AAA'. Post-fix RP(rank -1) must precede it.
        _row(conn, tmdb=-29, tid=10, rk="rk-rp", title="ZZZ Stale",
             kind="plex_upload", theme_present=0, placement_rk="rk-rp-dead")
        _row(conn, tmdb=-30, tid=11, rk="rk-pu", title="AAA Valid",
             kind="plex_upload", theme_present=1)
        conn.commit()
    order = _order(client, "link")
    assert order.index("rk-rp") < order.index("rk-pu"), (
        "RP must sort before a valid PU (its own rank, not the PU bucket)")


# ── PL sort: RP sorts as await (3), not the red 'broken' bucket (0) ─────

def test_pl_sort_broken_sidecar_before_rp(client):
    c, db = client
    with sqlite3.connect(db) as conn:
        # A genuinely broken sidecar (theme_present=0, real folder) titled 'ZZZ';
        # an RP plex_upload titled 'AAA'. Pre-fix both = bucket 0 (broken) →
        # title tiebreak put AAA(RP) first. Post-fix the sidecar is broken(0),
        # RP is await(3), so the sidecar must precede RP.
        _row(conn, tmdb=-31, tid=12, rk="rk-broken", title="ZZZ Broken",
             kind="hardlink", theme_present=0, media_folder="/data/m/zzz")
        _row(conn, tmdb=-29, tid=10, rk="rk-rp", title="AAA Stale",
             kind="plex_upload", theme_present=0, placement_rk="rk-rp-dead")
        conn.commit()
    order = _order(client, "pl")
    assert order.index("rk-broken") < order.index("rk-rp"), (
        "a broken sidecar (PL bucket 0) must sort before an RP await row (3)")


# ── source pins ─────────────────────────────────────────────────────────

def test_sort_keys_pin_the_stale_pu_state():
    from app.web.api import _LIBRARY_SORTS_MAIN, _LIB_STALE_PU_SQL
    frag = _LIB_STALE_PU_SQL
    assert (frag + " THEN 3") in _LIBRARY_SORTS_MAIN["pl"]
    assert (frag + " THEN -1") in _LIBRARY_SORTS_MAIN["link"]
