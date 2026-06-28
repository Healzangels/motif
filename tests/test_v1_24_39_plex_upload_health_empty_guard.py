"""v1.24.39 — verify_placement_health doesn't flip uploads to RP on an empty enum.

Review #5: the plex_upload staleness pass UPDATEs theme_present=0 for every
plex_upload whose plex_rating_key isn't a live plex_items row. That 0-stamp
drives the RP badge + the v1.24.29 auto re-push, so it's destructive. If a
failed/aborted enum leaves plex_items EMPTY, the NOT EXISTS matches EVERY upload
→ a mass false re-push storm. The fix skips the 0-stamps when plex_items is
empty (EXISTS is meaningless against an empty table; a real library with uploads
always has items). A genuine mass Plex re-add leaves plex_items FULL with fresh
rks, so the legit re-push is unaffected.
"""
from __future__ import annotations

import pytest

from app.core import plex_enum
from app.core.db import get_conn, init_db
from app.core.plex_enum import now_iso

NOW = now_iso()


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "m.db"
    init_db(p)
    return p


def _theme(c, tid, tmdb):
    c.execute(
        "INSERT OR IGNORE INTO plex_sections (section_id, title, type, is_anime,"
        " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
        "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
    c.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, "
        " last_seen_sync_at, first_seen_sync_at) "
        "VALUES (?, 'movie', ?, 'X', 'plex_orphan', ?, ?)", (tid, tmdb, NOW, NOW))


def _pu(c, tmdb, rk, theme_present):
    c.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, edition_key, "
        " media_folder, placed_at, placement_kind, plex_rating_key, "
        " plex_refreshed, theme_present) "
        "VALUES ('movie', ?, '1', '', '', ?, 'plex_upload', ?, 1, ?)",
        (tmdb, NOW, rk, theme_present))


def _item(c, rk, tmdb, tid):
    c.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, "
        " guid_tmdb, title, edition_key, has_theme, first_seen_at, last_seen_at) "
        "VALUES (?, '1', 'movie', ?, ?, 'X', '', 0, ?, ?)", (rk, tid, tmdb, NOW, NOW))


def _present(db, tmdb):
    with get_conn(db) as c:
        return c.execute("SELECT theme_present FROM placements WHERE tmdb_id=?",
                         (tmdb,)).fetchone()[0]


# ── the guard: empty plex_items must NOT flip uploads to stale ───────────

def test_empty_plex_items_preserves_prior_theme_present(db):
    # a previously-good upload (theme_present=1), but plex_items is EMPTY (a
    # failed enum). The dead-looking rk must NOT be stamped 0.
    with get_conn(db) as c:
        _theme(c, 1, -1)
        _pu(c, -1, "rk-1", theme_present=1)  # no plex_items at all
        c.commit()
    res = plex_enum.verify_placement_health(db)
    assert res["plex_upload_skipped"] is True
    assert _present(db, -1) == 1, "must preserve prior state, not false-flip to RP"
    assert res["plex_upload_stale"] == 0


def test_populated_plex_items_still_flips_a_genuinely_dead_rk(db):
    # plex_items non-empty (the re-added item enumerated with a fresh rk); the
    # upload's old rk is genuinely gone → correctly stamped stale.
    with get_conn(db) as c:
        _theme(c, 2, -2)
        _pu(c, -2, "dead", theme_present=1)
        _item(c, "fresh-rk", -2, 2)  # library DID enumerate — just not 'dead'
        c.commit()
    res = plex_enum.verify_placement_health(db)
    assert res["plex_upload_skipped"] is False
    assert _present(db, -2) == 0


def test_live_rk_still_stamped_present_even_if_others_missing(db):
    # confirmed-present stamps are always applied (never gated).
    with get_conn(db) as c:
        _theme(c, 3, -3)
        _pu(c, -3, "live", theme_present=0)
        _item(c, "live", -3, 3)
        c.commit()
    plex_enum.verify_placement_health(db)
    assert _present(db, -3) == 1


def test_source_has_empty_guard():
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "plex_enum.py").read_text()
    assert "if plex_items_total == 0:" in src
