"""v1.22.32 (audit) — reaper false 'theme lost' on an unlinked themed sibling.

Before dispatching a plex_theme_lost notification for a stale plex_items row,
the v1.18.89 reaper checks still_p: 'does any SURVIVING row for the same
(mt, tmdb) still serve a theme?'. Pre-fix that INNER JOIN'd themes on
pi.theme_id, so a surviving sibling Plex still themes (has_theme=1) but motif
never linked (theme_id NULL — a multi-edition rk, or a anime-agent match TDB didn't
cover) was invisible → the reaper fired a FALSE 'theme lost' while Plex was
still serving the title on that sibling. The fix mirrors the candidate set's
COALESCE: it also matches surviving rows by plex_items.guid_tmdb directly.

The scenario uses NO themes row (both rks carry guid_tmdb only, theme_id NULL)
so the upsert can't re-link the surviving sibling — the exact theme_id-NULL
state the pre-fix INNER JOIN missed.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
PLEX_ENUM_PY = (REPO / "app" / "core" / "plex_enum.py").read_text()

SEC = "1"
TID = 100  # shared guid_tmdb; NO themes row exists, so theme_id stays NULL
THEAT = "/data/Movies/Troy (2004) {edition-theatrical}"
DCUT = "/data/Movies/Troy (2004) {edition-directors cut}"


def _item(rk, *, has_theme, folder):
    from app.core.plex import PlexLibraryItem
    return PlexLibraryItem(
        rating_key=rk, section_id=SEC, media_type="movie",
        title="Troy", year="2004", guid_imdb=None, guid_tmdb=TID,
        guid_tvdb=None, folder_path=folder, has_theme=has_theme,
        plex_theme_uri="",
    )


def _prime_reaper_grace(db):
    """v0.51.128: pre-age every plex_items row to one miss below the reaper's
    _REAP_MISS_THRESHOLD so this single stale enum's increment crosses it. These
    tests exercise the reaper's TIER logic (still_p suppression), not the new
    miss-counter, so they need the pre-v0.51.128 single-pass reap. A fresh DB
    still starts every row at 0 misses, so the transient-glitch guard stays
    covered by its own v0.51.128 behavioral test."""
    import sqlite3
    from app.core.plex_enum import _REAP_MISS_THRESHOLD
    with sqlite3.connect(db) as c:
        c.execute("UPDATE plex_items SET consecutive_missing = ?",
                  (_REAP_MISS_THRESHOLD - 1,))
        c.commit()


@pytest.fixture
def seeded(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    db = tmp_path / "motif.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,"
            "'2026-05-01T00:00:00','2026-05-01T00:00:00')")
        # Stale candidate (theatrical) — has_theme=1, theme_id NULL,
        # guid_tmdb=100. Plex no longer returns it → reaped.
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " guid_tmdb, title, year, has_theme, local_theme_file, folder_path,"
            " first_seen_at, last_seen_at) VALUES ('S1','1','movie',?,'Troy',"
            "2004,1,0,?,'2026-05-01T00:00:00','2026-05-19T00:00:00+00:00')",
            (TID, THEAT))
        # Surviving sibling (director's cut) — has_theme=1, theme_id NULL,
        # guid_tmdb=100. Plex still returns it (it's in `items`).
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " guid_tmdb, title, year, has_theme, local_theme_file, folder_path,"
            " first_seen_at, last_seen_at) VALUES ('S2','1','movie',?,'Troy',"
            "2004,1,0,?,'2026-05-01T00:00:00','2026-05-23T00:00:00+00:00')",
            (TID, DCUT))
        conn.commit()
    # Capture notify.dispatch calls (plex_enum's late `from . import notify`
    # binds the module, so attribute patching takes effect at call time).
    calls = []
    import app.core.notify as _notify
    monkeypatch.setattr(
        _notify, "dispatch",
        lambda *a, **k: calls.append(k.get("event_kind")))
    return db, calls


def test_surviving_unlinked_themed_sibling_suppresses_lost(seeded):
    db, calls = seeded
    from app.core import plex_enum
    # Plex returns ONLY the surviving sibling, still themed. S1 is stale.
    _prime_reaper_grace(db)
    plex_enum._upsert_items(
        db, [_item("S2", has_theme=True, folder=DCUT)], section_id=SEC)
    with sqlite3.connect(db) as conn:
        rks = sorted(r[0] for r in conn.execute(
            "SELECT rating_key FROM plex_items WHERE section_id='1'"))
    assert rks == ["S2"], "stale S1 must be reaped, S2 survives"
    assert "plex_theme_lost" not in calls, (
        "v1.22.32: a surviving themed sibling (theme_id NULL) must suppress "
        "the lost-theme notification")


def test_no_surviving_themed_sibling_still_fires(seeded):
    db, calls = seeded
    from app.core import plex_enum
    # The sibling survives but is now UNthemed → no survivor serves a theme.
    _prime_reaper_grace(db)
    plex_enum._upsert_items(
        db, [_item("S2", has_theme=False, folder=DCUT)], section_id=SEC)
    assert "plex_theme_lost" in calls, (
        "control: with no surviving themed sibling, the lost notification "
        "must still fire")


def test_still_p_uses_left_join_and_guid_clause():
    i = PLEX_ENUM_PY.index("still_p = conn.execute(")
    # v1.23.64: widened 600 -> 900 — the new plex_sections included-gate JOIN
    # pushed the guid clause further down the query.
    block = PLEX_ENUM_PY[i:i + 900]
    assert "LEFT JOIN themes t ON t.id = pi.theme_id" in block
    assert "pi.guid_tmdb = ?" in block
    assert "INNER JOIN themes" not in block, (
        "v1.22.32: still_p must not INNER JOIN themes (drops theme_id-NULL "
        "siblings)")
    # v1.23.64: only a survivor in an INCLUDED section suppresses the loss — a
    # stale has_theme=1 row in a disabled section must not mask a real loss.
    assert "JOIN plex_sections ps" in block
    assert "ps.included = 1" in block


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
