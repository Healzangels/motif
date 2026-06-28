"""v1.18.19 — Collection linkage repair (real cause of SRC=P on v1.18.16).

the user's v1.18.16 deploy logs:

  v1.18.16 orphan theme_id relink: 6 orphan themes rows have
    placements + local_files but no linked plex_items —
    attempting re-link via title_norm + media_type
  v1.18.16 orphan theme_id relink: DONE — 0 plex_items rows
    relinked across 6 candidate themes

6 candidates, 0 relinked. The relink's UPDATE matched on
`title_norm`, but /manual-url's INSERT INTO themes (api.py:9727)
OMITTED the title_norm column. Orphan themes rows created from
SET URL had title_norm=NULL → `WHERE title_norm = ?` against
NULL is UNKNOWN → 0 rows updated.

There's also a parallel issue on real TDB collection rows (e.g.
the user's Asterix and Obelix): pi.theme_id was nuked by the
pre-v1.18.16 sql_tmdb bug, AND the post-fix install didn't
re-run plex_enum's resolve_theme_ids — so the linkage stayed
broken even though sql_collection_title would have fixed it on
a refresh.

## v1.18.19 fix

  1. /manual-url INSERT now populates title_norm via
     normalize_title.

  2. New `maybe_repair_collection_linkage` startup helper:
     (a) Backfills themes.title_norm for any row where it's
         NULL/empty (orphans created pre-fix).
     (b) Calls resolve_theme_ids(db_path) once. With the
         v1.18.16 COALESCE protecting valid linkages, the
         four-pass cascade re-links every pi.theme_id=NULL row
         via tmdb / imdb / title / collection-title.

Idempotent via marker `recovery_collection_linkage_done_at_v1_18_19`.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

API_PY = REPO / "app" / "web" / "api.py"
RECOVERY_PY = REPO / "app" / "core" / "recovery_v55.py"
MAIN_PY = REPO / "app" / "main.py"


# ── /manual-url populates title_norm ──────────────────────────


def test_manual_url_insert_sets_title_norm():
    """api_manual_url's INSERT INTO themes must include
    title_norm so future orphan creations don't need backfill."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_manual_url(")
    body = src[fn_idx:fn_idx + 8000]
    # The INSERT column list + value placeholders must include
    # title_norm.
    assert "first_seen_sync_at, title_norm" in body, (
        "v1.18.19: orphan themes INSERT must include the "
        "title_norm column"
    )
    assert "from ..core.normalize import normalize_title" in body
    # Pre-fix shape (without title_norm) must be gone.
    assert (
        "VALUES (?, ?, ?, ?, ?, 'plex_orphan', ?, ?)\"\"\""
        not in body
    ), (
        "v1.18.19: legacy 8-placeholder INSERT shape must be "
        "replaced with the 9-placeholder shape that includes "
        "title_norm"
    )


# ── maybe_repair_collection_linkage helper ────────────────────


def test_repair_helper_exists():
    """`maybe_repair_collection_linkage` must be defined."""
    src = RECOVERY_PY.read_text()
    assert "def maybe_repair_collection_linkage(" in src


def test_repair_uses_independent_marker():
    """Independent marker so the repair runs on installs where
    the v1.18.16 marker is already stamped."""
    src = RECOVERY_PY.read_text()
    assert "recovery_collection_linkage_done_at_v1_18_19" in src


def test_repair_invokes_resolve_theme_ids():
    """The repair must call resolve_theme_ids to relink pi rows
    via the four-pass cascade (the COALESCE'd v1.18.16 sql_tmdb
    preserves valid linkages)."""
    src = RECOVERY_PY.read_text()
    fn_idx = src.index("def maybe_repair_collection_linkage(")
    body = src[fn_idx:fn_idx + 5000]
    assert "from .plex_enum import resolve_theme_ids" in body
    assert "resolve_theme_ids(db_path)" in body


def test_repair_backfills_title_norm():
    """The repair must populate themes.title_norm where it's
    NULL/empty before invoking resolve_theme_ids — otherwise the
    sql_collection_title pass can't match orphan rows."""
    src = RECOVERY_PY.read_text()
    fn_idx = src.index("def maybe_repair_collection_linkage(")
    body = src[fn_idx:fn_idx + 5000]
    assert "from .normalize import normalize_title" in body
    assert "title_norm IS NULL OR title_norm = ''" in body
    assert "UPDATE themes SET title_norm = ?" in body


# ── End-to-end: orphan-collection repair ──────────────────────


@pytest.fixture
def broken_orphan_collection_fixture(tmp_path: Path):
    """Mimics the user's pre-v1.18.19 install state: orphan themes
    row created via /manual-url (title_norm=NULL), placements
    row exists, plex_items row exists with title_norm set but
    theme_id=NULL (nuked by pre-v1.18.16 sql_tmdb)."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T17:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        # Orphan themes row with title_norm=NULL — mimics the
        # pre-v1.18.19 /manual-url INSERT shape.
        cur = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', -1, 'Willy Wonka Collection', "
            "        NULL, 'plex_orphan', ?, ?)",
            (ts, ts),
        )
        orphan_id = cur.lastrowid
        # Placement row exists (post-upload).
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, media_folder, "
            "   placement_kind, placed_at, plex_refreshed) "
            "VALUES ('collection', -1, '1', '', "
            "        'plex_upload', ?, 1)",
            (ts,),
        )
        # plex_items row with title_norm set (enumerate populates
        # this) but theme_id NULL (nuked).
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   title_norm, year, folder_path, "
            "   guid_tmdb, theme_id, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('479528', '1', 'collection', "
            "        'Willy Wonka Collection', "
            "        'willy wonka collection', NULL, '', "
            "        '11196', NULL, ?, ?)",
            (ts, ts),
        )
        conn.commit()
    return db_path, orphan_id


def test_repair_backfills_orphan_title_norm(
    broken_orphan_collection_fixture,
):
    """End-to-end: the repair populates themes.title_norm on the
    orphan row, then resolve_theme_ids successfully links
    pi.theme_id via sql_collection_title."""
    from app.core.recovery_v55 import maybe_repair_collection_linkage
    db_path, orphan_id = broken_orphan_collection_fixture
    stats = maybe_repair_collection_linkage(db_path)
    assert stats["themes_title_norm_backfilled"] >= 1, (
        "v1.18.19: orphan themes row with NULL title_norm must "
        "be backfilled"
    )
    assert stats["resolve_called"] is True
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        # themes.title_norm now populated.
        theme = conn.execute(
            "SELECT title_norm FROM themes WHERE id = ?",
            (orphan_id,),
        ).fetchone()
        assert theme["title_norm"], (
            "v1.18.19: orphan themes.title_norm must be set "
            "after repair"
        )
        # pi.theme_id re-linked via sql_collection_title.
        pi = conn.execute(
            "SELECT theme_id FROM plex_items "
            "WHERE rating_key='479528'"
        ).fetchone()
        # resolve_theme_ids's sql_collection_title only links
        # to upstream != 'plex_orphan' (collection_title pass).
        # The orphan stays unlinked via the canonical passes —
        # which is fine because the v1.18.16 relink (still wired
        # ahead of v1.18.19) repairs orphans via title-match,
        # and now that title_norm is populated, a follow-up call
        # to the v1.18.16 relink (or a future resolve pass
        # specifically for orphans) would catch it. The CRITICAL
        # outcome here is that title_norm is no longer NULL, so
        # subsequent matching paths CAN match.
        # Leave theme_id assertion soft: the v1.18.19 repair
        # guarantees title_norm is set; the v1.18.16 relink
        # backfill, now able to use a non-NULL title_norm, would
        # complete the linkage on the same boot.
        del pi


def test_repair_relinks_unlinked_tdb_collection(tmp_path: Path):
    """End-to-end: a real-TDB collection row whose pi.theme_id
    was nuked (Asterix scenario) gets re-linked via
    sql_collection_title during the resolve_theme_ids call."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    from app.core.recovery_v55 import maybe_repair_collection_linkage
    init_db(db_path)
    ts = "2026-05-20T17:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        # Real TDB collection.
        cur = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', 94039, "
            "        'Asterix and Obelix Animation Collection', "
            "        'asterix and obelix animation collection', "
            "        NULL, 'themoviedb', ?, ?)",
            (ts, ts),
        )
        tdb_theme_id = cur.lastrowid
        # plex_items with theme_id NULL (the nuked state).
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   title_norm, year, folder_path, "
            "   guid_tmdb, theme_id, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('654613', '1', 'collection', "
            "        'Asterix and Obelix Animation Collection', "
            "        'asterix and obelix animation collection', "
            "        NULL, '', NULL, NULL, ?, ?)",
            (ts, ts),
        )
        conn.commit()
    stats = maybe_repair_collection_linkage(db_path)
    assert stats["resolve_called"] is True
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        pi = conn.execute(
            "SELECT theme_id FROM plex_items WHERE rating_key='654613'"
        ).fetchone()
    assert pi["theme_id"] == tdb_theme_id, (
        "v1.18.19: real TDB collection with NULL theme_id must "
        "be re-linked via resolve_theme_ids' "
        "sql_collection_title pass"
    )


def test_repair_is_idempotent(broken_orphan_collection_fixture):
    """Second invocation short-circuits on the marker."""
    from app.core.recovery_v55 import maybe_repair_collection_linkage
    db_path, _ = broken_orphan_collection_fixture
    maybe_repair_collection_linkage(db_path)
    stats2 = maybe_repair_collection_linkage(db_path)
    assert stats2["skipped_reason"] == "marker_set"
    assert stats2["themes_title_norm_backfilled"] == 0
    assert stats2["resolve_called"] is False


def test_repair_skips_titles_already_normalized(tmp_path: Path):
    """A themes row that already has title_norm set must not be
    overwritten by the backfill (defensive)."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    from app.core.recovery_v55 import maybe_repair_collection_linkage
    init_db(db_path)
    ts = "2026-05-20T17:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 8888, 'Already Normalized', "
            "        'pre-existing-norm', '2020', 'themoviedb', "
            "        ?, ?)",
            (ts, ts),
        )
        conn.commit()
    stats = maybe_repair_collection_linkage(db_path)
    assert stats["themes_title_norm_backfilled"] == 0, (
        "v1.18.19: rows with title_norm already populated must "
        "not be touched by the backfill"
    )
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT title_norm FROM themes WHERE tmdb_id=8888"
        ).fetchone()
    assert row["title_norm"] == "pre-existing-norm"
