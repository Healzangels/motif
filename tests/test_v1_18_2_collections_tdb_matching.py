"""v1.18.2 — collection theme matching + on-disk staging path.

Three v1.18.0 regressions caught in production testing of the
fresh /collections tab:

  1. **resolve_theme_ids never matched collection rows.** The
     standard 3-pass UPDATE (tmdb_id → imdb_id → title+year)
     skipped collections in two ways:
       - Many Plex installs don't return `tmdb://` Guid on
         collection metadata (legacy Plex Movie Agent + tag-based
         collections lack TMDB GUIDs), so the tmdb pass missed.
       - The title-fallback pass required `pi.year IS NOT NULL`
         + `t.year = pi.year`; collections have no year on either
         side. `NULL = NULL` is FALSE in SQL → no match.
     Net effect: TDB sync added 136 collection theme records but
     the existing plex_items collection rows stayed at
     theme_id=NULL, rendering as no_tdb / P / – in the library
     UI with no link to the TDB record.

  2. **Sync's orphan-promotion didn't fire for collections.** Same
     year-required bug as above: `if orphan is None and title and
     year:` short-circuited when year was None. User SET URL on a
     collection → orphan at tmdb_id=-N; next sync added the real
     collection at tmdb_id=<positive>; promotion didn't fire; the
     user's URL stayed on the now-stranded orphan, disconnected
     from the TDB record. the user's Willy Wonka Collection test
     reproduced exactly this shape.

  3. **Collection theme downloads landed alongside movies/tv** on
     disk. Theme files went to `themes/movies/<title>/theme.mp3`
     (or `themes/tv/...` for TV-section collections), mixed with
     regular movie/tv themes. the user's request: "let's have all
     themes go to collections folder then broken further into
     movies, tv, anime like our normal folder structure just within
     a collections folder." Worker now routes collection downloads
     under `themes/collections/<section_subdir>/<canonical>/`.

Fixes:

  * `_resolve_theme_ids_impl` gains a fourth `sql_collection_title`
    UPDATE pass — title_norm-only match scoped to
    media_type='collection' on BOTH sides, after the standard
    three. Year-free shape applies ONLY to collections; movies/tv
    title fallback keeps the year predicate (Wonka 1971 vs 2023
    remake disambiguation stays intact).

  * `_upsert_theme` adds a collection-specific orphan-promotion
    branch: title-only match (no year) when the incoming sync
    record has `media_type='collection'`. Same `titles_equal`
    comparator as the movie/tv branch.

  * `_do_download` for `media_type='collection'` re-bases
    `media_root` from `themes/<section_subdir>/` to
    `themes/collections/<section_subdir>/`. Per-section split
    inside the collections/ parent stays legible at a glance.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

WORKER_PY = REPO / "app" / "core" / "worker.py"
PLEX_ENUM_PY = REPO / "app" / "core" / "plex_enum.py"
SYNC_PY = REPO / "app" / "core" / "sync.py"


# ── resolve_theme_ids fourth pass (collection title-only) ─────


def test_resolve_theme_ids_has_collection_title_pass():
    """`_resolve_theme_ids_impl` must declare a `sql_collection_title`
    UPDATE statement scoped to media_type='collection' on both
    sides, with NO year predicate (collections have no year)."""
    src = PLEX_ENUM_PY.read_text()
    fn_start = src.index("def _resolve_theme_ids_impl(")
    # Bound the inspection at end of the immediately-following
    # auto-bridge block.
    fn_end = src.index("\ndef resolve_theme_ids", fn_start + 1) if (
        "\ndef resolve_theme_ids" in src[fn_start + 1:]
    ) else len(src)
    block = src[fn_start:fn_end]
    assert "sql_collection_title" in block, (
        "v1.18.2: _resolve_theme_ids_impl must declare a "
        "sql_collection_title UPDATE for collection rows"
    )
    # Pin the year-free shape and the media_type='collection' scope.
    coll_sql_idx = block.index("sql_collection_title = f")
    # Read the SQL body — bound to the first triple-quote close.
    coll_sql_body = block[coll_sql_idx:coll_sql_idx + 1200]
    assert "media_type = 'collection'" in coll_sql_body
    # Counter-pin: the collection pass must NOT have a year
    # equality (the whole point is year-less matching).
    assert "t.year = plex_items.year" not in coll_sql_body, (
        "v1.18.2: collection title pass must NOT compare year — "
        "collections have no year on either side and the equality "
        "would always be NULL = NULL → false"
    )


def test_resolve_theme_ids_chunk_loop_executes_collection_pass():
    """The chunk loop must execute sql_collection_title alongside
    the three existing UPDATEs. Without this wire-up the new SQL
    would be defined but never run."""
    src = PLEX_ENUM_PY.read_text()
    fn_start = src.index("def _resolve_theme_ids_impl(")
    fn_end = src.index("\ndef resolve_theme_ids", fn_start + 1) if (
        "\ndef resolve_theme_ids" in src[fn_start + 1:]
    ) else len(src)
    block = src[fn_start:fn_end]
    # Find the chunk-loop transaction block.
    txn_idx = block.index("with get_conn(db_path) as conn, transaction(conn):")
    txn_block = block[txn_idx:txn_idx + 2000]
    assert "conn.execute(sql_collection_title" in txn_block, (
        "v1.18.2: chunk loop must execute sql_collection_title "
        "alongside the existing three UPDATEs"
    )


# ── Orphan promotion: collection title-only match ─────────────


def test_sync_upsert_theme_promotes_collection_orphan_by_title():
    """`_upsert_theme` must gain a collection-specific orphan
    promotion branch that matches by title alone (no year).
    Without this, a sync's collection record arriving after a
    user's SET URL on the same Plex collection leaves the orphan
    row stranded."""
    src = SYNC_PY.read_text()
    fn_start = src.index("def _upsert_theme(")
    fn_end = src.index("\ndef ", fn_start + 1)
    body = src[fn_start:fn_end]
    assert 'media_type == "collection"' in body
    assert (
        "WHERE upstream_source = 'plex_orphan'\n"
        "                     AND media_type = 'collection'"
    ) in body, (
        "v1.18.2: collection orphan promotion must scope by "
        "media_type='collection' and skip the year filter"
    )
    # Counter-pin: the new branch must NOT add a year filter.
    coll_branch = body[body.index('media_type == "collection"'):]
    # Cut at the next promotion / write site.
    coll_branch = coll_branch.split("if orphan:", 1)[0]
    assert "AND year =" not in coll_branch, (
        "v1.18.2: collection orphan promotion must NOT filter on "
        "year — collection records don't carry a year"
    )


# ── Download path under themes/collections/<section>/ ─────────


def test_do_download_routes_collections_under_collections_parent():
    """`_do_download` must re-base media_root for collection rows
    so the file lands under `themes/collections/<section_subdir>/`
    instead of `themes/<section_subdir>/`. Keeps the staging tree
    legible — the user: 'collections folder then broken further
    into movies, tv, anime.'"""
    src = WORKER_PY.read_text()
    fn_start = src.index("def _do_download(self, job:")
    # _do_place follows _do_download in the class definition.
    fn_end = src.index("\n    def _do_place(self, job:", fn_start + 1)
    body = src[fn_start:fn_end]
    # Pin the conditional re-base.
    assert 'if media_type == "collection":' in body, (
        "v1.18.2: _do_download must branch on collection to "
        "re-base media_root"
    )
    assert "media_root.parent / \"collections\" / media_root.name" in body, (
        "v1.18.2: collection media_root must be "
        "<themes_dir>/collections/<section_subdir>/"
    )


# ── End-to-end DB-level: collection orphan promotion ─────────


@pytest.fixture
def fresh_db(tmp_path: Path) -> Path:
    db = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db)
    return db


def test_collection_orphan_gets_promoted_by_title_only(fresh_db: Path):
    """End-to-end fixture: simulate the Willy Wonka Collection
    scenario.

    1. User SET URL on a collection → orphan themes row at
       tmdb_id=-27 with `upstream_source='plex_orphan'`.
    2. Sync arrives with the real TDB record for the same
       collection at tmdb_id=<positive>, no year.
    3. Sync's _upsert_theme orphan-promotion must catch the
       orphan via title-only match (year is None on both sides)
       and migrate tmdb_id=-27 → real.
    """
    from app.core.sync import _upsert_theme
    ts = "2026-05-19T23:52:00"
    with sqlite3.connect(fresh_db) as conn:
        conn.row_factory = sqlite3.Row
        # Pre-seed orphan (the post-SET-URL state).
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, year, "
            "   youtube_url, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', -27, 'Willy Wonka Collection', "
            "        NULL, "
            "        'https://www.youtube.com/watch?v=myqzUur4K98', "
            "        'plex_orphan', ?, ?)",
            (ts, ts),
        )
        conn.commit()
        # Sync record arriving from movie_collections/themoviedb/.
        record = {
            "id": 2156,
            "name": "Willy Wonka Collection",
            "youtube_theme_url": (
                "https://www.youtube.com/watch?v=somethingelse"
            ),
        }
        is_new, url_changed, _, _ = _upsert_theme(
            conn,
            media_type="collection",
            tmdb_id=2156,
            record=record,
            upstream_source="themoviedb",
            sync_ts="2026-05-20T00:00:00",
        )
    # The orphan must have been PROMOTED (not a new insert).
    assert is_new is False, (
        "v1.18.2: orphan promotion must catch the existing "
        "Willy Wonka Collection orphan by title alone and "
        "treat the upsert as an UPDATE, not a new INSERT"
    )
    with sqlite3.connect(fresh_db) as conn:
        conn.row_factory = sqlite3.Row
        # The themes row should now sit at tmdb_id=2156 with
        # upstream_source='themoviedb', not plex_orphan.
        promoted = conn.execute(
            "SELECT tmdb_id, upstream_source, title FROM themes "
            "WHERE media_type='collection'"
        ).fetchall()
    assert len(promoted) == 1, (
        "v1.18.2: orphan promotion must replace the existing "
        "row in-place — not leave a stale orphan"
    )
    assert promoted[0]["tmdb_id"] == 2156
    assert promoted[0]["upstream_source"] == "themoviedb"


def test_resolve_theme_ids_links_collection_by_title(fresh_db: Path):
    """End-to-end: a collection plex_items row (no guid_tmdb,
    no year) gets linked to its themes row via the new
    title-only SQL pass. Confirms the SQL actually runs and
    UPDATEs rows."""
    from app.core.plex_enum import resolve_theme_ids
    ts = "2026-05-19T12:00:00"
    with sqlite3.connect(fresh_db) as conn:
        conn.row_factory = sqlite3.Row
        # Seed plex_sections (required by FK).
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, 1, ?, ?)",
            (ts, ts),
        )
        # Seed themes row (the TDB-synced collection).
        from app.core.normalize import normalize_title
        title = "Harry Potter Collection"
        title_norm = normalize_title(title)
        themes_id = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, "
            "   year, youtube_url, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', 1241, ?, ?, NULL, "
            "        'https://www.youtube.com/watch?v=test', "
            "        'themoviedb', ?, ?)",
            (title, title_norm, ts, ts),
        ).lastrowid
        # Seed plex_items collection row with NO guid_tmdb + no
        # year (the "legacy Plex Movie Agent didn't write GUIDs"
        # case).
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   title_norm, year, first_seen_at, last_seen_at) "
            "VALUES ('rk-100', '1', 'collection', ?, ?, NULL, ?, ?)",
            (title, title_norm, ts, ts),
        )
        conn.commit()
    # Run resolve.
    resolve_theme_ids(fresh_db, section_id="1")
    # Verify the linkage stuck.
    with sqlite3.connect(fresh_db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT theme_id FROM plex_items WHERE rating_key='rk-100'"
        ).fetchone()
    assert row["theme_id"] == themes_id, (
        f"v1.18.2: resolve_theme_ids must link a collection "
        f"plex_items row to its themes row via title_norm "
        f"alone (no year). Got theme_id={row['theme_id']}, "
        f"expected {themes_id}."
    )


def test_resolve_theme_ids_collection_pass_skips_orphan_themes(fresh_db: Path):
    """The collection-title pass must `upstream_source !=
    'plex_orphan'` — same exclusion as the movie/tv title pass.
    Otherwise a plex_items collection row would bond to an
    existing orphan instead of the real TDB record (when both
    exist for the same title, which is the post-orphan-promotion
    transient state)."""
    from app.core.plex_enum import resolve_theme_ids
    from app.core.normalize import normalize_title
    ts = "2026-05-19T12:00:00"
    title = "Test Collection"
    title_norm = normalize_title(title)
    with sqlite3.connect(fresh_db) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, 1, ?, ?)",
            (ts, ts),
        )
        # Two themes rows for the SAME title — one orphan, one TDB.
        # The TDB row is the one we want resolve to pick.
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', -50, ?, ?, 'plex_orphan', ?, ?)",
            (title, title_norm, ts, ts),
        )
        tdb_id = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', 9999, ?, ?, 'themoviedb', ?, ?)",
            (title, title_norm, ts, ts),
        ).lastrowid
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   title_norm, first_seen_at, last_seen_at) "
            "VALUES ('rk-200', '1', 'collection', ?, ?, ?, ?)",
            (title, title_norm, ts, ts),
        )
        conn.commit()
    resolve_theme_ids(fresh_db, section_id="1")
    with sqlite3.connect(fresh_db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT theme_id FROM plex_items WHERE rating_key='rk-200'"
        ).fetchone()
    assert row["theme_id"] == tdb_id, (
        f"v1.18.2: collection title pass must prefer the TDB "
        f"row over the orphan with the same title. Got "
        f"theme_id={row['theme_id']}, expected TDB {tdb_id}."
    )
