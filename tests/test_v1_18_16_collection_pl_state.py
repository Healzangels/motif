"""v1.18.16 — collection PL state + filter fixes + theme_id linkage repair.

the user's audit on v1.18.15 collections deploy:

  > Few interesting problems, when filtering on !P no results or
  > when filtering on canonical missing no results but checking
  > both and have results in screenshot. Also the bigger problem
  > is after providing a user url or themerrdb download it show
  > DL green but it always show amber PL as we're not actually
  > placing the theme.mp3 for collections we're using the API to
  > insert it.

Three coordinated bugs, one root cause + two presentation issues.

## Root cause (Issue 2: SRC=P + PL=amber on collections)

`plex_enum.resolve_theme_ids` sql_tmdb pass writes the subquery
result UNCONDITIONALLY:

```sql
UPDATE plex_items SET theme_id = (
    SELECT t.id FROM themes t
    WHERE ... AND t.upstream_source != 'plex_orphan'
)
WHERE pi.guid_tmdb IS NOT NULL
  ...
```

When a user SETs a URL on a Plex collection (e.g. Willy Wonka):

  1. /manual-url creates a synthetic orphan themes row
     (tmdb_id=-1, upstream_source='plex_orphan').
  2. /manual-url stamps pi.theme_id to the orphan's id (api.py:9713).
  3. Worker downloads + uploads via API. placements row inserted
     with media_folder='', placement_kind='plex_upload'. lf row
     inserted with source_kind='url'. Everything looks right.
  4. NEXT plex_enum cycle runs resolve_theme_ids. sql_tmdb sees:
     - pi.guid_tmdb IS NOT NULL (Plex provided the real TMDB
       collection id, e.g. 11196 for Wonka).
     - Subquery: `t.tmdb_id = 11196 AND upstream != 'plex_orphan'`
       — returns NULL (no real TDB theme for this collection).
     - UPDATE writes NULL. pi.theme_id linkage destroyed.
  5. Library JOIN chain breaks: pi.theme_id NULL → t.* NULL →
     p.* NULL → lf.* NULL. Row falls through to SRC='P' (the
     bottom-of-CASE Plex-agent branch) and PL=amber (await).

Fix: COALESCE(subquery, theme_id) so a no-match preserves the
existing linkage instead of nuking it. New + reused TDB themes
still promote correctly (the subquery returns non-NULL when a
real match exists, and COALESCE picks the non-NULL value).

Also affects movies + shows in the same orphan-with-real-tmdb
scenario, just less commonly because TDB coverage for those is
higher than for collections.

## Issue 1 (filter bug)

Post-stat matchers `_row_matches_pl` and `_row_matches_attn`
use Python `not it.get("media_folder")` which treats `''` as
falsy — so collection plex_upload placements get classified as
"awaiting placement." When the SQL await predicate runs alone
(`p.media_folder IS NULL`) collections correctly DON'T match
(empty string isn't NULL). The mismatch surfaces when both
`await` and `broken` STATUS pills are selected together: the
`broken` selection triggers post-stat mode, SQL branches are
skipped, and the post-stat OR matcher catches collections
through the broken await predicate. the user's observation:
"!P alone = 0, broken alone = 0, both = 2."

Fix: add `placement_kind == 'plex_upload'` exclusion to the
await predicate in both Python matchers + the JS
`awaitingApproval` derivation. Collections with a successful
upload are correctly classified as `on` (green PL).

## Issue 3 (tooltip)

PUSH TO PLEX / RE-PUSH tooltips say "Plex folder" which doesn't
match the API-upload reality for collections. Worker dispatch
already routes collections to `_do_place_collection` at
worker.py:1734 — only the tooltip needed updating.

## v1.18.16 changes

  * `app/core/plex_enum.py:sql_tmdb` — wrap subquery in COALESCE
    to preserve existing linkage on no-match.
  * `app/web/api.py:_row_matches_pl` — exclude plex_upload from
    await/off predicates, count it as `on`.
  * `app/web/api.py:_row_matches_attn` — same exclusion in the
    `await` branch.
  * `app/web/static/app.js:awaitingApproval` — same exclusion.
  * `app/web/static/app.js:placeItems` — branch PUSH TO PLEX /
    RE-PUSH tooltips on `themeMt === 'collection'`.
  * `app/core/recovery_v55.maybe_relink_orphan_theme_ids` — new
    one-shot backfill that repairs already-broken installs by
    re-linking pi.theme_id for orphan themes that lost their
    linkage. Idempotent via marker
    `recovery_orphan_theme_id_relink_done_at`.
  * `app/main.py` — wire the relink backfill into startup.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

PLEX_ENUM_PY = REPO / "app" / "core" / "plex_enum.py"
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
RECOVERY_PY = REPO / "app" / "core" / "recovery_v55.py"
MAIN_PY = REPO / "app" / "main.py"


# ── Fix 1: sql_tmdb preserves theme_id on no-match ────────────


def test_sql_tmdb_uses_coalesce_to_preserve_linkage():
    """plex_enum.resolve_theme_ids sql_tmdb must wrap the
    subquery in COALESCE(subquery, theme_id) so a no-match
    doesn't NULL out the existing linkage."""
    src = PLEX_ENUM_PY.read_text()
    # The COALESCE wrapping must be present in sql_tmdb.
    sql_tmdb_start = src.index("sql_tmdb = f\"\"\"")
    sql_tmdb_block = src[sql_tmdb_start:sql_tmdb_start + 2000]
    assert "COALESCE(" in sql_tmdb_block, (
        "v1.18.16: sql_tmdb must COALESCE the subquery to "
        "preserve existing theme_id on no-match"
    )
    assert ", theme_id)" in sql_tmdb_block, (
        "v1.18.16: COALESCE fallback must be `theme_id` (the "
        "existing column value)"
    )


def test_sql_tmdb_preserves_orphan_linkage_e2e(tmp_path: Path):
    """End-to-end: orphan-promoted plex_items row with a real
    pi.guid_tmdb keeps its synthetic theme_id when no TDB-tracked
    theme exists for that tmdb_id."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    from app.core.plex_enum import resolve_theme_ids
    init_db(db_path)
    ts = "2026-05-20T15:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        # Synthetic orphan themes row + matching plex_items row
        # with pi.guid_tmdb set (mimicking the user's collection
        # where Plex provided the real TMDB id).
        cur = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', -1, 'Wonka', "
            "        'wonka', NULL, 'plex_orphan', ?, ?)",
            (ts, ts),
        )
        orphan_id = cur.lastrowid
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   title_norm, year, folder_path, guid_tmdb, "
            "   local_theme_file, theme_id, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('479528', '1', 'collection', 'Wonka', "
            "        'wonka', NULL, '', '11196',"
            "        0, ?, ?, ?)",
            (orphan_id, ts, ts),
        )
        conn.commit()
    # No TDB-tracked theme exists for tmdb_id=11196. Pre-fix
    # resolve_theme_ids would write NULL to pi.theme_id;
    # post-fix it keeps the orphan linkage.
    resolve_theme_ids(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        pi = conn.execute(
            "SELECT theme_id FROM plex_items "
            "WHERE rating_key = '479528'"
        ).fetchone()
    assert pi["theme_id"] == orphan_id, (
        "v1.18.16: orphan linkage must survive resolve_theme_ids "
        "when no TDB theme exists for pi.guid_tmdb (pre-fix the "
        "subquery returned NULL and the UPDATE nuked the "
        "linkage)"
    )


def test_sql_tmdb_still_promotes_to_real_tdb_theme(tmp_path: Path):
    """When a real TDB theme DOES exist for pi.guid_tmdb, the
    orphan linkage gets promoted to the real one. Regression
    guard: COALESCE must not block the happy-path promotion."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    from app.core.plex_enum import resolve_theme_ids
    init_db(db_path)
    ts = "2026-05-20T15:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        orphan_cur = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', -7, 'Orphan Title', "
            "        'orphan title', '2020', 'plex_orphan', ?, ?)",
            (ts, ts),
        )
        orphan_id = orphan_cur.lastrowid
        real_cur = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 99888, 'Real TDB Title', "
            "        'real tdb title', '2020', 'themoviedb', ?, ?)",
            (ts, ts),
        )
        real_id = real_cur.lastrowid
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   title_norm, year, folder_path, guid_tmdb, "
            "   local_theme_file, theme_id, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('555', '1', 'movie', 'Real TDB Title', "
            "        'real tdb title', '2020', '', '99888',"
            "        0, ?, ?, ?)",
            (orphan_id, ts, ts),
        )
        conn.commit()
    resolve_theme_ids(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        pi = conn.execute(
            "SELECT theme_id FROM plex_items WHERE rating_key='555'"
        ).fetchone()
    assert pi["theme_id"] == real_id, (
        "v1.18.16: orphan→real TDB promotion must still happen "
        "when a real theme exists for pi.guid_tmdb"
    )


# ── Fix 2: post-stat matchers exclude plex_upload from await ──


def test_row_matches_pl_excludes_plex_upload_from_await():
    """_row_matches_pl must NOT classify a plex_upload placement
    as 'await' (the row IS placed, just via API). It must count
    it as 'on' instead. Source-grep guard."""
    src = API_PY.read_text()
    fn_idx = src.index("def _row_matches_pl(")
    body = src[fn_idx:fn_idx + 2500]
    assert "is_plex_upload" in body, (
        "v1.18.16: _row_matches_pl must compute an "
        "is_plex_upload gate to exclude API-uploaded "
        "collections from the await predicate"
    )
    assert 'placement_kind") == "plex_upload"' in body


def test_row_matches_attn_excludes_plex_upload_from_await():
    """_row_matches_attn (STATUS pill OR-matcher) must NOT
    classify plex_upload placements as await."""
    src = API_PY.read_text()
    # The function is local to _library_main_query; search by the
    # await-branch comment marker we added.
    assert "is_plex_upload = (" in src, (
        "v1.18.16: _row_matches_attn await branch must compute "
        "is_plex_upload"
    )


def test_js_awaiting_approval_excludes_plex_upload():
    """JS awaitingApproval gate must exclude plex_upload so the
    PL pill renders 'on' (green) for collection uploads."""
    js = APP_JS.read_text()
    assert "isPlexUpload = it.placement_kind === 'plex_upload'" in js, (
        "v1.18.16: app.js must compute isPlexUpload"
    )
    # And use it in the awaitingApproval expression.
    aa_idx = js.index("const awaitingApproval")
    aa_block = js[aa_idx:aa_idx + 400]
    assert "!isPlexUpload" in aa_block, (
        "v1.18.16: awaitingApproval must exclude plex_upload"
    )


# ── Fix 3: tooltip branches on collection media type ──────────


def test_push_tooltip_collection_branch():
    """PUSH TO PLEX tooltip must branch on collection media_type
    so the wording reflects the API-upload mechanism."""
    js = APP_JS.read_text()
    assert "isCollectionRow = themeMt === 'collection'" in js
    assert "Upload the downloaded theme to Plex via the collection-theme API" in js
    assert "Re-upload the canonical to Plex's collection-theme API" in js


# ── Fix 4: maybe_relink_orphan_theme_ids backfill ─────────────


def test_relink_helper_exists():
    """`maybe_relink_orphan_theme_ids` must be defined."""
    src = RECOVERY_PY.read_text()
    assert "def maybe_relink_orphan_theme_ids(" in src


def test_relink_uses_independent_marker():
    """Independent marker so the relink runs on installs where
    earlier recovery markers are already stamped."""
    src = RECOVERY_PY.read_text()
    assert "recovery_orphan_theme_id_relink_done_at" in src


@pytest.fixture
def broken_linkage_fixture(tmp_path: Path):
    """Mimics the user's collections state: orphan themes row +
    placements + local_files exist, but pi.theme_id is NULL
    (broken by the pre-v1.18.16 sql_tmdb bug)."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T15:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        cur = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('collection', -1, 'Wonka', "
            "        'wonka', NULL, 'plex_orphan', ?, ?)",
            (ts, ts),
        )
        orphan_id = cur.lastrowid
        # Placements row proves user already configured this.
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, media_folder, "
            "   placement_kind, placed_at, plex_refreshed) "
            "VALUES ('collection', -1, '1', '', "
            "        'plex_upload', ?, 1)",
            (ts,),
        )
        # plex_items row WITHOUT theme_id (the broken state).
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   title_norm, year, folder_path, "
            "   guid_tmdb, theme_id, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('479528', '1', 'collection', 'Wonka', "
            "        'wonka', NULL, '', '11196',NULL, ?, ?)",
            (ts, ts),
        )
        conn.commit()
    return db_path, orphan_id


def test_relink_repairs_broken_linkage(broken_linkage_fixture):
    """End-to-end: relink helper finds the broken row and
    restores pi.theme_id."""
    from app.core.recovery_v55 import maybe_relink_orphan_theme_ids
    db_path, orphan_id = broken_linkage_fixture
    stats = maybe_relink_orphan_theme_ids(db_path)
    assert stats["scanned"] >= 1
    assert stats["relinked"] >= 1
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        pi = conn.execute(
            "SELECT theme_id FROM plex_items "
            "WHERE rating_key='479528'"
        ).fetchone()
    assert pi["theme_id"] == orphan_id


def test_relink_is_idempotent(broken_linkage_fixture):
    """Second invocation short-circuits on the marker."""
    from app.core.recovery_v55 import maybe_relink_orphan_theme_ids
    db_path, _ = broken_linkage_fixture
    maybe_relink_orphan_theme_ids(db_path)
    stats2 = maybe_relink_orphan_theme_ids(db_path)
    assert stats2["skipped_reason"] == "marker_set"
    assert stats2["relinked"] == 0


def test_relink_skips_intact_rows(tmp_path: Path):
    """An orphan with intact pi.theme_id linkage must not be
    re-touched (and the linked plex_items row must keep its
    existing valid theme_id)."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    from app.core.recovery_v55 import maybe_relink_orphan_theme_ids
    init_db(db_path)
    ts = "2026-05-20T15:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        cur = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', -2, 'Intact', 'intact', "
            "        '2020', 'plex_orphan', ?, ?)",
            (ts, ts),
        )
        orphan_id = cur.lastrowid
        # placement to satisfy the helper's "interacted with"
        # filter.
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, media_folder, "
            "   placement_kind, placed_at, plex_refreshed) "
            "VALUES ('movie', -2, '1', "
            "        '/data/media/movies/Intact (2020)', "
            "        'hardlink', ?, 1)",
            (ts,),
        )
        # plex_items WITH linkage already intact.
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   title_norm, year, folder_path, "
            "   guid_tmdb, theme_id, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('1234', '1', 'movie', 'Intact', "
            "        'intact', '2020', "
            "        '/data/media/movies/Intact (2020)', "
            "        NULL, ?, ?, ?)",
            (orphan_id, ts, ts),
        )
        conn.commit()
    stats = maybe_relink_orphan_theme_ids(db_path)
    # No candidates (the helper's EXISTS check skips orphans
    # with already-linked plex_items).
    assert stats["scanned"] == 0
    assert stats["relinked"] == 0
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        pi = conn.execute(
            "SELECT theme_id FROM plex_items WHERE rating_key='1234'"
        ).fetchone()
    assert pi["theme_id"] == orphan_id
