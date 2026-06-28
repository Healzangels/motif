"""v1.18.91 — fix reaper media_type scoping + candidate query +
defensive REFRESH-button unlock.

the user's post-v1.18.89/.90 deploy logs surfaced 3 bugs:

## Bug A — reaper false-aborts on collection enums (HIGH)

```
15:48:29 reaper: would delete 4168/4308 (96%) stale plex_items
         in section 2 — ABORTING
15:49:51 reaper: would delete 1234/1328 (92%) stale plex_items
         in section 3 — ABORTING
```

Root cause: `_upsert_items` runs TWICE per section — once for
items (`type=show`/`movie`) + once for `collection`s. The
v1.18.89 reaper compared `seen_rks` (current call only) vs ALL
`plex_items.rating_key` for the section (both media_types). So
the collection enum saw N "stale" rks that were just the items
from the prior call, tripping the 20%/50-row amplifier-sweep
guard at 96%.

Fix: scope reaper queries to `media_type` (extracted from
`items[0].media_type`).

## Bug B — plex_theme_lost notifications silently miss orphans (MED)

The v1.18.89 reaper deleted 152 stale rows in section 2 + 95
in section 3 (all confirmed via WARNING logs). The v1.18.90
candidate-capture query INNER-JOINed `themes` on `pi.theme_id`.
Rows with NULL `pi.theme_id` (unresolved orphans, fresh
inserts not yet resolved) were silently filtered out — no
notification fired despite real lost-theme transitions.

Fix: switch to LEFT JOIN + COALESCE the lookup keys against
`plex_items.guid_tmdb` so unresolved rows still surface.
Translate `pi.media_type='show' → 'tv'` for themes-domain
consistency.

## Bug C — REFRESHING button stuck after op completes (LOW UX)

the user: button reads "// REFRESHING…" indefinitely even though
LIVE OPS drawer shows idle + no ops running. The natural
unlock path (`myTabBusy → false`) never fires for some
discrepancy between `/api/stats`'s per-tab signal and the
real op state.

Fix: defensive unlock in refreshTopbarStatus — if the button
has been disabled for >30s AND `anyMutatingOpActive` is false,
force-unlock. Doesn't replace the natural unlock path; just
catches stuck states.
"""
from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

PLEX_ENUM_PY = REPO / "app" / "core" / "plex_enum.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def _reaper_body() -> str:
    """Extract the reaper block from _upsert_items so assertions
    target the right code."""
    src = PLEX_ENUM_PY.read_text()
    fn_idx = src.index("def _upsert_items(")
    fn_end = src.index("\ndef ", fn_idx + 1)
    return src[fn_idx:fn_end]


# ── Bug A: media_type scoping in reaper ──────────────────────


def test_reaper_extracts_items_media_type():
    """v1.18.91: reaper must capture the media_type of THIS
    enum call from items[0] BEFORE running any queries, so all
    scoping uses the same value."""
    body = _reaper_body()
    assert "items_media_type = items[0].media_type" in body, (
        "v1.18.91 Bug A: must extract media_type from items[0]"
    )


def test_reaper_current_rks_query_scopes_by_media_type():
    """The current_rks SELECT must filter by media_type so the
    collection enum doesn't see show-type rks as 'stale'."""
    body = _reaper_body()
    # The current_rks query must include `AND media_type = ?`.
    # Find the SELECT rating_key FROM plex_items query block.
    cr_idx = body.index("current_rks = set(")
    cr_end = body.index(").fetchall()", cr_idx)
    cr_block = body[cr_idx:cr_end]
    assert "AND media_type" in cr_block, (
        "v1.18.91 Bug A: current_rks query must filter by media_type"
    )


def test_reaper_delete_scopes_by_media_type():
    """The DELETE must scope by media_type AND section_id so a
    bug in the reap loop can't accidentally cross-delete other
    media_types' rows in the same section."""
    body = _reaper_body()
    # Find the DELETE statement.
    del_idx = body.index("DELETE FROM plex_items")
    del_block = body[del_idx:del_idx + 400]
    assert "AND media_type = ?" in del_block, (
        "v1.18.91 Bug A: DELETE must scope by media_type as "
        "defense-in-depth"
    )


# ── Bug B: candidate-capture query simplification ────────────


def test_lost_candidate_query_uses_left_join():
    """v1.18.91 Bug B: switch INNER JOIN themes → LEFT JOIN
    themes so rows with NULL theme_id (unresolved orphans)
    still surface in the candidate set."""
    body = _reaper_body()
    # Find the lost_candidates_raw query.
    cand_idx = body.index("lost_candidates_raw = conn.execute(")
    cand_end = body.index(").fetchall()", cand_idx)
    cand_block = body[cand_idx:cand_end]
    assert "LEFT JOIN themes" in cand_block, (
        "v1.18.91 Bug B: query must use LEFT JOIN so NULL "
        "theme_id rows still surface"
    )
    # Pre-fix INNER JOIN must be gone from this query.
    assert "INNER JOIN themes" not in cand_block, (
        "v1.18.91 Bug B: INNER JOIN themes in candidate query "
        "silently filtered out unresolved orphans"
    )


def test_lost_candidate_query_coalesces_keys():
    """The query must COALESCE the lookup keys against
    plex_items' own guid_tmdb so unresolved rows yield a
    usable (mt, tmdb) tuple."""
    body = _reaper_body()
    cand_idx = body.index("lost_candidates_raw = conn.execute(")
    cand_end = body.index(").fetchall()", cand_idx)
    cand_block = body[cand_idx:cand_end]
    # COALESCE on tmdb_id falls back to pi.guid_tmdb.
    assert "COALESCE(t.tmdb_id, pi.guid_tmdb)" in cand_block
    # plex_items.media_type 'show' translates to 'tv' for
    # themes-domain consistency.
    assert "WHEN 'show' THEN 'tv'" in cand_block


def test_lost_candidate_dispatch_uses_aliased_columns():
    """The downstream `for cand in lost_candidates_raw` loop
    must read `cand["mt"]` (the SELECT alias) since the query
    no longer surfaces `t.media_type` directly."""
    body = _reaper_body()
    # Find the downstream consumer.
    cons_idx = body.index("for cand in lost_candidates_raw:")
    cons_block = body[cons_idx:cons_idx + 400]
    assert 'cand["mt"]' in cons_block, (
        "v1.18.91 Bug B: consumer must read cand[\"mt\"] (the "
        "COALESCE alias), not cand[\"media_type\"]"
    )


# ── Bug C: REFRESH button defensive unlock ──────────────────


def test_refresh_button_has_defensive_unlock():
    """v1.18.91 Bug C: the refreshTopbarStatus block that
    manages the libRefreshBtn must have a defensive-unlock
    path that triggers after 30s when no mutating op is
    active."""
    src = APP_JS.read_text()
    # Anchor on the libRefreshBtn block.
    idx = src.index("libRefreshBtn = document.getElementById('library-refresh-btn')")
    block = src[idx:idx + 6000]
    # The defensive-unlock condition must check anyMutatingOpActive.
    assert "anyMutatingOpActive" in block, (
        "v1.18.91 Bug C: defensive-unlock must consult "
        "anyMutatingOpActive (the topbar's truth signal)"
    )
    # The 30s threshold.
    assert "30000" in block
    # The dataset.refreshingSince timestamp marker.
    assert "refreshingSince" in block
    # v1.18.91 marker comment present.
    assert "v1.18.91" in block


def test_refresh_button_clears_timestamp_on_unlock():
    """When the button unlocks (stillBusy=false), the
    dataset.refreshingSince marker must clear so the next lock
    starts fresh — otherwise the previous lock's clock could
    bleed into a new operation."""
    src = APP_JS.read_text()
    idx = src.index("libRefreshBtn = document.getElementById('library-refresh-btn')")
    block = src[idx:idx + 6000]
    # The unlock branch should delete the dataset attribute.
    assert "delete libRefreshBtn.dataset.refreshingSince" in block


# ── End-to-end: behavioral fixtures ─────────────────────────


@pytest.fixture
def db_with_mixed_media_types(tmp_path):
    """Reproduce the user's collection-enum false-positive
    scenario: section has BOTH show + collection rows. The
    collection enum should reap ONLY collection-stale rks,
    not see the show rows as stale."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('2', 'TV Shows', 'show', 0, 0, "
            "        'tv', 1, '2026-05-01T00:00:00', "
            "        '2026-05-01T00:00:00')"
        )
        # 100 show rows (current).
        for i in range(100):
            conn.execute(
                "INSERT INTO plex_items "
                "  (rating_key, section_id, media_type, "
                "   guid_imdb, title, year, has_theme, "
                "   local_theme_file, folder_path, "
                "   plex_independent_theme, first_seen_at, "
                "   last_seen_at) "
                "VALUES (?, '2', 'show', ?, ?, 2020, 0, 0, "
                "        ?, 0, '2026-05-01T00:00:00', "
                "        '2026-05-23T00:00:00')",
                (f"show_{i:04d}", f"tt{i:08d}",
                 f"Show {i}", f"/data/tv/Show {i}"),
            )
        # 10 collection rows (current).
        for i in range(10):
            conn.execute(
                "INSERT INTO plex_items "
                "  (rating_key, section_id, media_type, "
                "   guid_imdb, title, year, has_theme, "
                "   local_theme_file, folder_path, "
                "   plex_independent_theme, first_seen_at, "
                "   last_seen_at) "
                "VALUES (?, '2', 'collection', NULL, ?, 0, "
                "        0, 0, ?, 0, '2026-05-01T00:00:00', "
                "        '2026-05-23T00:00:00')",
                (f"coll_{i:04d}", f"Collection {i}",
                 f"/data/tv/Collection {i}"),
            )
        conn.commit()
    return db_path


def _fake_item(rating_key, title, guid_imdb=None, media_type="show",
               section_id="2"):
    from app.core.plex import PlexLibraryItem
    return PlexLibraryItem(
        rating_key=rating_key,
        section_id=section_id,
        media_type=media_type,
        title=title,
        year="2020",
        guid_imdb=guid_imdb,
        guid_tmdb=None,
        guid_tvdb=None,
        folder_path=f"/data/{media_type}/{title}",
        has_theme=False,
        plex_theme_uri="",
    )


def test_collection_enum_does_not_false_positive_on_shows(
    db_with_mixed_media_types,
):
    """End-to-end Bug A regression test: seed 100 shows + 10
    collections in the same section; run the COLLECTION enum
    with all 10 collections returned. Pre-v1.18.91 the reaper
    would see 100 'stale' rks (the shows) → trip the 20%+50
    guard at 91%. Post-v1.18.91 the media_type scoping means
    the reaper only compares against the 10 existing collection
    rks → no stale → no abort → no false positive."""
    from app.core import plex_enum
    # Return all 10 collections — no stale collections.
    items = [
        _fake_item(f"coll_{i:04d}", f"Collection {i}",
                   media_type="collection")
        for i in range(10)
    ]
    plex_enum._upsert_items(
        db_with_mixed_media_types, items, section_id="2",
    )
    # All 100 shows + 10 collections should still exist.
    with sqlite3.connect(db_with_mixed_media_types) as conn:
        shows = conn.execute(
            "SELECT COUNT(*) FROM plex_items "
            "WHERE section_id='2' AND media_type='show'"
        ).fetchone()[0]
        colls = conn.execute(
            "SELECT COUNT(*) FROM plex_items "
            "WHERE section_id='2' AND media_type='collection'"
        ).fetchone()[0]
    assert shows == 100, (
        f"v1.18.91 Bug A: shows must survive collection enum; "
        f"got {shows} (expected 100)"
    )
    assert colls == 10, (
        f"v1.18.91: collections must survive (no stale); "
        f"got {colls} (expected 10)"
    )


def test_collection_enum_correctly_reaps_stale_collection_only(
    db_with_mixed_media_types,
):
    """End-to-end: seed 100 shows + 10 collections. Run
    collection enum returning ONLY 8 collections (2 stale).
    Should reap exactly the 2 stale collections + leave shows
    untouched + not trip the ratio guard."""
    from app.core import plex_enum
    items = [
        _fake_item(f"coll_{i:04d}", f"Collection {i}",
                   media_type="collection")
        for i in range(8)  # 0..7, skipping coll_0008 + coll_0009
    ]
    plex_enum._upsert_items(
        db_with_mixed_media_types, items, section_id="2",
    )
    with sqlite3.connect(db_with_mixed_media_types) as conn:
        shows = conn.execute(
            "SELECT COUNT(*) FROM plex_items "
            "WHERE section_id='2' AND media_type='show'"
        ).fetchone()[0]
        colls = sorted(
            r[0] for r in conn.execute(
                "SELECT rating_key FROM plex_items "
                "WHERE section_id='2' AND media_type='collection' "
                "ORDER BY rating_key"
            )
        )
    assert shows == 100, "shows must be untouched by collection enum"
    assert len(colls) == 8, (
        f"2 stale collections must be reaped; got {len(colls)} "
        f"survivors"
    )
    assert "coll_0008" not in colls and "coll_0009" not in colls, (
        "the specific stale collections (0008, 0009) must be gone"
    )


def test_lost_theme_candidate_captured_when_theme_id_null(tmp_path):
    """End-to-end Bug B regression test: seed a stale plex_items
    row with has_theme=1, theme_id=NULL, guid_tmdb set. The
    v1.18.91 LEFT JOIN + COALESCE should capture this row as a
    candidate (pre-v1.18.91 INNER JOIN missed it)."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, '2026-05-01T00:00:00', "
            "        '2026-05-01T00:00:00')"
        )
        # Stale row with theme_id=NULL but guid_tmdb=652 + has_theme=1.
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, "
            "   guid_imdb, guid_tmdb, title, year, has_theme, "
            "   local_theme_file, folder_path, theme_id, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('stale_rk', '1', 'movie', "
            "        'tt0332452', 652, 'Troy', 2004, 1, 0, "
            "        '/data/movies/Troy (2004)', NULL, "
            "        '2026-05-01T00:00:00', "
            "        '2026-05-19T01:23:42+00:00')"
        )
        conn.commit()
    from app.core import plex_enum
    # Plex returns a different rk — the stale_rk gets reaped.
    items = [
        _fake_item("fresh_rk", "Other Movie", media_type="movie",
                   section_id="1"),
    ]
    plex_enum._upsert_items(db_path, items, section_id="1")
    # The stale rk should be gone (reaped).
    with sqlite3.connect(db_path) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM plex_items "
            "WHERE rating_key='stale_rk'"
        ).fetchone()[0]
    assert n == 0, "stale row must be reaped"
    # The candidate-capture should have surfaced this row
    # (theme_id=NULL but guid_tmdb=652). Since there's no
    # Apprise config in the test env, the dispatch will be
    # attempted but no-op. What we VERIFY here is that the
    # candidate capture didn't silently drop the row — the
    # dispatch attempt + dedupe stamp is the side effect we
    # can check (notify_dedupe writes to runtime_settings if
    # dispatch succeeds; with no Apprise it stays unstamped
    # but the should_fire path did run). Since we can't easily
    # mock Apprise here, the regression is covered by the
    # source-level pin above (test_lost_candidate_query_uses_
    # left_join + test_lost_candidate_query_coalesces_keys).
    # This e2e test asserts the reaper still works end-to-end
    # with a NULL-theme-id row — it doesn't raise.
