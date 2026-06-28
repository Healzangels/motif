"""v1.18.89 — plex_enum reaps stale plex_items rows.

the user's repro: removed Troy from Plex → re-added it → SRC=P
persisted because the OLD rating_key's plex_items row stuck
around with has_theme=1. Investigation found 25 stale rows
across 3 sections (8 in anime alone with has_theme=1 — latent
phantom-P risks).

## Root cause

Pre-v1.18.89 `plex_enum_section` was add-and-update only. When
Plex stopped returning a rating_key (item removed from a section,
or removed from Plex entirely), the corresponding plex_items
row stayed in motif's DB forever — keeping its old has_theme +
plex_independent_theme + plex_theme_verified_ok state. The
library row query then rendered phantom state from the stale
row.

## Fix

After the upsert pass in `_upsert_items`, compute the set of
rks Plex returned this enum vs the set of existing plex_items
for the same section. The set difference is the stale rks →
DELETE.

## Safety guards (class-9 amplifier-sweep, v1.18.10 lesson)

1. **Skip on empty Plex response.** If `seen_rks` is empty
   (Plex returned 0 items), don't mass-delete based on what
   could be a transient API hiccup.
2. **Skip on suspicious deletion ratio.** If stale_count > 50
   AND stale_count > 20% of the section's current rows, abort
   the reap. Plex may have returned a smaller catalog than
   expected (auth glitch, partial response, plugin bug); refuse
   to amplify the damage with mass-deletion.

Below the threshold, sample the first 5 deleted rows in the
log warning so operators have a breadcrumb if something
unexpected disappears.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

PLEX_ENUM_PY = REPO / "app" / "core" / "plex_enum.py"


# ── Source-level pins ────────────────────────────────────────


def test_reaper_block_exists_in_upsert_items():
    """The reap block lives in `_upsert_items` after the upsert
    pass + before `resolve_theme_ids` (so resolve sees the
    cleaned-up set)."""
    src = PLEX_ENUM_PY.read_text()
    fn_idx = src.index("def _upsert_items(")
    fn_end = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:fn_end]
    # Must contain the v1.18.89 marker.
    assert "v1.18.89 reaper" in body, (
        "v1.18.89: reaper block must carry the version marker"
    )
    # Must compute seen_rks from items.
    assert "seen_rks = set(" in body
    # Must compute current_rks from plex_items.
    assert "current_rks = set(" in body
    # Set difference for stale.
    assert "stale_rks = current_rks - seen_rks" in body
    # DELETE must scope by section_id (defense in depth — even if
    # somehow rks crossed sections, the WHERE clause prevents
    # accidental cross-section damage).
    assert (
        "DELETE FROM plex_items" in body
        and "section_id = ?" in body
    )


def test_reaper_skips_on_empty_seen_rks():
    """Safety guard: if Plex returned 0 items for the section,
    refuse to mass-delete. This blocks the amplifier-sweep
    pattern where a transient API hiccup wipes the section's
    tracked rows."""
    src = PLEX_ENUM_PY.read_text()
    fn_idx = src.index("def _upsert_items(")
    fn_end = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:fn_end]
    # The reaper block gates on `if seen_rks:` (truthy check
    # on the set — empty set falls through, skipping the reap).
    assert "if seen_rks:" in body, (
        "v1.18.89: reaper must skip when seen_rks is empty"
    )


def test_reaper_skips_when_deletion_ratio_exceeds_threshold():
    """Safety guard: if would-delete > 50 AND > 20% of section,
    abort the reap. Same shape as v1.18.10's amplifier-sweep
    lesson — defensive DELETE patterns must fail-safe under
    suspected broken state."""
    src = PLEX_ENUM_PY.read_text()
    fn_idx = src.index("def _upsert_items(")
    fn_end = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:fn_end]
    # Both threshold components present.
    assert "> 50" in body or ">= 50" in body
    assert "> 20" in body or ">= 20" in body
    # Log warning on abort path so operator sees the trip.
    assert "ABORTING" in body or "abort" in body.lower()


def test_reaper_logs_deletion_sample():
    """When the reap fires, log a sample of the first ~5 deleted
    rows so operators have a breadcrumb if something unexpected
    disappears (per the v1.18.5 cold-path-needs-MORE-logging
    sub-pattern)."""
    src = PLEX_ENUM_PY.read_text()
    fn_idx = src.index("def _upsert_items(")
    fn_end = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:fn_end]
    # Sample query against plex_items by rating_key.
    assert "SELECT rating_key, title FROM plex_items" in body
    # Logged at warning level (operator-visible).
    assert "log.warning" in body
    # Mentions "deleted" so the message is findable.
    assert "deleted" in body.lower()


# ── End-to-end: behavioral with seeded DB ─────────────────────


@pytest.fixture
def db_with_stale_plex_items(tmp_path):
    """Reproduce the user's Troy state: section has both a fresh
    rk + a stale rk for the same theme. Plex's current enum
    would return only the fresh one; the reaper should delete
    the stale."""
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
        # Old stale rk — Plex no longer returns this.
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, "
            "   guid_imdb, title, year, has_theme, "
            "   local_theme_file, folder_path, "
            "   plex_independent_theme, first_seen_at, "
            "   last_seen_at) "
            "VALUES ('473665', '1', 'movie', "
            "        'tt0332452', 'Troy', 2004, 1, 0, "
            "        '/data/movies/Troy (2004)', 1, "
            "        '2026-05-01T00:00:00', "
            "        '2026-05-19T01:23:42+00:00')"
        )
        # Fresh rk — Plex returns this in the current enum.
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, "
            "   guid_imdb, title, year, has_theme, "
            "   local_theme_file, folder_path, "
            "   plex_independent_theme, first_seen_at, "
            "   last_seen_at) "
            "VALUES ('709300', '1', 'movie', "
            "        'tt0332452', 'Troy', 2004, 0, 0, "
            "        '/data/movies/Troy (2004)', 0, "
            "        '2026-05-23T00:00:00', "
            "        '2026-05-23T18:11:29+00:00')"
        )
        # An unrelated fresh row in the same section (no
        # reaping should affect this).
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, "
            "   guid_imdb, title, year, has_theme, "
            "   local_theme_file, folder_path, "
            "   plex_independent_theme, first_seen_at, "
            "   last_seen_at) "
            "VALUES ('500000', '1', 'movie', "
            "        'tt9999999', 'Other Movie', 2020, 0, 0, "
            "        '/data/movies/Other (2020)', 0, "
            "        '2026-05-23T00:00:00', "
            "        '2026-05-23T18:11:29+00:00')"
        )
        conn.commit()
    return db_path


def _fake_item(rating_key, title, guid_imdb, section_id="1"):
    """Construct a minimal PlexLibraryItem-shaped object for
    _upsert_items input. Mirrors the actual dataclass at
    app/core/plex.py:98."""
    from app.core.plex import PlexLibraryItem
    return PlexLibraryItem(
        rating_key=rating_key,
        section_id=section_id,
        media_type="movie",
        title=title,
        year="2004",
        guid_imdb=guid_imdb,
        guid_tmdb=None,
        guid_tvdb=None,
        folder_path=f"/data/movies/{title}",
        has_theme=False,
        plex_theme_uri="",
    )


def test_reaper_deletes_stale_rk(db_with_stale_plex_items):
    """End-to-end: seed DB with stale + fresh rks for Troy, run
    _upsert_items with only the fresh rk in the items list. The
    stale rk must be deleted; the fresh + unrelated rows survive."""
    from app.core import plex_enum
    items = [
        _fake_item("709300", "Troy", "tt0332452"),
        _fake_item("500000", "Other Movie", "tt9999999"),
    ]
    plex_enum._upsert_items(
        db_with_stale_plex_items, items, section_id="1",
    )
    with sqlite3.connect(db_with_stale_plex_items) as conn:
        rks = sorted(
            r[0] for r in conn.execute(
                "SELECT rating_key FROM plex_items "
                "WHERE section_id='1'"
            )
        )
    assert rks == ["500000", "709300"], (
        "v1.18.89: stale rk 473665 must be reaped; fresh + "
        "unrelated rks must survive"
    )


def test_reaper_skips_on_empty_items(db_with_stale_plex_items):
    """Safety: if Plex returned 0 items for the section, the
    reaper must NOT mass-delete the existing rows. Common
    cause: transient Plex API hiccup, partial enumeration."""
    from app.core import plex_enum
    # Empty items list — simulates Plex returning nothing for
    # this section.
    plex_enum._upsert_items(
        db_with_stale_plex_items, [], section_id="1",
    )
    with sqlite3.connect(db_with_stale_plex_items) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM plex_items WHERE section_id='1'"
        ).fetchone()[0]
    assert n == 3, (
        "v1.18.89: empty-items must not trigger mass-delete "
        f"(expected 3 rows preserved, got {n})"
    )


def test_reaper_skips_when_ratio_exceeds_threshold(tmp_path):
    """Safety: if >50 rows AND >20% would be deleted, abort.
    Seed a section with 100 stale rows + 10 fresh rks Plex
    returns; the reaper would delete 90 (90% — way over 20%
    threshold) so it must abort instead."""
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
        # 100 existing rows.
        for i in range(100):
            conn.execute(
                "INSERT INTO plex_items "
                "  (rating_key, section_id, media_type, "
                "   guid_imdb, title, year, has_theme, "
                "   local_theme_file, folder_path, "
                "   plex_independent_theme, first_seen_at, "
                "   last_seen_at) "
                "VALUES (?, '1', 'movie', ?, ?, 2020, 0, 0, "
                "        ?, 0, '2026-05-01T00:00:00', "
                "        '2026-05-01T00:00:00')",
                (f"rk_{i:04d}", f"tt{i:08d}",
                 f"Movie {i}", f"/data/movies/Movie {i}"),
            )
        conn.commit()
    from app.core import plex_enum
    # Plex returns only 10 items — would-delete = 90 (90%, > 20%
    # threshold + > 50). Reaper must abort.
    items = [
        _fake_item(f"rk_{i:04d}", f"Movie {i}", f"tt{i:08d}")
        for i in range(10)
    ]
    plex_enum._upsert_items(db_path, items, section_id="1")
    with sqlite3.connect(db_path) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM plex_items WHERE section_id='1'"
        ).fetchone()[0]
    assert n == 100, (
        "v1.18.89: amplifier-sweep guard must abort when "
        "would-delete > 50 AND > 20% of section. Expected 100 "
        f"rows preserved; got {n} (some were deleted — guard failed)"
    )


def test_reaper_allows_small_section_with_high_ratio(tmp_path):
    """Edge case: small section (< 50 rows) with >20% ratio
    should still reap, since the ratio guard only trips when
    BOTH conditions hit. A small section is allowed to lose
    most of its rows (could be legit — a section being
    deprecated)."""
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
        # 10 existing rows.
        for i in range(10):
            conn.execute(
                "INSERT INTO plex_items "
                "  (rating_key, section_id, media_type, "
                "   guid_imdb, title, year, has_theme, "
                "   local_theme_file, folder_path, "
                "   plex_independent_theme, first_seen_at, "
                "   last_seen_at) "
                "VALUES (?, '1', 'movie', ?, ?, 2020, 0, 0, "
                "        ?, 0, '2026-05-01T00:00:00', "
                "        '2026-05-01T00:00:00')",
                (f"rk_{i:04d}", f"tt{i:08d}",
                 f"Movie {i}", f"/data/movies/Movie {i}"),
            )
        conn.commit()
    from app.core import plex_enum
    # Plex returns only 2 — would-delete = 8 (80%). > 20% but
    # NOT > 50, so the guard does NOT trip. Reaper proceeds.
    items = [
        _fake_item(f"rk_{i:04d}", f"Movie {i}", f"tt{i:08d}")
        for i in range(2)
    ]
    plex_enum._upsert_items(db_path, items, section_id="1")
    with sqlite3.connect(db_path) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM plex_items WHERE section_id='1'"
        ).fetchone()[0]
    assert n == 2, (
        "v1.18.89: small section (<50 rows) should not trip the "
        "ratio guard even at high ratio. Expected 2 rows; got "
        f"{n}"
    )
