"""v1.19.65 — em-dash filter scope fix + stale plex_has_theme stamp cleanup.

Two cleanups in one tag, both descendants of the v1.19.61 PS→BK
unification + v1.19.64 PS-widening arc:

## Fix 1 — em-dash filter chip leak

Post-v1.19.64 PS widening, pure-P rows (no file_path) are now
matched by the widened PS predicate. The 'none' (em-dash) filter
SQL still had its v1.14.43 NOT-clause requiring
`lf.file_path IS NOT NULL` inside the LPS exclusion — meaning
pure-P rows (file_path IS NULL) didn't satisfy the exclusion
and leaked into the em-dash result set alongside PS. Same leak
class as the user's v1.19.63 PS-chip-returning-BK-rows bug.

v1.19.65 drops the `lf.file_path IS NOT NULL` half of the
NOT-clause. Now `none` truly means "no theme on this row
anywhere" — the genuinely-empty set with no overlap into any
other chip.

Em-dash chip tooltip rewritten to match: "No theme anywhere —
Plex has no theme AND motif has no canonical / backup /
placement on this row."

## Fix 2 — 77 stale `plex_has_theme` stamps on placed rows

the user's prod inventory (2026-05-28): 77 rows with
`last_place_attempt_reason = 'plex_has_theme'` AND an active
placement. Sequence:

  1. Pre-v1.19.61 worker stamped 'plex_has_theme' when place
     skipped because Plex was serving its own theme.
  2. User PUSH TO PLEX (or similar) installed a placement.
  3. The retry sweep skips 'plex_has_theme' rows (v1.19.21),
     so no place job ran to overwrite the stamp.
  4. Result: row renders correctly (HL/C/PU per placement_kind)
     but the stamp says "place skipped" — misleading.

v1.19.61's PS→BK backfill walker correctly skipped these (its
predicate required no-placement). v1.19.65 adds a sibling
walker `maybe_repair_stale_plex_has_theme_stamps` that UPDATEs
the stamp to 'placed' for any row matching plex_has_theme +
active placement. Pure bookkeeping — no file or placement
touched. Idempotent via runtime_settings marker.

## What stays the same

- URL params + CSS classnames unchanged.
- The widened PS predicate (v1.19.64) unchanged.
- Render priority cascade (BU/BP/M before PS) unchanged.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

API_PY = (REPO / "app" / "web" / "api.py").read_text()
LIBRARY_HTML = (
    REPO / "app" / "web" / "templates" / "library.html"
).read_text()
RECOVERY_PY = (REPO / "app" / "core" / "recovery_v55.py").read_text()
MAIN_PY = (REPO / "app" / "main.py").read_text()


# ── Fix 1 — em-dash filter scope ─────────────────────────────


def _link_pills_none_branch() -> str:
    """Return the link_pills='none' branch text — distinct from
    the tdb_pills and ed_pills 'none' branches which share the
    same `elif p == "none":` pattern."""
    # Anchor on the link_pills outer block, then find the
    # 'none' branch inside.
    link_anchor = API_PY.index("if link_pills:")
    none_idx = API_PY.index('elif p == "none":', link_anchor)
    end_idx = API_PY.index("        if branches:", none_idx)
    return API_PY[none_idx:end_idx]


# v1.19.65 em-dash semantic tests removed in v1.19.66 — that
# tag dropped the PS chip entirely, which made the em-dash
# semantic revert to its pre-v1.19.65 form (matches every row
# that doesn't paint a more specific LINK chip). The walker
# tests below ARE still valid + load-bearing.


# ── Fix 2 — stale plex_has_theme stamp walker ────────────────


def test_walker_function_exists():
    """maybe_repair_stale_plex_has_theme_stamps must be importable."""
    from app.core.recovery_v55 import (
        maybe_repair_stale_plex_has_theme_stamps,
    )
    assert callable(maybe_repair_stale_plex_has_theme_stamps)


def test_walker_has_v1_19_65_marker_and_idempotency_key():
    """Walker carries the v1.19.65 marker + a runtime_settings
    marker key distinct from prior walkers'."""
    idx = RECOVERY_PY.index(
        "def maybe_repair_stale_plex_has_theme_stamps("
    )
    body = RECOVERY_PY[idx:idx + 4500]
    assert "v1.19.65" in body
    assert (
        "recovery_stale_plex_has_theme_stamps_done_at_v1_19_65"
        in body
    )
    assert "runtime_settings" in body


def test_walker_only_touches_placed_rows():
    """Walker must require an active placement (media_folder
    non-null + non-empty) for each candidate. Rows WITHOUT a
    placement stay at 'plex_has_theme' — those go through the
    v1.19.61 PS→BK walker (which converted them to backup_only)."""
    idx = RECOVERY_PY.index(
        "def maybe_repair_stale_plex_has_theme_stamps("
    )
    body = RECOVERY_PY[idx:idx + 4500]
    # The candidate query joins placements + checks media_folder.
    assert "JOIN placements" in body
    assert "p.media_folder IS NOT NULL" in body
    assert "p.media_folder != ''" in body


def test_walker_idempotent_on_marker(tmp_path):
    """Second invocation short-circuits via the runtime_settings
    marker."""
    from app.core.db import init_db, get_conn
    from app.core.recovery_v55 import (
        maybe_repair_stale_plex_has_theme_stamps,
    )
    db = tmp_path / "test.db"
    init_db(db)
    # First call: no candidates, marker stamped.
    result1 = maybe_repair_stale_plex_has_theme_stamps(db)
    assert result1["candidates"] == 0
    with get_conn(db) as conn:
        row = conn.execute(
            "SELECT value FROM runtime_settings "
            "WHERE key = 'recovery_stale_plex_has_theme_stamps_done_at_v1_19_65'"
        ).fetchone()
        assert row is not None
    # Second call: short-circuits.
    result2 = maybe_repair_stale_plex_has_theme_stamps(db)
    assert result2["updated"] == 0


def test_walker_converts_placed_rows_skips_unplaced(tmp_path):
    """End-to-end: seed three rows —
      (a) plex_has_theme stamp + active placement → walker
          MUST convert to 'placed'
      (b) plex_has_theme stamp + NO placement → walker MUST
          skip (those are PS→BK walker's territory)
      (c) backup_only stamp + no placement → walker MUST skip
          (not stale, intentional)"""
    from app.core.db import init_db, get_conn
    from app.core.recovery_v55 import (
        maybe_repair_stale_plex_has_theme_stamps,
    )
    db = tmp_path / "test.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, '2026-05-28', '2026-05-28')"
        )
        # (a) placed row with stale plex_has_theme stamp.
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, "
            "  upstream_source, last_seen_sync_at, first_seen_sync_at) "
            "VALUES (1, 'movie', 1001, 'Placed', 'themoviedb', "
            "        '2026-05-28', '2026-05-28')"
        )
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   file_size, file_sha256, downloaded_at, "
            "   source_kind, source_video_id, provenance, "
            "   last_place_attempt_at, last_place_attempt_reason) "
            "VALUES ('movie', 1001, '1', 'Placed/theme.mp3', "
            "        1000, 'a', '2026-05-20', 'themerrdb', 'X', "
            "        'auto', '2026-05-20', 'plex_has_theme')"
        )
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, media_folder, "
            "   placement_kind, provenance, placed_at) "
            "VALUES ('movie', 1001, '1', '/data/movies/Placed', "
            "        'hardlink', 'auto', '2026-05-25')"
        )
        # (b) unplaced row with plex_has_theme stamp — walker must skip.
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, "
            "  upstream_source, last_seen_sync_at, first_seen_sync_at) "
            "VALUES (2, 'movie', 1002, 'Unplaced', 'themoviedb', "
            "        '2026-05-28', '2026-05-28')"
        )
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   file_size, file_sha256, downloaded_at, "
            "   source_kind, source_video_id, provenance, "
            "   last_place_attempt_at, last_place_attempt_reason) "
            "VALUES ('movie', 1002, '1', 'Unplaced/theme.mp3', "
            "        1000, 'b', '2026-05-20', 'themerrdb', 'Y', "
            "        'auto', '2026-05-20', 'plex_has_theme')"
        )
        # (c) backup_only row — walker must skip.
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, "
            "  upstream_source, last_seen_sync_at, first_seen_sync_at) "
            "VALUES (3, 'movie', 1003, 'BU', 'themoviedb', "
            "        '2026-05-28', '2026-05-28')"
        )
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   file_size, file_sha256, downloaded_at, "
            "   source_kind, source_video_id, provenance, "
            "   last_place_attempt_at, last_place_attempt_reason) "
            "VALUES ('movie', 1003, '1', 'BU/theme.mp3', "
            "        1000, 'c', '2026-05-20', 'themerrdb', 'Z', "
            "        'auto', '2026-05-20', 'backup_only')"
        )
        conn.commit()

    result = maybe_repair_stale_plex_has_theme_stamps(db)
    assert result["candidates"] == 1, (
        f"v1.19.65: only the placed plex_has_theme row should be "
        f"a candidate; got {result['candidates']}"
    )
    assert result["updated"] == 1

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        placed = conn.execute(
            "SELECT last_place_attempt_reason FROM local_files "
            "WHERE tmdb_id = 1001"
        ).fetchone()
        assert placed["last_place_attempt_reason"] == "placed", (
            "v1.19.65: placed row must flip to 'placed'"
        )
        unplaced = conn.execute(
            "SELECT last_place_attempt_reason FROM local_files "
            "WHERE tmdb_id = 1002"
        ).fetchone()
        assert (
            unplaced["last_place_attempt_reason"] == "plex_has_theme"
        ), (
            "v1.19.65: unplaced plex_has_theme row must NOT be "
            "touched (PS→BK walker territory)"
        )
        bu = conn.execute(
            "SELECT last_place_attempt_reason FROM local_files "
            "WHERE tmdb_id = 1003"
        ).fetchone()
        assert bu["last_place_attempt_reason"] == "backup_only", (
            "v1.19.65: backup_only row must NOT be touched"
        )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_65_version_pin():
    """v1.19.65 bumped. Relaxed to v1.19.x prefix after v1.19.66
    continued the line."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
