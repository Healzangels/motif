"""v1.19.10 — plex_enum verification re-check uses julianday.

Deferred follow-up from v1.19.5's journal entry. Same bug
class as the v1.19.5 scheduler fix:

  String comparison of ISO-T timestamps (`2026-05-24T...+00:00`,
  written by now_iso()) against SQLite's space-separated
  `datetime('now', '-N days')` boundary is lexicographic.
  T (0x54) > space (0x20). Any ISO-T timestamp on the SAME
  date as the boundary always compares "greater" regardless
  of actual time. The verification re-check cadence at
  plex_enum.py:1636/1639 silently drifted by time-of-day:
  some rows past their TTL stayed unverified, some within
  TTL got re-checked prematurely.

## Fix

Replace `<datetime: column> < datetime('now', '-N days')`
with `<julianday: column> < julianday('now', '-N days')`
for both branches (verified_ok=0 → -7 days, verified_ok=1
→ -30 days). julianday() converts both sides to numeric
Julian-day fraction; comparison is then format-agnostic +
correct.

## What's pinned

- Both `< datetime('now', '-X days')` sites in plex_enum.py
  switched to julianday()
- 7-day and 30-day TTL boundaries preserved
- Marker references v1.19.5 lineage + now_iso() format
- End-to-end: insert plex_items with verified_at older
  than TTL; verify the sweep includes them via the SQL.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
PLEX_ENUM_PY = REPO / "app" / "core" / "plex_enum.py"


# ── Source pins ──────────────────────────────────────────────


def test_verification_sweep_uses_julianday():
    """Both `< datetime('now', '-N days')` comparisons in the
    verification sweep SQL must use julianday() instead.
    Latent v1.19.5-class bug fixed in v1.19.10."""
    src = PLEX_ENUM_PY.read_text()
    # Locate the sweep SQL — anchored on the unique
    # `plex_theme_verified_ok IS NULL` literal that's only in
    # this one query.
    idx = src.index("plex_theme_verified_ok IS NULL")
    block = src[idx:idx + 1500]
    # No remaining raw datetime() comparisons in this sweep.
    raw_compares = re.findall(
        r"plex_theme_verified_at < datetime\(",
        block,
    )
    assert not raw_compares, (
        f"v1.19.10: sweep SQL must use julianday() on both "
        f"sides; found {len(raw_compares)} raw "
        f"`< datetime(...)` comparisons. "
        f"Block: {block[:600]}"
    )
    # Must use julianday() on the LHS (plex_theme_verified_at).
    assert "julianday(plex_theme_verified_at)" in block, (
        "v1.19.10: LHS must be julianday(plex_theme_verified_at)"
    )
    # And the RHS boundary must also be julianday().
    assert "julianday('now', '-7 days')" in block
    assert "julianday('now', '-30 days')" in block


def test_marker_references_v1_19_5_lineage():
    """The v1.19.10 fix is a direct follow-up of v1.19.5's
    same-class fix in scheduler.py. Marker should reference
    that lineage so the bug pattern is searchable."""
    src = PLEX_ENUM_PY.read_text()
    idx = src.index("plex_theme_verified_ok IS NULL")
    # Look in the comment block above the SQL (find the nearest
    # preceding `# v1.19.10:` marker).
    pre = src[max(0, idx - 2000):idx]
    assert "v1.19.10" in pre
    assert "v1.19.5" in pre, (
        "v1.19.10: marker should reference v1.19.5 lineage "
        "(same lex-vs-numeric-comparison bug class)"
    )
    # Reference now_iso() (the write-side format that
    # triggered the lex mismatch).
    assert "now_iso" in pre


# ── Behavioral pin: SQL exercises correctly ──────────────────


def test_julianday_sweep_includes_old_verified_at(tmp_path):
    """Execute the SQL form directly against a seeded DB.
    Rows with verified_at older than TTL should appear in
    the result. Pre-v1.19.10 the lex comparison would have
    excluded ISO-T timestamps on the same date as the
    boundary."""
    from app.core.db import init_db
    db = tmp_path / "motif.db"
    init_db(db)

    with sqlite3.connect(db) as conn:
        # Section so the plex_items insert satisfies foreign
        # keys / has a valid section_id reference downstream
        # (the sweep itself doesn't join sections but the
        # schema may require it).
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, "
            "   discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, "
            "        '2026-05-24T00:00:00', "
            "        '2026-05-24T00:00:00')"
        )
        # An old verified_ok=1 row (>30 days ago — should be
        # re-verified). ISO-T format, same as now_iso().
        # Use a date well in the past so the test isn't
        # time-of-day sensitive.
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, "
            "   has_theme, local_theme_file, "
            "   plex_theme_verified_ok, "
            "   plex_theme_verified_at, "
            "   title, year, folder_path, "
            "   plex_independent_theme, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('rk-old-ok', '1', 'movie', "
            "        1, 0, 1, "
            "        '2020-01-01T00:00:00+00:00', "
            "        'Old OK', 2020, '/data/x', 0, "
            "        '2020-01-01T00:00:00+00:00', "
            "        '2020-01-01T00:00:00+00:00')"
        )
        # An old verified_ok=0 row (>7 days ago — should also
        # be re-verified).
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, "
            "   has_theme, local_theme_file, "
            "   plex_theme_verified_ok, "
            "   plex_theme_verified_at, "
            "   title, year, folder_path, "
            "   plex_independent_theme, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('rk-old-fail', '1', 'movie', "
            "        1, 0, 0, "
            "        '2020-01-01T00:00:00+00:00', "
            "        'Old Fail', 2020, '/data/y', 0, "
            "        '2020-01-01T00:00:00+00:00', "
            "        '2020-01-01T00:00:00+00:00')"
        )
        # Fresh verified_ok=1 row (within 30d TTL — should NOT
        # be in the sweep result).
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, "
            "   has_theme, local_theme_file, "
            "   plex_theme_verified_ok, "
            "   plex_theme_verified_at, "
            "   title, year, folder_path, "
            "   plex_independent_theme, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('rk-fresh-ok', '1', 'movie', "
            "        1, 0, 1, "
            "        datetime('now'), "
            "        'Fresh OK', 2020, '/data/z', 0, "
            "        datetime('now'), datetime('now'))"
        )
        conn.commit()

    # Execute the v1.19.10 form of the sweep SQL directly.
    # This mirrors the production query's predicate.
    with sqlite3.connect(db) as conn:
        result = conn.execute("""
            SELECT rating_key FROM plex_items
            WHERE has_theme = 1
              AND local_theme_file = 0
              AND (
                   plex_theme_verified_ok IS NULL
                OR (plex_theme_verified_ok = 0
                    AND (plex_theme_verified_at IS NULL
                         OR julianday(plex_theme_verified_at)
                            < julianday('now', '-7 days')))
                OR (plex_theme_verified_ok = 1
                    AND (plex_theme_verified_at IS NULL
                         OR julianday(plex_theme_verified_at)
                            < julianday('now', '-30 days')))
              )
            ORDER BY rating_key
        """).fetchall()

    rks = {r[0] for r in result}
    # Old rows must be included (their verified_at is years ago).
    assert "rk-old-ok" in rks, (
        f"v1.19.10: old verified_ok=1 row (Jan 2020) past 30d "
        f"TTL must be in sweep result; got {rks}"
    )
    assert "rk-old-fail" in rks, (
        f"v1.19.10: old verified_ok=0 row (Jan 2020) past 7d "
        f"TTL must be in sweep result; got {rks}"
    )
    # Fresh row must NOT be included.
    assert "rk-fresh-ok" not in rks, (
        f"v1.19.10: fresh verified_ok=1 row (today) within 30d "
        f"TTL must NOT be in sweep result; got {rks}"
    )


def test_no_remaining_lex_compare_sites_in_plex_enum(tmp_path):
    """Audit guard: no `< datetime('now', '-N <units>')`
    pattern remains in plex_enum.py at all. If a future patch
    adds another time-comparison site against an ISO-T-
    written column, this catches it and prompts using
    julianday() instead."""
    src = PLEX_ENUM_PY.read_text()
    # The pattern this test guards against: any `< datetime(`
    # form. v1.19.10 replaced all such sites with julianday().
    bad = re.findall(
        r"< datetime\('now'",
        src,
    )
    assert not bad, (
        f"v1.19.10: no remaining `< datetime('now', ...)` "
        f"comparisons should exist in plex_enum.py "
        f"(use julianday() instead — see v1.19.5 + v1.19.10 "
        f"fixes for the rationale). Found: {bad}"
    )
