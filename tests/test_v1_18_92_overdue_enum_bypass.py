"""v1.18.92 — bypass contentChangedAt-skip when section is overdue.

the user's clean-state check found 12 stale plex_items rows in
Section 1 (Movies) that the v1.18.89 reaper couldn't reach.
Root cause: the reaper only runs inside `_upsert_items`, but
the contentChangedAt-skip path bails BEFORE `_upsert_items`
is called. Quiet sections (where Plex hasn't bumped
contentChangedAt) accumulate stale rows indefinitely.

## Fix

Even when contentChangedAt matches, force a full enum if the
section's last full enum is older than 24h. The signal is
MAX(plex_items.last_seen_at) for the section — last_seen_at
is bumped on every upsert.

After v1.18.92 deploys + the next plex_enum on a quiet
section, the bypass triggers + full enum runs + reaper
cleans the backlog. Future plex_enum runs continue to skip
normally (contentChangedAt still matches + last_seen_at is
fresh) until 24h elapses again.

## What's pinned

- `_section_enum_overdue(db_path, section_id, hours=24)` helper
  exists and returns the right answer for fresh / stale /
  empty / bad-timestamp section states.
- The skip block in run_plex_enum consults the helper +
  bypasses the skip when overdue.
- Inline marker references the v1.18.89 reaper design gap so
  future archaeology finds the lineage.
"""
from __future__ import annotations

import re
import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

PLEX_ENUM_PY = REPO / "app" / "core" / "plex_enum.py"


# ── Helper source-level pins ─────────────────────────────────


def test_section_enum_overdue_helper_exists():
    """`_section_enum_overdue(db_path, section_id, *, hours=24)`
    must be defined at module scope (not inside run_plex_enum)."""
    src = PLEX_ENUM_PY.read_text()
    assert "def _section_enum_overdue(" in src


def test_helper_uses_max_last_seen_at_signal():
    """The signal must be MAX(plex_items.last_seen_at) for the
    section — last_seen_at is bumped on every upsert, so its
    max across the section is the cheapest 'when did we last
    enumerate' proxy without a new schema column."""
    src = PLEX_ENUM_PY.read_text()
    fn_idx = src.index("def _section_enum_overdue(")
    fn_end = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "MAX(last_seen_at)" in body
    assert "FROM plex_items" in body
    assert "WHERE section_id = ?" in body


def test_helper_failsafe_on_bad_timestamp():
    """Bad / unparseable timestamps must NOT trigger a surprise
    full enum (return False). Same fail-safe stance v1.18.89's
    reaper takes on amplifier-sweep — defensive sweeps fail
    closed."""
    src = PLEX_ENUM_PY.read_text()
    fn_idx = src.index("def _section_enum_overdue(")
    fn_end = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:fn_end]
    # Exception path returns False.
    assert "return False" in body
    assert "except" in body


# ── Skip-block source-level pins ─────────────────────────────


def _src_strflat() -> str:
    """Flatten adjacent string literals so messages split
    across lines for line-length still match substring checks."""
    src = PLEX_ENUM_PY.read_text()
    return re.sub(r'"\s*"', "", src)


def test_skip_block_consults_overdue_helper():
    """The contentChangedAt-skip block must call the new helper
    + bypass the skip when overdue."""
    src = _src_strflat()
    # Find the skip block via its (flattened) anchor message.
    idx = src.index("skipped (no changes since contentChangedAt")
    # Walk back to find the start of the if/should_skip block.
    block_start = src.rfind("content_changed_at_match", 0, idx)
    block = src[block_start:idx + 200]
    assert "_section_enum_overdue" in block, (
        "v1.18.92: skip block must consult _section_enum_overdue"
    )
    # The 24h threshold should be passed explicitly so future
    # changes don't accidentally widen the silence window.
    assert "hours=24" in block


def test_skip_bypass_logs_at_info_level():
    """When the bypass triggers, log at INFO so operators can
    see in docker logs why a full enum ran despite Plex saying
    nothing changed. Per the v1.18.5 cold-path-needs-MORE-
    logging sub-pattern."""
    src = _src_strflat()
    # Adjacent string literals concatenate; the flat source has
    # the full message in one piece.
    idx = src.index("bypassing contentChangedAt-skip")
    block = src[max(0, idx - 200):idx + 200]
    assert "log.info" in block


def test_skip_block_marker_references_reaper_gap():
    """The v1.18.92 marker must reference the v1.18.89 reaper
    design gap so future archaeology has the lineage."""
    src = PLEX_ENUM_PY.read_text()
    idx = src.index("v1.18.92")
    block = src[idx:idx + 800]
    assert "v1.18.89 reaper" in block, (
        "v1.18.92: marker should reference v1.18.89 reaper"
    )


# ── Behavioral helper tests ──────────────────────────────────


@pytest.fixture
def db_with_section(tmp_path):
    """Bare DB + one section with no plex_items rows."""
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
        conn.commit()
    return db_path


def _seed_plex_item(db_path, section_id, rk, last_seen_at):
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, "
            "   guid_imdb, title, year, has_theme, "
            "   local_theme_file, folder_path, "
            "   plex_independent_theme, first_seen_at, "
            "   last_seen_at) "
            "VALUES (?, ?, 'movie', ?, ?, 2020, 0, 0, "
            "        ?, 0, ?, ?)",
            (rk, section_id, f"tt{rk}", f"Movie {rk}",
             f"/data/movies/Movie {rk}",
             last_seen_at, last_seen_at),
        )
        conn.commit()


def test_helper_returns_false_for_section_with_no_items(
    db_with_section,
):
    """No plex_items yet → no need to force a full enum (and
    nothing to reap either). Return False."""
    from app.core.plex_enum import _section_enum_overdue
    assert _section_enum_overdue(db_with_section, "1") is False


def test_helper_returns_false_for_fresh_section(db_with_section):
    """Section with a recent enum (< 24h) → no bypass needed.
    Return False."""
    now = datetime.now(timezone.utc)
    fresh = (now - timedelta(hours=1)).isoformat(timespec="seconds")
    _seed_plex_item(db_with_section, "1", "100", fresh)
    from app.core.plex_enum import _section_enum_overdue
    assert _section_enum_overdue(db_with_section, "1") is False


def test_helper_returns_true_for_overdue_section(db_with_section):
    """Section with last_seen_at > 24h ago → overdue. Return True."""
    now = datetime.now(timezone.utc)
    stale = (now - timedelta(hours=30)).isoformat(timespec="seconds")
    _seed_plex_item(db_with_section, "1", "100", stale)
    from app.core.plex_enum import _section_enum_overdue
    assert _section_enum_overdue(db_with_section, "1") is True


def test_helper_threshold_is_configurable(db_with_section):
    """The `hours` kwarg controls the threshold so future
    callers can tighten (e.g., 12h) or loosen (e.g., 72h)
    without code changes."""
    now = datetime.now(timezone.utc)
    mid = (now - timedelta(hours=12)).isoformat(timespec="seconds")
    _seed_plex_item(db_with_section, "1", "100", mid)
    from app.core.plex_enum import _section_enum_overdue
    # 12h-old row vs 24h threshold → not overdue
    assert _section_enum_overdue(db_with_section, "1", hours=24) is False
    # 12h-old row vs 6h threshold → overdue
    assert _section_enum_overdue(db_with_section, "1", hours=6) is True


def test_helper_section_scoping(db_with_section):
    """Overdue check must scope by section_id — a stale row in
    a different section shouldn't trigger this section's
    bypass."""
    now = datetime.now(timezone.utc)
    stale = (now - timedelta(hours=30)).isoformat(timespec="seconds")
    # Seed a different section.
    with sqlite3.connect(db_with_section) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('2', 'TV', 'show', 0, 0, "
            "        'tv', 1, '2026-05-01T00:00:00', "
            "        '2026-05-01T00:00:00')"
        )
        conn.commit()
    _seed_plex_item(db_with_section, "2", "200", stale)
    # Section 2 has the stale row → overdue.
    # Section 1 has nothing → not overdue.
    from app.core.plex_enum import _section_enum_overdue
    assert _section_enum_overdue(db_with_section, "2") is True
    assert _section_enum_overdue(db_with_section, "1") is False


def test_helper_handles_z_suffix_timestamp(db_with_section):
    """Some timestamps come back with Z-suffix instead of
    +00:00 — fromisoformat needs the Z normalized."""
    now = datetime.now(timezone.utc)
    stale_z = (now - timedelta(hours=30)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    _seed_plex_item(db_with_section, "1", "100", stale_z)
    from app.core.plex_enum import _section_enum_overdue
    assert _section_enum_overdue(db_with_section, "1") is True


def test_helper_handles_bad_timestamp_gracefully(db_with_section):
    """Garbage timestamp → return False (don't crash, don't
    force surprise enum)."""
    _seed_plex_item(db_with_section, "1", "100", "not-a-timestamp")
    from app.core.plex_enum import _section_enum_overdue
    assert _section_enum_overdue(db_with_section, "1") is False
