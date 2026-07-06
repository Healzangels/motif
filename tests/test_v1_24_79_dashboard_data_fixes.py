"""v1.24.79 — dashboard data fixes from the dashboard audit.

1. ADDED TODAY / ADDED THIS WEEK (+ the 30-day download-insight queries)
   string-compared an ISO-'T' timestamp (placed_at / created_at stored as
   'YYYY-MM-DDTHH:MM:SS+00:00') against datetime('now',...) which renders
   'YYYY-MM-DD HH:MM:SS' (space separator). 'T'(0x54) > ' '(0x20), so any row on
   the SAME date as the threshold but an EARLIER time sorts >= the threshold and
   is over-counted (prod showed ADDED THIS WEEK = 15 vs the true 14). Fix: wrap
   the stored column in datetime() so both sides are canonical.

2. PER-SECTION COVERAGE mislabeled the synthetic Collections row as 'MOVIES /
   STD' (the typeLabel ternary had no collections branch). Now 'COLLECTIONS'
   with no STD/4K suffix. (v0.51.31: the // COVERAGE COMPARISON block this
   note once mirrored was removed as a duplicate — the branch lives on in
   renderSectionCoverage.)
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── #1 timestamp boundary ────────────────────────────────────────────────────


def test_added_counts_wrap_timestamp_in_datetime():
    assert "datetime(placed_at) >= datetime('now', '-1 day')" in API_PY
    assert "datetime(placed_at) >= datetime('now', '-7 day')" in API_PY
    # this guards has_insight_downloads (the 30-day EXISTS gate) — a rolling
    # window that legitimately stays datetime()-wrapped. (v0.51.90 changed the
    # SEPARATE per-day DOWNLOAD ACTIVITY chart query to a DATE()-aligned window;
    # that's pinned by test_v0_51_90_download_activity_utc.)
    assert "datetime(created_at) >= datetime('now', '-30 days')" in API_PY
    # the bare string-compare forms (the bug) must be gone everywhere.
    assert "WHERE placed_at >= datetime('now'" not in API_PY
    assert " created_at >= datetime('now'" not in API_PY


def test_added_today_boundary_excludes_same_date_earlier_time():
    from app.core.db import init_db
    fd, p = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = Path(p)
    init_db(db)
    # 0.50.0: fully deterministic (was wall-clock-relative via now-25h, which
    # flaked in the ~00:00–01:00 UTC window when the old row straddled a date
    # boundary so the bare compare no longer over-counted). A FIXED
    # space-separated threshold (the shape datetime('now','-1 day') emits) +
    # FIXED ISO-'T' rows reproduce the exact bug regardless of when it runs:
    # `old` is genuinely BEFORE the threshold but, on the SAME date, its 'T'
    # separator (chr 84) string-sorts >= the threshold's space (chr 32).
    threshold = "2026-06-14 12:00:00"
    old = "2026-06-14T08:00:00+00:00"    # 4h before threshold → genuinely old
    fresh = "2026-06-15T08:00:00+00:00"  # day after threshold → genuinely recent
    conn = sqlite3.connect(db)
    for i, ts in enumerate((old, fresh), start=1):
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, "
            "media_folder, placement_kind, provenance, placed_at) "
            "VALUES ('movie', ?, '1', ?, 'hardlink', 'auto', ?)",
            (i, f"/x{i}", ts))
    conn.commit()
    fixed = conn.execute(
        "SELECT COUNT(*) FROM placements "
        "WHERE datetime(placed_at) >= ?", (threshold,)).fetchone()[0]
    buggy = conn.execute(
        "SELECT COUNT(*) FROM placements "
        "WHERE placed_at >= ?", (threshold,)).fetchone()[0]
    conn.close()
    db.unlink(missing_ok=True)
    assert fixed == 1, "datetime() wrap counts only the genuinely-recent row"
    assert buggy == 2, "bare string compare over-counts the older same-date row"


# ── #2 collections label ─────────────────────────────────────────────────────


def test_collections_section_type_label():
    idx = APP_JS.index("const isCollections = s.tab === 'collections';")
    block = APP_JS[idx:idx + 400]
    assert "isCollections     ? 'COLLECTIONS'" in block
    assert "isCollections ? '' :" in block  # no STD/4K suffix for collections
