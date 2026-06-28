"""v0.50.9 — ORPHAN SCAN drift labels render without underscores.

the user: make the drift chips not have the `_` (MOTIF_ENTRY_MISSING →
"MOTIF ENTRY MISSING", PLEX_FETCH_FAILED → "PLEX FETCH FAILED"). Display-only:
the raw drift_type still drives filtering, tone (DRIFT_TONE), and the per-type
action set.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
ORPHANS = (REPO / "app" / "web" / "templates" / "orphans.html").read_text()


def test_drift_label_helper_defined():
    assert "const driftLabel =" in ORPHANS
    assert ".replace(/_/g, ' ')" in ORPHANS


def test_helper_applied_at_display_sites():
    # the filter chip label, the DRIFT cell, and the filter-status line all
    # render via driftLabel(...).
    assert "driftLabel(dt)" in ORPHANS                 # per-type filter chips
    assert "driftLabel(f.drift_type)" in ORPHANS       # DRIFT column cell
    assert "driftLabel(activeDriftFilter)" in ORPHANS  # filter-status + empty state


def test_filtering_still_uses_raw_drift_type():
    """Display-only — the data-side filter compares the raw drift_type."""
    assert "f.drift_type === activeDriftFilter" in ORPHANS
