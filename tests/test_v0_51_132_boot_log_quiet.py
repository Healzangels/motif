"""v0.51.132 — quieter, clearer logs.

(1) recovery_v55's v1.24.34 edition-coverage walker is the LAST recovery walker
    still wired at boot (v1.21.0 retired the other 15). Its "marker already set —
    skipping" line fired on the INFO boot log every restart — the last recurring
    boot-noise line. Downgraded to DEBUG (the INFO event is when it actually runs
    + stamps the marker).

(2) resolve_theme_ids logged "scanned N plex_items rows" where N is the CUMULATIVE
    sum of rowcounts across its ~7 idempotent match-UPDATE passes — routinely far
    larger than the row count, which read like a runaway table scan. Reworded to
    report rows-processed and link-writes separately.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
RECOVERY = (REPO / "app" / "core" / "recovery_v55.py").read_text()
PLEX_ENUM = (REPO / "app" / "core" / "plex_enum.py").read_text()


def test_edition_coverage_skip_is_debug_not_info():
    i = RECOVERY.index("edition-coverage: marker already set")
    # the log call opening the ~120 chars before the message must be log.debug(
    head = RECOVERY[i - 120:i]
    assert "log.debug(" in head
    assert "log.info(" not in head


def test_resolve_theme_ids_log_not_mislabelled_scanned():
    # the confusing "scanned N plex_items rows" wording is gone …
    assert "scanned %d plex_items rows" not in PLEX_ENUM
    # … replaced by the rows / link-writes split.
    assert "theme_id link-writes" in PLEX_ENUM
