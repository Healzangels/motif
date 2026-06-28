"""v1.19.3 — /queue JOBS thead alignment + sticky-unsnap fix.

OBSOLETE — v1.19.6 replaced the sticky-<thead>-on-<table>
architecture this file's tests pinned. The whole class of
bugs (thead height not matching chips-bar opposite, thead
unsnap on scroll-up, sticky-flicker on tbody re-render) was
worked around through three CSS patches (v1.16.13 +
v1.19.2 + v1.19.3) before the architectural fix landed in
v1.19.6: drop the table entirely, use a sibling
.jobs-grid-header outside the vertical scroll container.

The contracts this file's tests pinned are now enforced by:
  - test_v1_19_6_jobs_grid_layout.py (new architecture pins)
  - test_v1_19_2_queue_layout_alignment.py
    test_jobs_grid_header_matches_chips_bar_height (replaces
    the v1.19.3 paired-height pin)
  - test_v1_16_13_logs_polish.py
    test_v1_19_6_replaced_thead_with_grid (replaces the
    v1.19.3 sticky-killer pins)

Keeping this file as a single stub so the version-marker
chain (.find('v1.19.3') in archaeology contexts) still
resolves. If the version-marker pin is no longer
load-bearing, this file can be deleted in a future
cleanup tag.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def test_v1_19_3_marker_lineage_preserved_in_css():
    """The v1.19.3 marker should remain referenced somewhere
    so archaeology grep ('v1.19.3') still finds the lineage
    even after the rules it pinned were obsoleted in v1.19.6."""
    css = (REPO / "app" / "web" / "static" / "app.css").read_text()
    # v1.19.6's comment block in the .jobs-scroll-x section
    # references v1.19.3 as part of the lineage.
    assert "v1.19.3" in css, (
        "v1.19.3 / v1.19.6: the v1.19.6 comment block should "
        "reference v1.19.3 in the lineage chain (along with "
        "v1.16.13 + v1.19.2) so archaeology finds the WHY "
        "for the architectural change"
    )
