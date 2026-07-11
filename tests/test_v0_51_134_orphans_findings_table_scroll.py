"""v0.51.134 — CSS-audit T1 (P0): orphans FINDINGS table is swipeable on mobile.

The /orphans FINDINGS table (7 cols: TITLE ID RK DRIFT DETAILS ENTRIES ACTION)
was wrapped in `<div class="jobs-scroll">` — a class that has NO CSS rule. Its
`.jobs-scroll-x` ancestor (the horizontal-scroll container) was removed in
v1.22.56 with the split layout; the bare `.jobs-scroll` never existed. With no
scroll context and body{overflow-x:hidden}, the table spilled its right edge
under the clip below ~760px and the ACTION column (// RE-PUSH / // LET PLEX
SERVE / × PURGE / // PROBE) was unreachable on a phone.

Fix: the shared `.table-scroll` wrapper (overflow-x:auto scoped to ≤1080px —
the same swipe context #library-table and the IMPORT preview table use).
Harness-proven at a 375px layout viewport: the last ACTION button went from
reachable-after-scroll=false (.jobs-scroll, overflow-x:visible) to true
(.table-scroll, overflow-x:auto).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
ORPHANS = (REPO / "app" / "web" / "templates" / "orphans.html").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_findings_table_wrapped_in_table_scroll():
    # the FINDINGS <table> must be directly preceded by a .table-scroll opener.
    i = ORPHANS.index('<table class="table table-tight">')
    head = ORPHANS[i - 400:i]
    assert 'class="table-scroll"' in head, (
        "the FINDINGS table needs the shared .table-scroll swipe context"
    )


def test_dead_jobs_scroll_class_is_gone():
    # the bare (non -x / -y) .jobs-scroll class must not wrap the table.
    assert 'class="jobs-scroll"' not in ORPHANS, (
        "'.jobs-scroll' has no CSS rule (its .jobs-scroll-x ancestor went in "
        "v1.22.56) — re-adding it re-breaks mobile reachability"
    )


def test_table_scroll_swipe_context_still_defined():
    # guard the CSS side of the contract: .table-scroll gets overflow-x:auto in
    # the ≤1080px tier (where the table actually exceeds a phone viewport).
    assert ".table-scroll { overflow-x: auto; -webkit-overflow-scrolling: touch; }" in APP_CSS
