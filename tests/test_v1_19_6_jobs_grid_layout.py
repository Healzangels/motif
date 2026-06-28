"""v1.19.6 — /queue JOBS panel: list-grid architecture + scroll affordance.

the user's third report (2026-05-24) of the JOBS thead detaching
from the section header on scroll-back-to-top, even after three
CSS workarounds (v1.16.13 overflow-anchor + v1.19.2 z-index +
v1.19.3 border-collapse + height). `<table>` + `position: sticky`
+ tbody innerHTML rewrites is a fragile combo on every browser.

## Fix (architectural)

Drop the `<table>` entirely. Use a nested-overflow grid:

    .jobs-scroll-x      — outer wrapper, overflow-x: auto
      .jobs-grid        — width: max-content, holds:
        .jobs-grid-header (sibling, never scrolls vertically)
        .jobs-scroll-y  — inner wrapper, overflow-y: auto
          ol#jobs-body
            li.jobs-grid-row (per-job row)

Header + rows scroll HORIZONTALLY together (columns stay
aligned). Only rows scroll VERTICALLY. No sticky needed → no
detach possible.

Each row uses `display: grid` with the same column template
the header uses → columns align even though they're in
different parent containers (the grid-template-columns
declaration is the contract).

## Horizontal-scroll affordance

the user's ask: "have an arrow or something that indicates you
can scroll that way." CSS fade-gradient on the right edge
(via `::after`) appears when there's content to scroll to;
mirrored on the left (`::before`) when the user has scrolled
right. JS toggles `data-can-scroll-right` /
`data-can-scroll-left` on the .jobs-scroll-x element via
scroll + resize listeners.

## What's pinned

- Template uses .jobs-scroll-x / .jobs-grid / .jobs-grid-header /
  .jobs-scroll-y / .jobs-list structure (NO <table>, NO <thead>)
- Each <li class="jobs-grid-row"> mirrors the header's
  grid-template-columns
- JS renderer emits <li> with <span> cells (NOT <tr>/<td>)
- _updateJobsScrollAffordance toggles data-can-scroll-* attrs
- CSS fade pseudo-elements gated on those data attrs
- Marker references v1.16.13 + v1.19.2 + v1.19.3 lineage so
  the WHY is searchable
"""
from __future__ import annotations

from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
QUEUE_HTML = REPO / "app" / "web" / "templates" / "queue.html"


# ── Template structure ───────────────────────────────────────


def test_queue_html_no_table_in_jobs_panel():
    """The <table> + <thead> + <tbody> architecture is replaced
    by the list-grid pattern. Any <table> tag in the JOBS panel
    would re-introduce the sticky-thead bug class.

    HTML comments are stripped before the check — the v1.19.6
    comment block intentionally references `<table>` /
    `<thead>` as part of the explanatory text."""
    import re
    html = QUEUE_HTML.read_text()
    jobs_idx = html.index('data-logpanel="jobs"')
    events_idx = html.index('data-logpanel="events"')
    jobs_section = html[jobs_idx:events_idx]
    # Strip HTML comments + Jinja comments.
    stripped = re.sub(r"<!--.*?-->", "", jobs_section,
                       flags=re.DOTALL)
    stripped = re.sub(r"\{#.*?#\}", "", stripped, flags=re.DOTALL)
    assert "<table" not in stripped, (
        "v1.19.6: JOBS panel must NOT use <table> — the sticky-"
        "thead bug class (v1.16.13/v1.19.2/v1.19.3) is "
        "architectural, dropped entirely in v1.19.6"
    )
    assert "<thead" not in stripped
    assert "<tbody" not in stripped


def test_queue_html_uses_grid_structure():
    """JOBS panel must use the new selectors: .jobs-scroll-x
    wrapper, .jobs-grid inner, .jobs-grid-header for column
    labels, .jobs-scroll-y for vertical scroll, .jobs-list
    for the <ol>."""
    html = QUEUE_HTML.read_text()
    jobs_idx = html.index('data-logpanel="jobs"')
    events_idx = html.index('data-logpanel="events"')
    jobs_section = html[jobs_idx:events_idx]
    for selector in ("jobs-grid", "jobs-grid-header",
                     "jobs-scroll-y", "jobs-list"):
        assert selector in jobs_section, (
            f"v1.19.6: JOBS panel must include `{selector}` "
            f"class in the template"
        )
    # v1.22.56: the horizontal-scroll wrapper is gone (full-width panel).
    assert "jobs-scroll-x" not in jobs_section


def test_queue_html_header_columns_match_render_columns():
    """The header <div class="jobs-grid-header"> must declare
    the same 7 columns the JS render emits per row (ID, TYPE,
    ITEM, STATE, TIME, NOTE, ACTION). If they drift the columns
    won't align visually."""
    html = QUEUE_HTML.read_text()
    # The header is one block with 7 <span>s.
    header_idx = html.index('class="jobs-grid-row jobs-grid-header"')
    # Walk to the end of this div block.
    block_end = html.index("</div>", header_idx)
    header_block = html[header_idx:block_end]
    for col in ("ID", "TYPE", "ITEM", "STATE",
                "TIME", "NOTE", "ACTION"):
        assert f">{col}<" in header_block, (
            f"v1.19.6: jobs-grid-header must include column "
            f"label '{col}'"
        )


# ── JS render ────────────────────────────────────────────────


def test_jobs_body_renders_li_not_tr():
    """The render output for each job row must emit <li> with
    grid spans, not <tr>/<td>. Pin the new shape."""
    js = APP_JS.read_text()
    # Locate the render template for #jobs-body.
    body_idx = js.index("const _jobsHtml = data.jobs.map(")
    # Window must be wide enough to span the full template
    # literal — render block is ~4000 chars including the
    # action-cell ternary + retry-hint block.
    # Window must reach past the render template + the
    # empty-state fallback string (full block is ~6500 chars).
    block = js[body_idx:body_idx + 8000]
    assert '<li class="jobs-grid-row">' in block, (
        "v1.19.6: each job row must render as <li class="
        "'jobs-grid-row'> with grid spans"
    )


def test_jobs_empty_state_uses_jobs_list_empty_class():
    """Empty-state message must be a <li class='jobs-list-empty'>
    (not the legacy <tr><td colspan=7>). Pin to make sure the
    empty fallback was also migrated."""
    js = APP_JS.read_text()
    body_idx = js.index("const _jobsHtml = data.jobs.map(")
    # Window must reach past the render template + the
    # empty-state fallback string (full block is ~6500 chars).
    block = js[body_idx:body_idx + 8000]
    assert 'class="jobs-list-empty' in block, (
        "v1.19.6: empty-state fallback must use "
        ".jobs-list-empty class"
    )


# ── Horizontal-scroll affordance ─────────────────────────────


# ── Lineage marker ───────────────────────────────────────────


def test_v1_19_6_marker_references_prior_workarounds():
    """The v1.19.6 marker should reference the lineage chain
    (v1.16.13 + v1.19.2 + v1.19.3) so future debugging can
    trace why the table architecture was abandoned."""
    css = APP_CSS.read_text()
    # Anchor on the v1.19.6 jobs-grid comment block specifically.
    # v1.19.63 added a --violet-bright comment that contains the
    # substring "v1.19.6" via "v1.19.63" — use a non-digit
    # lookahead to skip matches that are actually v1.19.6X.
    search_from = 0
    while True:
        idx = css.index("v1.19.6", search_from)
        next_char = css[idx + len("v1.19.6"):idx + len("v1.19.6") + 1]
        if not next_char.isdigit():
            break
        search_from = idx + 1
    block = css[idx:idx + 3000]
    for v in ("v1.16.13", "v1.19.2", "v1.19.3"):
        assert v in block, (
            f"v1.19.6: marker must reference {v} in the lineage "
            f"chain so the sticky-thead workaround history is "
            f"discoverable from the new architecture comment"
        )
