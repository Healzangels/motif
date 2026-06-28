"""v1.19.19 / v1.19.20 — LOGS page UI fixes.

v1.19.19 attempted three fixes but got every one wrong:
  1. Bg color: added --bg-elev-2 to event-stream when the user's
     intent was to DEMOTE the JOBS side to body bg, not promote
     the events side to elevated bg.
  2. Op-row alignment: replaced grid with flex variant, losing
     the ID column on op rows.
  3. Bar height: typography matched (mono + tiny) but the 38px
     min-height container with floating ~14px text still read
     visually taller than the ~26px chip button opposite.

v1.19.20 corrects all three:
  1. Bg revert: BOTH .event-stream and .jobs-scroll-x lose
     their bg → content areas inherit body --bg. Only the sub-
     header bars (chips-bar, jobs-grid-header) keep --bg-elev-2.
  2. Subgrid: .jobs-grid declares the column template once,
     .jobs-grid-row uses `grid-template-columns: subgrid` to
     inherit. Every row shares column widths regardless of
     per-row content. Op rows render with the same 7 cells.
  3. Height match: both sub-headers compressed to 32px min-
     height with 3px outer padding. Chip button (~26px) and
     header text (~14px) both center in ~32px bars.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _block(css: str, selector: str) -> str:
    start = css.index(selector + " {") + len(selector) + 2
    end = start + css[start:].index("}")
    return css[start:end]


# ── Fix 1: bg revert ─────────────────────────────────────────


def test_event_stream_no_explicit_bg():
    """Content area should inherit body --bg. v1.19.19 added
    --bg-elev-2 here by mistake — reverted in v1.19.20."""
    block = _block(CSS, ".event-stream")
    assert "background:" not in block or "/* v1.19.20" in block, (
        "v1.19.20: .event-stream must NOT set background; "
        "content area inherits body --bg"
    )


def test_jobs_scroll_y_no_explicit_bg():
    """Content area inherits body --bg (the elevated bar is on
    .jobs-grid-header). v1.22.56: .jobs-scroll-x was removed with the
    horizontal-scroll layout; the vertical-scroll body is the container
    now and must NOT carry --bg-elev-2 (which would look elevated against
    the EVENT STREAM panel's body-bg content area)."""
    block = _block(CSS, ".jobs-scroll-y")
    assert "var(--bg-elev-2)" not in block, (
        "v1.19.20/v1.22.56: the JOBS scroll body must inherit body --bg"
    )


def test_sub_header_bars_keep_elev_bg():
    """The sub-header bars (chips-bar, jobs-grid-header) DO
    keep --bg-elev-2 so they read as elevated dividers above
    their respective content areas. Only the content areas
    are demoted."""
    assert "var(--bg-elev-2)" in _block(CSS, ".chips-bar")
    assert "var(--bg-elev-2)" in _block(CSS, ".jobs-grid-header")


# ── Fix 2: column alignment via fixed-width columns ──────────


def test_jobs_grid_is_plain_container_not_grid():
    """v1.19.22: .jobs-grid is a plain container (header + vertical-
    scroll body stacked), NOT a grid — the rows declare their own grid
    template. v1.22.56 dropped the `min-width: max-content` (that sized
    the grid to the widest row for the now-removed horizontal scroll);
    the full-width container just spans 100% and the fluid row columns
    fit it."""
    block = _block(CSS, ".jobs-grid")
    assert "display: grid" not in block, (
        "v1.19.22: .jobs-grid must NOT be a grid container — the data "
        "rows declare their own grid template"
    )
    assert "min-width: max-content" not in block, (
        "v1.22.56: max-content sizing was the horizontal-scroll trick — "
        "removed with the full-width layout"
    )


def test_jobs_grid_row_uses_fluid_columns():
    """v1.22.56: every .jobs-grid-row uses IDENTICAL FLUID grid-template-
    columns so the row always fits the full-width panel and truncates
    (the v1.19.22 fixed px widths + min-width:max-content forced the old
    horizontal scroll). Six proportional `minmax(0, Nfr)` columns +
    ACTION fixed (v1.23.85: 180px, up from 130px — fits the longer
    "// ACK FAILURE" label) for the buttons. The header uses the same
    template so columns stay aligned. No subgrid (the v1.19.20 mistake)."""
    block = _block(CSS, ".jobs-grid-row")
    assert "display: grid" in block
    assert "grid-template-columns:" in block
    assert "grid-template-columns: subgrid" not in block
    assert "columns: subgrid" not in block
    assert "grid-column: 1 / -1" not in block
    # Fluid columns + a fixed ACTION track.
    assert "minmax(0," in block, (
        "v1.22.56: jobs-grid-row columns must be fluid minmax(0, Nfr) "
        "so the row fits the full-width panel without horizontal scroll"
    )
    assert "fr)" in block
    assert "180px" in block, "ACTION column stays fixed for the buttons"
    # The old per-column fixed px widths are gone (they forced horizontal
    # overflow). v1.23.85: 180px dropped from this list — it's now the
    # legit fixed ACTION track (asserted present above); the fluid
    # minmax/fr assertions guard the real "no fixed per-column" contract.
    for px in ("170px", "100px"):
        assert px not in block, (
            f"v1.22.56: fixed column width {px} should be gone — "
            "columns are now fluid"
        )


def test_op_row_variant_removed():
    """v1.19.19's flex op-row variant is gone — all rows
    render through the standard grid path."""
    assert ".jobs-grid-row--op {" not in CSS, (
        "v1.19.20: .jobs-grid-row--op variant removed — subgrid "
        "handles op-row alignment without a separate layout"
    )


def test_app_js_op_row_branch_removed():
    """The renderJobs `if (isOpProgress)` early-return branch is
    gone. All rows go through the same 7-cell render."""
    # Scope to renderJobs region.
    snippet = APP_JS[APP_JS.index("jobs-body"):APP_JS.index("jobs-body") + 25000]
    # The compact-branch's distinct return signature should be
    # absent. (isOpProgress as a variable name may persist for
    # other uses — e.g. cancellable gating — so don't pin on
    # the bare identifier.)
    assert 'class="jobs-grid-row jobs-grid-row--op"' not in snippet, (
        "v1.19.20: the --op variant render branch is removed"
    )
    assert "op-label" not in snippet
    assert "op-state" not in snippet
    assert "op-time" not in snippet


def test_per_row_job_render_uses_7_cells():
    """Standard render still emits the 7 cells in order:
    ID, TYPE, ITEM, STATE, TIME, NOTE, ACTION."""
    snippet = APP_JS[APP_JS.index("jobs-body"):APP_JS.index("jobs-body") + 25000]
    assert '<li class="jobs-grid-row">' in snippet
    assert "${actionCell}" in snippet


# ── Fix 3: bar height match ──────────────────────────────────


def test_jobs_grid_header_min_height_32():
    """v1.19.20 compresses the sub-header from 38px → 32px so
    its content (mono text ~14px + halo) matches the visual
    height of the chip button (~26px) opposite."""
    block = _block(CSS, ".jobs-grid-header")
    assert "min-height: 32px" in block


def test_chips_bar_min_height_32():
    """chips-bar paired-height contract — both sub-headers at
    32px. Chip button (~26px) sits in ~3px halo top + bottom."""
    block = _block(CSS, ".chips-bar")
    assert "min-height: 32px" in block


def test_jobs_grid_header_uses_mono_font():
    """Typography parity preserved from v1.19.19 — header uses
    mono+--t-tiny matching the chip button text."""
    block = _block(CSS, ".jobs-grid-header")
    assert "font-family: var(--font-mono)" in block
    assert "font-size: var(--t-tiny)" in block
    assert "letter-spacing: 0.2em" not in block


def test_jobs_scroll_y_fills_viewport_height():
    """v1.22.56: the JOBS body fills the viewport height (single full-
    width panel) via a clamp() — replacing the old fixed 700px that
    paired it to the EVENT STREAM pane's scrollbar in the split layout."""
    block = _block(CSS, ".jobs-scroll-y")
    assert "clamp(" in block and "100vh" in block, (
        "v1.22.56: jobs-scroll-y must use a viewport-relative clamp() "
        "height so the single panel fills the screen"
    )
    assert "max-height: 700px" not in block, (
        "v1.22.56: the fixed 700px split-pane height is gone"
    )
