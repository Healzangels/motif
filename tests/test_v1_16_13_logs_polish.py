"""v1.16.13 — /queue secondary-bar height parity + sticky-thead anchor fix.

Two follow-ups on v1.16.11's LOGS-page work:

1. **Chips-bar height parity** — v1.16.11 matched the JOBS sticky
   thead padding to `.chips-bar` (both 10px/18px). But the chip
   element inside `.chips-bar` carries its own 6px vertical
   padding, so the chips-bar still rendered ~10px taller than a
   text-only thead row at the same y-band. the user: "make the
   event viewer filtering side slightly narrower to match the
   left side." Trim `.chips-bar` vertical padding 10px → 6px so
   the chip's internal padding accounts for the height; horizontal
   padding stays at 18px to align with block-head and thead.

2. **Sticky thead "bouncing" on scroll back to top** — the bottom
   border fix landed in v1.16.11, but the thead still visibly
   shifted by a few pixels during scroll-back-to-top. Root cause
   was the browser's automatic scroll-anchor algorithm: /queue
   polls + re-renders tbody on a cadence, and when a re-render
   landed mid-scroll the browser tried to keep the visually-
   pinned row in place by adjusting `scrollTop`, which made the
   sticky thead bounce. the user: "top row isn't locked."
   `overflow-anchor: none` on `.jobs-scroll` disables that
   compensation — sticky thead now stays pinned through tbody
   rewrites.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def test_chips_bar_padding_matches_thead_height():
    """`.chips-bar` vertical padding pairs with the JOBS sub-
    header opposite it. v1.16.13 set both to 6px; v1.19.20
    tightened to 3px after the user flagged the chip BUTTON
    (visible bordered shape) read smaller than the surrounding
    bar — both bars compressed to 32px to make the chip fill
    most of its container."""
    css = APP_CSS.read_text()
    anchor = css.index("\n.chips-bar {")
    block = css[anchor:anchor + 1000]
    assert "padding: 3px 18px;" in block, (
        "v1.19.20: .chips-bar vertical padding is 3px (was 6px "
        "pre-v1.19.20) so the 32px container has just enough "
        "halo for the ~26px chip button — matches the "
        ".jobs-grid-header bar opposite at the same y-band."
    )


def test_jobs_scroll_y_disables_scroll_anchor():
    """v1.19.6 replaced .jobs-scroll (single bidirectional scroll
    container holding a <table>) with .jobs-scroll-x (outer
    horizontal scroll) + .jobs-scroll-y (inner vertical scroll
    holding a list of <li> rows). The overflow-anchor: none
    guard moved to .jobs-scroll-y — same intent (prevent
    browser scroll-anchor from nudging scrollTop during
    re-render cadence), just on the new container."""
    css = APP_CSS.read_text()
    anchor = css.index(".jobs-scroll-y {")
    block = css[anchor:anchor + 1500]
    assert "overflow-anchor: none" in block, (
        "v1.16.13 / v1.19.6: .jobs-scroll-y (new vertical scroll "
        "container) must set overflow-anchor: none — same intent "
        "as the original v1.16.13 fix on the now-retired "
        ".jobs-scroll selector."
    )


def test_v1_19_6_replaced_thead_with_grid():
    """v1.19.6 obsoleted the sticky-<thead>-table architecture
    that v1.16.11 / v1.16.13 / v1.19.2 / v1.19.3 stacked
    workarounds on. The .jobs-grid-header is now a sibling
    <div> of the .jobs-scroll-y vertical scroll container —
    no sticky needed, no detach possible.

    v1.19.20 update: paired-height target moved 38px → 32px
    after the user's repro that the 38px bar read visually larger
    than the chip button opposite. Both sub-headers are now
    32px content-driven."""
    css = APP_CSS.read_text()
    assert ".jobs-grid-header" in css, (
        "v1.19.6: column-header row must be .jobs-grid-header "
        "(replaced the sticky <thead> approach)"
    )
    # The grid header must still match .chips-bar height at the
    # same y-band — paired-height contract still applies; target
    # height moved to 32px in v1.19.20.
    idx = css.index(".jobs-grid-header {")
    end = css.index("}", idx)
    block = css[idx:end]
    assert "min-height: 32px" in block, (
        "v1.19.20: .jobs-grid-header must use the 32px paired-"
        "height target (was 38px pre-v1.19.20)"
    )
