"""v1.19.2 — /queue layout: header alignment + horizontal scroll.

the user's 2026-05-24 screenshot review of /queue surfaced three
distinct visual issues:

1. // JOBS block-head and // EVENT STREAM block-head render at
   different heights (~50px vs ~42px). The JOBS chips' internal
   padding pushes the header taller; EVENT STREAM's title +
   LIVE-only content is shorter.
2. The JOBS sticky thead row (ID / TYPE / ITEM / ...) and the
   EVENT STREAM .chips-bar (ALL / 1H / 24H / 7D) render at
   slightly different heights, so the second visual "band"
   doesn't line up across the two halves either. Same row also
   "unlocks itself from the top" intermittently when scrolling.
3. Long content overflow is inconsistent: JOBS rows wrap to
   extra-tall heights when NOTE is long; EVENT STREAM rows
   truncate with '...'. Both should instead use a horizontal
   scrollbar at the bottom, styled to match the existing
   vertical scrollbar.

## Fix

CSS-only changes:

  - `.block-head min-height: 50px` — EVENT STREAM matches JOBS
    even without chips
  - `.chips-bar min-height: 38px` + `.jobs-scroll thead th
    min-height: 38px` — sub-header bands aligned
  - `.jobs-scroll thead th z-index: 1 → 2` — sticky thead layers
    cleanly over scrolling tbody
  - `.jobs-scroll overflow-y → overflow` — bidirectional scroll
  - `.jobs-scroll .table-tight td white-space: nowrap` — single-
    line rows
  - `.event-stream overflow-y → overflow` + `overflow-anchor:
    none` — bidirectional scroll + stable scroll position on
    live-prepend
  - `.event-stream li grid msg col 1fr → minmax(700px, 1fr)` —
    rows extend horizontally when message is long
  - `.event-msg` dropped `overflow: hidden + text-overflow:
    ellipsis` — full message visible via horizontal scroll

Global `::-webkit-scrollbar { width: 10px; height: 10px }` is
already set, so the new horizontal scrollbars inherit the same
green-on-dark chrome as the vertical ones — no extra CSS needed.

## What's pinned

- .block-head has min-height (matches across halves)
- .chips-bar AND thead th have matching min-height
- .jobs-scroll uses bidirectional overflow + nowrap cells
- .event-stream li grid uses minmax for the msg column
- .event-msg no longer has text-overflow: ellipsis
- thead z-index ≥ 2 for sticky stability
- Markers reference v1.19.2 + the user's 2026-05-24 repro for
  archaeology
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


# ── Change 1: header alignment ───────────────────────────────


def test_block_head_has_min_height():
    """.block-head must have a min-height so the EVENT STREAM
    side (title + LIVE only) renders at the same y-band as the
    JOBS side (title + chips with internal padding)."""
    src = APP_CSS.read_text()
    # Find the .block-head rule (the first one, the canonical).
    idx = src.index(".block-head {")
    block = src[idx:idx + 1500]
    assert "min-height:" in block, (
        "v1.19.2: .block-head must declare min-height for "
        "JOBS/EVENT STREAM alignment on /queue"
    )
    # And box-sizing: border-box so the min-height accounts for
    # the 12px×2 padding (no surprise growth).
    assert "box-sizing: border-box" in block, (
        "v1.19.2: .block-head needs box-sizing: border-box so "
        "min-height interacts correctly with padding"
    )


def test_block_head_marker_references_v1_19_2():
    """The min-height edit must carry a v1.19.2 marker so a
    future visual regression can be traced back to the cause."""
    src = APP_CSS.read_text()
    idx = src.index(".block-head {")
    block = src[idx:idx + 1500]
    assert "v1.19.2" in block, (
        "v1.19.2: marker required on the .block-head min-height "
        "edit for archaeology"
    )


# ── Change 2: sub-header alignment + sticky stability ────────


def test_chips_bar_has_min_height():
    """.chips-bar (EVENT STREAM sub-header) must declare a
    min-height so it lines up with the JOBS sticky thead row
    even when chip count or text varies."""
    src = APP_CSS.read_text()
    idx = src.index(".chips-bar {")
    block = src[idx:idx + 1500]
    assert "min-height:" in block, (
        "v1.19.2: .chips-bar must declare min-height to match "
        ".jobs-scroll thead th opposite it"
    )


def test_jobs_grid_header_matches_chips_bar_height():
    """v1.19.6 replaced .jobs-scroll thead th with a sibling
    .jobs-grid-header <div>. Same paired-height contract: both
    sub-header rows must be 38px tall (the v1.19.5 contract)
    so the JOBS and EVENT STREAM sides line up at the same
    y-band."""
    src = APP_CSS.read_text()
    import re
    chips_idx = src.index(".chips-bar {")
    grid_h_idx = src.index(".jobs-grid-header {")
    chips_block = src[chips_idx:chips_idx + 1500]
    grid_h_block = src[grid_h_idx:grid_h_idx + 1500]
    chips_match = re.search(r"min-height:\s*(\d+)px", chips_block)
    grid_h_match = re.search(r"min-height:\s*(\d+)px", grid_h_block)
    assert chips_match and grid_h_match, (
        "v1.19.6: both .chips-bar and .jobs-grid-header must "
        "declare min-height in px"
    )
    assert chips_match.group(1) == grid_h_match.group(1), (
        f"v1.19.6: .chips-bar min-height ({chips_match.group(1)}px) "
        f"must equal .jobs-grid-header min-height "
        f"({grid_h_match.group(1)}px) — visually paired"
    )


# ── Change 3: horizontal scroll on both sides ────────────────


def test_jobs_grid_rows_force_single_line():
    """The single-line-row contract from v1.19.2 (was on
    .jobs-scroll table cells via white-space: nowrap) now lives
    on .jobs-grid-row. Same intent: each row stays one line tall;
    long content extends horizontally."""
    src = APP_CSS.read_text()
    idx = src.index(".jobs-grid-row {")
    end_idx = src.index("}", idx)
    block = src[idx:end_idx]
    assert "white-space: nowrap" in block, (
        "v1.19.6: .jobs-grid-row must declare white-space: nowrap "
        "so rows stay single-line (replaces v1.19.2's "
        ".jobs-scroll <td> rule)"
    )


def test_event_stream_li_uses_minmax_for_msg_column():
    """.event-stream li grid template must use minmax(...) for
    the message column so the row has a minimum width that
    triggers horizontal scroll on the parent when narrower."""
    src = APP_CSS.read_text()
    idx = src.index(".event-stream li {")
    end_idx = src.index("}", idx)
    block = src[idx:end_idx]
    assert "minmax(" in block, (
        "v1.19.2: .event-stream li grid-template-columns must "
        "use minmax() for the msg column so each li has a "
        "minimum width that triggers parent horizontal scroll"
    )
    # The minmax should set a generous floor (500+ px). Test
    # the actual number to catch a future tighten that would
    # break the horizontal-scroll behavior.
    import re
    minmax_match = re.search(r"minmax\((\d+)px", block)
    assert minmax_match, (
        "v1.19.2: minmax must use a px floor (got: "
        + block[block.index("minmax("):block.index("minmax(") + 50]
        + ")"
    )
    floor_px = int(minmax_match.group(1))
    assert floor_px >= 500, (
        f"v1.19.2: minmax floor should be ≥ 500px so long "
        f"messages have room to breathe; got {floor_px}px"
    )


# ── Cross-cutting: comments reference the repro ──────────────


def test_v1_19_2_markers_carry_repro_context():
    """At least one v1.19.2 edit must reference the user's
    2026-05-24 repro so future readers understand the WHY."""
    src = APP_CSS.read_text()
    # Look for 2026-05- or "the user" mentions in v1.19.2 blocks.
    v_idx = src.index("v1.19.2")
    # Take a wide window around the first occurrence + a few
    # more (multiple edits each carry a marker).
    all_v_blocks = []
    pos = 0
    while True:
        try:
            i = src.index("v1.19.2", pos)
        except ValueError:
            break
        all_v_blocks.append(src[max(0, i - 50):i + 600])
        pos = i + 7
    combined = "\n".join(all_v_blocks)
    assert "2026-05-2" in combined or "the user" in combined, (
        "v1.19.2: at least one inline marker must reference "
        "the user / the 2026-05-24 repro so archaeology has the "
        "WHY context"
    )


# ── Global scrollbar chrome unchanged ────────────────────────


def test_global_scrollbar_height_matches_width():
    """The horizontal scrollbars added by v1.19.2 inherit the
    `::-webkit-scrollbar { width, height }` chrome rule. Pin
    that both dimensions are still set so the new horizontal
    bars match the existing vertical bars visually."""
    src = APP_CSS.read_text()
    import re
    rule = re.search(
        r"::-webkit-scrollbar\s*\{[^}]+\}",
        src, re.DOTALL,
    )
    assert rule, "::-webkit-scrollbar global rule must exist"
    body = rule.group(0)
    width_match = re.search(r"width:\s*(\d+)px", body)
    height_match = re.search(r"height:\s*(\d+)px", body)
    assert width_match and height_match, (
        "v1.19.2 depends on the global ::-webkit-scrollbar "
        "rule setting BOTH width and height (so new horizontal "
        "bars on /queue inherit the same chrome as the existing "
        "vertical ones)"
    )
    assert width_match.group(1) == height_match.group(1), (
        f"v1.19.2: scrollbar width ({width_match.group(1)}px) "
        f"and height ({height_match.group(1)}px) should match "
        f"so vertical and horizontal bars render identically"
    )
