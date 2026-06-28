"""v1.15.130 — focus-visible on library filter pills.

Design-audit follow-up to v1.15.119 (which added focus-visible
to `.tab` + `.lib-flag-pill`). Same a11y gap class surfaced on
five more interactive primitives — every keyboard-interactive
element with hover styling should have a matching focus-visible
outline.

## Findings

Five filter-pill primitives had `:hover` rules but no
`:focus-visible` rule in the global outline block at
app.css:508+:

  - `.tdb-pill-btn` — TDB filter pills (TDB / TDB↑ / TDB△ /
    TDB✗ / etc.)
  - `.attn-pill-btn` — STATUS filter pills (▲ / !M / !P / ↺)
  - `.state-pill-btn` — DL / PL / LINK / ED filter pills (each
    row has 3-4 of these)
  - `.ed-pill-btn` — edition filter pill
  - `.pill-filter-clear` — the // ALL and // CLEAR buttons next
    to each filter row

Tabbing through any of these gave no visible focus indication —
keyboard users couldn't tell which filter was about to fire on
Enter.

## Fix

All five added to the global focus-visible selector list
alongside `.btn`, `.chip`, `.tab`, `.lib-flag-pill`, etc. Same
2px cyan outline + 2px offset visual.

Each also joined the matching focus-suppression list above so
mouse-driven focus still doesn't paint the outline (only
keyboard focus does, per the v1.12.1 contract).

## Tests
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def test_five_filter_pill_primitives_in_focus_visible_list():
    """All five primitives must appear in the global
    :focus-visible block."""
    src = APP_CSS.read_text()
    # Find the focus-visible block.
    fv_idx = src.index(":focus-visible {")
    # Walk back enough to see the full selector list.
    head = src[max(0, fv_idx - 1200):fv_idx]
    expected = [
        ".tdb-pill-btn:focus-visible",
        ".attn-pill-btn:focus-visible",
        ".state-pill-btn:focus-visible",
        ".ed-pill-btn:focus-visible",
        ".pill-filter-clear:focus-visible",
    ]
    missing = [s for s in expected if s not in head]
    assert not missing, (
        f"v1.15.130: missing from focus-visible block: {missing}. "
        "Each filter-pill primitive must paint a keyboard-focus "
        "outline."
    )


def test_focus_suppression_is_global():
    """v1.15.131 replaced the explicit per-class :focus suppression
    list with a universal `:focus { outline: none; }` rule. That
    covers ALL interactive primitives (including ops.css ones that
    were previously dropping through), so the per-class list is no
    longer needed — the global rule satisfies the v1.15.130
    contract for these five and 50+ other primitives at once.

    Sanity check: the universal rule must exist."""
    src = APP_CSS.read_text()
    # Strip comments so a comment mentioning the pattern doesn't
    # false-positive.
    import re
    code = re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)
    assert re.search(r"(?:^|\n)\s*:focus\s*\{[^}]*outline:\s*none", code), (
        "v1.15.131: universal `:focus { outline: none; }` rule "
        "missing. Without it, mouse-click leaves the browser-"
        "default focus ring on every clickable element."
    )
