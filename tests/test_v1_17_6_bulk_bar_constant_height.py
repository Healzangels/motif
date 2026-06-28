"""v1.17.6 — bulk-action bar constant height regardless of button count.

the user's repro: "see how the bulk actions bar is now size and
height, when you have too many shows it grows in height as the
bulk actions below text has to stack itself. And if enough are
added the size of the export csv box grows in height. Would love
to make it consistent no matter how many there are and the
height and box size remain constant."

Root cause: `#library-bulk-bar` inherits `.missing-banner`'s
default flex shape — `flex-wrap: nowrap` but children
compressible. At ≥7 buttons each child gets squeezed below its
natural width, browser text-wrap kicks in inside each button
("// SELECT ALL\nFILTERED"), and the left "N selected · bulk
actions below" caption stacks to 3 lines. Bar grows ~30-80px.

Fix: scope CSS to `#library-bulk-bar` so:
- `flex-wrap: nowrap` + `overflow-x: auto` — bar stays one row;
  horizontal scroll when content exceeds container width
- `> *` `flex-shrink: 0` — children don't compress below
  natural width
- `.btn { white-space: nowrap }` — button labels stay
  single-line
- `.missing-banner-text` allowed shrink but pinned `min-width:
  160px` + `white-space: nowrap` so the caption stays visible
  and intact
- `min-height: 56px` — empty + crowded states feel identical
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def _bulk_bar_block() -> str:
    """Return the CSS block scoped to #library-bulk-bar.

    The multiple #library-bulk-bar rules cluster together with an
    inline `/* The left ... caption */` comment between them. Use
    a fixed window after the first #library-bulk-bar anchor so
    all sibling rules are captured."""
    css = APP_CSS.read_text()
    # v1.18.46: anchor-based slicing. Pre-fix this was a fixed
    # 2000-char window (widened to 3000 in v1.18.44). Now slices
    # to the next top-level rule that's clearly outside the
    # bulk-bar cluster — the `.libraries-table` selector starts
    # the libraries-page table rules. Survives any future
    # additions to the bulk-bar block.
    from _slice_helpers import slice_to_next
    return slice_to_next(
        css, "#library-bulk-bar {",
        ".libraries-table",
    )


def test_bulk_bar_overflows_horizontally_with_nowrap():
    """The bar must keep one row when buttons exceed container
    width. Pre-fix the bar wrapped / squeezed children, growing
    the vertical height.

    v1.17.18: the horizontal-scroll fallback (`overflow-x: auto`)
    was retired in favor of a `// MORE ▾` overflow dropdown — the
    scroll was undiscoverable and clipped buttons silently. The
    `flex-wrap: nowrap` contract (one-row constant height) is
    preserved; overflow is now handled by the JS-driven dropdown
    instead of CSS scroll. See test_v1_17_18_bulk_bar_overflow.py
    for the new contract."""
    block = _bulk_bar_block()
    assert "flex-wrap: nowrap" in block
    # v1.17.18 swapped `overflow-x: auto` for `overflow: hidden`.
    # v1.17.21 flipped to `overflow: visible` because hidden
    # clipped the // MORE ▾ dropdown panel that
    # absolutely-positions below the bar. Pin the new contract
    # while still defending the "no horizontal scroll" rule.
    assert "overflow: visible" in block, (
        "v1.17.21: bar should use overflow:visible — hidden "
        "clipped the dropdown panel; the JS layout already "
        "prevents real overflow before paint, so the paranoia "
        "guard was paying a real bug cost."
    )
    # Counter-pin: the v1.17.18 retirement of overflow-x:auto
    # still holds.
    assert "overflow-x: auto" not in block, (
        "v1.17.18: bar must NOT use overflow-x:auto — the "
        "// MORE ▾ dropdown handles overflow now."
    )


def test_bulk_bar_pins_min_height():
    """A `min-height` anchors the bar visually so empty + crowded
    states feel identical."""
    block = _bulk_bar_block()
    assert "min-height: 56px" in block


def test_bulk_bar_children_dont_shrink_below_natural_width():
    """`#library-bulk-bar > * { flex-shrink: 0 }` keeps every
    direct child (buttons + caption) at its natural width — pre-
    fix browser shrinkage of children was the upstream cause of
    button-text wrap + caption stacking."""
    block = _bulk_bar_block()
    assert "#library-bulk-bar > *" in block
    assert "flex-shrink: 0" in block


def test_bulk_bar_buttons_no_text_wrap():
    """`.btn` inside the bulk bar must use `white-space: nowrap`
    so labels like `// SELECT ALL FILTERED` stay single-line."""
    block = _bulk_bar_block()
    assert "#library-bulk-bar .btn" in block
    # The .btn rule sets white-space: nowrap.
    btn_idx = block.index("#library-bulk-bar .btn")
    btn_rule = block[btn_idx:btn_idx + 200]
    assert "white-space: nowrap" in btn_rule


def test_bulk_bar_caption_pins_min_width_and_no_wrap():
    """The left "N selected" caption (`.missing-banner-text`) pins a
    min-width floor + stays on a SINGLE line.

    History: v1.19.98 changed it to `white-space: normal` to stop the
    overlap, but the user found the wrap "double-stacks" + looks out of
    place. v1.20.1 settled on `flex: 1` (claim leftover space, no
    overlap) + `white-space: nowrap` + `overflow: hidden` +
    `text-overflow: ellipsis` (single line, truncates gracefully)."""
    block = _bulk_bar_block()
    assert "#library-bulk-bar .missing-banner-text" in block
    caption_idx = block.index("#library-bulk-bar .missing-banner-text")
    caption_rule = block[caption_idx:caption_idx + 800]
    assert "min-width: 160px" in caption_rule
    # v1.20.1: single-line — nowrap + ellipsis (not the v1.19.98 wrap).
    assert "white-space: nowrap" in caption_rule
    assert "flex: 1;" in caption_rule
    assert "overflow: hidden" in caption_rule
    assert "text-overflow: ellipsis" in caption_rule
    assert "white-space: normal" not in caption_rule


def test_bulk_bar_overrides_dont_affect_other_missing_banner_callsites():
    """The fix is scoped to `#library-bulk-bar` specifically —
    other consumers of `.missing-banner` (e.g. the dashboard's
    backfill banner) must keep their existing wrap-friendly
    behavior. Verify the general `.missing-banner` rule still
    exists unmodified."""
    css = APP_CSS.read_text()
    # The base `.missing-banner` rule above the v1.17.6 block.
    base_idx = css.index(".missing-banner {")
    base_rule = css[base_idx:base_idx + 400]
    # Base rule uses `display: flex; align-items: center` —
    # neither overflow-x nor flex-wrap: nowrap should be in the
    # base. Those are scoped to #library-bulk-bar only.
    assert "overflow-x: auto" not in base_rule
    assert "min-height: 56px" not in base_rule
