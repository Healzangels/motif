"""v1.15.138 — kill the sticky :focus-visible ring after a keyboard
event on a mouse-focused element.

the user on v1.15.137:

> still seeing the esc problem on some places like the filter
> chips. If I click T, then esc, then click T again to unselect,
> it's now unselected but it has a square around it like it's
> still selected from pressing esc … you can see when it's
> clicked then you press esc the surround square box becomes
> extra thick. Upon testing more it looks like it doesn't have
> to be esc but any keyboard click causes it.

## Root cause

v1.15.131 wired the focus-ring contract:

  - `:focus { outline: none; }` — kills the mouse-click ring on
    every focused element
  - `.chip:focus-visible, .src-key-btn:focus-visible, … {
       outline: 2px solid var(--cyan); outline-offset: 2px; }`
    — paints a cyan ring on keyboard-driven focus only

The contract holds IF focus modality stays mouse OR stays
keyboard. But Chromium/WebKit/Firefox all use a heuristic where
a focused-by-mouse element gets RE-CLASSIFIED to focus-visible
the moment a keyboard event fires anywhere in the document. So:

  1. Click `.src-key-btn[data-src-filter=T]` → chip focused via
     mouse, no ring
  2. Press Esc (or Tab, or arrow keys, or a letter) → browser
     flips the currently focused chip to `:focus-visible`
  3. Cyan 2px outline + 2px offset now paints around the chip
  4. Click `T` again to deselect → toggle works, but the chip
     remains focused → outline persists → chip LOOKS selected
     even though it's not

## Fix — `app/web/static/app.js`

Top-level global delegated `mousedown` listener: for any target
that matches one of the focus-visible-listed primitives, call
`preventDefault()`. This blocks the browser's default focus-
transfer-on-mousedown for mouse interactions, so the chip never
gets focused from a click → no later keyboard event can flip
`:focus-visible` on it.

Keyboard focus still works: Tab navigation doesn't fire
mousedown, so Tab → chip → Enter → click handler still moves
focus to the chip and shows the keyboard-nav ring. Accessibility
preserved.

Selector list mirrors the `:focus-visible` block in
`app/web/static/app.css:552-565`. This test enforces the mirror
so a future addition to the focus-visible list also gets the
mousedown-suppression.

## Tests
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def _strip_css_comments(src: str) -> str:
    return re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)


def _read_no_mouse_focus_list() -> list[str]:
    """Parse the NO_MOUSE_FOCUS_SEL JS array literal in app.js."""
    src = APP_JS.read_text()
    m = re.search(
        r"const NO_MOUSE_FOCUS_SEL\s*=\s*\[(.*?)\]\.join",
        src, re.DOTALL,
    )
    assert m, "v1.15.138: NO_MOUSE_FOCUS_SEL array literal missing"
    body = m.group(1)
    return re.findall(r"'(\.[A-Za-z0-9_-]+)'", body)


def _read_focus_visible_selectors() -> list[str]:
    """Parse the comma-separated selector list before the
    `:focus-visible { outline: 2px solid var(--cyan); … }` rule
    in app.css."""
    src = _strip_css_comments(APP_CSS.read_text())
    # Find the rule body. The selector list is everything between
    # the previous `}` and the opening `{` of that block.
    rule_idx = src.index("outline: 2px solid var(--cyan)")
    block_open = src.rfind("{", 0, rule_idx)
    # Walk back to the previous `}` to bound the selector list.
    prev_close = src.rfind("}", 0, block_open)
    sel_text = src[prev_close + 1:block_open]
    # Pull every `.foo:focus-visible` token.
    return re.findall(r"(\.[A-Za-z0-9_-]+):focus-visible", sel_text)


def test_global_mousedown_handler_registered():
    """The top-level global listener must exist — it's the v1.15.138
    mechanism that breaks the keyboard-event → focus-visible flip
    on a mouse-focused element."""
    src = APP_JS.read_text()
    assert "document.addEventListener('mousedown'," in src, (
        "v1.15.138: global mousedown listener missing — the fix is "
        "structurally absent."
    )
    # The handler body must contain preventDefault on a closest()
    # match of the NO_MOUSE_FOCUS_SEL set.
    handler_idx = src.index("document.addEventListener('mousedown',")
    handler_block = src[handler_idx:handler_idx + 400]
    assert "closest(NO_MOUSE_FOCUS_SEL)" in handler_block
    assert "e.preventDefault()" in handler_block


def test_no_mouse_focus_list_mirrors_focus_visible_css():
    """The JS selector array must list every primitive that the
    CSS :focus-visible block paints a ring for. Otherwise a
    keyboard event on a click-focused element of an un-mirrored
    type would still flip the cyan ring on.

    Equivalent in BOTH directions — neither side may grow without
    the other (else the contract drifts)."""
    js_set = set(_read_no_mouse_focus_list())
    css_set = set(_read_focus_visible_selectors())
    missing_in_js = css_set - js_set
    extra_in_js = js_set - css_set
    assert not missing_in_js, (
        f"v1.15.138: {sorted(missing_in_js)} have `:focus-visible` "
        "rings in app.css but are MISSING from NO_MOUSE_FOCUS_SEL "
        "in app.js — a keyboard event on a click-focused element "
        "of these types will still flip the sticky cyan outline."
    )
    assert not extra_in_js, (
        f"v1.15.138: {sorted(extra_in_js)} are in NO_MOUSE_FOCUS_SEL "
        "but have no `:focus-visible` ring in app.css — either add "
        "the CSS rule or drop them from the JS list (they don't "
        "need the suppression)."
    )


def test_chip_and_src_key_btn_in_suppression_list():
    """Specific anchors for the bug the user reported — SRC `T` chip
    is `.src-key-btn` (+ `.link-badge`); job/event filter chips are
    `.chip`. Both must be in the list."""
    js_list = _read_no_mouse_focus_list()
    assert ".chip" in js_list, (
        "v1.15.138: `.chip` missing from NO_MOUSE_FOCUS_SEL — the "
        "// JOBS + // EVENT STREAM filter chips would still suffer "
        "the focus-visible stick after a keyboard event."
    )
    assert ".src-key-btn" in js_list, (
        "v1.15.138: `.src-key-btn` missing — the user's exact repro "
        "(SRC T chip + Esc) would still bug out."
    )
    assert ".link-badge" in js_list, (
        "v1.15.138: `.link-badge` missing — SRC legend buttons "
        "carry both .src-key-btn AND .link-badge classes; the "
        "preventDefault must apply to either class match."
    )
