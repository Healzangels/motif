"""v1.15.127 — native checkbox + scrollbar styling.

Design-audit pass on visual gaps where motif fell through to
browser-default chrome that clashes with the green CRT palette.

## Findings

**Checkboxes (14 sites)** — every `<input type="checkbox">` in
templates + JS-injected ones rendered with the browser's default
blue accent color (Chrome: cobalt; Safari: system blue). Against
the green palette this read as "system-injected widget,"
breaking the otherwise-cohesive theming.

**Scrollbars** — no custom styling. WebKit / Blink browsers
rendered the default light-gray bar against motif's dark CRT
chrome. Gecko handled it slightly better via auto-dark detection
but still light. Both stuck out against the rest of the surface.

## Fixes

Single-line `accent-color: var(--green)` on
`input[type="checkbox"]` tints the native checkmark without the
`appearance: none` custom-checkbox rabbit hole. Keyboard +
screen-reader behavior stays intact; only the visual tint
changes.

For scrollbars, the two-axis approach handles both engines:

  - Gecko: `scrollbar-color: var(--line-bright) var(--bg)` +
    `scrollbar-width: thin`
  - WebKit/Blink: `::-webkit-scrollbar-*` rules with --line-bright
    thumb, --bg track, --green-deep hover

The thumb uses `border: 2px solid var(--bg)` for the inset
"floating bar" look that matches motif's table-divider weight.

## Tests
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def _strip_comments(src: str) -> str:
    return re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)


def test_checkbox_uses_brand_accent_color():
    """`input[type="checkbox"]` must declare `accent-color: var
    (--green)` so the native checkmark renders in the brand
    palette instead of the browser default."""
    src = _strip_comments(APP_CSS.read_text())
    pattern = re.compile(
        r'input\[type="checkbox"\]\s*\{[^}]*'
        r"accent-color:\s*var\(--green\)",
        re.DOTALL,
    )
    assert pattern.search(src), (
        "v1.15.127: `input[type=\"checkbox\"] { accent-color: var"
        "(--green); }` rule missing. Without it, motif's 14+ "
        "checkboxes render with the browser-default blue accent "
        "against the green CRT palette."
    )


def test_scrollbar_gecko_color_tokens():
    """Gecko: `scrollbar-color: <thumb> <track>` + `scrollbar-
    width: thin` for the dark CRT theme."""
    src = _strip_comments(APP_CSS.read_text())
    # v1.15.133: thumb token bumped --line-bright → --fg-mute.
    # Accept either form (both are brand tokens; future tuning
    # may shift again).
    assert (
        "scrollbar-color: var(--fg-mute) var(--bg)" in src
        or "scrollbar-color: var(--line-bright) var(--bg)" in src
    )
    assert "scrollbar-width: thin" in src


def test_scrollbar_webkit_track_thumb_hover():
    """WebKit/Blink: track + thumb + hover rules with brand
    tokens. The thumb's `border: 2px solid var(--bg)` is what
    creates the inset 'floating bar' look that matches motif's
    table-divider visual weight."""
    src = _strip_comments(APP_CSS.read_text())
    assert "::-webkit-scrollbar-track" in src
    assert "::-webkit-scrollbar-thumb" in src
    assert "::-webkit-scrollbar-thumb:hover" in src
    # Hover bumps to --green-deep so the affordance reads as
    # interactive rather than static.
    assert "var(--green-deep)" in src[
        src.index("::-webkit-scrollbar-thumb:hover"):
        src.index("::-webkit-scrollbar-thumb:hover") + 200
    ]


def test_scrollbar_thumb_uses_design_tokens_not_raw_color():
    """Defense against future refactors that swap brand tokens
    for raw color values.

    v1.15.133: thumb color bumped --line-bright → --fg-mute so
    the bar reads as a solidly styled affordance instead of
    blending with the dark chrome dividers. Accept either token
    so a future tuning pass can shift palettes without breaking
    the lint."""
    src = _strip_comments(APP_CSS.read_text())
    idx = src.index("::-webkit-scrollbar-thumb {")
    block = src[idx:idx + 300]
    has_brand_token = (
        "background: var(--fg-mute)" in block
        or "background: var(--line-bright)" in block
    )
    assert has_brand_token, (
        "v1.15.127/.133: scrollbar thumb must use a brand color "
        "token (--fg-mute or --line-bright) to stay in sync with "
        "the rest of the chrome palette."
    )
