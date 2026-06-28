"""v1.15.132 — chip-family glyph centering pass.

the user on v1.15.131:

> Looking at our chip letters and symbols a lot of them don't
> look centered in their box, can we look at all those chips
> and make sure the inside is centered.

## The bug class

`inline-block` chip primitives let the line-box baseline drive
content positioning. Letters sit at the baseline (~80% of font-
size down from line-box top), so:

  - Uppercase letter chips (T / U / A / M / P) sit slightly low
    in their box
  - Glyph chips (▲ / ⚿ / ↺ / ! / +P) sit at varying heights
    depending on each codepoint's ascender/descender
  - Mixed letter+glyph chips (TDB↑ / TDB✗) misalign internally

`.state-pill-btn` (v1.13.x) and `.ed-pill-btn` (v1.13.71) already
use the canonical fix: `display: inline-flex` + `align-items:
center` + `justify-content: center` + `line-height: 1`. Flex
centering positions content at the box's geometric middle
regardless of font metrics.

## Fix

Apply the same pattern to the rest of the chip family:

  - `.pill` (STATUS pills: CLEAN / CONFLICT / VERIFY / etc.)
  - `.chip` (filter chips: ALL / THEMED / UNTHEMED / etc.)
  - `.link-badge` (single-letter SRC + link kind: T / U / A /
    M / P / HL / C / PS — including `.src-key-btn` which extends
    `.link-badge`)
  - `.tdb-pill` + `.tdb-pill-btn` (TDB↑ / TDB△ / TDB✗ etc.)
  - `.attn-pill` + `.attn-pill-btn` (▲ / ⚿ / !M / !P / ↺)
  - `.lib-flag-pill` (A / 4K role pills)

`.state-pill-btn` and `.ed-pill-btn` already had the pattern —
left unchanged (the pass is purely about REACHING the standard,
not changing what's already there).

## What didn't change

  - Per-chip padding values stay the same — each chip's box
    size is tuned for its content
  - The letter-spacing values stay the same — desired for
    multi-character chips
  - `.attn-pill`'s `min-width: 18px` stays — keeps single-glyph
    pills the same width as the smallest letter pill

## Tests
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def _strip_comments(src: str) -> str:
    return re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)


def _rule_body(src: str, selector: str) -> str:
    """Extract the body of a top-level rule with the given selector."""
    src = _strip_comments(src)
    pattern = re.compile(
        rf"(?:^|\n){re.escape(selector)}\s*\{{([^}}]*)\}}",
        re.MULTILINE,
    )
    m = pattern.search(src)
    assert m, f"selector `{selector}` not found at top level"
    return m.group(1)


def test_pill_uses_flex_centering():
    body = _rule_body(APP_CSS.read_text(), ".pill")
    assert "display: inline-flex" in body
    assert "align-items: center" in body
    assert "justify-content: center" in body
    assert "line-height: 1" in body


def test_chip_uses_flex_centering():
    body = _rule_body(APP_CSS.read_text(), ".chip")
    assert "display: inline-flex" in body
    assert "align-items: center" in body
    assert "justify-content: center" in body
    assert "line-height: 1" in body


def test_link_badge_uses_flex_centering():
    body = _rule_body(APP_CSS.read_text(), ".link-badge")
    assert "display: inline-flex" in body
    assert "align-items: center" in body
    assert "justify-content: center" in body
    assert "line-height: 1" in body


def test_tdb_pill_uses_flex_centering():
    body = _rule_body(APP_CSS.read_text(), ".tdb-pill")
    assert "display: inline-flex" in body
    assert "align-items: center" in body
    assert "justify-content: center" in body
    assert "line-height: 1" in body


def test_attn_pill_uses_flex_centering():
    """The `.attn-pill` previously used inline-block + text-align:
    center — switched to flex for predictable glyph placement
    on mixed letter+symbol contents (▲ / ⚿ / !M / ↺)."""
    body = _rule_body(APP_CSS.read_text(), ".attn-pill")
    assert "display: inline-flex" in body
    assert "align-items: center" in body
    assert "justify-content: center" in body
    assert "line-height: 1" in body
    # min-width stays — it's the equal-width contract across pills.
    assert "min-width: 18px" in body


def test_lib_flag_pill_uses_flex_centering():
    body = _rule_body(APP_CSS.read_text(), ".lib-flag-pill")
    assert "display: inline-flex" in body
    assert "align-items: center" in body
    assert "justify-content: center" in body
    # line-height: 1 was already there pre-v1.15.132.
    assert "line-height: 1" in body


def test_state_pill_btn_pattern_unchanged():
    """Sanity — the chip primitives that ALREADY used the flex
    pattern (`.state-pill-btn`, `.ed-pill-btn`) shouldn't have
    been touched by v1.15.132. Both must still center."""
    src = APP_CSS.read_text()
    sb = _rule_body(src, ".state-pill-btn")
    eb = _rule_body(src, ".ed-pill-btn")
    for body in (sb, eb):
        assert "display: inline-flex" in body
        assert "align-items: center" in body
        assert "justify-content: center" in body
