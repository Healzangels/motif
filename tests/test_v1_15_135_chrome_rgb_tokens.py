"""v1.15.135 — chrome-color RGB tokens (--bg-rgb + --line-rgb).

Design-audit follow-up to v1.15.113 (which added 11 brand-color
RGB tokens). Two chrome surfaces still hardcoded their RGB
triplet inline because `--bg` and `--line` weren't included in
that pass:

  - `.topbar` background — `rgba(10,13,12, 0.85)` (sticky header
    semi-transparent at 0.85 alpha so the radial-gradient body
    bg shimmers through via the `backdrop-filter: blur(6px)`)
  - `.dlg::backdrop` — same color/alpha (modal overlay)
  - `.brand` border-bottom — `rgba(28,38,34, 0.5)` (--line at
    0.5 alpha for a softer divider under the topbar)

## Fix

Added `--bg-rgb: 10, 13, 12;` and `--line-rgb: 28, 38, 34;`
alongside the brand-color RGB block in `:root`. Migrated the
three sites to `rgba(var(--bg-rgb), 0.85)` / `rgba(var
(--line-rgb), 0.5)` form.

## Tests
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def _strip_comments(src: str) -> str:
    return re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)


def test_bg_rgb_and_line_rgb_tokens_defined():
    src = _strip_comments(APP_CSS.read_text())
    assert "--bg-rgb:" in src
    assert "--line-rgb:" in src


def test_bg_rgb_values_match_bg_hex():
    """The triplet must match --bg's hex (#0a0d0c = 10,13,12)
    so a future --bg color change keeps the two tokens in sync."""
    src = _strip_comments(APP_CSS.read_text())
    assert "--bg: #0a0d0c" in src
    assert re.search(r"--bg-rgb:\s*10,\s*13,\s*12", src)


def test_line_rgb_values_match_line_hex():
    """Same contract — --line-rgb must match --line's hex
    (#1c2622 = 28,38,34)."""
    src = _strip_comments(APP_CSS.read_text())
    assert "--line: #1c2622" in src
    assert re.search(r"--line-rgb:\s*28,\s*38,\s*34", src)


def test_no_raw_bg_or_line_rgba_remains():
    """The three known callsites must use the token form now.
    Strips comments so the v1.15.135 doc text doesn't false-
    positive."""
    src = _strip_comments(APP_CSS.read_text())
    for raw in (
        "rgba(10,13,12",
        "rgba(10, 13, 12",
        "rgba(28,38,34",
        "rgba(28, 38, 34",
    ):
        assert raw not in src, (
            f"v1.15.135: raw {raw!r} remains in app.css. Use "
            "rgba(var(--bg-rgb), …) or rgba(var(--line-rgb), …)."
        )


def test_topbar_uses_bg_rgb_token():
    src = _strip_comments(APP_CSS.read_text())
    topbar_idx = src.index(".topbar {")
    block = src[topbar_idx:topbar_idx + 800]
    assert "rgba(var(--bg-rgb), 0.85)" in block


def test_dlg_backdrop_uses_bg_rgb_token():
    src = _strip_comments(APP_CSS.read_text())
    backdrop_idx = src.index(".dlg::backdrop")
    block = src[backdrop_idx:backdrop_idx + 300]
    assert "rgba(var(--bg-rgb), 0.85)" in block
