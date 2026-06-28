"""v1.17.21 — bulk-bar dropdown panel clip fix.

the user's screenshot showed the v1.17.18 `// MORE ▾` dropdown
panel rendering THROUGH the bar's bottom edge into the next
section. The panel was visually clipped — only a thin sliver
peeked out below the bar before the next page section
overlapped it.

## Root cause

`#library-bulk-bar { overflow: hidden }` (added defensively in
v1.17.18 to prevent any layout-mid-frame scrollbar flash).
`overflow: hidden` establishes a clip context for descendants —
including absolutely-positioned children. The dropdown's
`.row-menu-panel` uses `position: absolute; top: calc(100% + 4px)`
to drop BELOW the parent button — that "below" placement is
inside the bar's clip box, which has zero vertical room below
the bar's own height. So the panel renders inside the clip but
mostly outside the bar's visible area.

The v1.17.18 paranoia about "mid-layout scroll flash" was
theoretical — `_layoutBulkBar` runs synchronously from
`updateLibrarySelectionUi` BEFORE the browser paints, so a
real-world flash window doesn't exist.

## Fix

`overflow: hidden` → `overflow: visible` (the default). The
dropdown panel can now drop below the bar's box and render
above the next section (it has `z-index: 100`).
"""

from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
APP_INIT = REPO / "app" / "__init__.py"


_CSS_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)


def _bulk_bar_block() -> str:
    """Return the `#library-bulk-bar` rule body with CSS
    comments stripped — the v1.17.21 fix carries narrative
    that mentions the retired `overflow: hidden` value, so
    the comment text would false-positive against the
    "not in block" assertions below."""
    css = APP_CSS.read_text()
    anchor = css.index("#library-bulk-bar {")
    end = css.index("}", anchor)
    raw = css[anchor:end]
    return _CSS_COMMENT_RE.sub("", raw)


def test_bulk_bar_overflow_is_visible():
    """The bar must use `overflow: visible` so the // MORE ▾
    dropdown panel can render below the bar without being
    clipped by the parent's overflow box."""
    block = _bulk_bar_block()
    assert "overflow: visible" in block, (
        "v1.17.21: #library-bulk-bar must use overflow: visible "
        "to let the absolutely-positioned dropdown panel render "
        "below the bar's box."
    )


def test_bulk_bar_does_not_use_hidden_overflow():
    """Anti-regression: `overflow: hidden` was the v1.17.18
    paranoia that turned out to clip the dropdown. Don't bring
    it back."""
    block = _bulk_bar_block()
    assert "overflow: hidden" not in block, (
        "v1.17.21: #library-bulk-bar must NOT use overflow: "
        "hidden — it clips the dropdown panel below the bar. "
        "_layoutBulkBar runs synchronously before paint so the "
        "layout-flash worry is theoretical."
    )


def test_bulk_bar_does_not_revert_to_scroll():
    """Counter-pin: the v1.17.18 retirement of `overflow-x:
    auto` still holds — we don't want the horizontal scroll
    fallback back either."""
    block = _bulk_bar_block()
    assert "overflow-x: auto" not in block, (
        "v1.17.21: the v1.17.18 swap from horizontal scroll to "
        "the // MORE ▾ dropdown must survive — don't bring "
        "overflow-x: auto back."
    )


# ── Version pin (soft floor) ──────────────────────────────────


def test_version_pinned_at_or_above_1_17_21():
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m
    found = tuple(int(x) for x in m.groups())
    assert found >= (0, 17, 21), (
        f"v1.17.21: __version__ must be >= 1.17.21 "
        f"(found {'.'.join(str(x) for x in found)})."
    )
