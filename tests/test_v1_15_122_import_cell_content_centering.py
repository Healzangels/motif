"""v1.15.122 — import-preview cell content vertical centering.

the user's clarification on v1.15.116 after the v1.15.118 fix:

> See how the imdb falls centered on the line but then imported
> url is slightly above and the actions drop down option is way
> below the line. I want those all centered so they would all
> fall centered on the line — everything is centered horizontally
> [vertically in the cell].

## What v1.15.118 fixed and what it didn't

v1.15.118 added `#import-preview-table td { vertical-align:
middle; }` which centers the cell CONTENT line-box against the
cell's vertical mid-line. That worked for plain-text cells
(title, imdb — both render at t-base font-size).

What it didn't fix: cells whose content is a single inline-
block (`.url-link`, t-tiny) or form control (`.input-tiny`
select). Those cells' JS template literals look like:

    actionHtml = `
      <select class="input input-tiny" ...>...</select>`;

The leading whitespace (newline + indentation) gets emitted into
the cell as a whitespace text node. That text node renders at
the cell's inherited font-size (t-base 13px) and defines a text
baseline. The select / url-link then align their middles to
`baseline + half-x-height of the phantom text baseline`, not the
cell's geometric center.

Visually:
  - URL link (t-tiny 11px, smaller line-box) rides ABOVE center
  - Select (~28px tall form control) rides BELOW center

## Fix

`font-size: 0` on the two affected cells collapses the phantom
whitespace's line-box. With no text content (visible or
whitespace) at non-zero size, the cell's only inline content is
the child element. The cell's `vertical-align: middle`
(v1.15.118) then does the honest cell-content centering.

Children (.url-link, .input-tiny) re-establish their own
font-size in their own rules, so visible text inside the link /
inside the select renders at the right size.

## Tests

  - The font-size:0 rule exists on col-import-url + col-import-action
  - .url-link still has its own font-size declaration (so visible
    text doesn't collapse)
  - .input-tiny still has its own font-size declaration
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def _strip_comments(src: str) -> str:
    return re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)


def test_url_and_action_cells_have_font_size_zero():
    """Phantom-whitespace baseline killed by font-size:0 on both
    cells. Pre-fix the JS template literal's leading whitespace
    created a text node at t-base size whose baseline offset the
    inline-block / form-control children from the row centerline."""
    src = _strip_comments(APP_CSS.read_text())
    # The rule must target both cells together (kept as one block
    # so they stay in sync).
    pattern = re.compile(
        r"#import-preview-table\s+\.col-import-url,\s*\n?\s*"
        r"#import-preview-table\s+\.col-import-action\s*\{[^}]*"
        r"font-size:\s*0",
    )
    assert pattern.search(src), (
        "v1.15.122: cells .col-import-url + .col-import-action "
        "must share a `font-size: 0` rule. Pre-fix the cells "
        "rendered phantom whitespace at t-base size, pulling "
        "inline-block + form-control children off the row "
        "centerline."
    )


def test_url_link_keeps_its_own_font_size():
    """The cell's font-size:0 must be cancelled out by the
    child's explicit font-size. Otherwise the URL text would
    render at 0px (invisible)."""
    src = _strip_comments(APP_CSS.read_text())
    rule_idx = src.index("#import-preview-table .url-link {")
    rule = src[rule_idx:rule_idx + 500]
    assert "font-size: var(--t-tiny)" in rule, (
        "v1.15.122: .url-link must declare its own font-size — "
        "otherwise the parent cell's font-size:0 collapses the "
        "visible URL text."
    )


def test_input_tiny_keeps_its_own_font_size():
    """The select inside col-import-action uses .input-tiny which
    must declare its own font-size — otherwise the parent cell's
    font-size:0 would collapse the Apply/Skip option text."""
    src = _strip_comments(APP_CSS.read_text())
    rule_idx = src.index(".input-tiny {")
    rule = src[rule_idx:rule_idx + 500]
    assert "font-size: var(--t-tiny)" in rule, (
        "v1.15.122: .input-tiny must declare its own font-size "
        "so the select's options stay visible inside the cell "
        "with font-size:0."
    )


def test_cell_vertical_align_middle_still_in_place():
    """The v1.15.118 cell-level vertical-align middle is the
    prerequisite for v1.15.122's font-size:0 to actually center
    content. Don't let a future refactor drop it."""
    src = _strip_comments(APP_CSS.read_text())
    assert "#import-preview-table td { vertical-align: middle; }" in src, (
        "v1.15.122: the v1.15.118 td { vertical-align: middle; } "
        "rule must remain — it's what centers the cell content "
        "now that font-size:0 has collapsed the phantom baseline."
    )
