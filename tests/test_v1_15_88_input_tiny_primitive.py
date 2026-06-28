"""v1.15.88 — `.input-tiny` as a real primitive (the user's design-system rule).

the user's v1.15.87 design-system discipline rule prompted an audit
of the recurring CSS gaps caught during the v1.15.66-87 import-
preview cascade. The first acted-on follow-up: `.input-tiny`
was referenced in JS template literals (line 4646, 4655 of
app.js — the import-preview action `<select>`) but had **no
matching CSS rule** until v1.15.88. The select inherited only
`.input`'s 8/12 padding sized for full-width dialog inputs.

Three patches lived per-site on `.col-import-action select`:
* v1.15.80 — overflow defense (box-sizing + max-width + min-width)
* v1.15.83 — vertical-align:middle (line-box alignment)
* v1.15.87 — padding trim (geometric-height parity)

v1.15.88 promotes those properties up into the `.input-tiny`
primitive. The per-site rule is retired (now a comment block
explaining the migration). Future selects or inputs using the
`.input-tiny` class inherit all three defenses automatically —
no per-site duplication, no risk of forgetting one.

This is a design-system-discipline ship: net code reduction
(one primitive replaces a duplicated rule), centralized
canonical location, and the silent-gap class is closed.

Tests guard:
* The primitive exists with the canonical properties
* The per-site `.col-import-action select` rule has been
  retired (counter-guard against a future "let me just add a
  rule here" regression that would re-introduce duplication)
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def _input_tiny_rule() -> str:
    """Extract the .input-tiny rule body. Comments inside the body
    may contain `}` characters (e.g., when quoting other rules'
    syntax) — those are stripped before searching for the closing
    brace so we don't truncate mid-rule."""
    css = APP_CSS.read_text()
    rule_start = css.index(".input-tiny {")
    # Strip /* ... */ blocks from the slice AFTER the open brace
    # before looking for the close brace. Otherwise a `}` inside
    # a comment trips the search.
    after_open = css[rule_start + len(".input-tiny {"):]
    stripped = re.sub(r"/\*.*?\*/", "", after_open, flags=re.DOTALL)
    close_idx = stripped.index("}")
    return stripped[:close_idx]


# ── 1. .input-tiny primitive shape ────────────────────────────


def test_input_tiny_primitive_exists():
    """The .input-tiny class must have an actual CSS rule. Pre-v1.15.88
    the class was referenced in JS but had no rule (silent gap caught
    in v1.15.87)."""
    css = APP_CSS.read_text()
    assert ".input-tiny {" in css, (
        "v1.15.88: .input-tiny must be a real CSS rule. Pre-fix the "
        "class was used in JS template literals (app.js:4646/4655) "
        "but had no matching CSS rule — the silent-gap pattern "
        "the user's design-system rule targets."
    )


def test_input_tiny_has_compact_padding():
    """`.input-tiny` overrides `.input`'s 8/12 padding with 4/10 so
    the control's height matches surrounding table-tight cells.
    Was a per-site override on `.col-import-action select` (v1.15.87)
    before v1.15.88 migrated it to the primitive."""
    rule = _input_tiny_rule()
    assert "padding: 4px 10px" in rule, (
        "v1.15.88: .input-tiny needs `padding: 4px 10px`. The "
        "primitive's whole point is height-parity with table-tight "
        "cells (.table-tight padding 6/10 + text ~14 → cell ~26px); "
        ".input's inherited 8/12 padding made the control ~32px and "
        "stretched the row, creating the bottom-bias visual that "
        "v1.15.87 worked around per-site."
    )


def test_input_tiny_has_tiny_font_size():
    """Compact controls should use the t-tiny token to match other
    table-tight content."""
    rule = _input_tiny_rule()
    assert "font-size: var(--t-tiny)" in rule, (
        "v1.15.88: .input-tiny should use var(--t-tiny) — matches "
        ".table-tight cell font-size for visual consistency."
    )


def test_input_tiny_absorbs_v1_15_80_overflow_defenses():
    """The structural overflow defenses (box-sizing + max-width +
    min-width:0) move from `.col-import-action select` to
    `.input-tiny`. Future selects in tight cells get them for free."""
    rule = _input_tiny_rule()
    assert "box-sizing: border-box" in rule, (
        "v1.15.88 must preserve v1.15.80's box-sizing:border-box "
        "in the migrated location (.input-tiny). Without it form "
        "controls can overflow their parent."
    )
    assert "max-width: 100%" in rule, (
        "v1.15.88 must preserve v1.15.80's max-width:100% in "
        ".input-tiny — the hard ceiling against native chevron "
        "widening."
    )
    assert "min-width: 0" in rule, (
        "v1.15.88 must preserve v1.15.80's min-width:0 in "
        ".input-tiny — .input's flex-context 240px min-width is "
        "wrong for tight cells."
    )


def test_input_tiny_absorbs_v1_15_83_vertical_align():
    """v1.15.83's line-box vertical-align fix migrates into
    .input-tiny so every consumer benefits."""
    rule = _input_tiny_rule()
    assert "vertical-align: middle" in rule, (
        "v1.15.88 must preserve v1.15.83's vertical-align:middle "
        "in .input-tiny. Form controls default to baseline; in a "
        "table row with mixed cell heights, baseline-aligned "
        "controls sit at the bottom of the line box."
    )


def test_input_tiny_overrides_flex_default():
    """`.input` has `flex: 1` for the dialog-input case. In a tight
    cell the control should size to its content, not flex to fill —
    so `.input-tiny` overrides flex."""
    rule = _input_tiny_rule()
    assert "flex: 0 0 auto" in rule or "flex: none" in rule, (
        "v1.15.88: .input-tiny should override .input's `flex: 1` "
        "so tight-cell controls don't try to flex-grow."
    )


# ── 2. Counter-guard: per-site rule is retired ────────────────


def test_per_site_action_select_rule_retired():
    """The `#import-preview-table .col-import-action select` rule
    body was retired in v1.15.88 — its properties moved to
    `.input-tiny`. A future PR that re-introduces the rule (e.g.
    "let me just add a quick override here") would silently
    duplicate properties that already live at the primitive. This
    test fires on that pattern.

    The selector ITSELF may still appear in narrative comments
    explaining the migration — we check that no property
    declarations follow it (no `{ ... property: value; ... }`).
    """
    css = APP_CSS.read_text()
    # Look for the selector followed by an opening brace (a real
    # rule), not just the string in a comment.
    pattern = re.compile(
        r"#import-preview-table\s+\.col-import-action\s+select\s*\{",
    )
    # The match must NOT be inside a /* ... */ comment.
    css_no_comments = re.sub(r"/\*.*?\*/", "", css, flags=re.DOTALL)
    match = pattern.search(css_no_comments)
    assert match is None, (
        "v1.15.88: the per-site `.col-import-action select` rule "
        "was retired. If a future change needs to override behavior "
        "for that specific select, add the property to the "
        ".input-tiny primitive instead (or build a new modifier "
        "class) — don't re-introduce the duplicated rule."
    )


# ── 3. JS still references .input-tiny ─────────────────────────


def test_action_select_jsmarkup_uses_input_tiny_class():
    """Pin that the JS template literals continue to apply the
    `.input-tiny` class — otherwise the new primitive's styles
    won't reach the action select via the cascade."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # CLEAN and CONFLICT branches both render `class="input input-tiny"`.
    matches = re.findall(
        r'class="input input-tiny"\s+data-import-action',
        js,
    )
    assert len(matches) >= 2, (
        f"v1.15.88: expected >=2 `class=\"input input-tiny\" "
        f"data-import-action` matches in app.js (CLEAN + CONFLICT "
        f"render branches). Found: {len(matches)}. If the class was "
        "renamed, the primitive's styles won't reach the select."
    )
