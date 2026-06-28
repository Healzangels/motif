"""v1.15.87 — Action select sizes to natural content + height trim.

the user on v1.15.86 (after the v1.15.84 equal-flanking fix):

> "a bit better, it looks like might be still too close when there
>  is an action drop down option available. Also the drop down menu
>  is not centered on the row it looks to be sitting on the bottom"

Two issues in one ship:

## 1. URL→ACTION gap too tight on CLEAN rows

The v1.15.80 fix set the action select to `width: 100%` (defense
against native `<select>` chevron overflow). On CLEAN rows the
select has just two short options ("Apply" / "Skip") — natural
content width ~80px — but the explicit width:100% forced it to
fill the entire 160px ACTION column. The select's left edge
landed ~14px past the URL column's right edge (the td's
padding-left), tightening the URL→ACTION gap to ~44px while the
IMDB→CURRENT and CURRENT→URL gaps (after v1.15.84) were ~62px.

Fix: drop the explicit `width: 100%`. With `max-width: 100%`
still in place, the select sizes to its natural content
(~80px on CLEAN rows) and centers in the column via the cell's
text-align:center. URL→ACTION gap on CLEAN rows is now closer
to the inter-column rhythm. CONFLICT rows (with the wide
"Download backup (Plex stays)" option) still hit max-width:100%
and render at full column width — unchanged.

## 2. Apply dropdown sitting at the row's bottom

The v1.15.83 fix added `vertical-align: middle` to the select.
This aligns the select's vertical midpoint with the line-box
baseline, which is supposed to put it on the row's midline.
But the user still saw the dropdown sitting at the bottom of
the row.

Investigation: the JS renders `<select class="input input-tiny" ...>`
but `.input-tiny` has NO CSS rule (silent gap). The select
inherits ONLY `.input`'s `padding: 8px 12px` — making its
natural height ~32px while the surrounding text cells (using
`.table-tight` padding 6 + text ~14 + 6 = 26px) are shorter.
Row height stretches to fit the select; the select fills more
of the row vertically than the text content, creating a visual
bottom-bias even when the select's geometric center IS at the
row midline.

Fix: trim the select's padding from inherited 8/12 to 4/10.
Select height drops from ~32px to ~22px, closer to text cells.
Row height drops correspondingly. The dropdown reads as
centered in the row.
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def _action_select_rule() -> str:
    """v1.15.88: the .col-import-action select per-site rule was
    retired (its properties migrated to .input-tiny). The JS still
    sets class='input input-tiny' on the select so the cascade
    applies — but the canonical location is now the primitive.
    All v1.15.87 tests below read .input-tiny."""
    css = APP_CSS.read_text()
    rule_start = css.index(".input-tiny {")
    rule_end = css.index("}", rule_start)
    return css[rule_start:rule_end]


def _action_select_rule_no_comments() -> str:
    """Strip /* ... */ blocks so substring checks against the
    rule body don't get false positives from narrative comments
    that mention CSS values we're guarding against."""
    import re
    rule = _action_select_rule()
    return re.sub(r"/\*.*?\*/", "", rule, flags=re.DOTALL)


# ── 1. URL→ACTION spacing fix (width: auto) ──────────────────


def test_action_select_no_longer_pinned_to_full_column_width():
    """Counter-guard against `width: 100%` returning to the
    action select. The v1.15.87 fix removes it so the select
    sizes to its natural content on CLEAN rows. If a future
    refactor re-adds `width: 100%`, the tight URL→ACTION gap
    on CLEAN rows comes back — failing this test gives the
    refactorer the link back to the user's screenshot."""
    import re
    rule = _action_select_rule_no_comments()
    # `width: 100%` as a property declaration: line-anchored,
    # not `max-width: 100%` (which is allowed and required).
    # The check looks for `^\s*width:` to avoid the "max-width"
    # false positive.
    width_decl = re.search(r"^\s*width\s*:\s*100%", rule, re.MULTILINE)
    assert width_decl is None, (
        "v1.15.87: the action select must NOT have a `width: 100%` "
        "property declaration (max-width:100% IS allowed and required). "
        "Pre-fix the width:100% pinned the select to fill the entire "
        "column on CLEAN rows ('Apply'/'Skip' options), tightening "
        "the URL→ACTION gap below the inter-column rhythm the user's "
        "v1.15.84 equal-flanking fix established. Without width:100% "
        "the select sizes to natural content and centers via "
        "text-align:center on the cell."
    )


def test_action_select_max_width_safeguard_preserved():
    """v1.15.80's max-width:100% is the structural overflow
    defense — it MUST survive the v1.15.87 width: 100% removal.
    Without max-width:100% a CONFLICT row's 'Download backup
    (Plex stays)' option would expand the select past the
    column boundary (the v1.15.80 bug class)."""
    rule = _action_select_rule()
    assert "max-width: 100%" in rule, (
        "v1.15.87 must preserve v1.15.80's max-width:100% as the "
        "structural overflow guard. Without it CONFLICT rows' wide "
        "options re-introduce the action-column spill."
    )


# ── 2. Vertical-centering fix (padding trim) ─────────────────


def test_action_select_padding_trimmed_for_height_parity():
    """v1.15.87: explicit `padding: 4px 10px` overrides the
    inherited `.input` 8/12 so the select's height matches
    the surrounding text cells more closely. Without this
    override the row stretches to fit the select's ~32px
    natural height, and the select fills more of the lower
    half of the row than the upper — the user's "looks to be
    sitting on the bottom"."""
    rule = _action_select_rule()
    assert "padding: 4px 10px" in rule, (
        "v1.15.87: action select needs explicit `padding: 4px 10px` "
        "to match the .table-tight cell rhythm. Without it the "
        "select inherits .input's 8/12 padding (no .input-tiny "
        "rule exists) and renders ~32px tall vs text cells ~26px "
        "— stretching the row and creating the bottom-aligned "
        "perception the user flagged."
    )


def test_action_select_vertical_align_middle_preserved():
    """v1.15.83's vertical-align:middle should survive — it's
    the line-box-level fix while v1.15.87's padding trim is the
    geometric fix. Both work together."""
    rule = _action_select_rule()
    assert (
        "vertical-align: middle" in rule
        or "vertical-align:middle" in rule
    ), (
        "v1.15.83's vertical-align:middle on the select must "
        "remain alongside v1.15.87's padding trim. Both serve "
        "the same goal (vertical centering) at different layers."
    )


def test_action_select_box_sizing_border_box_preserved():
    """v1.15.80's box-sizing:border-box is also load-bearing —
    survives v1.15.87 unchanged."""
    rule = _action_select_rule()
    assert "box-sizing: border-box" in rule, (
        "v1.15.87 must preserve box-sizing:border-box. Without it "
        "padding adds to width and the select can overflow the "
        "column (v1.15.80 bug class)."
    )
