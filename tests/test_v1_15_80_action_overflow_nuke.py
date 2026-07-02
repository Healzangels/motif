"""v1.15.80 — kill the recurring action-column overflow + VERIFY-on-DUPLICATE noise.

the user's v1.15.77 feedback round (also v1.15.71/73/76 prior):
"the action is still spilling out over the border edge. This
issue keeps spilling over from release to release can we make
sure we get this nailed down in this release."

Each prior round shaved column widths in response to the same
visual report. Width-shaving alone hadn't worked because the
root cause is the native `<select>` element ignoring its
column's `width:100%` — default `box-sizing:content-box` adds
the browser native padding + chevron-width on top of the
declared width, so the rendered select hugs (or slightly
exceeds) the column boundary even at 140px.

v1.15.80 fixes it with three defenses:
* box-sizing:border-box on both the table + the action select
  so widths are inclusive of padding/border
* max-width:100% as a hard ceiling — the select cannot exceed
  its column regardless of native rendering
* col-import-action trimmed 140 → 120 with explicit padding-
  right:12px for breathing room from the panel boundary
* min-width:0 on the select so flex contexts can shrink it
  below the browser native default if needed

Plus VERIFY-on-DUPLICATE cleanup. the user: "what does the
duplicate verify status indicate?" VERIFY means the row
matched via title+year fallback (not exact IMDB). For
DUPLICATE rows the badge is irrelevant — no apply, no need
to verify. Suppress.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── 1. VERIFY badge suppressed on DUPLICATE rows ────────────


def test_verify_badge_hidden_on_duplicate_rows():
    """The VERIFY badge must NOT render for DUPLICATE rows.
    DUPLICATE is a definitional no-op — no apply, no need to
    verify the match. The badge was visual noise on those rows."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindImportPanel()")
    fn_end = js.index("// ---- Config form", fn_start)
    body = js[fn_start:fn_end]
    # The verify-gate must include a duplicate exclusion.
    assert "r.status !== 'duplicate'" in body, (
        "v1.15.80: verify badge gate must exclude duplicate rows "
        "— the badge is irrelevant for no-op rows"
    )


# ── 2. Table-level box-sizing + max-width safeguards ────────


def test_import_table_uses_border_box_sizing():
    """The table must declare box-sizing:border-box so width:100%
    is inclusive of padding/border. Without it the table can
    exceed its container width by the padding amount."""
    css = APP_CSS.read_text()
    # v0.51.5: anchor on the multi-line base rule — a one-liner
    # `#import-preview-table { min-width: 880px; }` mobile floor now precedes it.
    sel_idx = css.index("#import-preview-table {\n")
    block_end = css.index("}", sel_idx)
    block = css[sel_idx:block_end]
    assert "box-sizing: border-box" in block, (
        "v1.15.80: #import-preview-table must declare box-sizing:"
        "border-box so width:100% accounts for padding/border"
    )
    assert "max-width: 100%" in block, (
        "v1.15.80: #import-preview-table must cap at max-width:"
        "100% as a hard ceiling against any cascaded width override"
    )


# ── 3. Action select can't exceed its column ────────────────


def test_action_select_has_border_box_and_max_width_safeguards():
    """The action <select> must declare box-sizing:border-box +
    max-width:100% so it can never render wider than its column.
    Native <select> elements include chevron-rendering width that
    bleeds past width:100% under default box-sizing.

    v1.15.88: these properties migrated from the per-site
    `#import-preview-table .col-import-action select` rule into
    the `.input-tiny` primitive — the JS template literal uses
    `class="input input-tiny"` so the cascade still applies, but
    the canonical location is now the primitive. Check `.input-tiny`
    directly."""
    css = APP_CSS.read_text()
    rule_start = css.index(".input-tiny {")
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    assert "box-sizing: border-box" in rule, (
        "v1.15.80 → v1.15.88: .input-tiny must be "
        "box-sizing:border-box so its native padding/chevron "
        "doesn't add to width:100%"
    )
    assert "max-width: 100%" in rule, (
        "v1.15.80 → v1.15.88: .input-tiny needs max-width:100% — "
        "the hard ceiling that prevents browser native rendering "
        "from pushing the control past its parent"
    )


def test_action_column_has_padding_right_for_breathing_room():
    """The action column gains explicit padding-right so the
    rendered select has visible margin from the panel boundary
    instead of hugging it (the visual "spill" the user flagged)."""
    css = APP_CSS.read_text()
    rule_start = css.index("#import-preview-table .col-import-action {")
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    assert "padding-right:" in rule, (
        "v1.15.80: .col-import-action must declare padding-right "
        "for breathing room between the select and the panel edge"
    )


# ── 4. Column widths sum check tightens further ─────────────


def test_column_widths_sum_under_800px():
    """v1.15.80 pinned this at <650 as a counter-guard while
    width-shaving was still the proposed fix path. Once the
    structural fix landed (border-box + max-width:100% +
    min-width:0 on the select), the budget cap stopped serving
    as overflow defense — the rendering can't exceed the column
    regardless of width. v1.15.83 raises the cap to 800 to allow
    the cluster-spacing fix (right-side columns widened so the
    IMDB / CURRENT / URL / ACTION cluster isn't visually bunched).
    The structural rules still prevent any actual overflow."""
    import re
    css = APP_CSS.read_text()
    cols = ["col-state", "col-import-status", "col-imdb",
            "col-import-current", "col-import-url",
            "col-import-action"]
    total = 0
    for col in cols:
        rule_start = css.index(f"#import-preview-table .{col} {{")
        rule_end = css.index("}", rule_start)
        rule = css[rule_start:rule_end]
        m = re.search(r"width:\s*(\d+)px", rule)
        assert m, f"v1.15.80: missing pixel width for .{col}"
        total += int(m.group(1))
    assert total < 800, (
        f"v1.15.83: fixed-width columns sum to {total}px — over the "
        "800px ceiling. Even with structural overflow defense in "
        "place, leaving TITLE less than ~500px on a 1280-wide "
        "viewport would start cutting common-length titles off."
    )
