"""v1.15.76 — import polish round 6 (the user feedback on v1.15.74).

Three issues:

1. **Info button stretching weirdly.** v1.15.73 added a custom
   .import-info-btn rule with 1px 8px padding but didn't override
   .btn-tiny's 72px min-width. Result: the "i" button rendered as
   a thin tall rectangle 72px wide, much taller than necessary
   for a single glyph. In the CURRENT cell (90px wide) it shoved
   the U badge sideways and made the cell appear to overflow into
   the IMPORTED URL column — likely the "actions are also still
   spilling over" the user still saw. Library rows already solved
   this with .row-info-btn { min-width: 30px !important; }
   v1.15.76: reuse the library class + the ⓘ glyph.

2. **// PREVIEW IMPORT button tone.** the user: "let's also make
   the preview import button purple to match other url button
   looks." The import workflow is user-URL-themed (writes
   user_overrides, flips rows to U-source), so it should adopt
   the violet U-tone (.btn.lib-source-user — same color as the
   SOURCE → USER URL action button + the U row pill + the
   source-pie U slice). Was .btn-info (cyan) from v1.15.66.

3. **Action column overflow.** Likely root cause is the
   stretched info button in CURRENT pushing every subsequent
   cell rightward past its column boundary. With the info-
   button width fixed to 30px (.row-info-btn), CURRENT fits
   in 90px = 30 (badge) + 6 (gap) + 30 (button) = 66px + slack.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── 1. Info button reuses library .row-info-btn ─────────────


def test_import_info_btn_uses_library_row_info_btn_class():
    """The CURRENT cell's info button must carry the .row-info-btn
    class so it inherits the library's 30px min-width override.
    Without it the button inherits .btn-tiny's 72px min-width
    and renders as a thin stretched rectangle (the user: "weirdly
    stretching")."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindImportPanel()")
    fn_end = js.index("// ---- Config form", fn_start)
    body = js[fn_start:fn_end]
    # Find the infoBtn template literal.
    info_idx = body.index("const infoBtn")
    chunk = body[info_idx:info_idx + 800]
    assert "row-info-btn" in chunk, (
        "v1.15.76: import preview info button must use "
        ".row-info-btn (library's 30px-min-width glyph button) "
        "so it doesn't render as a stretched rectangle"
    )


def test_import_info_btn_uses_circled_info_glyph():
    """Match the library row info button's ⓘ glyph (U+24D8 circled
    latin small letter i). The plain 'i' character v1.15.73 used
    was visually weak; the circled glyph reads as a button hint
    even when there's no border."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindImportPanel()")
    fn_end = js.index("// ---- Config form", fn_start)
    body = js[fn_start:fn_end]
    info_idx = body.index("const infoBtn")
    chunk = body[info_idx:info_idx + 800]
    assert "ⓘ" in chunk or "ⓘ" in chunk, (
        "v1.15.76: info button glyph must be ⓘ (U+24D8) to match "
        "the library row info button — the user: 'same looking info "
        "button from the libraries section.'"
    )


def test_app_css_no_longer_has_custom_import_info_btn_rule():
    """Counter-guard: the v1.15.73 custom .import-info-btn rule
    must not be reintroduced. .row-info-btn handles the styling
    now — any custom width/padding here would fight the library
    convention and re-introduce stretching."""
    css = APP_CSS.read_text()
    # The selector pattern that would re-introduce custom sizing.
    # .row-info-btn pattern is fine to keep, just not a custom
    # rule SCOPED to #import-preview-table.
    bad_pattern = "#import-preview-table .import-info-btn {"
    assert bad_pattern not in css, (
        "v1.15.76: must not re-add the v1.15.73 custom "
        ".import-info-btn rule — let .row-info-btn handle sizing"
    )


# ── 2. // PREVIEW IMPORT button uses violet (lib-source-user) ──


def test_preview_import_button_uses_standard_tone():
    """v1.19.25 REVERSED this v1.15.76 stance. the user on
    v1.19.24: "can we also change the preview import button
    back to the standard button color." The button is now the
    default green .btn (no .lib-source-user modifier). The
    intent grouping with the U-row palette is gone visually
    but the page header + intro paragraph still anchor the
    workflow's identity."""
    html = SETTINGS_HTML.read_text()
    pos = html.index('id="import-preview-btn"')
    btn_start = html.rfind("<button", 0, pos)
    btn_end = html.index(">", pos)
    btn = html[btn_start:btn_end + 1]
    assert "lib-source-user" not in btn, (
        "v1.19.25 (reverses v1.15.76): // PREVIEW IMPORT button "
        "must NOT use .lib-source-user — the user wants standard "
        "green button"
    )
    assert 'class="btn btn-tiny"' in btn


def test_preview_import_button_no_longer_btn_info():
    """Counter-guard: the v1.15.66 .btn-info (cyan) class must
    be gone from the preview button — would clash with the new
    violet tone."""
    html = SETTINGS_HTML.read_text()
    pos = html.index('id="import-preview-btn"')
    btn_start = html.rfind("<button", 0, pos)
    btn_end = html.index(">", pos)
    btn = html[btn_start:btn_end + 1]
    assert "btn-info" not in btn, (
        "v1.15.76: btn-info (cyan) must not coexist with "
        "lib-source-user (violet) on the preview button"
    )


# ── 3. CURRENT cell sized to fit badge + library info button ──


def test_current_cell_fits_badge_plus_library_info_button():
    """The CURRENT column width (90px) must accommodate the
    badge (~30px) + gap (6px) + library info button (.row-info-btn,
    min-width 30px) = 66px. With the v1.15.73-75 stretched
    button (72px min-width from .btn-tiny) the math was
    30+6+72 = 108px, overflowing the 90px cell and pushing
    downstream columns right.

    This test pins the COMBINATION: column width >= max(
    badge + info button + gap, 60px).
    """
    import re
    css = APP_CSS.read_text()
    # CURRENT column width.
    cur_idx = css.index("#import-preview-table .col-import-current")
    cur_end = css.index("}", cur_idx)
    cur_rule = css[cur_idx:cur_end]
    m = re.search(r"width:\s*(\d+)px", cur_rule)
    assert m, "v1.15.76: .col-import-current must declare width in px"
    cur_width = int(m.group(1))
    # .row-info-btn min-width must be 30 (library convention).
    row_info_idx = css.index(".row-info-btn {")
    row_info_end = css.index("}", row_info_idx)
    row_info = css[row_info_idx:row_info_end]
    m = re.search(r"min-width:\s*(\d+)px", row_info)
    assert m, "Precondition: .row-info-btn must declare min-width in px"
    info_min = int(m.group(1))
    # Allow some breathing room for padding/border.
    needed = info_min + 30 + 6 + 16  # badge + gap + padding slack
    assert cur_width >= needed, (
        f"v1.15.76: CURRENT cell width {cur_width}px is too narrow "
        f"to fit badge + {info_min}px info button + gap + padding "
        f"(needs ≥ {needed}px). Risk: cell overflow pushes ACTION "
        "column right past the table boundary."
    )
