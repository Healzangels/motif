"""v1.15.73 — import polish round 5 (the user feedback on v1.15.71).

Four issues:

1. **File picker still triggers on label-text click.** v1.15.70
   narrowed the file <label for="import-csv-file"> to only wrap
   label-text + input, but `<label for=...>` semantics ALSO fire
   the input on label-text clicks. For a TEXT input that's
   harmless; for a FILE input it pops the OS file dialog.
   v1.15.73 replaces the <label> with a plain <div>. Only the
   native input button opens the picker now.

2. **CURRENT cell + IMPORTED URL cell not vertically aligned.**
   v1.15.71 stacked src-badge ABOVE the URL in CURRENT (column
   flex) so neither overflowed the column, but the result was
   a 2-line CURRENT cell next to a 1-line IMPORTED URL cell —
   rows didn't align. v1.15.73: drop the URL preview from
   CURRENT entirely. The cell renders src-badge + a small info
   icon button that opens the standard // MOTIF INFO dialog
   (the user: "show the info button which allows you to see the
   current info row that row").

3. **CLEAN + skip is confusing for identical-URL rows.**
   the user: "when status is Clean and action is skip because
   it's an identical import then status should be something
   like duplicate or identical match." v1.15.73 splits
   DUPLICATE out as its own status.

4. **Action column still spilling on narrower viewports.**
   v1.15.71 widths summed to 1000+TITLE; on 1366-wide screens
   the action column overflowed. v1.15.73 totals 776+TITLE
   (CURRENT slimmed 260→90 since it only holds badge + icon).
"""
from __future__ import annotations

import csv
import io
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import get_conn, init_db, transaction


REPO = Path(__file__).resolve().parent.parent
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
API_PY = REPO / "app" / "web" / "api.py"


# ── 1. File picker no longer label-wrapped ──────────────────


def _file_input_ancestors(html: str) -> str:
    """Return the immediate parent tag containing the file input."""
    pos = html.index('id="import-csv-file"')
    # Walk back to the nearest opening tag.
    open_lt = html.rfind("<", 0, pos)
    open_gt = html.index(">", pos)
    return html[open_lt:open_gt + 1]


def test_import_file_input_IS_label_wrapped_no_for():
    """REVERSED twice since v1.15.73; this pins the CURRENT shape.

      - v1.15.73 (this tag): banned the <label> wrapper — put the
        file input in a plain <div> so label-text clicks couldn't
        pop the OS picker.
      - v1.19.25: REVERSED — the user preferred the theme-upload
        modal's wrapping shape; the input went back INSIDE a
        <label>. The footgun is avoided by dropping the `for=`
        attribute (label-text click focuses the input but does NOT
        open the file dialog — DESIGN_SYSTEM.md § 2, v1.19.25).
      - v1.22.55: settings redesign — the wrapper class is now
        `field-row` (hybrid two-column field) instead of
        `form-label`, but the invariant is unchanged: input nested
        directly in a <label>, no `for=`.

    Pre-v1.22.55 the OLD `test_import_file_input_is_NOT_wrapped_in_label`
    assertion (still pinning the reversed v1.15.73 direction) only
    passed by accident — the intermediate `form-label-row` <div> in
    the old markup satisfied its naive "nearest <div> wins" walk. The
    redesign removed that filler div and exposed the contradiction
    with this file's own `test_no_for_attr_targets_file_input_in_panel`
    + test_v1_19_25 + test_v1_19_39. Corrected here to the real shape."""
    html = SETTINGS_HTML.read_text()
    pos = html.index('id="import-csv-file"')
    # Nearest enclosing OPEN tag walking back must be a <label>
    # (the input is a direct child of the field-row label), and there
    # must be no intervening <div> filler.
    last_label = html.rfind("<label", 0, pos)
    last_div = html.rfind("<div", 0, pos)
    assert last_label > last_div, (
        "v1.19.25/v1.22.55: the CSV file input must be a direct child "
        "of a <label> (field-row) — not wrapped in a filler <div>"
    )
    label_open = html[last_label:html.index(">", last_label) + 1]
    assert "field-row" in label_open, (
        "v1.22.55: the import file wrapper is the hybrid field-row label"
    )
    assert 'for=' not in label_open, (
        "v1.19.39: the wrapping <label> must NOT carry a `for=` "
        "attribute — that is what keeps the label-text click safe"
    )


def test_no_for_attr_targets_file_input_in_panel():
    """Three-axis preference history on this attribute:

      - v1.15.70/.73: ban `for=` on file-input labels. Clicking
        label-text popped the picker before the user read the
        hint.
      - v1.19.25: REVERSED — the user preferred the theme-upload
        modal's wrapping shape. Required `for=` on the import
        label.
      - **v1.19.39**: REVERSED again. The post-v1.19.36 design
        audit found the v1.19.36 doc update (which said "no
        `for=` attribute, input as direct child") silently
        lied about codebase consistency — library upload-dlg
        already complied; settings.html didn't. v1.19.39 fixed
        the code to match the doc.

    Pin the post-v1.19.39 invariant (the v1.15.73 direction).
    Both prior reversal contexts preserved in the docstring."""
    import re
    html = SETTINGS_HTML.read_text()
    html_clean = re.sub(r"{#.*?#}", "", html, flags=re.DOTALL)
    panel_start = html_clean.index('data-panel="import"')
    panel = html_clean[panel_start:]
    assert 'for="import-csv-file"' not in panel, (
        "v1.19.39 (reverses v1.19.25): the import panel must "
        "NOT carry for=\"import-csv-file\" — DESIGN_SYSTEM.md "
        "§ 2 form layout (updated v1.19.36) says no `for=` "
        "attribute when the input is a direct child of the label"
    )


# ── 2. CURRENT cell renders src badge + info icon ───────────


def test_import_row_current_cell_renders_info_button():
    """The CURRENT cell must include an info button that opens the
    standard // MOTIF INFO dialog. Replaces the v1.15.71 inline
    URL preview that didn't vertically align with IMPORTED URL."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindImportPanel()")
    fn_end = js.index("// ---- Config form", fn_start)
    body = js[fn_start:fn_end]
    assert "import-info-btn" in body, (
        "v1.15.73: CURRENT cell must render an .import-info-btn "
        "alongside the src letter badge"
    )
    # Click delegation calls openInfoDialog with theme_media_type +
    # theme_tmdb_id from data attributes.
    assert "openInfoDialog" in body, (
        "v1.15.73: info button click must call openInfoDialog so "
        "the // MOTIF INFO panel opens with the row's metadata"
    )


def test_import_row_current_cell_no_longer_renders_inline_url():
    """The v1.15.71 inline URL preview in CURRENT must be gone —
    replaced with the info button. If the URL <span class="url-cell">
    reappears INSIDE current-stack, the alignment bug returns."""
    import re
    js = APP_JS.read_text()
    fn_start = js.index("function bindImportPanel()")
    fn_end = js.index("// ---- Config form", fn_start)
    body = js[fn_start:fn_end]
    # Strip JS comments — narrative may reference the removed pattern.
    body_clean = re.sub(r"//[^\n]*", "", body)
    body_clean = re.sub(r"/\*.*?\*/", "", body_clean, flags=re.DOTALL)
    # The currentInner template literal must NOT include url-cell.
    # (IMPORTED URL still uses url-cell, but inside the importedInner
    # template — different variable.)
    current_inner_idx = body_clean.index("const currentInner =")
    # End at the next declaration that mentions imported_url so we
    # don't falsely detect url-cell from IMPORTED URL's template.
    # v1.15.77: importedInner became a `let / if / else` block (no
    # longer `const importedInner = ...`), so anchor on the more-
    # stable "r.imported_url" reference which lives in the
    # importedInner branch.
    current_inner_end = body_clean.index("r.imported_url",
                                           current_inner_idx)
    current_inner = body_clean[current_inner_idx:current_inner_end]
    assert "url-cell" not in current_inner, (
        "v1.15.73: CURRENT cell must NOT render an inline url-cell "
        "URL preview — the user's alignment complaint. Info button "
        "covers URL-disclosure via the dialog"
    )


# v1.15.73 added a .import-info-btn CSS rule to style the small
# info button with 1px 8px padding + t-tiny font. v1.15.76 retired
# the rule and switched the button to .row-info-btn (the canonical
# library row info-button pattern, 30px min-width). The custom rule
# fought .btn-tiny's 72px min-width and rendered as a stretched
# rectangle (the user: "the U and info button is weirdly stretching").
# The assertion that lived here is superseded by the v1.15.76 test
# `test_import_info_btn_uses_library_row_info_btn_class`.


# ── 3. DUPLICATE status type ────────────────────────────────


def test_api_categorizes_identical_url_as_duplicate(tmp_path, monkeypatch):
    """Backend test: a row whose imported URL exactly matches the
    current user_override URL must return status='duplicate' (not
    'clean' as v1.15.66-72 did)."""
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path,
                        data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    app = create_app(settings)
    client = TestClient(app)
    # Seed a theme + matching user_override.
    same_url = "https://www.youtube.com/watch?v=duplicate01"
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO themes
                 (media_type, tmdb_id, imdb_id, title, year,
                  upstream_source, last_seen_sync_at,
                  first_seen_sync_at)
               VALUES ('movie', 73001, 'tt73001', 'Dup', 2020,
                       'themoviedb', '2026-01-01', '2026-01-01')"""
        )
        conn.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, "
            "  youtube_url, set_at, set_by, note, section_id) "
            "VALUES ('movie', 73001, ?, '2026-01-01', 'admin', 'x', '')",
            (same_url,),
        )
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Title", "IMDB", "Youtube_URL"])
    w.writerow(["Dup (2020)", "tt73001", same_url])
    resp = client.post(
        "/api/import/preview",
        files={"file": ("t.csv", buf.getvalue().encode(), "text/csv")},
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["counts"].get("duplicate") == 1, (
        f"v1.15.73: identical-URL row must categorize as duplicate "
        f"(not clean). Got counts={data['counts']}"
    )
    assert data["rows"][0]["status"] == "duplicate"
    assert data["rows"][0]["default_action"] == "skip"


def test_api_counts_dict_initializes_duplicate_key():
    """Counts dict must zero-initialize `duplicate` so the UI's
    chip renderer + downstream consumers don't have to KeyError-
    check."""
    src = API_PY.read_text()
    apply_start = src.index('@app.post("/api/import/preview")')
    apply_end = src.index('@app.post', apply_start + 1)
    body = src[apply_start:apply_end]
    assert '"duplicate": 0' in body, (
        "v1.15.73: preview counts dict must include duplicate=0 "
        "so the chip renderer reads it without defensive fallback"
    )


def test_app_js_status_label_maps_duplicate():
    """The JS statusLabel map must include 'duplicate' → 'DUPLICATE'
    so the row's STATUS cell renders human-readable text."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindImportPanel()")
    fn_end = js.index("// ---- Config form", fn_start)
    body = js[fn_start:fn_end]
    assert "duplicate: 'DUPLICATE'" in body, (
        "v1.15.73: statusLabel map must translate duplicate → "
        "'DUPLICATE' for the STATUS cell display"
    )


def test_app_js_summary_chip_includes_duplicate():
    """The header summary chip (// PREVIEW RESULTS subheading)
    must include DUPLICATE in its parts list."""
    js = APP_JS.read_text()
    fn_start = js.index("function renderSummary(counts)")
    fn_end = js.index("function ", fn_start + 1)
    body = js[fn_start:fn_end]
    assert "DUPLICATE" in body, (
        "v1.15.73: renderSummary must surface DUPLICATE count in "
        "the // PREVIEW RESULTS subheading chip"
    )


# ── 4. Column widths sum below 1000px ───────────────────────


def test_column_widths_sum_under_900px():
    """Sum of fixed pixel column widths must stay under 900px so
    the table fits on a 1366-wide viewport (typical laptop) with
    room for a flexible TITLE column. v1.15.71 totals were 1000+
    TITLE; on narrower screens action overflowed."""
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
        assert m, f"v1.15.73: missing pixel width for .{col}"
        total += int(m.group(1))
    assert total < 900, (
        f"v1.15.73: fixed-width columns sum to {total}px — over "
        "the 900px ceiling that leaves room for TITLE on a 1366-"
        "wide viewport. Action column overflow returns above 900."
    )
