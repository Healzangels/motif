"""v1.15.69 — import preview table rebuild + hint paragraph removal.

the user's v1.15.68 feedback:

  "The text next to the check box next to STATUS TITLE IMDB
   CURRENT IMPORTED URL ACTION I believe those are supposed to
   be headers for the columns below. Also the rows below are
   smooshed together. The - And the U are connected right next
   to the imdb. Also the description Each row has an apply
   checkbox in column 1. ... is overly verbose and not overly
   helpful. Can we fix the gui in general to match the other
   sections"

### Root cause (the .grid bug)

v1.15.66 introduced the preview table with `class="grid"`. But
`.grid` is `display: grid` in app.css — a CSS Grid container
declaration that OVERRIDES the native `display: table` on the
<table> element. The <thead> row's <th>s rendered as grid
cells (all squished onto one line below the master checkbox)
and the <tbody> rows used a different sizing computation so
columns didn't align with headers. No other template in motif
used `class="grid"` on a table — I'd invented the class without
checking app.css.

### Fix

* Switch the import-preview table from `class="grid"` to
  `class="table table-tight"` (the library's table pattern).
* Add column-class hints (.col-state / .col-import-status /
  .col-title / .col-imdb / .col-import-current /
  .col-import-url / .col-import-action) so the column widths
  are predictable.
* `#import-preview-table { table-layout: fixed; width: 100%; }`
  pins column widths so long URLs ellipsis-truncate instead of
  growing the table past its container.
* `.url-cell` class on the URL <td>s for the ellipsis + title
  tooltip pattern.

### Hint paragraph removed

v1.15.68 added a "Each row has an apply checkbox in column 1..."
hint above the table. v1.15.69 retires it — with the table
rendering correctly, the bare checkbox + title tooltip is
enough. The verbose paragraph was the user's complaint.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


# ── 1. Table class fix (the .grid → .table swap) ────────────


def test_import_preview_table_uses_library_table_class():
    """The preview table must use class="table" (the canonical
    library-table pattern) — NOT class="grid", which is
    `display: grid` in app.css and breaks <table> column layout."""
    html = SETTINGS_HTML.read_text()
    pos = html.index('id="import-preview-table"')
    # Walk back to the <table tag and capture its class.
    tag_start = html.rfind("<table", 0, pos)
    tag_end = html.index(">", pos)
    tag = html[tag_start:tag_end + 1]
    assert 'class="table' in tag, (
        "v1.15.69: import preview <table> must use class=\"table\" "
        "(library pattern). class=\"grid\" is display:grid which "
        "breaks column layout"
    )
    assert 'class="grid"' not in tag, (
        "v1.15.69: class=\"grid\" on the <table> would re-introduce "
        "the v1.15.66 squished-column bug"
    )


def test_import_preview_thead_uses_column_classes():
    """Each <th> must carry the matching .col-* class so the
    table-layout:fixed column widths apply."""
    html = SETTINGS_HTML.read_text()
    table_start = html.index('id="import-preview-table"')
    thead_end = html.index("</thead>", table_start)
    thead = html[table_start:thead_end]
    for cls in (
        "col-state",         # checkbox
        "col-import-status",
        "col-title",
        "col-imdb",
        "col-import-current",
        "col-import-url",
        "col-import-action",
    ):
        assert f'class="{cls}"' in thead or f'col-state"' in thead, (
            f"v1.15.69: thead must include <th class=\"{cls}\"> so "
            "the table-layout:fixed column widths apply"
        )


def test_app_css_import_preview_table_has_fixed_layout():
    """The CSS rule that pins column widths must exist; without
    table-layout:fixed long URLs would grow the table past its
    container width again (similar visual symptom as the .grid
    bug — wide columns squish narrow ones)."""
    css = APP_CSS.read_text()
    # v1.15.80 split the rule across multiple lines (added
    # box-sizing/max-width). Anchor on the selector + check for
    # table-layout:fixed anywhere in the same block.
    sel_idx = css.index("#import-preview-table {")
    block_end = css.index("}", sel_idx)
    block = css[sel_idx:block_end]
    assert "table-layout: fixed" in block, (
        "v1.15.69: app.css must pin #import-preview-table to "
        "table-layout:fixed so long URLs ellipsis instead of "
        "overflowing the column"
    )


def test_app_css_url_cell_truncates_with_ellipsis():
    """URL cells must truncate with ellipsis (overflow:hidden +
    text-overflow:ellipsis + white-space:nowrap). Without these
    rules a 200-character URL would force the column wider and
    squish neighbors back into smoosh territory."""
    css = APP_CSS.read_text()
    rule_start = css.index("#import-preview-table .url-cell")
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    assert "text-overflow: ellipsis" in rule
    assert "white-space: nowrap" in rule
    assert "overflow: hidden" in rule


def test_app_js_row_uses_column_classes():
    """Row HTML generated by JS must carry the same .col-* classes
    as the thead so column 1 (checkbox) lines up with column-1
    header etc."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindImportPanel()")
    fn_end = js.index("// ---- Config form", fn_start)
    body = js[fn_start:fn_end]
    # Each row must include td.col-state, td.col-import-status,
    # td.col-title, etc.
    for cls in (
        '"col-state"',
        '"col-import-status"',
        '"col-title"',
        '"col-imdb',          # .col-imdb mono
        '"col-import-current"',
        '"col-import-url"',
        '"col-import-action"',
    ):
        assert cls in body, (
            f"v1.15.69: row <td>s must carry class={cls} so columns "
            "align with the thead column classes"
        )


# ── 2. Hint paragraph removed ────────────────────────────────


def test_import_preview_hint_paragraph_is_gone():
    """the user: 'overly verbose and not overly helpful.' The v1.15.68
    hint paragraph above the preview table must not be in the
    template. The bare checkbox + title tooltip is enough now
    that the columns actually align."""
    html = SETTINGS_HTML.read_text()
    assert 'id="import-preview-hint"' not in html, (
        "v1.15.69: the verbose v1.15.68 hint paragraph must be "
        "removed — the user flagged it as unhelpful"
    )


# ── 3. Master checkbox keeps tooltip (slimmer affordance) ────


def test_import_master_checkbox_keeps_title_tooltip():
    """The master checkbox <th> must still carry a `title=` so
    hover surfaces the bulk-select behavior. Without the hint
    paragraph this is the user's primary affordance for what the
    checkbox does."""
    html = SETTINGS_HTML.read_text()
    cb_pos = html.index('id="import-row-select-all"')
    th_start = html.rfind("<th", 0, cb_pos)
    th_end = html.index(">", cb_pos)
    th_tag = html[th_start:th_end]
    assert "title=" in th_tag, (
        "v1.15.69: master checkbox <th> must carry a title attribute "
        "so the bulk-select behavior is discoverable on hover"
    )
