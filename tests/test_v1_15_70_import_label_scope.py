"""v1.15.70 — file-picker <label> click-anywhere bug.

the user: "clicking anywhere on the import section acts like
you're clicking the choose file button."

Root cause: v1.15.66 wrapped the entire form-grid block in a
single `<label class="form-label">` — the label-text, the file
input, the "Round-trip flow:" hint paragraph, the // PREVIEW
IMPORT button, AND the status span. By HTML spec, clicks
anywhere inside a `<label>` containing a form control fire on
that control. So clicking // PREVIEW IMPORT, or the muted
status text, or any blank space inside the panel, opened the
OS file picker on the wrapped `<input>`.

Fix: `<label for="import-csv-file">` only wraps the label-text
+ the input. Buttons + status span sit in a sibling
`.form-input-actions` div OUTSIDE the label so clicks there
don't trigger the input.

Side benefit: dropped the verbose `block-intro` paragraph above
the picker + the "Round-trip flow:" hint paragraph that used
to live inside the label. the user's v1.15.68 verdict "overly
verbose and not overly helpful" applies — the picker is
self-explanatory now.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


# Helpers ─────────────────────────────────────────────────────


def _import_panel(html: str) -> str:
    """Return the substring covering the IMPORT panel."""
    start = html.index('data-panel="import"')
    # Walk forward to the next tab-panel start (or the end of
    # the {% endblock %}) — whichever comes first.
    end_candidates = []
    next_panel = html.find('data-panel="', start + 1)
    if next_panel != -1:
        end_candidates.append(next_panel)
    endblock = html.find("{% endblock %}", start)
    if endblock != -1:
        end_candidates.append(endblock)
    end = min(end_candidates) if end_candidates else len(html)
    return html[start:end]


def _label_for_input(panel: str, input_id: str) -> str:
    """Return the `<label>...</label>` substring whose `for=` attr
    (or implicit wrap) matches the given input id."""
    # The file input uses `for="import-csv-file"`. Find a label
    # with that for-attr.
    for_attr = f'for="{input_id}"'
    pos = panel.index(for_attr)
    tag_start = panel.rfind("<label", 0, pos)
    tag_end = panel.index("</label>", pos) + len("</label>")
    return panel[tag_start:tag_end]


# ── Fix #1: file-picker <label> (v1.15.70) — superseded ────
#
# v1.15.70 narrowed the file-picker <label for="import-csv-file">
# to only wrap label-text + input. the user's v1.15.73 follow-up:
# even with that fix, clicking the "CSV FILE" label-text STILL
# fired the picker — that's intrinsic `<label for=...>` behavior.
# v1.15.73 replaced the <label> with a plain <div> entirely, so
# only the native input button opens the picker. The four tests
# that lived here (asserting the label scope) are superseded by
# the v1.15.73 guards in tests/test_v1_15_73_*.py.


# ── Fix #2: verbose block-intro paragraph removed ───────────


def _strip_comments(html: str) -> str:
    """Strip Jinja {# ... #} comments + HTML <!-- ... --> comments so
    test assertions match RENDERED markup, not narrative comment text
    that may reference removed elements by name."""
    import re
    html = re.sub(r"{#.*?#}", "", html, flags=re.DOTALL)
    html = re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)
    return html


# v1.15.70 retired the verbose v1.15.66 block-intro paragraph
# ("Expects CSV columns ..."). v1.15.71 re-added a CONCISE one
# per the user: "lets provide some clear information on the tip
# explaining the import feature ... use the same style/format as
# our other sections in settings." The new presence + length
# assertions live in tests/test_v1_15_71_*.


def test_import_panel_no_longer_has_roundtrip_flow_paragraph():
    """The 'Round-trip flow:' hint that used to live inside the
    file label is gone too — same verbose-copy verdict, plus it
    was inside the label (contributing to the click-anywhere bug)."""
    html = SETTINGS_HTML.read_text()
    panel = _strip_comments(_import_panel(html))
    assert "Round-trip flow:" not in panel, (
        "v1.15.70: the 'Round-trip flow:' hint paragraph must be "
        "removed — both because it was verbose AND because it sat "
        "inside the file <label> contributing to the click-anywhere "
        "file-picker bug"
    )


# ── v1.15.66 existing controls still present after restructure ──


def test_import_preview_button_still_present_after_restructure():
    """Counter-test: moving the button out of the label must not
    remove the button itself. JS handler binds on this id."""
    html = SETTINGS_HTML.read_text()
    panel = _import_panel(html)
    assert 'id="import-preview-btn"' in panel
    assert 'id="import-preview-status"' in panel
    assert 'id="import-csv-file"' in panel


def test_import_preview_button_is_inside_form_input_actions():
    """After restructuring, the preview button + status span
    sit in a sibling .form-input-actions div outside the label.
    Matches the form-actions pattern used elsewhere in settings."""
    html = SETTINGS_HTML.read_text()
    panel = _import_panel(html)
    actions_start = panel.index('class="form-input-actions"')
    actions_end = panel.index("</div>", actions_start)
    actions = panel[actions_start:actions_end]
    assert 'id="import-preview-btn"' in actions, (
        "v1.15.70: // PREVIEW IMPORT button must live in the sibling "
        ".form-input-actions div (outside the label)"
    )
    assert 'id="import-preview-status"' in actions
