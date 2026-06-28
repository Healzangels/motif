"""v1.19.30 — file picker styling generalized to all motif inputs.

v1.15.77 styled `::file-selector-button` for the import CSV file
input only (scoped to `#import-csv-file`). The /movies UPLOAD
THEME modal's MP3 file input (`#upload-file`) fell back to the
browser's default gray pill — looked out of place against the
dark CRT palette.

the user on v1.19.29: "can we make the upload theme choose file
section the same color as the import one."

## Fix

Widened the selector from `#import-csv-file::file-selector-button`
to `.input::file-selector-button`. Both motif file
inputs carry the .input class so both now pick up the dark
motif styling. Future file inputs that follow the convention
get it for free.

## What's pinned

- CSS rule uses `.input::file-selector-button`
  (NOT the legacy `#import-csv-file` ID anchor).
- Both file inputs in the templates carry `class="input"` so
  the new selector matches them.
- :hover + :active states still defined on the same general
  selector.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
LIBRARY_HTML = (
    REPO / "app" / "web" / "templates" / "library.html"
).read_text()
SETTINGS_HTML = (
    REPO / "app" / "web" / "templates" / "settings.html"
).read_text()


def test_file_picker_rule_uses_generalized_selector():
    """The base ::file-selector-button rule must target
    .input[type="file"], NOT the v1.15.77 #import-csv-file ID."""
    assert (
        '.input::file-selector-button {' in CSS
    ), (
        "v1.19.30: file picker styling must use the generalized "
        ".input[type=\"file\"] selector"
    )
    assert (
        '#import-csv-file::file-selector-button' not in CSS
    ), (
        "v1.19.30: legacy ID-scoped rule must be removed — "
        "the generalized selector covers it"
    )


def test_hover_and_active_rules_also_generalized():
    """The :hover + :active counterparts must use the same
    generalized selector so the upload-theme picker also gets
    interactive feedback."""
    assert (
        '.input::file-selector-button:hover {' in CSS
    )
    assert (
        '.input::file-selector-button:active {' in CSS
    )


def test_library_upload_file_input_carries_input_class():
    """The /movies UPLOAD THEME modal's #upload-file must have
    class="input" so the generalized selector matches it."""
    idx = LIBRARY_HTML.index('id="upload-file"')
    # Walk back to the <input opening.
    backwards = LIBRARY_HTML[max(0, idx - 200):idx]
    assert 'class="input"' in backwards, (
        "v1.19.30: #upload-file must carry class=\"input\" so "
        "the generalized file-picker selector matches it"
    )


def test_settings_import_file_input_carries_input_class():
    """The /settings IMPORT CSV file input must still have
    class="input" (regression guard for the v1.15.77 styling
    that already worked)."""
    idx = SETTINGS_HTML.index('id="import-csv-file"')
    backwards = SETTINGS_HTML[max(0, idx - 200):idx]
    assert 'class="input"' in backwards
