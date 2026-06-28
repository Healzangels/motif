"""v1.19.31 — pin collections page inherits file picker styling.

v1.19.30 generalized the file picker selector from
`#import-csv-file::file-selector-button` to
`.input::file-selector-button` so both file inputs in motif
(#import-csv-file in settings.html, #upload-file in library.html)
pick up the dark CRT styling.

The /collections page renders the SAME library.html template as
/movies, /tv, /anime — so technically the upload-dlg + #upload-file
also reach /collections, and the generalized selector covers it
automatically. The v1.19.30 test pinned both file inputs by ID but
did NOT explicitly pin that /collections is on the inheritance path.

the user on v1.19.30: "can we make sure collections is included too."

## What this guards

Two regressions could quietly break collections-page styling:

1. Someone splits /collections off to a dedicated template that
   has a different file input (or no upload-dlg at all). The
   v1.19.30 test wouldn't catch this — it pins #upload-file in
   library.html, not the /collections render path.

2. Someone narrows the CSS selector back to an ID-scoped form
   (e.g. `#upload-file::file-selector-button`). The v1.19.30
   test already guards the inverse — the legacy
   `#import-csv-file` selector — but a future narrow to
   `#upload-file` would slip past, breaking /settings.

## What's pinned

- `collections_page` in api.py renders the `library.html`
  template (NOT a dedicated collections template).
- library.html still contains the `upload-dlg` with the
  `#upload-file` file input — the dialog the user sees on
  /collections is the same one styled by v1.19.30.
- The CSS rule's selector is the generalized `.input` form
  (NOT narrowed to either #upload-file or #import-csv-file).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
LIBRARY_HTML = (
    REPO / "app" / "web" / "templates" / "library.html"
).read_text()
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_collections_page_renders_library_html():
    """The /collections route must render library.html (the
    template that contains the upload-dlg with #upload-file).
    A future split to a dedicated collections template would
    silently strip the upload affordance + the v1.19.30 styling."""
    # Find the collections_page handler.
    idx = API_PY.index("async def collections_page(")
    # Slice the body — handlers are short, 800 chars covers it.
    body = API_PY[idx:idx + 800]
    assert '"library.html"' in body, (
        "v1.19.31: collections_page must render library.html so "
        "the upload-dlg (and the v1.19.30 file picker styling) "
        "reach /collections"
    )
    # Pin the tab key so a later refactor doesn't accidentally
    # send /collections to a non-collections branch of the JS.
    assert '"tab": "collections"' in body, (
        "v1.19.31: collections_page must pass tab='collections' "
        "so the renderer branches into the collection-specific "
        "affordances"
    )


def test_library_html_still_owns_upload_dlg():
    """The upload-dlg with #upload-file must live in library.html
    (the template /collections renders). If a future refactor
    extracts the dialog to a different template, /collections
    loses the dialog AND the file picker styling."""
    assert '<dialog class="dlg" id="upload-dlg">' in LIBRARY_HTML, (
        "v1.19.31: library.html must own the upload-dlg so it "
        "reaches /collections via the shared template"
    )
    # The file input itself must still carry class="input" — the
    # v1.19.30 generalized selector hinges on this.
    idx = LIBRARY_HTML.index('id="upload-file"')
    backwards = LIBRARY_HTML[max(0, idx - 200):idx]
    assert 'class="input"' in backwards, (
        "v1.19.31: #upload-file must keep class=\"input\" so the "
        "generalized .input::file-selector-button selector matches "
        "it on /collections too"
    )


def test_css_selector_stays_generalized_not_renarrowed_to_id():
    """Counter-guard against re-narrowing. If someone scopes the
    rule back to `#upload-file` (mirroring the v1.15.77 mistake
    that v1.19.30 fixed), /settings's import picker loses the
    styling. If they scope it to `#import-csv-file`, /movies +
    /collections lose it. The generalized .input form must
    remain the only anchor."""
    assert '.input::file-selector-button {' in CSS, (
        "v1.19.31: file picker base rule must use the generalized "
        ".input::file-selector-button selector"
    )
    assert '#upload-file::file-selector-button' not in CSS, (
        "v1.19.31: must NOT narrow the rule to #upload-file — "
        "that would strip /settings's import picker styling"
    )
    assert '#import-csv-file::file-selector-button' not in CSS, (
        "v1.19.31: must NOT narrow the rule back to "
        "#import-csv-file — that would strip /collections + "
        "/movies upload-dlg styling (the v1.19.30 regression)"
    )
