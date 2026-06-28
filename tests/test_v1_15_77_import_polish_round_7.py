"""v1.15.77 — import polish round 7 (the user feedback on v1.15.76).

Four issues:

1. **Choose File button has no :active feedback.** The native
   `<input type="file">` button rendered as the browser default
   (gray pill) against motif's dark CRT palette — no visible
   hover or click-down state. the user: "the choose file button
   doesn't really look like it clicks down when clicking."
   v1.15.77 styles `::file-selector-button` to match the motif
   button taxonomy (mono font, green border, transparent bg) +
   adds explicit :hover + :active states.

2. **Action column still spilling.** v1.15.73-76 widths summed
   to 776 + TITLE — on the user's viewport the table overflowed
   horizontally. v1.15.77 cuts URL 300 → 140 (compact link
   form added this round only needs ~14 chars of room) and
   ACTION 160 → 140. New total: 594 + TITLE.

3. **Imported URL "looks a bit off"** as a wide ellipsis-
   truncated full URL. v1.15.77 replaces it with a compact
   "▶ {video_id}" link that opens the URL in a new tab —
   click to preview, hover for the full URL via title=.

4. **IMDB should be an imdb.com link** (matches the library
   pattern). v1.15.77 wraps the IMDB cell content in an
   <a href="https://www.imdb.com/title/{tt-id}"> using the
   same shape as the library table.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── 1. File picker button styling ───────────────────────────


def test_file_picker_button_has_motif_styling():
    """The ::file-selector-button pseudo-element must be styled
    so the native gray pill picks up motif's button taxonomy
    (mono font, green border, transparent bg). Without this the
    button reads as foreign-looking and clicks have no visible
    feedback against the dark page."""
    css = APP_CSS.read_text()
    selector = '.input::file-selector-button'
    assert selector in css, (
        "v1.15.77: must style ::file-selector-button for the "
        "import CSV file input — the native gray pill has no "
        "visible feedback against the dark CRT palette"
    )
    # Pull the base rule.
    base_idx = css.index(f"{selector} {{")
    base_end = css.index("}", base_idx)
    base = css[base_idx:base_end]
    assert "font-family: var(--font-mono)" in base, (
        "v1.15.77: file picker button must use motif's mono font"
    )
    assert "cursor: pointer" in base


def test_file_picker_button_has_active_state():
    """The :active state must invert the button (filled green,
    dark text) so the click-down feedback is unmissable —
    the user's complaint that the button "doesn't really look
    like it clicks down."""
    css = APP_CSS.read_text()
    active_selector = '.input::file-selector-button:active'
    assert active_selector in css, (
        "v1.15.77: ::file-selector-button must have a :active "
        "rule so the click-down state is visible"
    )
    rule_start = css.index(f"{active_selector} {{")
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    # Inverted: filled bg + dark text.
    assert "background:" in rule and "color:" in rule, (
        "v1.15.77: :active must change both background and color "
        "for unambiguous click-down feedback"
    )


def test_file_picker_button_has_hover_state():
    """Counter-affordance: :hover must be distinct from default
    so the user knows the button is hoverable before clicking."""
    css = APP_CSS.read_text()
    hover_selector = '.input::file-selector-button:hover'
    assert hover_selector in css, (
        "v1.15.77: file picker button must have a :hover state"
    )


# ── 2. Action column overflow guard tightened ───────────────
# v1.15.77's 700px cap test retired in v1.15.83. It was an
# iterative width-shaving counter-guard from before the v1.15.80
# structural fix (border-box + max-width:100% on the select)
# closed the overflow bug class. With the rendering guarantees
# in place, the width budget no longer doubles as overflow
# defense — v1.15.80's column-sum test (now 800px) owns the
# title-readability ceiling alone.


# ── 3. Compact play-link for IMPORTED URL ───────────────────


def test_imported_url_uses_compact_play_link():
    """The IMPORTED URL cell must render as a compact play-link
    ("▶ {vid}") that opens the URL in a new tab. the user: "the
    import url being so long looks a bit off, let's explore
    ways to make that look better." Click → preview the video;
    hover → full URL via title attr."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindImportPanel()")
    fn_end = js.index("// ---- Config form", fn_start)
    body = js[fn_start:fn_end]
    # Anchor on the importedInner construction.
    imp_idx = body.index("importedInner")
    chunk = body[imp_idx:imp_idx + 1500]
    assert "url-link" in chunk, (
        "v1.15.77: IMPORTED URL cell must use the .url-link "
        "class (compact play-link)"
    )
    assert "▶" in chunk, (
        "v1.15.77: the compact link must include the ▶ glyph as "
        "a visual play affordance"
    )
    assert 'target="_blank"' in chunk, (
        "v1.15.77: the link must open the URL in a new tab so "
        "the user can preview without leaving the import preview"
    )
    assert "extractYouTubeVideoId" in chunk, (
        "v1.15.77: must extract the YouTube video ID for the "
        "compact label (full-URL fallback for SoundCloud)"
    )


def test_app_css_url_link_styled():
    """The .url-link class must be styled in app.css — violet
    tone (matches the user-URL palette), ellipsis on overflow
    so long SoundCloud IDs truncate inside the 140px column."""
    css = APP_CSS.read_text()
    rule_start = css.index("#import-preview-table .url-link {")
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    assert "color: var(--violet)" in rule, (
        "v1.15.77: .url-link must use --violet to match the "
        "user-URL palette (consistent with link-badge-user etc.)"
    )
    assert "text-overflow: ellipsis" in rule, (
        "v1.15.77: .url-link must ellipsis-truncate so long "
        "URLs don't push past the column width"
    )


def test_imported_url_no_longer_renders_full_url_text_node():
    """Counter-guard: the IMPORTED URL cell must NOT render
    the URL as a bare text-node span (the v1.15.71-76 pattern).
    A re-introduction would bring back the "too long" rendering
    the user flagged."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindImportPanel()")
    fn_end = js.index("// ---- Config form", fn_start)
    body = js[fn_start:fn_end]
    # Slice the importedInner expression specifically.
    imp_idx = body.index("importedInner")
    # End where the row's <td>s start being assembled.
    end_idx = body.index("tr.innerHTML", imp_idx)
    chunk = body[imp_idx:end_idx]
    # The v1.15.66 pattern was a <span class="url-cell"> wrapping
    # the full URL. Must not survive in the importedInner build.
    assert "<span class=\"url-cell\"" not in chunk, (
        "v1.15.77: IMPORTED URL must not use the v1.15.66 "
        "url-cell span (full URL ellipsis) — must use the "
        "v1.15.77 compact .url-link form"
    )


# ── 4. IMDB cell renders as imdb.com link ───────────────────


def test_imdb_cell_renders_as_imdb_link():
    """The IMDB cell must wrap the tt-ID in an <a> targeting
    imdb.com/title/{tt-id}, matching the library row pattern
    (the user: "make the imdb id a url to the imdb page same as
    the libraries section")."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindImportPanel()")
    fn_end = js.index("// ---- Config form", fn_start)
    body = js[fn_start:fn_end]
    imdb_idx = body.index("imdbInner")
    chunk = body[imdb_idx:imdb_idx + 800]
    assert "https://www.imdb.com/title/" in chunk, (
        "v1.15.77: IMDB cell must link to imdb.com/title/{id} — "
        "matches the library row pattern"
    )
    assert 'target="_blank"' in chunk, (
        "v1.15.77: IMDB link must open in a new tab so the user "
        "stays on the preview page"
    )


def test_imdb_cell_link_renders_via_imdb_inner_template_var():
    """The row template must reference the imdbInner expression
    (not htmlEscape(r.imdb) directly) so the link is wired."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindImportPanel()")
    fn_end = js.index("// ---- Config form", fn_start)
    body = js[fn_start:fn_end]
    # The col-imdb cell must reference ${imdbInner}.
    assert 'col-imdb mono">${imdbInner}' in body, (
        "v1.15.77: the col-imdb <td> must render ${imdbInner} "
        "(the link template), not the bare htmlEscape(r.imdb)"
    )
