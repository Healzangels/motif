"""v1.14.85 — OPEN ROW navigates to library page; CLEAR button hidden when collapsed.

the user:
> "clicking row opens the info card for the error but it
>  doesn't actually open to a row"
> "also can we make the clear button for the history and
>  provenance not shown until the section is unfurrled as it
>  adds to the clutter/don't want people to accidently click"

## Two fixes

### A. OPEN ROW navigation (cross-page)

v1.14.82's OPEN ROW button called openInfoDialog same-page.
Worked (after v1.14.83 promoted info-dlg to base.html) but
left the user on /queue with no library context after
closing the dialog. v1.14.85 makes the button NAVIGATE to
/movies (or /tv) with `?info_open=<tmdb>&info_mt=<mt>
&info_section=<sid>` URL params; the library page's load
handler reads the params and auto-opens the dialog over a
populated library. Closing the dialog now leaves the user
on the row's library tab where they can take follow-up
actions via the SOURCE menu.

### B. CLEAR button hidden when section is collapsed

The CLEAR buttons in the INFO card's // HISTORY and
// PROVENANCE summary rows rendered alongside the
collapsed section title — adding clutter and risking
accidental clicks. v1.14.85: CSS hides them when the
parent `<details>` isn't `[open]`. Expanding the section
deliberately reveals CLEAR.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JS = REPO / "app" / "web" / "static" / "app.js"
CSS = REPO / "app" / "web" / "static" / "app.css"


# ── Fix A: OPEN ROW navigates ──────────────────────────────────


def test_open_row_button_sets_window_location():
    """The handler must set window.location.href to navigate
    away from /queue, not just open the dialog overlay."""
    js = JS.read_text()
    fn_start = js.index("function bindQueue() {")
    fn_end = js.index("\n  }\n", fn_start)
    body = js[fn_start:fn_end]
    # The OPEN ROW branch contains a navigation.
    handler_idx = body.index('reprobe-open-row')
    # Slice widened in v1.14.90 — added ?fourk= + ?q= URL params
    # to the handler, pushing the param-set lines further down.
    handler_body = body[handler_idx:handler_idx + 3500]
    assert "window.location.href" in handler_body


def test_open_row_url_includes_all_three_info_params():
    """The navigation URL must carry info_open (tmdb_id),
    info_mt (media_type), and optionally info_section (section
    id when present). The library page reads all three to
    auto-open the dialog."""
    js = JS.read_text()
    fn_start = js.index("function bindQueue() {")
    fn_end = js.index("\n  }\n", fn_start)
    body = js[fn_start:fn_end]
    handler_idx = body.index('reprobe-open-row')
    # Slice widened in v1.14.90 — added ?fourk= + ?q= URL params
    # to the handler, pushing the param-set lines further down.
    handler_body = body[handler_idx:handler_idx + 3500]
    assert "params.set('info_open'" in handler_body
    assert "params.set('info_mt'" in handler_body
    assert "params.set('info_section'" in handler_body


def test_open_row_path_inference_from_media_type():
    """media_type='movie' → /movies; everything else (tv/show)
    → /tv. Anime sections map to whichever media_type their
    items have (the URL puts them in the relevant tab; the
    library page handles anime-detection separately)."""
    js = JS.read_text()
    fn_start = js.index("function bindQueue() {")
    fn_end = js.index("\n  }\n", fn_start)
    body = js[fn_start:fn_end]
    assert "mt === 'movie' ? '/movies' : '/tv'" in body


def test_library_page_load_parses_info_open_params():
    """On library page load (path is /movies, /tv, or /anime),
    the JS must read the three info_* URL params and call
    openInfoDialog. Deferred via setTimeout so the dialog
    opens over a populated library (not a loading shell)."""
    js = JS.read_text()
    anchor = js.index(
        "v1.14.85: ?info_open=<tmdb_id>"
    )
    # v0.51.213: bound by the block's own catch, not a fixed byte window — at 1369/2000
    # this was two-thirds consumed and would eventually fail as a phantom invariant break
    # (the v0.51.141-143 slice trap).
    block = js[anchor:js.index("URLSearchParams not supported", anchor)]
    # Path-guarded so the parser doesn't fire on /dash, /queue, etc.
    assert "path === '/movies' || path === '/tv' || path === '/anime'" in block
    # v0.51.213: /collections must be here too — collection notifications, canonical-health
    # and loudness-audit all deep-link there, and without it the card silently never opens.
    assert "path === '/collections'" in block
    # Reads all three params.
    assert "sp.get('info_open')" in block
    assert "sp.get('info_mt')" in block
    assert "sp.get('info_section')" in block
    # Calls openInfoDialog after the deferred timeout.
    assert "setTimeout(" in block
    # v0.51.219: match the call PREFIX, not the full argument list — the arity grew when
    # deep-links learned to carry info_edition, and a verbatim match broke on a change that
    # preserved this invariant (openInfoDialog is called with the parsed params). The
    # edition argument is covered by test_v0_51_219.
    assert "openInfoDialog(infoMt, infoOpen, infoSection" in block


def test_library_page_only_opens_when_both_required_params_present():
    """The auto-open must NOT fire on `?info_open=X` alone
    (without info_mt) — openInfoDialog needs both. Pre-fix
    pattern: gate the call on infoOpen && infoMt both being
    truthy."""
    js = JS.read_text()
    anchor = js.index(
        "v1.14.85: ?info_open=<tmdb_id>"
    )
    # v0.51.306: structural end (the gate's catch line), not a 2000-byte
    # window — the consume-once strip grew the block past the old width.
    block = js[anchor:js.index("URLSearchParams not supported", anchor)]
    assert "if (infoOpen && infoMt)" in block


# ── Fix B: CLEAR button hidden when collapsed ──────────────────


def test_clear_button_hidden_when_history_section_collapsed():
    """CSS rule: `.history-section:not([open]) .info-clear-btn`
    sets display: none. Reveals the CLEAR button only when the
    user explicitly expands the section."""
    css = CSS.read_text()
    assert ".history-section:not([open]) .info-clear-btn {" in css
    # The hide rule body.
    rule_idx = css.index(".history-section:not([open]) .info-clear-btn {")
    rule_block = css[rule_idx:rule_idx + 200]
    assert "display: none;" in rule_block


def test_v1_14_85_marker_explains_clear_button_visibility_change():
    """v1.14.85 marker on the .info-clear-btn rule explains
    the visibility change so future readers see why the
    button is conditional."""
    css = CSS.read_text()
    assert "v1.14.85: only visible when the parent <details> is open" in css
