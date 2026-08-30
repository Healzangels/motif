"""v1.14.82 — REPROBE error events get inline OPEN ROW + REPROBE AGAIN buttons in /queue.

the user: "lets do the suggested surface a clearer per-row error
list, I don't know if the the live ops drawer would be best
since it could be cluttered or if making it apparent in the
logs so it can be resolved with a purge etc."

The REPROBE worker logs WARNING-level events with
component=reprobe and detail={rating_key, reason} for each
sidecar-bearing row that couldn't be classified. Pre-fix the
operator could see the rating_key in the message but had to
manually map rk → row before they could PURGE / UNPLACE / etc.

## Fix

  1. Server: enrich the log_event detail with media_type,
     tmdb_id, section_id, title (resolved via plex_items
     LEFT JOIN themes at row-fetch time, looked up by rk in
     the per-future error handler).
  2. Client: in /queue's events list, for component=reprobe
     events render:
       • A hint span describing the reason in operator-friendly
         terms ("Empty 0-byte theme.mp3 at the Plex folder…")
       • An OPEN ROW button (visible when media_type/tmdb_id
         resolve) that opens the INFO dialog so the operator
         can PURGE/UNPLACE/etc via the existing SOURCE menu
       • A REPROBE AGAIN button that re-fires the global
         /api/admin/reprobe-plex-themes endpoint
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JS = REPO / "app" / "web" / "static" / "app.js"
CSS = REPO / "app" / "web" / "static" / "app.css"
API = REPO / "app" / "web" / "api.py"


# ── Server: log_event detail enrichment ────────────────────────


def test_reprobe_select_pulls_theme_join_columns():
    """The reprobe SQL must LEFT JOIN themes via pi.theme_id so
    media_type / tmdb_id / theme.title are available for the
    per-rk error enrichment. plex_items.title is also pulled as
    a fallback for plex-orphan rows where theme_id is NULL."""
    py = API.read_text()
    fn_start = py.index("def _reprobe_plex_themes_run(")
    body = py[fn_start:fn_start + 8000]
    # Anchor on the v1.14.82 marker comment.
    assert "v1.14.82: pull theme.media_type" in body
    # JOIN + projected columns.
    assert "LEFT JOIN themes t ON t.id = pi.theme_id" in body
    assert "t.media_type" in body
    assert "t.tmdb_id" in body
    assert "t.title AS theme_title" in body
    assert "pi.title AS plex_title" in body


def test_row_by_rk_index_built_for_error_lookup():
    """The per-future error handler needs to look up the
    original row by rating_key (the future tuple only carries
    rk + verdict + err). The dict comprehension must exist."""
    py = API.read_text()
    fn_start = py.index("def _reprobe_plex_themes_run(")
    body = py[fn_start:fn_start + 8000]
    assert 'row_by_rk = {r["rating_key"]: r for r in rows}' in body


def test_log_event_detail_carries_resolved_row_context():
    """The log_event call in the error branch must include
    media_type, tmdb_id, section_id, title (in addition to the
    pre-existing rating_key + reason). theme_title falls back
    to plex_title when theme_id is NULL."""
    py = API.read_text()
    # Anchor on the v1.14.82 marker on the log_event call.
    anchor = py.index(
        "v1.14.82: enrich the detail with row context"
    )
    block = py[anchor:anchor + 3000]
    # All four enrichment fields are present.
    assert '"media_type":' in block
    assert '"tmdb_id":' in block
    assert '"section_id":' in block
    assert '"title":' in block
    # The fallback chain for the title.
    assert 'theme_title' in block
    assert 'plex_title' in block


# ── Client: events render adds the action UI ──────────────────


def test_events_render_parses_detail_for_reprobe_component():
    """The events render must JSON.parse(e.detail) to access
    the enriched fields on component=reprobe rows."""
    js = JS.read_text()
    # Anchor on the v1.14.82 marker block.
    anchor = js.index(
        "v1.14.82: per-event inline actions for component=reprobe"
    )
    block = js[anchor:anchor + 5000]
    # Component check + detail parse.
    assert "e.component === 'reprobe'" in block
    assert "JSON.parse(e.detail)" in block


def test_open_row_button_renders_when_ids_resolved():
    """The OPEN ROW button is conditional on media_type +
    tmdb_id being resolved (not NULL — happens when the row's
    theme_id was non-null at log time). Plex-orphan rows
    (theme_id NULL) skip the button rather than rendering it
    in a broken state."""
    js = JS.read_text()
    anchor = js.index(
        "v1.14.82: per-event inline actions for component=reprobe"
    )
    block = js[anchor:anchor + 5000]
    # Conditional: both media_type AND tmdb_id must be present.
    assert "det.media_type && det.tmdb_id !== null" in block
    # Button form.
    assert "data-act=\"reprobe-open-row\"" in block
    assert "// OPEN ROW" in block


def test_reprobe_again_button_always_renders_for_reprobe_events():
    """REPROBE AGAIN re-fires the global job; doesn't need any
    per-row resolution. Renders unconditionally on
    component=reprobe events."""
    js = JS.read_text()
    anchor = js.index(
        "v1.14.82: per-event inline actions for component=reprobe"
    )
    block = js[anchor:anchor + 5000]
    assert "data-act=\"reprobe-again\"" in block
    assert "// REPROBE AGAIN" in block


def test_hint_text_maps_each_reason_to_operator_help():
    """Each known reason string must map to a human-readable
    hint. Pin the 4 well-known reasons + the catch-all so a
    future change can't silently drop a hint mapping."""
    js = JS.read_text()
    anchor = js.index(
        "v1.14.82: per-event inline actions for component=reprobe"
    )
    block = js[anchor:anchor + 5000]
    assert "'sidecar empty'" in block
    assert "'sidecar missing'" in block
    assert "'plex range-GET failed'" in block
    assert "'empty response'" in block
    assert "probe error:" in block


# ── Client: click handlers wired ──────────────────────────────


def test_open_row_click_handler_navigates_to_library_with_info_params():
    """Clicking OPEN ROW must NAVIGATE to the library page with
    info_open + info_mt + info_section URL params so closing the
    dialog leaves the operator on the row's library tab.

    v1.14.85: behavior moved from a same-page openInfoDialog
    call to a navigation. the user's repro: clicking OPEN ROW on
    /queue popped the dialog over /queue, but closing it left
    him on /queue with no row to act on. Navigating to /movies
    or /tv first means the row is right there underneath the
    dialog. The library page's load handler reads the URL
    params and auto-fires openInfoDialog after loadLibrary."""
    js = JS.read_text()
    fn_start = js.index("function bindQueue() {")
    fn_end = js.index("\n  }\n", fn_start)
    body = js[fn_start:fn_end]
    assert 'closest(\'button[data-act="reprobe-open-row"]\')' in body
    # The handler now sets window.location.href.
    assert "window.location.href" in body
    # URL params for the auto-open path.
    assert "info_open" in body
    assert "info_mt" in body
    assert "info_section" in body
    # tabPath inferred from media_type.
    # v0.51.308: the ternary grew an anime branch — pin the routing
    # INVARIANTS (movie -> /movies, anime -> /anime, default /tv).
    # v0.51.309: anime is tested BEFORE the movie short-circuit (movie-
    # typed anime sections live on /anime) — pin membership + ORDER.
    assert "mt === 'movie' ? '/movies'" in body
    assert "dataset.anime === '1' ? '/anime'" in body
    assert (body.index("dataset.anime === '1'")
            < body.index("mt === 'movie' ? '/movies'"))


def test_library_page_auto_opens_info_dialog_from_url_params():
    """The library page (movies/tv/anime) must read
    ?info_open + ?info_mt + ?info_section on load and call
    openInfoDialog after loadLibrary so the dialog renders over
    a populated library."""
    js = JS.read_text()
    # Anchor on the v1.14.85 marker in the post-loadLibrary block.
    anchor = js.index(
        "v1.14.85: ?info_open=<tmdb_id>&info_mt=<movie|tv>"
    )
    # v0.51.306: structural end (the gate's catch line), not a 2000-byte
    # window — the consume-once strip grew the block past the old width.
    block = js[anchor:js.index("URLSearchParams not supported", anchor)]
    # Path guard.
    assert "path === '/movies' || path === '/tv' || path === '/anime'" in block
    # Reads all three params.
    assert "sp.get('info_open')" in block
    assert "sp.get('info_mt')" in block
    assert "sp.get('info_section')" in block
    # Calls openInfoDialog with them. v0.51.219: match the call PREFIX — the arity grew
    # when deep-links learned to carry info_edition; the invariant (openInfoDialog gets the
    # parsed params) is unchanged. The edition arg is covered by test_v0_51_219.
    assert "openInfoDialog(infoMt, infoOpen, infoSection" in block
    # Defers past loadLibrary's tbody render.
    assert "setTimeout(" in block


def test_reprobe_again_click_handler_posts_to_admin_endpoint():
    """REPROBE AGAIN button must POST to the existing
    /api/admin/reprobe-plex-themes endpoint and refresh /queue
    past the 1s topbar TTL so the new op shows promptly."""
    js = JS.read_text()
    fn_start = js.index("function bindQueue() {")
    fn_end = js.index("\n  }\n", fn_start)
    body = js[fn_start:fn_end]
    assert 'closest(\'button[data-act="reprobe-again"]\')' in body
    assert "api('POST', '/api/admin/reprobe-plex-themes')" in body
    # Confirmation prompt to avoid accidental fires.
    assert "confirm(" in body


# ── CSS: hint + actions row spans the full event-stream grid ──


def test_event_hint_and_actions_grid_full_span():
    """The .event-stream li uses display: grid with 4 columns.
    .event-hint and .event-actions must use grid-column: 1 / -1
    so they render as full-width sub-rows beneath the main
    event line (not squeezed into the existing 4-column
    layout)."""
    css = CSS.read_text()
    # Both classes use the full-row grid span.
    hint_idx = css.index(".event-hint {")
    hint_block = css[hint_idx:hint_idx + 300]
    assert "grid-column: 1 / -1;" in hint_block

    actions_idx = css.index(".event-actions {")
    actions_block = css[actions_idx:actions_idx + 300]
    assert "grid-column: 1 / -1;" in actions_block
