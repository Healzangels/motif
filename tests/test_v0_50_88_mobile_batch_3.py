"""v0.50.88 — mobile bug batch round 3 (a much bigger sweep from on-device
testing) + one accessibility fix found via browser console.

1. TOPBAR: op-mini's job-progress pill (label + bar) had no mobile cap, and
   .topbar-status (the topbar grid's third column) had no shrink floor —
   together they could force the whole topbar wider than the viewport
   whenever a job was running, pushing ?/logout off-screen (the user).

2. LOGS/JOBS: the fixed 180px ACTION column left the other 6 fluid columns
   a few px each on mobile — unreadable. LOGS/EVENT STREAM: the fixed
   TIME/LEVEL/COMPONENT columns left MESSAGE a sliver so narrow every word
   broke one character per line.

3. LOGIN: the v0.50.86 min-height fix only grows .auth-card when content
   exceeds its floor — the error-banner state needed more room than the
   no-error state, and the card wasn't set up to add any (the user: "clipping
   the circle ... if not worse when password is incorrect").

4. GLOSSARY: re-clicking // GLOSSARY while already open called showModal()
   on an already-open <dialog> (throws) instead of closing it, so the button
   stayed permanently .open/highlighted (the user).

5. INFO CARD: the download ↓ link wrapped onto its own flush-left line below
   the play bar instead of sharing it (the user: "looks out of place").

6. SETTINGS: a full mobile-overflow audit of every settings tab found one
   table missed by the v0.50.87 pass — IMPORT's preview table.

7. ACCESSIBILITY: #ops-drawer's static aria-hidden="true" was never cleared
   on open, so focusing any descendant (e.g. the × close button) while it
   was visible tripped Chrome's aria-hidden/focus-retention block (seen in
   the user's mobile console).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()
LOGIN = (REPO / "app" / "web" / "templates" / "login.html").read_text()
SETTINGS = (REPO / "app" / "web" / "templates" / "settings.html").read_text()


def _mobile_block(css: str) -> str:
    """The @media (max-width: 600px) block — same one the v0.50.48/86/87
    mobile fixes all share. Slice from its opening brace to the matching
    top-level close by brace-counting. v0.50.93: returns '' when the file has
    no such block (ops.css lost its mobile block when the dead op-mini caps
    were removed — see below)."""
    if "@media (max-width: 600px) {" not in css:
        return ""
    i = css.index("@media (max-width: 600px) {")
    depth = 0
    j = i
    while True:
        if css[j] == "{":
            depth += 1
        elif css[j] == "}":
            depth -= 1
            if depth == 0:
                return css[i:j + 1]
        j += 1


MOBILE_APP_CSS = _mobile_block(APP_CSS)
MOBILE_OPS_CSS = _mobile_block(OPS_CSS)


# ── 1. topbar overflow when a job is running ────────────────────────────

def test_op_mini_shrinks_on_mobile():
    # v0.50.93: the v0.50.88 ops.css caps (.op-mini-label max-width:80px /
    # .op-mini-bar width:40px) were superseded by the v0.50.91 #op-mini
    # full-width strip in app.css (ID selector wins), then removed as dead.
    # The mobile op-mini sizing now lives on the strip in MOBILE_APP_CSS.
    assert ".op-mini .op-mini-label { max-width: 80px; }" not in MOBILE_OPS_CSS
    assert "#op-mini .op-mini-label { max-width: 55%; }" in MOBILE_APP_CSS
    assert "#op-mini .op-mini-bar { width: 90px;" in MOBILE_APP_CSS


def test_op_mini_desktop_rule_unchanged():
    # the base (desktop) rules keep their original, larger sizing —
    # only the mobile override shrinks them.
    assert "max-width: 220px;" in OPS_CSS
    assert "width: 90px;" in OPS_CSS


def test_topbar_status_shrinks_and_wraps_on_mobile():
    # v0.51.3: the status cluster is now the `status` grid area (topbar is a
    # 2-row areas grid on a phone — see test_v0_51_3_mobile_nav_row), but it
    # still shrinks + wraps exactly as v0.50.88 established.
    assert ".topbar-status { grid-area: status; min-width: 0; flex-wrap: wrap; justify-content: flex-end; }" in MOBILE_APP_CSS


def test_topbar_nav_column_floored_on_mobile():
    # v0.51.3 SUPERSEDED the v0.50.88 floored middle-column approach: the 7-tab
    # nav no longer shares the row as a crushed `minmax(24px, 1fr)` sliver — it
    # gets its own full-width second row. The floored-column grid is gone; the
    # new 2-row layout is asserted in test_v0_51_3_mobile_nav_row.
    assert "grid-template-columns: auto minmax(24px, 1fr) auto;" not in MOBILE_APP_CSS
    assert 'grid-template-areas: "brand status" "nav nav";' in MOBILE_APP_CSS


# ── 2. LOGS/JOBS + LOGS/EVENT STREAM unreadable on mobile ───────────────

def test_jobs_grid_gets_shared_horizontal_scroll_on_mobile():
    assert ".jobs-grid { overflow-x: auto; -webkit-overflow-scrolling: touch; }" in MOBILE_APP_CSS
    assert ".jobs-grid-row { min-width: 760px; }" in MOBILE_APP_CSS
    assert ".jobs-scroll-y { width: max-content; }" in MOBILE_APP_CSS


def test_jobs_grid_row_desktop_rule_still_single_line():
    # the v1.19.2 contract (each row stays one line; the mobile min-width
    # widens it, doesn't remove the nowrap behavior).
    idx = APP_CSS.index(".jobs-grid-row {")
    end_idx = APP_CSS.index("}", idx)
    assert "white-space: nowrap" in APP_CSS[idx:end_idx]


def test_event_stream_stacks_message_below_meta_on_mobile():
    assert ".event-stream li { grid-template-columns: auto auto 1fr; row-gap: 2px; }" in MOBILE_APP_CSS
    assert ".event-msg { grid-column: 1 / -1; }" in MOBILE_APP_CSS


def test_event_stream_desktop_rule_still_uses_minmax():
    # first occurrence == the base/desktop rule (the mobile override is a
    # later, second occurrence further down the file).
    idx = APP_CSS.index(".event-stream li {")
    end_idx = APP_CSS.index("}", idx)
    assert "minmax(" in APP_CSS[idx:end_idx]


# ── 3. login auth-card clips on the error (invalid password) state ─────

def test_login_error_state_gets_a_modifier_class():
    assert 'class="auth-card{% if error %} auth-card-has-error{% endif %}"' in LOGIN


def test_auth_card_error_state_grows_taller_on_mobile():
    assert ".auth-card.auth-card-has-error { min-height: min(600px, 150vw); }" in MOBILE_APP_CSS


def test_auth_card_base_rule_unchanged():
    # the v0.50.86 fix (plain circle, no error) is untouched — only the
    # error state gets the extra mobile height budget.
    idx = APP_CSS.index(".auth-card {")
    end_idx = APP_CSS.index("}", idx)
    block = APP_CSS[idx:end_idx]
    assert "min-height: min(460px, 92vw);" in block
    assert "width: min(460px, 92vw);" in block


# ── 4. glossary button stuck open/highlighted on re-click ───────────────

def test_glossary_button_toggles_instead_of_always_opening():
    fn_anchor = APP_JS.index("function initHelpMode() {")
    fn_end = APP_JS.index("\n  // v1.13.50:", fn_anchor)
    body = APP_JS[fn_anchor:fn_end]
    assert "if (dlg.open) {" in body
    assert "dlg.close();" in body
    assert "showModalNoFocusRing(dlg);" in body
    # the close listener that clears .open must still be present — the
    # toggle's close() branch relies on it firing.
    assert "dlg.addEventListener('close', () => open.classList.remove('open'));" in body


# ── 5. info-card download link no longer strands below the play bar ────

def test_info_play_row_is_a_flex_row_that_keeps_both_on_one_line():
    idx = APP_CSS.index(".info-play-row {")
    end_idx = APP_CSS.index("}", idx)
    block = APP_CSS[idx:end_idx]
    assert "display: flex;" in block
    assert "flex-wrap: wrap;" in block


def test_info_audio_fills_the_play_row():
    # v0.51.29: the ↓ download sibling was removed, so the player no longer
    # needs the 0-basis line-sharing trick.
    # v0.51.58: the player is WIDTH-CAPPED (was max-width:none / full-bleed) so
    # it's a tidy control in the value column, not a bar stretching the whole 1fr
    # (the user: it "goes so far to the right"). width:100% + flex still fill up to
    # the cap + shrink on narrow phones.
    # v0.51.60: cap 340->360 to match the thumbnail width (the user).
    idx = APP_CSS.index(".info-audio {")
    end_idx = APP_CSS.index("}", idx)
    block = APP_CSS[idx:end_idx]
    assert "min-width: 0;" in block
    assert "max-width: 360px;" in block and "width: 100%;" in block
    assert "max-width: none;" not in block


def test_info_play_row_dd_wraps_the_audio_and_download_link():
    assert '<dt>play</dt><dd class="info-play-row">' in APP_JS
    assert "class=\"info-audio\"" in APP_JS


# ── 6. settings IMPORT preview table missed by the v0.50.87 sweep ──────

def test_import_preview_table_wrapped_in_table_scroll():
    i_table = SETTINGS.index('<table class="table table-tight" id="import-preview-table">')
    i_wrap = SETTINGS.rindex('<div class="table-scroll">', 0, i_table)
    assert SETTINGS[i_wrap:i_table].count("<div") == 1
    i_close_table = SETTINGS.index("</table>", i_table)
    i_close_wrap = SETTINGS.index("</div>", i_close_table)
    assert i_close_table < i_close_wrap


# ── 7. ops-drawer aria-hidden/focus-retention console violation ────────

def test_open_drawer_clears_aria_hidden():
    fn_anchor = OPS_JS.index("function openDrawer() {")
    fn_end = OPS_JS.index("\n  function closeDrawer() {", fn_anchor)
    body = OPS_JS[fn_anchor:fn_end]
    assert "drawer.removeAttribute('aria-hidden');" in body


def test_close_drawer_blurs_focus_then_restores_aria_hidden():
    fn_anchor = OPS_JS.index("function closeDrawer() {")
    fn_end = OPS_JS.index("\n  // ── DOM wiring", fn_anchor)
    body = OPS_JS[fn_anchor:fn_end]
    assert "drawer.contains(document.activeElement)" in body
    assert "document.activeElement.blur();" in body
    assert "drawer.setAttribute('aria-hidden', 'true');" in body
    # the blur/re-hide must happen BEFORE is-open is removed (which starts
    # the fade-out) so a screen reader never sees a visible, un-hidden
    # drawer with focus still trapped inside it.
    assert body.index("drawer.setAttribute('aria-hidden', 'true');") < body.index("drawer.classList.remove('is-open');")
