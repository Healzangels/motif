"""v0.50.87 — settings mobile overflow (batch 2) + drop the eager sync-probe.

Four more issues found testing motif on a phone, plus one functional bug.

1. API TOKENS: the 6-column table had no scroll context — spilled past its card's
   edge on mobile with the trailing columns (CREATED/LAST USED/ACTIONS)
   unreachable.

2. NOTIFICATIONS / SCHEDULE (and every OTHER settings tab with a second button):
   .form-actions had no flex-wrap, so a SAVE button + its status span + a second
   button (// TEST NOTIFICATION, // PROBE TRANSPORT) together exceeded the phone
   viewport width and the excess just spilled past the card edge instead of
   dropping to a 2nd line.

3. Functional bug: PLEX → // SYNC TRANSPORT's // PROBE TRANSPORT fired an actual
   network round-trip 800ms after EVERY settings-page load/refresh — not only
   when the user was looking at that tab, since `sync-probe-btn` lives in the DOM
   for every tab (inactive panels are just display:none). No sibling probe on the
   page (TEST COOKIES / PROBE PLEX THEMES / TEST NOTIFICATION / TEST PLEX)
   auto-fires; this one uniquely did. Now click-to-run like the others.

4. PLEX → LIBRARY SECTIONS: the two 7-column tables (ID/SECTION/TYPE/MGD/ROLE/
   LOCATIONS/ACTIONS) had no scroll context either, taking the MGD/ROLE
   checkboxes off-screen with them.

Fixes 1 and 4 both reuse the existing #library-table .table-scroll pattern
(library.html) — same wrapper class, same CSS, just applied to two more tables.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
SETTINGS = (REPO / "app" / "web" / "templates" / "settings.html").read_text()


def _rule(sel: str) -> str:
    i = APP_CSS.index(sel)
    return APP_CSS[i:APP_CSS.index("}", i) + 1]


# ── 1. API TOKENS table scrolls instead of spilling ─────────────────────────

def test_tokens_table_wrapped_in_table_scroll():
    i_table = SETTINGS.index('<table class="table" id="tokens-table">')
    i_wrap = SETTINGS.rindex('<div class="table-scroll">', 0, i_table)
    # the wrapper is the nearest preceding open div — nothing else sits between.
    assert SETTINGS[i_wrap:i_table].count("<div") == 1
    i_close_table = SETTINGS.index("</table>", i_table)
    i_close_wrap = SETTINGS.index("</div>", i_close_table)
    assert i_close_table < i_close_wrap


# ── 2. .form-actions wraps instead of spilling ──────────────────────────────

def test_form_actions_wraps_on_narrow_viewports():
    rule = _rule(".form-actions {")
    assert "flex-wrap: wrap;" in rule
    # unaffected properties still present (no regression to the v1.18.59/.61 fixes).
    assert "white-space: normal" in rule
    assert "border-top: 1px dashed var(--line)" in rule


# ── 3. // PROBE TRANSPORT no longer auto-fires on every settings load ──────

def test_sync_probe_is_click_only_like_its_siblings():
    fn_anchor = APP_JS.index("function bindSyncProbe() {")
    fn_end = APP_JS.index("\n  function bindConfigSaves()", fn_anchor)
    body = APP_JS[fn_anchor:fn_end]
    # the click handler still exists…
    assert "btn.addEventListener('click', () => { runProbe().catch(()=>{}); });" in body
    # …but the v1.13.2 auto-fire-on-load is gone.
    assert "setTimeout(() => { runProbe().catch(()=>{}); }, 800);" not in body
    assert "Auto-probe once on load" not in body


# ── 4. LIBRARY SECTIONS tables scroll instead of spilling ──────────────────

def test_both_libraries_tables_wrapped_in_table_scroll():
    for marker in ('<table class="table libraries-table">',
                   '<table class="table libraries-table libraries-table-tv">'):
        i_table = SETTINGS.index(marker)
        i_wrap = SETTINGS.rindex('<div class="table-scroll">', 0, i_table)
        assert SETTINGS[i_wrap:i_table].count("<div") == 1
        i_close_table = SETTINGS.index("</table>", i_table)
        i_close_wrap = SETTINGS.index("</div>", i_close_table)
        assert i_close_table < i_close_wrap


def test_libraries_table_tv_margin_still_applies():
    # the mobile fix wraps each table in its own div — the TV table's own
    # top-margin (spacing between the two tables) must survive that wrap.
    assert ".libraries-table-tv { margin-top: var(--gap-6); }" in APP_CSS
