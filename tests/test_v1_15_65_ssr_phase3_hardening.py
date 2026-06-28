"""v1.15.65 — bug-hunt follow-up to v1.15.64 SSR phase 3.

Three silent issues caught by an audit immediately after shipping
v1.15.64 + the yt-dlp review:

1. **Three connections per dashboard SSR.**
   _dashboard_ssr_state opened 3 separate `with get_conn(...)`
   blocks (row totals, plex_items coverage, v1.15.64 EXISTS
   visibility). Plus _topbar_ssr_state opens its own. = 4
   sqlite3.connect calls per dashboard render. Consolidated
   to 1; guard in test_v1_15_64_ssr_phase3.py prevents
   re-introducing fan-out.

2. **EXISTS predicates too permissive vs JS gates.**
   v1.15.64 used cheap EXISTS probes that didn't mirror what the
   chart's GROUP BY / data-arrival gate actually checks. A theme
   with `failure_kind` unacked but with the section sfa-acked
   would trip my SSR gate (block visible) while the chart's
   SFA-aware GROUP BY returned 0 rows → JS hides → exactly the
   one-tick visible-then-empty flash we were trying to kill.
   Tightened to use `_FAILURES_SFA_*` + the JS gates' `>= 2`
   thresholds for syncs / downloads. Guard in
   test_v1_15_64_ssr_phase3.py.

3. **Invalid hash blanks the settings page.**
   `/settings#blah` (lowercase but not a real panel name) made
   the inline SSR script stamp `<html data-settings-tab="blah">`.
   The CSS rule `html[data-settings-tab] .tab-panel { display:
   none !important; }` hid every panel via !important;
   bindSettingsTabs's `valid.includes(initial)` skipped showTab
   for the unknown name → blank page (no panel visible at all).
   Two-layer fix: the inline script now validates against a
   hardcoded panel allowlist (so the attribute never gets set
   for unknown panels), AND bindSettingsTabs falls back to
   `paths` when the attribute is present but the hash isn't a
   real tab (defense in depth for devtools / future drift).
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
BASE_HTML = REPO / "app" / "web" / "templates" / "base.html"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── Fix #3a: inline script uses panel allowlist ──────────────


def test_base_html_settings_tab_script_uses_panel_allowlist():
    """The v1.15.64 head script used only `/^[a-z]+$/` to filter.
    `/settings#blah` (or any letters-only string that isn't a
    panel) tripped the CSS !important hide-all-panels rule
    without ever being passed to bindSettingsTabs as a valid
    tab. v1.15.65: hardcoded panel array; only known names
    stamp the attribute."""
    html = BASE_HTML.read_text()
    head = html[:html.index("</head>")]
    # Must declare the panels array.
    m = re.search(
        r"var\s+panels\s*=\s*\[([^\]]+)\]", head
    )
    assert m, (
        "v1.15.65: base.html head script must declare a `panels` "
        "array allowlisting the legal data-panel values — bare "
        "/^[a-z]+$/ admits any letters string (incl. `blah`) and "
        "trips the CSS hide-all-panels rule"
    )
    script_panels = set(
        re.findall(r"'([a-z]+)'", m.group(1))
    )
    # Cross-source check vs settings.html.
    html_panels = set(
        re.findall(r'data-panel="([a-z]+)"', SETTINGS_HTML.read_text())
    )
    assert script_panels == html_panels, (
        f"v1.15.65: base.html head script panels {script_panels} "
        f"differ from settings.html data-panel values "
        f"{html_panels} — drift would silently break deep-links"
    )
    # And the membership check must use indexOf, not the loose regex.
    assert "panels.indexOf(h)" in head, (
        "v1.15.65: hash must be checked against the allowlist via "
        "panels.indexOf(h) >= 0 — a bare regex test admits invalid "
        "panel names"
    )


def test_base_html_settings_tab_script_no_longer_uses_bare_regex():
    """Counter-guard: the bare `/^[a-z]+$/.test(h)` gate that
    v1.15.64 shipped must not be the GATE anymore (regex may
    still be used for input sanitization-before-allowlist if
    desired, but it must NOT be the sole gate)."""
    html = BASE_HTML.read_text()
    head = html[:html.index("</head>")]
    # Find any line that uses the regex .test as the conditional.
    # The new pattern uses `panels.indexOf(h) >= 0` instead.
    # If `/^[a-z]+$/.test(h)` is the conditional in an `if`, that's a regression.
    assert "if (/^[a-z]+$/.test(h))" not in head, (
        "v1.15.65: the bare regex `/^[a-z]+$/.test(h)` gate was "
        "replaced with the panel allowlist — re-adding it would "
        "reintroduce the blank-page bug for /settings#<unknown>"
    )


# ── Fix #3b: JS fallback for invalid data-settings-tab ───────


def test_app_js_settings_tabs_fallback_on_invalid_attribute():
    """Defense in depth: if `<html data-settings-tab="X">` is set
    to a value not matching any tab, bindSettingsTabs falls back
    to the `paths` default. Without this, devtools-stamped or
    future-drift attribute values would leave the settings page
    blank (CSS !important hides every panel; JS skips showTab
    for the unknown name)."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindSettingsTabs()")
    fn_end = js.index("\n  function ", fn_start + 1)
    body = js[fn_start:fn_end]
    # Fallback to 'paths' when initial hash isn't in `valid` AND
    # the SSR attribute is present.
    assert "getAttribute('data-settings-tab')" in body, (
        "v1.15.65: bindSettingsTabs must check the SSR-stamped "
        "attribute to detect a drift case where the attribute is "
        "set but the hash isn't a real panel"
    )
    assert "showTab('paths')" in body, (
        "v1.15.65: bindSettingsTabs must fall back to showTab('paths') "
        "when invalid SSR attribute is present — else CSS !important "
        "leaves the page blank"
    )
