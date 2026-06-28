"""v1.15.64 — SSR phase 3: insight charts + settings tabs.

Two flash classes the user wanted gone:

1. **Insight blocks visible-then-empty flash.** dashboard.html
   ships 4 blocks (insight-failures, insight-syncs,
   insight-downloads, source-breakdown) that JS reveals after
   /api/stats responds. Pre-fix the blocks were inline
   `style="display:none"` and JS un-hid them post-paint. On a
   fresh, near-empty DB they'd briefly render with "—" or 0
   then either stay (if data) or hide again (if not) — a
   visible empty-state flash. _dashboard_ssr_state now ships
   4 EXISTS predicates that mirror the JS reveal gates so the
   HTML the browser receives already has the right initial
   visibility.

2. **Settings tab deep-link PATHS flash.** /settings#tokens
   nav showed PATHS (the default-visible panel) for one paint
   tick before bindSettingsTabs swapped it. Pre-fix bound
   panel visibility entirely to JS post-paint. Fix: inline
   `<head>` script reads `window.location.hash`, stamps
   `<html data-settings-tab="X">` pre-paint; CSS rules in
   app.css use that attribute to override the inline
   display:none on every panel except X. JS-side showTab also
   updates the attribute so post-click navigation continues
   to work — the SSR CSS !important would otherwise pin the
   deep-linked panel.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
BASE_HTML = REPO / "app" / "web" / "templates" / "base.html"
DASH_HTML = REPO / "app" / "web" / "templates" / "dashboard.html"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── 1. Insight-block visibility flags in _dashboard_ssr_state ──────


def test_dashboard_ssr_state_declares_insight_visibility_defaults():
    """_dashboard_ssr_state must default all 4 insight flags to
    False so the template's `{% if not has_X %}` renders as hidden
    when the EXISTS query block can't run (e.g. error path)."""
    src = API_PY.read_text()
    func_start = src.index("def _dashboard_ssr_state()")
    func_end = src.index("templates.env.globals[\"dashboard_ssr_state\"]",
                          func_start)
    body = src[func_start:func_end]
    for flag in (
        '"has_insight_failures": False',
        '"has_insight_syncs": False',
        '"has_insight_downloads": False',
        '"has_source_breakdown": False',
    ):
        assert flag in body, (
            f"v1.15.64: _dashboard_ssr_state must default {flag} "
            "so the error path renders blocks hidden (matches JS "
            "reveal-on-data-arrival contract)"
        )


def test_dashboard_ssr_state_visibility_predicates_mirror_js_gates():
    """v1.15.65 tightened the predicates so they mirror the JS
    reveal gates exactly (mirror-principle class P):

    * has_insight_failures uses _FAILURES_SFA_FROM_SQL + WHERE so a
      theme with failure_kind unacked but sfa-acked doesn't trip
      the gate (the failure_kinds chart would render zero rows
      and JS would hide → flash).
    * has_insight_syncs needs >= 2 sync_runs with finished_at (JS
      gate is `rows.length < 2`).
    * has_insight_downloads uses the same window the JS plot reads.
    * has_source_breakdown: plex_items existence is sufficient.
    """
    src = API_PY.read_text()
    func_start = src.index("def _dashboard_ssr_state()")
    func_end = src.index("templates.env.globals[\"dashboard_ssr_state\"]",
                          func_start)
    body = src[func_start:func_end]
    # Failures must consume the SFA-aware canonical FROM + WHERE.
    assert "_FAILURES_SFA_FROM_SQL" in body, (
        "v1.15.65: has_insight_failures must use _FAILURES_SFA_* "
        "to mirror the failure_kinds chart's per-(title, section) "
        "sfa-aware predicate — else sfa-acked sections trip a "
        "flash via SSR-true → JS-hide"
    )
    assert "_FAILURES_SFA_WHERE_SQL" in body
    # Syncs must require >= 2 finished rows.
    assert "FROM sync_runs" in body
    assert "finished_at IS NOT NULL" in body
    assert ">= 2" in body, (
        "v1.15.65: has_insight_syncs must require >= 2 finished "
        "sync_runs to mirror the JS `rows.length < 2` hide gate"
    )
    # Downloads window matches JS chart's 30-day plot.
    assert "EXISTS (SELECT 1 FROM jobs" in body
    assert "job_type = 'download'" in body
    assert "'-30 days'" in body
    # Source breakdown: any plex_items row suffices.
    assert "EXISTS (SELECT 1 FROM plex_items)" in body


def test_dashboard_ssr_state_uses_single_connection():
    """v1.15.65: pre-fix _dashboard_ssr_state opened 3 separate
    `with get_conn(...)` blocks per render — wasted overhead on
    every dashboard page load (and base.html SSR also opens its
    own connection for _topbar_ssr_state). Tightened to a single
    block. This guards against accidentally re-introducing the
    fan-out via a later add."""
    src = API_PY.read_text()
    func_start = src.index("def _dashboard_ssr_state()")
    func_end = src.index("templates.env.globals[\"dashboard_ssr_state\"]",
                          func_start)
    body = src[func_start:func_end]
    n_conns = body.count("with get_conn(settings.db_path) as conn:")
    assert n_conns == 1, (
        f"v1.15.65: _dashboard_ssr_state must open exactly one "
        f"connection, found {n_conns}. Fan-out across "
        "`with get_conn(...)` blocks costs 2+ sqlite3.connect per "
        "page load + complicates txn semantics"
    )


def test_dashboard_html_insight_blocks_use_ssr_flags():
    """Each of the 4 insight blocks must gate its initial
    style="display:none" behind the matching SSR flag."""
    html = DASH_HTML.read_text()
    for block_id, flag in [
        ("insight-failures-block", "has_insight_failures"),
        ("insight-syncs-block", "has_insight_syncs"),
        ("insight-downloads-block", "has_insight_downloads"),
        ("source-breakdown-block", "has_source_breakdown"),
    ]:
        idx = html.index(f'id="{block_id}"')
        # Look in the next ~400 chars for both the flag check + display:none
        block = html[idx:idx + 400]
        assert f"_ssr_dash.{flag}" in block, (
            f"v1.15.64: #{block_id} must check _ssr_dash.{flag} "
            "to suppress the visible-then-empty flash on fresh DBs"
        )
        assert 'style="display:none"' in block, (
            f"v1.15.64: #{block_id} must keep style=display:none "
            "as the default-hidden inline (gated by the flag check)"
        )


# ── 2. Settings tab SSR pre-paint hash handling ─────────────────


def test_base_html_settings_tab_head_script_exists():
    """base.html's <head> must inject the pre-paint script that
    reads window.location.hash + stamps <html data-settings-tab>.
    Without it, /settings#tokens flashes PATHS for one paint tick
    before bindSettingsTabs runs."""
    html = BASE_HTML.read_text()
    # Script must appear in <head> (before </head>).
    head_end = html.index("</head>")
    head = html[:head_end]
    assert "data-settings-tab" in head, (
        "v1.15.64: base.html <head> must stamp data-settings-tab "
        "pre-paint from window.location.hash"
    )
    assert "window.location.hash" in head, (
        "v1.15.64: the SSR script must read window.location.hash"
    )
    assert "window.location.pathname !== '/settings'" in head, (
        "v1.15.64: scope the data attribute to /settings only — "
        "stamping it on other pages risks collisions with their "
        "own hash anchors"
    )


def test_base_html_settings_tab_script_sanitizes_input():
    """The hash sanitization regex must restrict input to
    lowercase letters only. A more permissive regex would let
    `?{}` etc. land in the attribute value (CSS-selector unsafe)."""
    html = BASE_HTML.read_text()
    head = html[:html.index("</head>")]
    # The regex restricts to lowercase letters only.
    assert "/^[a-z]+$/" in head, (
        "v1.15.64: the SSR script must sanitize the hash with "
        "/^[a-z]+$/ — broader regexes risk CSS-selector "
        "injection via data attribute"
    )


def test_app_css_settings_tab_panel_rules_cover_every_panel():
    """app.css must override the inline display:none on every
    settings panel that matches the data-settings-tab value.
    Drift between settings.html panels + css rules would leave
    the deep-linked panel hidden on first paint."""
    css = APP_CSS.read_text()
    # Find the SSR block.
    block_start = css.index("html[data-settings-tab] .tab-panel")
    block_end = css.index("}", css.index("display: block !important;",
                                          block_start))
    block = css[block_start:block_end]
    for panel in (
        "paths", "plex", "downloads", "matching", "schedule",
        "runtime", "tokens", "password", "homepage",
    ):
        assert (
            f'html[data-settings-tab="{panel}"] .tab-panel'
            f'[data-panel="{panel}"]' in block
        ), (
            f"v1.15.64: app.css missing the SSR rule for "
            f"data-panel=\"{panel}\" — paints would leave the "
            "deep-linked panel hidden"
        )


def test_app_css_settings_tab_active_underline_rules_cover_every_tab():
    """Same coverage check for the tab-active underline. Without
    the active rule the tab bar would paint with no underline
    until JS landed — a different but related flash."""
    css = APP_CSS.read_text()
    # The tab-active mirror rules.
    for panel in (
        "paths", "plex", "downloads", "matching", "schedule",
        "runtime", "tokens", "password", "homepage",
    ):
        sel = (
            f'html[data-settings-tab="{panel}"] '
            f'#settings-tabs .tab[data-tab="{panel}"]'
        )
        assert sel in css, (
            f"v1.15.64: app.css missing the active-tab SSR rule "
            f"for data-tab=\"{panel}\""
        )


def test_app_css_settings_tab_panels_match_settings_html_panels():
    """The set of data-panel values in settings.html must match
    the set of panels covered by the SSR CSS rules. Drift in
    either direction (new panel without CSS, or stale CSS for a
    deleted panel) trips this."""
    import re
    html = SETTINGS_HTML.read_text()
    panels_in_html = set(re.findall(r'data-panel="([a-z]+)"', html))
    css = APP_CSS.read_text()
    panels_in_css = set(re.findall(
        r'html\[data-settings-tab="([a-z]+)"\] \.tab-panel', css
    ))
    assert panels_in_html == panels_in_css, (
        f"v1.15.64: settings.html panels {panels_in_html} differ "
        f"from app.css SSR rules {panels_in_css} — every panel "
        "needs a matching SSR rule (and vice versa)"
    )


def test_app_js_show_tab_keeps_data_settings_tab_in_sync():
    """bindSettingsTabs.showTab must update <html data-settings-tab>
    on every tab click. Without it the SSR CSS `!important` for
    the deep-linked panel persists and clobbers the JS inline
    style:display swap — the panel that was deep-linked stays
    pinned even after the user clicks a different tab."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindSettingsTabs()")
    # showTab is a nested function — anchor on the next sibling
    # 2-space-indented top-level function in app.js.
    fn_end = js.index("\n  function ", fn_start + 1)
    body = js[fn_start:fn_end]
    assert "setAttribute('data-settings-tab'" in body, (
        "v1.15.64: showTab must keep <html data-settings-tab> in "
        "sync with the active tab; otherwise the SSR CSS rule's "
        "!important pins the deep-linked panel"
    )
