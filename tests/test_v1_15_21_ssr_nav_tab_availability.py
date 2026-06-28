"""v1.15.21 — SSR tab availability for the nav (no-flash on tab switch).

the user: "when switching between libraries, movies, tv, dash, settings
when you first click the libraries section disappear briefly, doesn't
appear to happen when clicking logs"

## Pre-fix

base.html hardcoded `style="display:none"` on the MOVIES / TV SHOWS /
ANIME nav links. JS-side `applyTabAvailability` revealed them after
/api/stats landed. The v1.11.33 localStorage cache made this faster
on subsequent navigations but the cached-restore still ran inside
the defer-loaded app.js — by the time it executed, the user had
already seen the hidden state for one paint cycle. LOGS didn't have
the inline display:none so didn't flash; the rest did.

## Fix

Server-side render the right initial visibility:
1. Register a Jinja env global `nav_tab_availability()` that does a
   single-row EXISTS query against plex_sections.
2. base.html nav uses Jinja conditionals around the `style="display:
   none"` so the HTML the browser receives already has the right
   visibility.

JS-side applyTabAvailability still runs for live updates (e.g. when
the user enables/disables a section in settings) and is a no-op when
the SSR matched. Belt and braces.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
BASE_HTML = REPO / "app" / "web" / "templates" / "base.html"


def test_nav_tab_availability_jinja_global_registered():
    """The helper must register itself as a Jinja env.globals entry
    so any template that extends base.html can call it. Pin both
    the function definition + the env.globals registration so a
    future refactor can't silently break the SSR path."""
    src = API_PY.read_text()
    assert "def _tab_availability_for_nav(" in src, (
        "v1.15.21: helper must exist in api.py"
    )
    assert 'templates.env.globals["nav_tab_availability"]' in src, (
        "v1.15.21: helper must be registered as a Jinja env global"
    )


def test_helper_queries_plex_sections_with_correct_predicates():
    """The SQL must check `included=1` (per the canonical FAIL chip
    predicate's section gating) and split movies / tv / anime by
    type + is_anime flag. Pin all three EXISTS branches."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _tab_availability_for_nav(")
    fn_end = src.index("templates.env.globals", fn_anchor)
    fn_body = src[fn_anchor:fn_end]
    assert "FROM plex_sections" in fn_body
    assert "included=1" in fn_body
    assert "type='movie'" in fn_body
    assert "type='show'" in fn_body
    assert "is_anime=1" in fn_body
    # All three return keys.
    assert "AS movies" in fn_body
    assert "AS tv" in fn_body
    assert "AS anime" in fn_body


def test_helper_falls_back_safely_on_db_error():
    """If the DB read fails (corrupt SQLite, schema mid-migration,
    first-run state), the helper must return a safe default (all
    hidden) rather than raising. JS still recovers from /api/stats
    on a working install."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _tab_availability_for_nav(")
    fn_end = src.index("templates.env.globals", fn_anchor)
    fn_body = src[fn_anchor:fn_end]
    assert "except Exception:" in fn_body
    # Safe default = all False.
    assert '"movies": False' in fn_body
    assert '"tv": False' in fn_body
    assert '"anime": False' in fn_body


def test_base_html_nav_uses_ssr_availability():
    """base.html must call `nav_tab_availability()` and apply
    the result via Jinja conditionals around the style="display:
    none" attributes. Pre-fix the inline display:none was
    unconditional. Pin the conditional shape so a future template
    refactor can't drop it and silently bring the flash back."""
    src = BASE_HTML.read_text()
    assert "nav_tab_availability()" in src, (
        "v1.15.21: base.html must invoke the SSR helper"
    )
    # Conditional MOVIES / TV / ANIME visibility.
    assert "_nav_ta.movies" in src
    assert "_nav_ta.tv" in src
    assert "_nav_ta.anime" in src


def test_base_html_no_unconditional_display_none_on_library_nav():
    """Defensive guard: the unconditional `style="display:none"`
    on the MOVIES / TV / ANIME nav links must be gone. Pre-fix
    pattern: `<a href="/movies" data-nav="movies" style="display:
    none">MOVIES</a>`. Post-fix the display:none lives inside
    a Jinja `{% if not _nav_ta.movies %}` block."""
    src = BASE_HTML.read_text()
    # The exact pre-fix substring must NOT appear.
    forbidden = [
        '"/movies" data-nav="movies" style="display:none"',
        '"/tv" data-nav="tv" style="display:none"',
        '"/anime" data-nav="anime" style="display:none"',
    ]
    for pat in forbidden:
        assert pat not in src, (
            f"v1.15.21: unconditional display:none on nav link "
            f"resurfaced — {pat!r}"
        )


def test_dash_logs_settings_nav_links_unchanged():
    """The DASH / LOGS / SETTINGS nav links must NOT have any
    SSR conditional applied — they're always visible. the user's
    repro specifically noted LOGS didn't flash; that invariant
    must hold post-fix."""
    src = BASE_HTML.read_text()
    # DASHBOARD always visible (no conditional). v0.50.24: label DASH → DASHBOARD.
    assert '<a href="/" data-nav="dashboard">DASHBOARD</a>' in src
    # LOGS always visible.
    assert '<a href="/queue" data-nav="queue">LOGS</a>' in src
    # SETTINGS always visible (the nav-attn dot inside has its own
    # display:none which is unrelated — it's a notification badge).
    assert '<a href="/settings" data-nav="settings">SETTINGS' in src
