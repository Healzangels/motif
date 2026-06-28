"""v1.15.8 — // TEST COOKIES button + structured cookies validation.

the user: "is there any way to test to make sure my cookies.txt
file is working properly? I see its being detected in the
startup log entry but how do I know its actually working"

## Pre-fix

The startup log line `cookies_file = /config/cookies.txt
(present)` only checks that the file EXISTS. The only signals
that cookies were actually working were indirect:
  - positive: a successful download
  - negative: a probe failure with cookies_expired

Both required kicking real work and watching for outcomes.

## Fix

New `POST /api/admin/test-cookies` endpoint runs a structured
5-step check:

  1. file_present — exists, readable, stat OK
  2. parseable — valid Netscape format, ≥1 cookie line
  3. youtube_cookies — has YouTube auth cookies (SID / HSID /
     SAPISID / __Secure-1PSID / etc.)
  4. not_expired — none of the auth cookies have expired
  5. yt_dlp_load — yt-dlp can load + use them (probes a known
     stable public video)

Returns `{ok: bool, checks: [{name, ok, detail}, ...]}`. The
first failed check short-circuits the rest (no point testing
format if file's missing).

UI: `// TEST COOKIES` button on Settings → PATHS tab next to
the COOKIES FILE input. Click renders an inline checklist
with green ✓ / red ✗ per item.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


# ── Backend endpoint ───────────────────────────────────────────


def test_endpoint_route_defined():
    """The /api/admin/test-cookies POST route must exist with
    admin auth."""
    src = API_PY.read_text()
    assert '@app.post("/api/admin/test-cookies")' in src
    # Function name follows the existing api_admin_* convention.
    assert "async def api_admin_test_cookies(" in src
    # Admin auth required.
    fn_start = src.index("async def api_admin_test_cookies(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "_require_admin(request)" in fn_body


def test_endpoint_runs_all_five_checks():
    """The endpoint must run all five named checks in sequence:
    file_present, parseable, youtube_cookies, not_expired,
    yt_dlp_load. Each appends to the checks list with a name +
    ok + detail."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_admin_test_cookies(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    for check_name in ("file_present", "parseable", "youtube_cookies",
                       "not_expired", "yt_dlp_load"):
        assert f'"name": "{check_name}"' in fn_body, (
            f"endpoint must include the {check_name!r} check"
        )


def test_endpoint_short_circuits_on_first_failure():
    """If file_present fails, the parseable check shouldn't
    run (no file to parse). Pin the early-return pattern via
    the missing-file branch."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_admin_test_cookies(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The early returns in the file-missing / read-failed branches.
    file_present_block = fn_body[:fn_body.index('"name": "parseable"')]
    assert file_present_block.count("return {") >= 2, (
        "Multiple short-circuit returns must exist for the "
        "file_present checks (missing path / file not found / "
        "stat failed)"
    )


def test_endpoint_recognizes_youtube_auth_cookies():
    """The auth-cookies check must look for the canonical
    YouTube session cookie names — pin the set so a future
    change doesn't accidentally drop one and silently degrade
    the check."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_admin_test_cookies(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The core trio (proves logged-in YouTube session).
    for name in ("SID", "HSID", "SSID", "SAPISID"):
        assert f'"{name}"' in fn_body, (
            f"auth_cookie_names must include {name!r}"
        )
    # Newer __Secure- variants for current YouTube auth.
    assert '"__Secure-1PSID"' in fn_body
    assert '"__Secure-3PSID"' in fn_body


def test_endpoint_handles_httponly_prefix():
    """Some browsers prefix HttpOnly cookie lines with
    `#HttpOnly_`. The parser must strip that marker so those
    cookies are counted, not skipped as comments."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_admin_test_cookies(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert '"#HttpOnly_"' in fn_body, (
        "Parser must strip the #HttpOnly_ prefix"
    )


def test_endpoint_yt_dlp_check_uses_stable_public_video():
    """The yt_dlp_load check must probe a known stable public
    video so the test doesn't false-fail on a video that
    happens to be removed. Rick Astley NGGYU has been on
    YouTube since 2009 — safe to bet on."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_admin_test_cookies(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "dQw4w9WgXcQ" in fn_body, (
        "yt_dlp check should probe a stable public video — "
        "Rick Astley NGGYU is the canonical safe pick"
    )


def test_endpoint_yt_dlp_check_passes_cookies_path():
    """The probe call must pass cookies_file=cookies_path so
    yt-dlp actually loads them. Pre-fix risk: probe runs
    without cookies → succeeds → check reports OK even though
    cookies were never tested."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_admin_test_cookies(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # Find the probe_youtube_url call.
    probe_anchor = fn_body.index("probe_youtube_url")
    probe_block = fn_body[probe_anchor:probe_anchor + 800]
    assert "cookies_file=cookies_path" in probe_block, (
        "yt_dlp probe must pass cookies_file=cookies_path"
    )


# ── Frontend wiring ────────────────────────────────────────────


def test_settings_template_has_button_and_results_container():
    """The COOKIES FILE label must contain the new button +
    results <ul>. Pin the IDs since the JS reads them directly."""
    html = SETTINGS_HTML.read_text()
    assert 'id="test-cookies-btn"' in html
    assert 'id="test-cookies-status"' in html
    assert 'id="test-cookies-results"' in html
    assert "TEST COOKIES" in html


def test_results_ul_default_hidden():
    """The results list must start hidden (style="display:none")
    so the section doesn't render with empty space pre-click."""
    html = SETTINGS_HTML.read_text()
    anchor = html.index('id="test-cookies-results"')
    block = html[anchor:anchor + 200]
    assert 'style="display:none"' in block


def test_js_handler_function_defined():
    """bindTestCookies must exist + be wired into page-load init
    alongside the other settings binders."""
    src = APP_JS.read_text()
    assert "function bindTestCookies()" in src
    # Wired into the init sequence.
    assert "bindTestCookies();" in src


def test_js_handler_posts_to_endpoint():
    """The click handler must POST to /api/admin/test-cookies."""
    src = APP_JS.read_text()
    fn_start = src.index("function bindTestCookies()")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "/api/admin/test-cookies" in fn_body
    assert "api('POST'" in fn_body


def test_js_renders_per_check_li_with_mark_label_detail():
    """Each check renders as a <li> with three spans (mark,
    label, detail). The mark is ✓ for ok or ✗ for fail."""
    src = APP_JS.read_text()
    fn_start = src.index("function bindTestCookies()")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "cookies-test-mark" in fn_body
    assert "cookies-test-label" in fn_body
    assert "cookies-test-detail" in fn_body
    # Mark mapping.
    assert "'✓'" in fn_body
    assert "'✗'" in fn_body
    # OK / fail class application.
    assert "cookies-test-ok" in fn_body
    assert "cookies-test-fail" in fn_body


def test_js_label_map_covers_all_check_names():
    """The frontend's check-name → human-label map must cover
    every check name the backend emits. A missed mapping
    falls back to the raw snake_case name (ugly but not
    broken); pin the mapping for clarity."""
    src = APP_JS.read_text()
    fn_start = src.index("function bindTestCookies()")
    fn_end = src.index("function ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    for check_name in ("file_present", "parseable", "youtube_cookies",
                       "not_expired", "yt_dlp_load"):
        assert f"{check_name}:" in fn_body, (
            f"labelMap must include the {check_name!r} key"
        )


# ── CSS for the checklist ──────────────────────────────────────


def test_css_defines_results_styles():
    """The .cookies-test-results / .cookies-test-mark / etc.
    classes must be defined so the checklist renders with the
    right typography + color tints."""
    src = APP_CSS.read_text()
    assert ".cookies-test-results" in src
    assert ".cookies-test-ok" in src
    assert ".cookies-test-fail" in src
    # Color tints — green for ok, red for fail.
    fail_anchor = src.index(".cookies-test-fail")
    fail_block = src[fail_anchor:fail_anchor + 400]
    assert "var(--red)" in fail_block
    ok_anchor = src.index(".cookies-test-ok")
    ok_block = src[ok_anchor:ok_anchor + 400]
    assert "var(--green)" in ok_block
