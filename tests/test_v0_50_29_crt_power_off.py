"""v0.50.29 — CRT power-on now fires on login + refresh; power-OFF on logout.

the user never saw the v0.50.7 power-on (it was sessionStorage-gated to once per
tab session). Now it fires after a login (login.html stamps motif:just-logged-in
on submit; base.html plays it on the authenticated landing page) and on a manual
refresh; a new power-OFF tube-collapse plays on the logout ⏻ click before nav.
"""
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()
LOGIN = (REPO / "app" / "web" / "templates" / "login.html").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_login_stamps_the_just_logged_in_flag_on_submit():
    assert "form.auth-form" in LOGIN
    assert "sessionStorage.setItem('motif:just-logged-in', '1')" in LOGIN


def test_power_on_fires_on_login_landing_not_failed_login():
    # reads + clears the flag, and only plays on a non-auth-page (real landing).
    assert "sessionStorage.removeItem('motif:just-logged-in')" in BASE
    assert "document.body.classList.contains('auth-page')" in BASE


def test_power_off_overlay_and_logout_wiring():
    assert 'id="crt-power-off"' in BASE
    # the ⏻ link is intercepted, the flicker plays, then it navigates.
    assert "querySelector('a.topbar-logout')" in BASE
    assert "off.classList.add('playing')" in BASE
    assert "link.getAttribute('href')" in BASE
    # v0.50.57: the flourish always plays — no reduced-motion skip (full CRT motion).
    assert "prefers-reduced-motion: reduce" not in BASE


def test_power_off_css_collapses_and_is_inert_at_rest():
    block = APP_CSS[APP_CSS.index(".crt-power-off {"):]
    block = block[:block.index("}")]
    assert "display: none" in block        # fully inert until played
    assert "pointer-events: none" in block
    assert "@keyframes crt-power-off-line" in APP_CSS
    assert "@keyframes crt-power-off-bg" in APP_CSS
