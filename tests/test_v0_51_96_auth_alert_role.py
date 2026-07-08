"""v0.51.96 — the login + setup error banners announce to screen readers.

Both auth templates render a server-side validation error as
`<div class="alert alert-error">! {{ error }}</div>` under `{% if error %}`.
Without an ARIA role a screen reader never announces a failed login / setup
error. v0.51.96 adds role="alert" (the standard live-region role for a
validation error) to both. These are the only two `.alert alert-error` sites.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
TPL = REPO / "app" / "web" / "templates"


def test_login_error_banner_has_alert_role():
    html = (TPL / "login.html").read_text()
    assert '<div class="alert alert-error" role="alert">' in html, (
        "v0.51.96: the login error banner must carry role=\"alert\" so a "
        "screen reader announces a failed login")


def test_setup_error_banner_has_alert_role():
    html = (TPL / "setup.html").read_text()
    assert '<div class="alert alert-error" role="alert">' in html, (
        "v0.51.96: the setup error banner must carry role=\"alert\"")


def test_no_roleless_alert_error_banner_remains():
    # Guard against a future auth page reintroducing a role-less error banner:
    # every `.alert alert-error` div in the templates must carry role="alert".
    for tpl in TPL.glob("*.html"):
        html = tpl.read_text()
        idx = 0
        while True:
            i = html.find('class="alert alert-error"', idx)
            if i == -1:
                break
            # the opening <div ...> tag this class lives in must include role=alert
            tag_start = html.rfind("<", 0, i)
            tag_end = html.find(">", i)
            tag = html[tag_start:tag_end]
            assert 'role="alert"' in tag, (
                f"{tpl.name}: an .alert alert-error banner is missing "
                f"role=\"alert\" (a11y) — {tag!r}")
            idx = tag_end + 1
