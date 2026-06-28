"""v0.50.1 — login screen: centred auth-card + spinning-record backdrop.

the user: "for the login page I would like the login box to be in the center
also I would like to make the background the spinning record motif icon since
we don't see it very much."

Two changes, CSS + template only:

1. The auth-card was top-pinned (`margin: 60px auto`) — top-heavy against a
   backdrop that radiates from screen-centre. base.html now stamps
   `body.auth-page` on the chrome-less (unauthenticated) view; app.css scopes a
   flex-column centre to it (card dead-centre, footer pinned bottom). Standard
   topbar pages carry no `auth-page` class, so they're untouched.

2. The login backdrop gains the motif record icon itself — a large, faint,
   slowly-spinning vinyl REUSING the v1.24.95 `.record-spinner .rec-*` icon (so
   the strokes can't drift from the loader). `.login-record` only rescales /
   dims / slows it. The v1.24.95 groove-ripples are kept.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
LOGIN = (REPO / "app" / "web" / "templates" / "login.html").read_text()
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()


# ── 1. centring ──────────────────────────────────────────────────────────────


def test_base_stamps_auth_page_class_when_unauthenticated():
    """base.html adds body.auth-page on the unauthenticated login view (the
    scope hook for the centring rules)."""
    assert 'class="auth-page"' in BASE
    # gated on NOT authenticated — same principal guard the topbar block uses.
    assert "is_authenticated" in BASE


def test_auth_page_centres_the_card():
    """body.auth-page makes .content a full-height flex centre so the card sits
    dead-centre instead of top-pinned."""
    assert "body.auth-page {" in APP_CSS
    i = APP_CSS.index("body.auth-page .content {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    assert "align-items: center" in block
    assert "justify-content: center" in block
    assert "flex: 1 0 auto" in block
    # the top-pin margin is neutralised so flex centring controls placement.
    assert "body.auth-page .auth-card { margin: 0; }" in APP_CSS


# ── 2. spinning-record backdrop ──────────────────────────────────────────────


def test_login_reuses_record_spinner_icon():
    """The backdrop record REUSES the canonical .record-spinner icon (both
    classes on the SVG) so its strokes stay identical to the loader's."""
    assert 'class="record-spinner login-record"' in LOGIN
    # the vinyl's defining parts are present (same markup as recordLoaderHtml).
    for part in ("rec-rim", "rec-grooves", "rec-groove-outer",
                 "rec-tick", "rec-spindle", "rec-hole"):
        assert part in LOGIN, f"login record missing {part}"
    # the v1.24.95 ripples are kept (record + ripples).
    assert LOGIN.count('class="login-ripple"') == 3


def test_login_record_is_large_faint_and_slow():
    """.login-record only rescales / dims / slows the reused icon."""
    i = APP_CSS.index(".login-record {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    assert "94vmin" in block          # v0.50.26: a touch larger again, still whole in frame
    assert "opacity: 0.13" in block   # faint backdrop, not foreground
    # spin slowed to a turntable idle (overrides the loader's 1.8s).
    j = APP_CSS.index(".login-record .rec-grooves {")
    sblock = APP_CSS[j:APP_CSS.index("}", j)]
    assert "animation-duration: 16s" in sblock
