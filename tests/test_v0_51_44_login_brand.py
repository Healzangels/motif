"""v0.51.44 — the login card head shows the topbar MOTIF wordmark + synth EQ.

the user: "On the login page instead of the top bar saying authenticate can we
make it say MOTIF with the synth like in the top bar in the upper left corner".

The login .auth-card-head reuses the topbar brand's real .brand-mark equalizer
(the 5 animated .brand-bar bands) + the MOTIF wordmark, instead of the ▰ glyph +
"AUTHENTICATE". Login-only — setup.html keeps its ▰ + FIRST RUN.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
LOGIN = (REPO / "app" / "web" / "templates" / "login.html").read_text()
SETUP = (REPO / "app" / "web" / "templates" / "setup.html").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _head(html: str) -> str:
    i = html.index('class="auth-card-head"')
    return html[i:html.index("</div>", i)]


def test_login_head_uses_the_brand_equalizer():
    head = _head(LOGIN)
    # the real topbar equalizer classes (reuse, not a mirrored copy) …
    assert "brand-mark auth-brand-mark" in head
    # … with the 5 synth bands …
    assert head.count('<i class="brand-bar"></i>') == 5
    # … and the MOTIF wordmark heading.
    assert 'class="auth-title auth-brand-name">motif</h1>' in head
    # the old ▰ / AUTHENTICATE title is gone from the login head.
    assert "AUTHENTICATE" not in head
    assert "auth-glyph" not in head


def test_login_brand_css_scales_and_uppercases():
    # the equalizer is scaled up for the login title …
    assert ".auth-brand-mark { height: 20px; gap: 3px; }" in APP_CSS
    assert ".auth-brand-mark .brand-bar { width: 3px; }" in APP_CSS
    # … and the wordmark uppercases to MOTIF like the topbar brand.
    assert ".auth-title.auth-brand-name { text-transform: uppercase; }" in APP_CSS


def test_setup_page_is_unchanged():
    # the change is login-only; the first-run page keeps its ▰ + FIRST RUN.
    head = _head(SETUP)
    assert 'class="auth-glyph">▰' in head
    assert "FIRST RUN" in head
    assert "auth-brand-mark" not in head
