"""v1.24.95 — record/music design animations (the user — aesthetic pass).

The motif icon is already a vinyl record (grooves + spindle), so:
  A · record-spinner loading state (replaces the bare "loading…" in the INFO
      card) — the vinyl icon spins ~1.8s/rev.
  · login groove-ripple background — concentric rings radiate behind the card.
  C · topbar brand-mark is now a 3-bar VU-meter equalizer (was a blinking ▰▰▰).

All subtle/slow, and all INFINITE so the existing global
`@media (prefers-reduced-motion: reduce)` rule auto-disables them. Bases are
chosen so the reduced-motion rest state is sane (full bars / invisible ripples /
un-rotated record).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
LOGIN = (REPO / "app" / "web" / "templates" / "login.html").read_text()
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()


def test_keyframes_exist_and_are_infinite():
    for kf in ("@keyframes record-spin", "@keyframes groove-ripple", "@keyframes brand-eq"):
        assert kf in APP_CSS, f"missing {kf}"
    # infinite → the global prefers-reduced-motion clamp disables them.
    for cls in (".record-spinner .rec-grooves", ".login-ripple", ".brand-mark .brand-bar"):
        i = APP_CSS.index(cls)
        block = APP_CSS[i:APP_CSS.index("}", i)]
        assert "infinite" in block, f"{cls} animation must be infinite (reduced-motion-covered)"


def test_record_loader_helper_and_use():
    assert "function recordLoaderHtml(" in APP_JS
    assert 'class="record-spinner"' in APP_JS
    assert "body.innerHTML = recordLoaderHtml('loading…')" in APP_JS
    # the "loading…" caption is preserved under the spinner.
    assert "loading…" in APP_JS


def test_login_ripple_background_present():
    assert 'class="login-bg"' in LOGIN
    assert LOGIN.count('class="login-ripple"') == 3
    # the card must sit above the ripple layer.
    block = APP_CSS[APP_CSS.index(".auth-card {"):APP_CSS.index(".auth-card {") + 400]
    assert "z-index: 1" in block


def test_brand_mark_is_equalizer_bars():
    assert BASE.count('class="brand-bar"') == 3
    assert "▰▰▰" not in BASE, "the static ▰▰▰ glyph was replaced by animated bars"
    # the old blink animation is gone from the brand-mark rule.
    i = APP_CSS.index(".brand-mark {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    assert "blink" not in block


def test_reduced_motion_rest_states_are_sane():
    # bars: no animation-fill-mode:forwards (so reduced-motion reverts to the
    # un-transformed base = full-height bars, not the 40%-scaled keyframe).
    i = APP_CSS.index(".brand-mark .brand-bar {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    assert "forwards" not in block
    # ripples: base opacity 0 so the clamp leaves them invisible, not solid rings.
    j = APP_CSS.index(".login-ripple {")
    rblock = APP_CSS[j:APP_CSS.index("}", j)]
    assert "opacity: 0" in rblock
