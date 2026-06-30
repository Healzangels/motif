"""v1.24.92 — INFO card cover layout reverted to option A (the user kept A over B).

After comparing A (v1.24.89) and B (v1.24.90) live, the user preferred A: the cover
top-LEFT (120px), title + scope chip + playback headline beside it, and the full
.dlg-grid as a FULL-WIDTH sibling below the hero (not nested beside the cover as
B had it). Option B's bigger-cover-+-meta-column lives at tag v1.24.90. The
v1.24.91 Esc-close focus fix is layout-independent and carries forward.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


# v0.50.64: scope to the FULL card (openInfoDialog). The universal bare
# card (renderBareInfoCard) reuses .info-hero / .info-hero-meta / .dlg-grid
# (posterless) and is defined EARLIER in the file, so a bare APP_JS.index()
# now lands on it. These are full-card layout guards — anchor past the bare
# card so they keep testing openInfoDialog's poster-left hero.
_CARD = APP_JS.index('async function openInfoDialog(')


def test_poster_is_first_hero_child():
    i_hero = APP_JS.index('<div class="info-hero">', _CARD)
    i_poster = APP_JS.index('${posterImgHtml}', i_hero)
    i_meta = APP_JS.index('<div class="info-hero-meta">', i_hero)
    assert i_hero < i_poster < i_meta


def test_grid_is_full_width_sibling_after_hero():
    i_hero = APP_JS.index('<div class="info-hero">', _CARD)
    # the meta + hero <div>s both close BEFORE the grid opens (grid un-nested).
    i_close = APP_JS.index('</div>\n      </div>', i_hero)
    i_grid = APP_JS.index('<dl class="dlg-grid">', i_hero)
    i_recovery = APP_JS.index('${recoveryPlaceholder}', i_grid)
    assert i_hero < i_close < i_grid < i_recovery


def test_cover_back_to_120():
    _s = APP_CSS.index(".info-poster {")
    block = APP_CSS[_s:APP_CSS.index("}", _s) + 1]
    assert "flex: 0 0 120px" in block
    assert "width: 120px" in block
    assert "180px" not in block, "v1.24.92: option B's 180 cover is gone"
    # the v1.24.85 anti-blowup pair must remain.
    assert "min-width: 0" in block
    assert "aspect-ratio: 2 / 3" in block


def test_playback_headline_present_and_styled():
    assert 'class="info-hero-playback' in APP_JS
    assert '_derivePlaybackSourceLabel()' in APP_JS
    assert '.info-hero-playback' in APP_CSS


def test_playback_row_not_duplicated_in_grid():
    assert 'playback source</dt>' not in APP_JS


def test_esc_close_focus_fix_carried_forward():
    # the v1.24.91 opener-blur is layout-independent — must survive the revert.
    assert "requestAnimationFrame" in APP_JS
    assert "dialog[open]" in APP_JS
    assert "body.querySelector('.info-poster')" in APP_JS
