"""v0.50.66 — bare INFO card shows the Plex poster like a themed item.

the user: "could we include the content's poster from Plex like a themed item."
The full card already renders a poster-left hero from the `/api/plex/art/{rk}`
same-origin proxy (numeric-rk-guarded, v1.24.52). The bare card already carries
the row's rating_key, so renderBareInfoCard builds the IDENTICAL
`<img class="info-poster">` as the first hero child — fully client-side, no new
data or round-trip. openBareInfoDialog attaches the same post-paint error
handler so a 404 / non-art collapses the hero to just the meta.
"""
from __future__ import annotations

from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── Static: the bare card uses the SAME poster pattern as the full card ──

def test_bare_card_poster_matches_full_card_pattern():
    """Same class, same proxy URL, same numeric-rk guard, same encodeURIComponent
    — so the two cards' posters stay visually + behaviourally identical."""
    start = APP_JS.index("function renderBareInfoCard(")
    end = APP_JS.index("function openBareInfoDialog(", start)
    src = APP_JS[start:end]
    assert 'class="info-poster"' in src
    # v0.51.310: .jpg spelling (IDS static-classification).
    assert "src=\"/api/plex/art/${encodeURIComponent(posterRk)}.jpg\"" in src
    # numeric-rk guard (Plex art proxy 400s on non-digits) mirrors the full card
    assert "/^\\d+$/.test(posterRk)" in src
    # poster is the FIRST hero child (poster-left layout), before the meta div
    i_poster = src.index("${posterImgHtml}")
    i_meta = src.index('<div class="info-hero-meta">')
    assert i_poster < i_meta


def test_open_bare_dialog_attaches_poster_error_handler():
    """A 404 / non-art response must remove the <img> so the hero collapses to
    just the meta (mirrors openInfoDialog's handler), not show a broken image."""
    start = APP_JS.index("function openBareInfoDialog(rk) {")
    end = APP_JS.index("\n  }", start)
    src = APP_JS[start:end]
    assert "body.querySelector('.info-poster')" in src
    assert "addEventListener('error', (ev) => ev.target.remove())" in src


# ── Behavioral: run renderBareInfoCard in quickjs ──

def _render_fn_src() -> str:
    start = APP_JS.index("function renderBareInfoCard(")
    end = APP_JS.index("function openBareInfoDialog(", start)
    return APP_JS[start:end]


def _run(call: str) -> str:
    quickjs = pytest.importorskip("quickjs")
    ctx = quickjs.Context()
    harness = (
        "var htmlEscape = function(s){return String(s===undefined||s===null?'':s)"
        ".replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')"
        ".replace(/\"/g,'&quot;').replace(/'/g,'&#39;');};\n"
        + _render_fn_src() + "\n"
        + call
    )
    return ctx.eval(harness)


def test_numeric_rating_key_renders_poster():
    out = _run("renderBareInfoCard({plex_title:'Blade Runner',rating_key:'9001'});")
    assert '<img class="info-poster"' in out
    assert 'src="/api/plex/art/9001.jpg"' in out


def test_non_numeric_rating_key_renders_no_poster():
    """A synthetic / non-digit rating_key must NOT emit a poster img (the proxy
    400s on non-digits) — the hero just shows the meta."""
    out = _run("renderBareInfoCard({plex_title:'X',rating_key:'themerrdb-42'});")
    assert "info-poster" not in out
    # missing/blank rating_key likewise yields no poster
    out2 = _run("renderBareInfoCard({plex_title:'X'});")
    assert "info-poster" not in out2


def test_poster_is_first_hero_child_at_runtime():
    out = _run("renderBareInfoCard({plex_title:'X',rating_key:'5'});")
    i_hero = out.index('<div class="info-hero">')
    i_img = out.index('<img class="info-poster"', i_hero)
    i_meta = out.index('<div class="info-hero-meta">', i_hero)
    assert i_hero < i_img < i_meta


def test_app_js_still_parses():
    quickjs = pytest.importorskip("quickjs")
    ctx = quickjs.Context()
    try:
        ctx.eval(APP_JS)
    except quickjs.JSException as e:
        assert "SyntaxError" not in str(e), f"app.js failed to parse: {e}"
