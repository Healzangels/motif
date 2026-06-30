"""v0.50.65 — code-review follow-ups for the v0.50.64 universal INFO card.

(1) RACE FIX: openBareInfoDialog paints #info-dlg-body synchronously. The full
card (openInfoDialog) is async and uses an `openInfoDialog._seq` in-flight guard
(v1.17.20, audit race #7) so a stale fetch can't clobber a newer open. The bare
path never awaited, so it never bumped _seq — meaning an in-flight full-card
fetch from a PRIOR themed-row click would resolve, pass its own
`_seq !== _myToken` check, and overwrite the bare card with the WRONG row. Fix:
bump openInfoDialog._seq at the top of openBareInfoDialog so any pending fetch
self-aborts.

(2) CLEANUP: dropped the dead `|| ''` from `const mt = it.plex_media_type`.
"""
from __future__ import annotations

from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _bare_dialog_src() -> str:
    start = APP_JS.index("function openBareInfoDialog(rk) {")
    end = APP_JS.index("\n  }", start)
    return APP_JS[start:end]


def test_open_bare_dialog_bumps_shared_seq():
    """The seq bump must live INSIDE openBareInfoDialog and BEFORE the body
    write, so a pending openInfoDialog fetch self-aborts instead of clobbering
    the synchronously-painted bare card."""
    src = _bare_dialog_src()
    assert "openInfoDialog._seq = (openInfoDialog._seq || 0) + 1;" in src
    # bump precedes the body innerHTML write (the thing being protected)
    i_bump = src.index("openInfoDialog._seq =")
    i_write = src.index("body.innerHTML")
    assert i_bump < i_write, "seq bump must run before painting the bare card"


def test_open_info_dialog_seq_guard_still_present():
    """The guard the fix relies on: openInfoDialog bumps _seq then re-checks it
    after the await. If this disappears, the bare-path bump protects nothing."""
    assert "openInfoDialog._seq = (openInfoDialog._seq || 0) + 1;" in APP_JS
    assert "if (openInfoDialog._seq !== _myToken) return;" in APP_JS


def test_media_type_no_redundant_default():
    assert "const mt = it.plex_media_type;" in APP_JS
    assert "const mt = it.plex_media_type || ''" not in APP_JS


# ── Behavioral: renderBareInfoCard still correct after the mt simplification ──

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


def test_undefined_media_type_renders_dash_not_crash():
    """With the `|| ''` gone, a row missing plex_media_type leaves mt undefined.
    The type cell must still render the em-dash and tmdbPath must fall to
    'movie' (undefined !== 'show'/'collection')."""
    out = _run("renderBareInfoCard({rating_key:'7',guid_tmdb:42});")
    assert "<dt>type</dt><dd>—</dd>" in out
    # undefined media type → movie tmdb path
    assert 'href="https://www.themoviedb.org/movie/42"' in out


def test_app_js_still_parses():
    quickjs = pytest.importorskip("quickjs")
    ctx = quickjs.Context()
    try:
        ctx.eval(APP_JS)
    except quickjs.JSException as e:
        assert "SyntaxError" not in str(e), f"app.js failed to parse: {e}"
