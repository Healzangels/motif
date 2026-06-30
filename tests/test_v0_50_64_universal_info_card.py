"""v0.50.64 — universal row INFO card.

Pre-tag only THEMED library rows (theme_tmdb present) got an ⓘ button → the
full ThemerrDB record card via api_item. Untracked/untemed rows had no way to
inspect their Plex metadata. the user: "give every row an info button/info card
so users can have more info about the row."

Fix: every row renders an ⓘ. Themed rows keep the full record card. Rows with
NO theme have no api_item record to fetch, so they open a BARE card built
entirely CLIENT-SIDE from the cached /api/library row (which already carries
title/year/section/edition/ids/folder/rating_key) — no backend round-trip, no
new endpoint, no async surface. The click handler routes on the presence of
data-id: present → openInfoDialog (full), absent → openBareInfoDialog (bare).

This file mixes static-source guards (the gate / routing / prefetch wiring)
with a quickjs harness that EXECUTES renderBareInfoCard across a plain row, an
edition row, and a sparse row (renderBareInfoCard depends only on a htmlEscape
global + JS builtins, so it runs standalone).
"""
from __future__ import annotations

from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── Static guards: the ⓘ always renders ──

def test_info_button_renders_for_every_row():
    """The themed branch keeps the full-card button (data-id); the else branch
    renders a bare button (data-rk, NO data-id) so untemed rows get an ⓘ too.
    If the `else` ever disappears, untracked rows silently lose the button."""
    assert "if (themed) {" in APP_JS
    # bare button: data-act=info + data-rk, but explicitly no data-id/data-mt
    bare = ('<button class="btn btn-tiny row-info-btn" data-act="info" '
            'data-rk="${htmlEscape(it.rating_key || \'\')}" title="Row details">ⓘ</button>')
    assert bare in APP_JS
    # the bare button must NOT carry a data-id (that's the routing discriminator)
    assert 'data-id' not in bare


# ── Static guards: the click handler routes on data-id ──

def test_click_handler_routes_bare_vs_full():
    assert "if (btn.dataset.id) {" in APP_JS
    assert "openBareInfoDialog(btn.dataset.rk);" in APP_JS


def test_hover_prefetch_skips_bare_rows():
    """Bare buttons have no data-id; prefetching with an undefined id would hit
    /api/items/.../undefined. The guard short-circuits before that."""
    assert "if (!btn.dataset.id) return;" in APP_JS


# ── Static guards: the two new functions exist ──

def test_bare_card_functions_defined():
    assert "function renderBareInfoCard(it) {" in APP_JS
    assert "function openBareInfoDialog(rk) {" in APP_JS
    # bare lookup is by rating_key against the visible-page cache
    assert ("(libraryState.items || []).find((row) => "
            "String(row.rating_key) === String(rk))" in APP_JS)
    # reuses the shared modal helper (no bespoke show path)
    assert "showModalNoFocusRing(dlg);" in APP_JS


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


_PLAIN_ROW = (
    "{plex_title:'Blade Runner',year:1982,plex_media_type:'movie',"
    "section_title:'Movies',section_id:'1',guid_imdb:'tt0083658',"
    "guid_tmdb:78,folder_path:'/data/Movies/Blade Runner (1982)',"
    "rating_key:'9001'}"
)


def test_plain_row_renders_full_metadata():
    out = _run(f"renderBareInfoCard({_PLAIN_ROW});")
    # title + year heading, reusing the shared card classes
    assert 'class="info-title">Blade Runner (1982)' in out
    assert 'class="info-hero"' in out
    assert 'class="dlg-grid"' in out
    # type / section
    assert "<dt>type</dt><dd>movie</dd>" in out
    assert "<dt>section</dt><dd>Movies</dd>" in out
    # imdb + tmdb links (movie → /movie/ path)
    assert 'href="https://www.imdb.com/title/tt0083658"' in out
    assert 'href="https://www.themoviedb.org/movie/78"' in out
    # folder + rating key
    assert "/data/Movies/Blade Runner (1982)" in out
    assert "<dt>rating key</dt>" in out and "9001" in out
    # the "add a theme" call to action points at the SOURCE menu
    assert "No theme yet" in out
    assert "SOURCE" in out


def test_show_row_uses_tv_tmdb_path():
    row = _PLAIN_ROW.replace("'movie'", "'show'").replace("guid_tmdb:78", "guid_tmdb:1399")
    out = _run(f"renderBareInfoCard({row});")
    assert 'href="https://www.themoviedb.org/tv/1399"' in out


def test_collection_row_uses_collection_tmdb_path():
    row = _PLAIN_ROW.replace("'movie'", "'collection'").replace("guid_tmdb:78", "guid_tmdb:645")
    out = _run(f"renderBareInfoCard({row});")
    assert 'href="https://www.themoviedb.org/collection/645"' in out


def test_edition_row_shows_edition_dt():
    row = _PLAIN_ROW[:-1] + ",plex_edition_title:'Final Cut'}"
    out = _run(f"renderBareInfoCard({row});")
    assert "<dt>edition</dt><dd>Final Cut</dd>" in out


def test_no_edition_dt_when_absent():
    out = _run(f"renderBareInfoCard({_PLAIN_ROW});")
    assert "<dt>edition</dt>" not in out


def test_sparse_row_degrades_to_dashes_no_crash():
    out = _run("renderBareInfoCard({rating_key:'5',plex_media_type:'movie'});")
    assert 'class="info-title">—' in out
    # missing ids render an em-dash, not a broken link
    assert "<dt>imdb</dt><dd><span class=\"muted\">—</span></dd>" in out
    assert "<dt>tmdb</dt><dd><span class=\"muted\">—</span></dd>" in out


def test_html_in_title_is_escaped():
    row = _PLAIN_ROW.replace("'Blade Runner'", "'<script>x</script>'")
    out = _run(f"renderBareInfoCard({row});")
    assert "<script>x</script>" not in out
    assert "&lt;script&gt;" in out


# ── Whole-file parse guard ──

def test_app_js_still_parses():
    quickjs = pytest.importorskip("quickjs")
    ctx = quickjs.Context()
    try:
        ctx.eval(APP_JS)
    except quickjs.JSException as e:
        assert "SyntaxError" not in str(e), f"app.js failed to parse: {e}"
