"""v1.24.17 — libKey (bulk-selection identity) collapsed multi-edition titles.

Edition edge-case sweep #2. `libKey(it)` keyed selection on
`theme_media_type:theme_tmdb` — but a multi-edition title's editions SHARE
theme_media_type+theme_tmdb while rendering as SEPARATE rows (the row dedup at
app.js ~7878 keys on folder_path). So both editions collapsed to ONE selection
key: checking one row checked both, the count was wrong, and `selectedRows` (a
Map keyed by libKey) kept only the LAST edition per title. Every bulk action
that walks selectedRows.values() — DOWNLOAD / PUSH / REVERT / LPS / RESTORE /
ADOPT SELECTED / ACCEPT-DECLINE ALL — was then starved of all-but-one edition
(and could act on the wrong one). The per-row rating_key payloads were correct;
they were just never reached for the dropped editions.

Fix: append rating_key (unique per plex_items row / edition) to libKey, mirroring
the render dedup's per-edition identity. The harness EXECUTES libKey in quickjs
(it's a pure function — reads only `it.*`, no globals).
"""
from __future__ import annotations

from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _libkey_src() -> str:
    i = APP_JS.index("function libKey(it) {")
    j = APP_JS.index("\n  function ", i + 1)
    return APP_JS[i:j]


def _run(expr_js: str):
    quickjs = pytest.importorskip("quickjs")
    ctx = quickjs.Context()
    return ctx.eval(_libkey_src() + "\n" + expr_js)


def test_libkey_distinguishes_two_editions_of_one_title():
    out = _run(
        "var a = libKey({theme_media_type:'tv', theme_tmdb:387,"
        " rating_key:'100', folder_path:'/x/Std'});"
        "var b = libKey({theme_media_type:'tv', theme_tmdb:387,"
        " rating_key:'200', folder_path:'/x/4K'});"
        "a + '|' + b")
    a, b = out.split("|")
    assert a != b, "two editions of one title must get DISTINCT selection keys"
    assert "100" in a and "200" in b


def test_libkey_stable_for_the_same_row():
    out = _run(
        "var r = {theme_media_type:'movie', theme_tmdb:5, rating_key:'42',"
        " folder_path:'/m/Heat'};"
        "libKey(r) === libKey(r)")
    assert out is True


def test_libkey_unthemed_row_uses_rating_key():
    # an unthemed row (no theme_tmdb) still gets a stable per-row key.
    out = _run(
        "libKey({plex_media_type:'movie', rating_key:'77', folder_path:'/m/Z'})")
    assert "77" in out


def test_libkey_source_includes_rating_key():
    assert "${it.rating_key || it.folder_path || ''}" in APP_JS
