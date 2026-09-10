"""v0.51.319 — the picker explains itself: OPENING 1, BLU-RAY, tooltips, a hint.

The operator found OP1 / BD / v2 confusing. One label layer (`_AT_WORDS`)
owns every word the picker shows; the catalogue slug survives as the row's
tooltip and as the provenance `slug`, so nothing downstream changes.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()


def _blk(anchor: str, end: str) -> str:
    i = APP_JS.index(anchor)
    return APP_JS[i:APP_JS.index(end, i)]


def test_one_label_layer_owns_every_word():
    w = _blk("const _AT_WORDS = {", "\n  };")
    assert "OP: 'OPENING', ED: 'ENDING'" in w
    assert "BD: 'BLU-RAY'" in w and "WEB: 'STREAMING'" in w and "DVD: 'DVD'" in w
    for key in ("clean", "glance", "name"):
        assert re.search(rf"{key}: \['pill [a-z-]+', '[A-Z ]+',\s*['\"].{{20,}}['\"]\]", w), f"{key} pill needs class, label AND a tooltip sentence"
    for tip in ("size", "source", "versions", "nsfw", "hint"):
        assert f"{tip}:" in w
    assert "first opening is the usual theme" in w


def test_rows_read_opening_n_and_keep_the_slug_as_tooltip():
    b = _blk("function _atThemeLabel(theme) {", "\n  }")
    assert "_AT_WORDS.type[theme.type]" in b and "return `${word} ${n}`;" in b
    assert "parseInt(String(theme.slug || '').replace(/^\\D+/, ''), 10) || 1" in b, (
        "the API's sequence is null for a lone OP — the number rides the slug")
    r = _blk("function _atThemeRows(seasonIdx, theme) {", "\n  function ")
    assert '<dt title="${htmlEscape(theme.slug)}">${htmlEscape(_atThemeLabel(theme))}</dt>' in r
    assert "_AT_WORDS.source[a.source] || a.source" in r, "unknown sources fall through unchanged"
    for tip in ("_AT_WORDS.tips.size", "_AT_WORDS.tips.source", "_AT_WORDS.tips.versions", "_AT_WORDS.tips.nsfw"):
        assert f'title="${{{tip}}}"' in r
    assert 'data-slug="${htmlEscape(theme.slug)}"' in r, "the provenance keeps the catalogue slug"


def test_header_pill_hint_and_group_headings():
    r = _blk("function renderAnimeThemesDialog(data) {", "\n  async function ")
    assert 'title="${htmlEscape(cw[2])}"' in r, "the confidence pill carries its sentence"
    assert "hint.textContent = _AT_WORDS.tips.hint;" in r and "hint.hidden = !(data.seasons && data.seasons.length);" in r
    assert "'// NAME MATCH'" in r and "'// SPECIALS'" in r
    assert "`// USE ${htmlEscape(_atThemeLabel(dtheme))}${dsong}" in r, "the default button reads USE OPENING 1 — song"
    assert '<p class="muted small" id="anime-themes-hint" hidden></p>' in BASE


def test_v0_51_319_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.319: " in init_py
