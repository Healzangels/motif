"""v0.51.320 — the ANIME THEMES picker + card entry point, polished.

Three things the operator's screenshots showed after deploying .319: the
RESOLVING state carried the explainer from the previous open, the rows had
no vertical rhythm, and the card's button sat off the section-chip row's
baseline. Rhythm is tokens-only CSS; the card button is a SOURCE-group
action row like the probe row; one binder serves both cards.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _blk(anchor: str, end: str) -> str:
    i = APP_JS.index(anchor)
    return APP_JS[i:APP_JS.index(end, i)]


def test_hint_hides_on_open_and_resolving_has_its_own_line():
    b = _blk("async function openAnimeThemesDialog(", "function closeAnimeThemesDialog(")
    assert "if (hint) hint.hidden = true;" in b, "no stale explainer from the previous open"
    assert '<p class="muted small anime-themes-resolving">// RESOLVING…</p>' in b


def test_rows_are_two_stacked_lines():
    r = _blk("function _atThemeRows(seasonIdx, theme) {", "\n  function ")
    assert '<dd class="anime-themes-row"><div>${song}</div><div class="anime-themes-pills">' in r
    assert "<br>" not in r, "no bare line breaks — the rhythm comes from the row's flex gap"


def test_rhythm_rules_use_tokens_only():
    i = APP_CSS.index("/* v0.51.320: the ANIME THEMES picker's rhythm")
    blk = APP_CSS[i:APP_CSS.index("#anime-themes-preview-row", i) + 80]
    for sel in ("#anime-themes-hint", "#anime-themes-warn", ".anime-themes-resolving", ".anime-themes-row",
                ".anime-themes-pills", "#anime-themes-body .dlg-grid", "#anime-themes-preview-row"):
        assert sel in blk, sel
    px = re.findall(r"\b(\d+)px\b", blk)
    assert px == ["1"], f"gaps come from --gap-* tokens; only the 1px dt nudge is literal, got {px}"
    assert "#" not in re.sub(r"#anime-themes[-a-z]*", "", blk), "no hardcoded colours"


def test_card_button_is_a_source_row_not_a_hero_chip():
    assert "const animeThemesRowHtml = (sc && sc.is_anime && ratingKey)" in APP_JS
    row = _blk("const animeThemesRowHtml = (sc && sc.is_anime && ratingKey)", "      : '';")
    assert "<dt>anime themes</dt><dd>" in row and 'data-act="anime-themes"' in row
    assert "openings and endings from AnimeThemes.moe" in row, "the probe row's muted meta shape"
    assert "${probeBtnHtml}\n        ${animeThemesRowHtml}`;" in APP_JS, "rendered in the SOURCE group after the probe row"
    scope = _blk("scopeChips = `<div class=\"info-scope-row\">`", "+ `</div>`;")
    assert 'data-act="anime-themes"' not in scope, "the hero chip row is chips again"


def test_bare_card_gets_the_button_on_anime_rows_and_one_binder_serves_both():
    bare = _blk("function renderBareInfoCard(it, { anime = false } = {}) {", "\n  // v0.50.64: open the bare card")
    assert "libraryState" not in bare, "a pure renderer — the v0.50.64 quickjs tests evaluate it alone"
    assert "${anime" in bare and 'data-act="anime-themes"' in bare
    assert "renderBareInfoCard(it, { anime: libraryState.tab === 'anime' })" in APP_JS, "the opener passes the gate in"
    assert APP_JS.count("_bindAnimeThemesCardButton(body);") == 2, "the full card and the bare card"
    b = _blk("function _bindAnimeThemesCardButton(body) {", "\n  }")
    assert "closeInfoDialog();" in b and "openAnimeThemesDialog({" in b and "computeSrcLetter(rowItem)" in b


def test_v0_51_320_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.320: " in init_py
