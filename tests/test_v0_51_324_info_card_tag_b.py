"""v0.51.324 — INFO card restructure, tag B: the rest of the card review.

  1. The source-video still leaves the body (a 360px frame above the folds,
     the tallest thing on an audio card) for the IDENTITY fold, as a 160px
     thumb beside its own "source video" row — still a link, same YouTube /
     oembed branches. The SoundCloud / Instagram / Facebook pair hides as one
     dt+dd group until the thumbnail lands.
  2. One fold voice — PROVENANCE / HISTORY drop the cyan / green-bright header
     tone for the reference folds' dim / fg; the .289/.290 overrides fold into
     the base rule.
  3. The bare card (rows with no ThemerrDB entry) is grouped like the full
     card: hero with the two-facts headline, AUDIO (plex serves + SERVING)
     when Plex has a theme, PLEX METADATA, then the copy.
  4. docs/DESIGN_SYSTEM.md tells the truth about the intent-flip tones and
     catalogues the tier-badge / info-scope-chip families.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from _slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
DESIGN = (REPO / "docs" / "DESIGN_SYSTEM.md").read_text()


def _rule(sel: str) -> str:
    return slice_between(APP_CSS, sel + " {", "}")


# ── 1. the still lives in the IDENTITY fold ────────────────────


def _video_row() -> str:
    return slice_between(APP_JS, "const _sourceVideoRow = (() => {", "    })();")


def test_still_is_an_identity_row_not_a_body_block():
    assert "${(() => {\n        // v1.15.129: source-aware thumbnail block" not in APP_JS
    ids = slice_between(APP_JS, "const _idsRows = `", "`;")
    assert ids.index("${derivationRow}") < ids.index("${_sourceVideoRow}")
    assert APP_JS.index("const _sourceVideoRow = (() => {") < APP_JS.index("const _idsRows = `")
    note = "note: _sourceVideoRow ? 'ids · derivation · source video' : 'ids · derivation'"
    assert f"_fold('identity', _idsRows, {{ {note} }})" in APP_JS


def test_video_row_keeps_the_diff_gate_and_both_branches():
    row = _video_row()
    assert row.index("if (diffSection) return '';") < row.index("tUrlSrc === 'youtube'"), (
        "the proposed-change diff already shows both thumbnails")
    assert row.count("<dt>source video</dt>") == 2, "YouTube + oembed branches"
    assert 'class="info-source-link"' in row and 'style="' not in row, "no inline styles"
    assert row.count('<div class="info-source-thumb-wrap">') == 2, "the v1.16.1 aspect wrapper"


def test_oembed_branch_hides_the_dt_dd_pair_together():
    row = _video_row()
    i = row.index('<div class="info-dl-group" data-sc-thumbnail-wrap hidden>')
    assert row.index("<dt>source video</dt>", i) < row.index("data-sc-oembed-url=", i) < row.index("</dd></div>", i)
    assert "display: contents" in _rule(".info-dl-group")
    assert "display: none" in _rule(".info-dl-group[hidden]")


def test_fold_thumb_css_and_the_retired_caption():
    fold = _rule(".info-fold-body .info-source-thumb-wrap")
    assert "width: 160px" in fold and "max-width: 100%" in fold and "margin: 0" in fold
    base = _rule(".info-source-thumb-wrap")
    assert "max-width: 360px" in base and "margin: 0 auto" in base, "the base shape is untouched"
    link = _rule(".info-source-link")
    assert "flex: 0 0 auto" in link and "text-decoration: none" in link
    assert "info-thumb-caption" not in APP_JS and ".info-thumb-caption {" not in APP_CSS


# ── 2. one fold voice ─────────────────────────────────────────


def test_one_fold_voice():
    title = _rule(".history-section-title")
    assert "color: var(--fg-dim)" in title and "text-transform: uppercase" in title
    assert ".history-section[open] .history-section-title { color: var(--fg); }" in APP_CSS
    caret_open = _rule(".history-section[open] .history-section-title::before")  # v0.51.340: the caret moved onto the title
    assert "color: var(--fg-dim)" in caret_open and "rotate(90deg)" in caret_open
    region = APP_CSS[APP_CSS.index(".history-section-title::before {"):APP_CSS.index(".info-clear-btn {")]
    for tone in ("--cyan", "--green-bright", "--green-deep"):
        assert tone not in region, f"{tone} is not a fold voice"
    for gone in (".info-fold .history-section-title {",
                 ".history-section.info-fold[open] .history-section-title",
                 ".history-section.info-fold[open] > summary::before"):
        assert gone not in APP_CSS, f"{gone}: the override folded into the base rule"


# ── 3. the bare card, grouped ─────────────────────────────────


def _render_bare(row_js: str) -> str:
    quickjs = pytest.importorskip("quickjs")
    start = APP_JS.index("function renderBareInfoCard(")
    src = APP_JS[start:APP_JS.index("function openBareInfoDialog(", start)]
    harness = (
        "var htmlEscape = function(s){return String(s===undefined||s===null?'':s)"
        ".replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')"
        ".replace(/\"/g,'&quot;').replace(/'/g,'&#39;');};\n"
        + src + f"\nrenderBareInfoCard({row_js});"
    )
    return quickjs.Context().eval(harness)


_ROW = ("{plex_title:'Bleach',year:2004,plex_media_type:'show',section_title:'Anime',"
        "section_id:'3',guid_tmdb:30984,folder_path:'/data/anime/Bleach',rating_key:'778'")


def test_bare_card_with_a_plex_theme_leads_with_audio():
    out = _render_bare(_ROW + ",plex_has_theme:1}")
    assert "nothing on disk · Plex serves its own theme" in out
    i_hero_end = out.index('</div>\n      </div>')
    i_audio = out.index('<div class="dlg-section info-group"><h4>// audio</h4>')
    i_meta = out.index('<div class="dlg-section info-group"><h4>// plex metadata</h4>')
    i_copy = out.index("No theme yet")
    assert i_hero_end < i_audio < i_meta < i_copy
    assert '<dt class="info-ctl-label info-ctl-label-play">plex serves <span class="tier-badge tier-badge-serving"' in out  # v0.51.340
    assert 'data-plex-theme="1"' in out and 'preload="none"' in out
    assert "<dt>plex theme</dt>" not in out and "what Plex serves for this item" not in out


def test_bare_card_without_a_plex_theme_has_no_audio_group():
    out = _render_bare(_ROW + "}")
    assert "nothing on disk · no theme — Plex metadata only" in out
    assert "// audio" not in out and "data-plex-theme" not in out
    assert out.index('<h4>// plex metadata</h4>') < out.index("<dt>rating key</dt>")
    assert "libraryState" not in slice_between(
        APP_JS, "function renderBareInfoCard(", "\n  // v0.50.64: open the bare card"), "the renderer stays pure"


# ── 4. the design-system doc tells the truth ───────────────────


def test_design_doc_names_the_promote_tones_and_the_badge_families():
    flip = next(l for l in DESIGN.splitlines() if l.startswith("* Intent-flip button pair"))
    assert "Both are `.btn-warn`" not in flip
    for tone in (".btn-promote-pb", ".btn-promote-tb", ".btn-promote-ab", ".btn-promote-ub"):
        assert tone in flip
    assert "state strip's HEADER" in flip
    assert "`.tier-badge` + `.tier-badge-X`" in DESIGN and "`-serving`" in DESIGN and "`-unplaced`" in DESIGN
    assert "`.info-scope-chip` + `-section` / `-edition`" in DESIGN
    assert ".info-hero + .recovery-section" in DESIGN, "the TRY THIS NEXT pattern says where the strip sits"


def test_v0_51_324_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.324: INFO card restructure, tag B" in init_py
