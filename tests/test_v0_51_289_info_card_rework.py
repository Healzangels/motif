"""v0.51.289 — INFO card design rework (the audit: "a bit overwhelming").

Four fixes from the design audit:
  1. Intent-based order — actionable surfaces (SOURCE, FILE & PLACEMENT,
     TRY THIS NEXT, PROPOSED CHANGE) first; reference detail last.
  2. Reference sections collapse by default via the // HISTORY details
     primitive (_fold) — identity, timeline, loudness, revisions. Loudness
     stays open only while actionable (ambiguous cut / LOUD marker).
  3. One header voice — // prefix + 0.15em tracking everywhere; folds keep
     the reference (dim) tone, cyan stays the audit-trail color.
  4. Density — EDIT AUDIO joined the play row; the source_kind /
     source_video_id codes moved off `downloaded` into IDENTITY's
     `derivation` row (v1.18.56 verifiability kept, at reference depth).
"""
from __future__ import annotations

from pathlib import Path

from _slice_helpers import slice_to_next

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _fold_helper() -> str:
    return slice_to_next(APP_JS, "const _fold = (title, rows,",
                         "const _idsRows")


def test_reference_folds_are_collapsed_by_default():
    fold = _fold_helper()
    # `open` is opt-in — a fold without it renders collapsed.
    assert "${open ? ' open' : ''}" in fold
    assert 'class="history-section info-fold"' in fold, (
        "folds must reuse the // HISTORY details primitive, not mint a twin")
    # identity + timeline + revisions pass no open flag (collapsed).
    assert "_fold('identity', _idsRows, { note: 'ids & derivation' })" in APP_JS
    # v0.51.290: timeline gained an open-on-failure exception — pin the
    # call, not the option literals (they live in .290's tests).
    assert "_fold('timeline', _timelineRows, {" in APP_JS


def test_loudness_fold_opens_only_while_actionable():
    i = APP_JS.index("_fold('loudness', _loudnessRows")
    call = APP_JS[i:APP_JS.index("})}", i)]
    assert "_ambiguousCut" in call, "the edition picker is a CTA — stay open"
    assert "loudness_marker === 'outlier'" in call, (
        "a LOUD row's leveling controls must not hide")


def test_actionable_sections_render_before_the_reference_tail():
    order = ["${_grp('source', _linksRows)}",
             "${_grp('file & placement', _onDiskRows)}",
             "${recoveryPlaceholder}",
             "${diffSection}",
             "_fold('identity'",
             "${auditPlaceholder}",
             "${historySection}"]
    idxs = [APP_JS.index(s) for s in order]
    assert idxs == sorted(idxs), (
        "actionable surfaces first, reference folds last — the audit's fix "
        "for the pre-.289 wall")


def test_edit_audio_joined_the_play_row():
    row = slice_to_next(APP_JS, '<dt>play</dt><dd class="info-play-row">',
                        "</dd>")
    assert 'data-act="edit-audio"' in row, (
        "one file, one row of controls — EDIT AUDIO rides the play row")
    assert "<dt>edit</dt>" not in APP_JS, "the separate edit grid row is gone"


def test_derivation_codes_moved_to_identity():
    ids = slice_to_next(APP_JS, "const _idsRows = ", "const _linksRows")
    assert "${derivationRow}" in ids
    dl = slice_to_next(APP_JS, "const dlBlock = ", "const hasBackupFile")
    assert "sourceKindHint" not in dl and "sourceVidHint" not in dl, (
        "the downloaded line sheds the debug codes (the density finding)")


def test_one_header_voice():
    # tracking unified at 0.15em (the h4 template pin lives in test_v0_51_62).
    h4 = APP_CSS[APP_CSS.index(".dlg-section h4 {"):]
    h4 = h4[:h4.index("}")]
    assert "letter-spacing: 0.15em" in h4
    # folds keep the reference tone at rest — dim, not cyan. The [open]
    # overrides are pinned as CASCADE OUTCOMES in test_v0_51_290 (the .289
    # string pin here was a mirror: its rule tied on specificity with the
    # later green-bright rule and silently lost — the ultra review's find).
    blk = APP_CSS[APP_CSS.index(".info-fold .history-section-title {"):]
    blk = blk[:blk.index("}")]
    assert "color: var(--fg-dim)" in blk


def test_v0_51_289_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.289: INFO card design rework" in init_py
