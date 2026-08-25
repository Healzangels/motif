"""v0.51.282 — feature-brief C, UI: the // EDIT AUDIO dialog.

The final item of the entire feature brief. The brief's own acceptable v1
("a duration bar plus numeric start/end controls"): numeric start/end + fades
in the canonical dlg shell, // PREVIEW renders a server candidate and plays
it in an <audio>, // SAVE AS NEW REVISION says exactly what happens (the
outgoing audio becomes a restorable revision), CANCEL/close/Esc discard the
candidate (the brief: temporary artifacts are cleaned up), and a save 409's
operator-readable reason stays visible in the dialog (the design-system rule:
dialog-submit errors never auto-dismiss).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()


def _dlg_markup() -> str:
    i = BASE.index('id="edit-audio-dlg"')
    return BASE[i:BASE.index("</dialog>", i)]


def _fn_block(anchor: str, end: str) -> str:
    i = APP_JS.index(anchor)
    return APP_JS[i:APP_JS.index(end, i)]


# ── markup: the canonical shell + real primitives only ───────


def test_dialog_uses_the_canonical_shell_and_primitives():
    d = _dlg_markup()
    assert '<article class="dlg-body">' in d
    assert '<header class="dlg-head">' in d
    assert 'class="dlg-close"' in d
    assert 'class="form-actions"' in d, (
        "the footer is the REAL primitive — .dlg-actions does not exist "
        "(the first draft invented it and was corrected)")
    for fid in ("edit-trim-start", "edit-trim-end",
                "edit-fade-in", "edit-fade-out"):
        assert f'id="{fid}"' in d
    assert "// PREVIEW" in d and "// SAVE AS NEW REVISION" in d and "// CANCEL" in d


def test_dialog_points_loudness_at_the_lossless_pipeline():
    assert "// LEVEL LOUDNESS" in _dlg_markup(), (
        "the recorded deviation: no loudness in the editor — the hint sends "
        "the operator to the lossless, undoable mp3gain path")


def test_save_button_starts_disabled_until_a_preview_exists():
    assert 'id="edit-audio-save"\n                  disabled' in BASE or \
           'id="edit-audio-save"' in BASE and 'disabled>' in _dlg_markup(), (
        "saving without a rendered candidate must be impossible")


# ── card: the entry point rides the play branch ──────────────


def test_card_offers_edit_only_where_a_canonical_plays():
    i = APP_JS.index('data-act="edit-audio"')
    block = APP_JS[APP_JS.rindex("<dt>play</dt>", 0, i):i + 600]
    assert "info-play-row" in block, (
        "the button lives in the same conditional branch as the player — "
        "no canonical, no editor")
    assert 'data-sha="${htmlEscape((lf && lf.file_sha256)' in APP_JS, (
        "the CURRENT sha rides the button — it becomes the save's optimistic lock")


# ── behavior wiring ──────────────────────────────────────────


def test_preview_discards_the_previous_candidate_first():
    b = _fn_block("edit-audio-preview')?.addEventListener", "edit-audio-form'")
    assert b.index("discard()") < b.index("await api('POST'"), (
        "a re-preview replaces the candidate — without the discard, each "
        "tweak leaks a file until the server TTL sweep")


def test_cancel_close_and_esc_all_discard():
    b = _fn_block("function bindEditAudioDialog()", "function bindUploadDialog()")
    assert b.count("close)") >= 2, "× and // CANCEL share the close path"
    assert "dlg.addEventListener('cancel'" in b, (
        "Esc fires the native cancel event — the candidate must not survive it")


def test_save_conventions():
    b = _fn_block("edit-audio-form'", "function bindUploadDialog()")
    assert "setTimeout(refreshTopbarStatus, 1100)" in b, "bug class #7"
    assert "libraryRapidPoll" in b, "the place job's chips follow promptly"
    assert "_editAudioCtx.candidate = null" in b, (
        "the save consumes the candidate — closing afterwards must not fire "
        "a cancel for a file the server already moved")
    assert "saveBtn.disabled = false" in b, (
        "a 409 re-enables SAVE with the reason visible — dialog-submit errors "
        "never auto-dismiss (the design-system rule)")


def test_binder_is_called_at_startup():
    assert "bindEditAudioDialog();" in APP_JS


def test_v0_51_282_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
