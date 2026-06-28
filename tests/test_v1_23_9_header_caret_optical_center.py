"""v1.23.9 — header caret optical centering + checkbox line-box fix.

the user on the v1.23.6 inline caret: "the carrot is centered on the
header right now it looks like its sitting at the bottom, it also
looks like we might have extra white space below the header."

Two measured causes (preview-browser ink metrics):
1. The 10px ▲ glyph's ink (ascent 5.6) centers ~1.5px below the 11px
   uppercase labels' midline (cap height 8.0) when the flex boxes are
   centered — the caret read as baseline-hung. A -1.5px relative nudge
   puts the ink centers within 0.01px of each other.
2. The select-all checkbox baseline-aligned (the v1.15.83 form-control
   class), inflating the header row ~2px — all of it landing below the
   labels. vertical-align: middle collapses the line box back to the
   text strut.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _block(selector: str) -> str:
    i = APP_CSS.index(selector + " {")
    return APP_CSS[i:APP_CSS.index("}", i)]


def test_caret_optical_nudge():
    # v1.23.13: the caret went out-of-flow (absolute) so the label
    # centers over the column; the -1.5px optical correction now
    # rides in the `top` calc instead of a relative nudge.
    block = _block(".table th .sort-indicator")
    assert "top: calc(50% - 1.5px);" in block, (
        "the 10px caret ink rides ~1.5px below the 11px caps' midline; "
        "without the nudge it reads as baseline-hung (the user: 'sitting "
        "at the bottom')"
    )
    assert "transform: translateY(-50%);" in block


def test_table_checkboxes_middle_align():
    assert (".table input[type=\"checkbox\"] { vertical-align: middle; }"
            in APP_CSS), (
        "baseline-aligned checkboxes inflate the header row below the "
        "labels — the v1.15.83 form-control bug class"
    )
