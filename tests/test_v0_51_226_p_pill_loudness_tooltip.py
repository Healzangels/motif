"""v0.51.226 — the SRC=P pill explains why its loudness meter cell is blank.

The library row's ▂▄▆ loudness meter is derived from motif's LOCAL copy of the theme
(_loudness_marker gates on `has_local_file`). A Plex-served row with no backup has no local
copy, so no reading, so a blank meter — which the operator hit as "why do Predators / The
Predator show no meter?".

The fix does NOT mark the row: P is the largest source population (~3,883 rows) and the
v0.51.192 render-site rule is "presence IS the signal — marking the boring majority is
noise". Instead the reason rides the SRC=P pill's existing hover text, gated so a BACKED-UP
P-row (TB/BK/plex_cloud — which does have a local file and DOES show a meter) keeps the
plain label. The gate is the row's own `loudness_marker` (present ⇒ has a meter ⇒ no note).
"""
from __future__ import annotations

from pathlib import Path

from _slice_helpers import slice_to_next

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _src_render_block() -> str:
    # the inline-SRC render inside renderLibraryRow: from the _pTitle definition to the
    # composite-+P block that follows the whole if/else chain.
    return slice_to_next(APP_JS, "const _pTitle = 'Plex agent / cloud theme'",
                         "const _primaryLetter = computeSrcLetter(it)")


def test_the_note_is_gated_on_the_row_having_no_meter():
    """The critical correctness bit: the 'no loudness reading' note must appear ONLY when
    the row shows no meter. A backed-up P-row (loudness_marker set) DOES have a local copy
    and DOES render a meter, so it must keep the plain label — else the tooltip would
    contradict the meter sitting right next to it."""
    blk = _src_render_block()
    # truthy branch (has a marker → has a meter) is the EMPTY string = plain label
    assert "it.loudness_marker ? ''" in blk, (
        "the note must be on the FALSY (no-marker) branch; a metered P-row keeps plain text")


def test_the_note_names_the_reason_and_the_fix():
    blk = _src_render_block()
    assert "no local copy" in blk
    assert "loudness reading" in blk
    assert "BACKUP THIS THEME" in blk, "must point at the action that gives it a meter"


def test_both_p_pill_sites_use_the_shared_title():
    """Two branches render SRC=P (v1.21.8 plex_cloud + the pure-P verified branch). Both
    must carry the enriched tooltip, and the old static string must be gone from BOTH —
    else one P-row shape explains the blank and the other doesn't (mirror drift)."""
    assert APP_JS.count('title="\' + _pTitle + \'"') == 2
    assert 'title="Plex agent / cloud theme"' not in APP_JS, (
        "the static P tooltip must be fully replaced by the _pTitle build at both sites")


def test_the_row_still_carries_loudness_marker_for_the_gate():
    """The gate reads it.loudness_marker off the row payload — api.py must still stamp it
    (v0.51.202 pops loudness_i but KEEPS loudness_marker). If that ever changes the note
    would fire on backed-up P-rows too."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    assert 'it["loudness_marker"] = _loudness_marker(' in api
