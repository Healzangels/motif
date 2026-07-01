"""v0.50.83 — bulk SWITCH TO API for file-sidecar (hardlink/copy) placements.

the user: "add a bulk action for when filtering for HL or similar to switch to api —
right now you'd have to go one by one." A // SWITCH TO API bulk button appears when the
selection holds file-sidecar themed rows; it loops the SAME per-row /switch-placement
endpoint the SOURCE menu uses (edition-scoped via rating_key), so no new backend.

Only rows currently on a FILE placement (hardlink/copy: media_folder set, NOT the
plex_upload sentinel media_folder='') are candidates — the per-row endpoint FLIPS
file↔api, so a plex_upload row would flip BACK to file and is excluded.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
LIB = (REPO / "app" / "web" / "templates" / "library.html").read_text()


# ── Template button ──

def test_bulk_button_declared():
    assert 'id="library-switch-to-api-btn"' in LIB
    assert "// SWITCH TO API" in LIB
    assert 'class="btn btn-tiny btn-plex"' in LIB  # Plex-family, like PUSH
    # hidden until the selection has eligible rows.
    i = LIB.index('id="library-switch-to-api-btn"')
    assert 'style="display:none"' in LIB[i - 120:i + 120]


# ── JS wiring ──

def test_count_bucket_predicate():
    # themed + on a FILE sidecar (media_folder set, not plex_upload) + not in-flight.
    assert "const switchToApiCount = effectiveCount(" in JS
    idx = JS.index("const switchToApiCount = effectiveCount(")
    body = JS[idx:idx + 260]
    assert "!it.job_in_flight && !!it.media_folder" in body
    assert "it.placement_kind !== 'plex_upload'" in body
    assert "themedPred(it)" in body


def test_button_visibility_gated_like_push():
    assert ("switchApiBtn.style.display = (!onTdbOnly && !onAttnUpdateFilter "
            "&& switchToApiCount > 0) ? '' : 'none';" in JS)
    assert "withCount('// SWITCH TO API', switchToApiCount)" in JS


def test_click_handler_loops_per_row_switch_placement_edition_scoped():
    # the handler exists…
    assert ("document.getElementById('library-switch-to-api-btn')?.addEventListener"
            in JS)
    # …reuses the per-row endpoint (no new backend), edition-scoped via rating_key.
    assert "`?rating_key=${encodeURIComponent(c.rk)}`" in JS
    assert "`/api/items/${c.mt}/${c.id}/switch-placement${_qs}`" in JS
    # same candidate eligibility as the count.
    assert ("const onFileSidecar = !it.job_in_flight\n"
            "                              && !!it.media_folder\n"
            "                              && it.placement_kind !== 'plex_upload';" in JS)
    # confirmed before firing; progress label like the PUSH bulk button.
    assert "if (!confirm(lines.join('\\n'))) return;" in JS
    assert "`// SWITCHING ${i + 1}/${candidates.length}`" in JS


# ── Behavioral: the eligibility predicate ──

def _run(js_body):
    quickjs = pytest.importorskip("quickjs")
    return json.loads(quickjs.Context().eval(
        "function themedPred(it){ return it.theme_media_type"
        " && it.theme_tmdb !== null && it.theme_tmdb !== undefined"
        " && it.upstream_source !== 'plex_orphan'; }\n"
        "function eligible(it){ return !it.job_in_flight && !!it.media_folder"
        " && it.placement_kind !== 'plex_upload' && themedPred(it); }\n"
        + js_body
    ))


def test_eligibility_only_matches_file_sidecar_themed_rows():
    rows = (
        "var rows = ["
        "{theme_media_type:'movie', theme_tmdb:1, media_folder:'/x', placement_kind:'hardlink'},"   # HL themed
        "{theme_media_type:'tv', theme_tmdb:2, media_folder:'/x', placement_kind:'copy'},"           # CP themed
        "{theme_media_type:'movie', theme_tmdb:3, media_folder:'', placement_kind:'plex_upload'},"   # already API
        "{theme_media_type:null, theme_tmdb:null, media_folder:'/x', placement_kind:'hardlink'},"    # unthemed
        "{theme_media_type:'movie', theme_tmdb:4, media_folder:'/x', placement_kind:'hardlink', job_in_flight:'place'},"  # busy
        "{theme_media_type:'movie', theme_tmdb:5, media_folder:null, placement_kind:null},"          # unplaced
        "{theme_media_type:'movie', theme_tmdb:6, media_folder:'/x', placement_kind:'hardlink', upstream_source:'plex_orphan'}"  # orphan
        "];\n"
    )
    # coerce to strict booleans — JS `&&` returns the last operand (a falsy null for the
    # unthemed row), which app.js consumes in a boolean `if`/count context.
    out = _run(rows + "JSON.stringify(rows.map(function(it){ return !!eligible(it); }));")
    assert out == [True, True, False, False, False, False, False]
