"""v0.50.15 — needle-drop on a placement transition (not only PUSH TO PLEX).

the user: "not seeing the animation when placing a new row." The v0.50.8
needle-drop was wired only to replaceTheme (PUSH TO PLEX), which re-pushes an
already-placed row — no not-placed → placed transition — so first-time theming
(DOWNLOAD / UPLOAD MP3 / ADOPT / RESTORE) never flashed.

flashPlacedTransitions() runs after each library render and drops a needle on
any row whose PL predicate (media_folder set OR plex_upload) just went true,
anchored to the row's data-rk <tr>. It seeds silently on first sight (so
existing placed rows don't flash on load / tab-switch), fires when the theme
actually LANDS (poll-observed, not on enqueue), and caps per render so a bulk
place can't flood the screen.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _fn(name: str) -> str:
    i = APP_JS.index(f"function {name}(")
    return APP_JS[i:i + 2400]


def test_detector_exists_and_is_called_after_render():
    assert "function flashPlacedTransitions(" in APP_JS
    # invoked from loadLibrary on the live item list, after the rows render.
    assert "flashPlacedTransitions(libraryState.items);" in APP_JS


def test_uses_the_v1_18_0_pl_predicate():
    body = _fn("flashPlacedTransitions")
    assert "!!it.media_folder || it.placement_kind === 'plex_upload'" in body


def test_seeds_silently_then_fires_only_on_transition():
    body = _fn("flashPlacedTransitions")
    # persistent per-key bookkeeping: seen + known-placed.
    assert "_seen" in body and "_placed" in body
    # the firing condition: now placed, previously SEEN, and NOT previously placed.
    assert "isPlaced && everSeen && !wasPlaced" in body


def test_bulk_safety_viewport_and_cap():
    body = _fn("flashPlacedTransitions")
    # per-render cap so a 50-row place can't spawn 50 needles.
    assert "fired >= 5" in body
    # off-screen rows are recorded but not flashed.
    assert "r.bottom < 0 || r.top > vh" in body


def test_row_anchor_and_align_mode():
    # the <tr> carries data-rk so the detector can resolve a row → element.
    assert 'data-rk="${htmlEscape(it.rating_key || \'\')}"' in APP_JS
    # the detector fires the needle in 'row' align mode.
    assert "needleDropAt(tr, { align: 'row' })" in APP_JS
    # needleDropAt honors the row align mode.
    nd = _fn("needleDropAt")
    assert "opts.align === 'row'" in nd


def test_push_to_plex_keeps_its_own_immediate_fire():
    # replaceTheme (PUSH/re-push of an already-placed row → no transition) still
    # flashes directly — the transition detector won't cover it.
    i = APP_JS.index("async function replaceTheme(")
    body = APP_JS[i:i + 1500]
    assert "needleDropAt(btn);" in body
