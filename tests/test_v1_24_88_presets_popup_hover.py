"""v1.24.88 — SAVED FILTERS popup: kill the lingering :hover highlight.

The popup (.library-presets-popup) is an overflow:hidden + border-radius +
box-shadow absolutely-positioned overlay that drops DOWN over the results table,
and its row-hover fill (.library-presets-popup-apply:hover) is a semi-transparent
rgba(cyan, 0.08). That combination produced stale composited hover tiles in
Chrome — the highlight "lasted long after hovering away, sometimes appearing
after already hovering away" (the user). Promoting the popup to its own compositing
layer (transform: translateZ(0)) isolates its paint so mouseleave clears cleanly.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _popup_block() -> str:
    start = APP_CSS.index(".library-presets-popup {")
    end = APP_CSS.index("}", start)
    return APP_CSS[start:end]


def test_popup_promoted_to_own_layer():
    # The layer-promotion lives in the .library-presets-popup rule (not on the
    # hover row), so it covers every hover fill inside the overlay.
    assert "translateZ(0)" in _popup_block(), (
        "v1.24.88: .library-presets-popup must keep transform:translateZ(0) so "
        "its semi-transparent hover fills repaint cleanly over the results table"
    )


def test_hover_fill_still_present():
    # Guard the actual symptom surface — the semi-transparent row-hover fill that
    # was leaving residue. The fix is the layer, not removing the highlight.
    assert ".library-presets-popup-apply:hover" in APP_CSS
    assert "rgba(var(--cyan-rgb), 0.08)" in APP_CSS
