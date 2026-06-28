"""v0.50.31 — SAVED FILTERS rows are blue on hover/press only.

The popover used to paint the currently-applied preset cyan via
`.library-presets-popup-apply.is-active` the moment it opened. The match was
honest (the applied filter is restored from the cross-tab snapshot on load), but
the user read the auto-paint as a confusing pre-selection: "only be blue when
selected or on hover but they are when you first open". v0.50.31 drops the
persistent row paint, keeps :hover, and adds :active for the click-moment
"selected" flash. The JS still toggles .is-active (it drives _activePresetId +
the bookmark star) — it just no longer styles the row.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_no_persistent_is_active_row_paint():
    # The persistent applied-row cyan rule is gone — that was the "blue when you
    # first open" the user objected to.
    assert ".library-presets-popup-apply.is-active" not in APP_CSS


def test_hover_and_active_are_the_only_blue_states():
    assert ".library-presets-popup-list .library-presets-popup-apply:hover" in APP_CSS
    assert ".library-presets-popup-list .library-presets-popup-apply:active" in APP_CSS


def test_js_still_toggles_is_active_for_star_and_active_id():
    # Functional active-tracking is unchanged — only the row's paint was removed.
    # The class still drives _activePresetId (delete-cleanup) + the .has-active star.
    assert "classList.toggle('is-active'" in APP_JS
    assert "_activePresetId" in APP_JS
    assert "has-active" in APP_JS
