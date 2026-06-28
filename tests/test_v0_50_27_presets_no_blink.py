"""v0.50.27 — the SAVED FILTERS popover no longer blinks while open.

the user: the saved-filters list blinks when the selector is opened. The 600ms
drift-detection setInterval(_updatePresetActiveState) re-toggled .is-active on the
list rows every tick; it now early-returns while the <details> menu is open.
"""
from pathlib import Path

APP_JS = (Path(__file__).resolve().parent.parent / "app" / "web" / "static" / "app.js").read_text()


def test_drift_interval_skips_while_popover_open():
    # the interval body guards on the menu's open attribute before repainting.
    i = APP_JS.index("Drift detection — refresh the bookmark active-state")
    block = APP_JS[i:i + 700]
    assert "if (menu.hasAttribute('open')) return;" in block
    assert "_updatePresetActiveState();" in block
    # the bare always-on form is gone.
    assert "setInterval(_updatePresetActiveState, 600)" not in APP_JS
