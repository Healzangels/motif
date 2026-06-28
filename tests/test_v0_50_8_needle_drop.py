"""v0.50.8 — needle-drop on PLACE (flavor pass 4/4, vinyl half; FINAL).

A tonearm drops onto a spinning record over the clicked PUSH TO PLEX button when
a place succeeds. app.js needleDropAt(btn) spawns a floating, self-cleaning
overlay anchored to the button's viewport rect (NOT the row DOM, which
re-renders on the next poll), then removes it after the animation via a single
timeout. Fired from replaceTheme's success branch. Skipped under reduced-motion
(the overlay is never created).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_helper_is_floating_self_cleaning_and_motion_safe():
    assert "function needleDropAt(" in APP_JS
    i = APP_JS.index("function needleDropAt(")
    # v0.50.15: 1400 → 1900 — needleDropAt gained a 'row' align mode (the
    # placement-transition fire anchors to a <tr>, not a button), pushing the
    # body.appendChild past the old window. Growth, not a regression.
    body = APP_JS[i:i + 1900]
    # reduced-motion: bail before creating anything.
    assert "prefers-reduced-motion: reduce" in body
    # anchored to the button's viewport rect, appended to <body> (not the row).
    assert "getBoundingClientRect" in body
    assert "document.body.appendChild(host)" in body
    # self-cleans via a single timeout (no animationend bubbling to reason about).
    assert "host.remove()" in body


def test_fired_from_place_success():
    """The flourish fires on a successful place (replaceTheme), with the btn."""
    assert "needleDropAt(btn);" in APP_JS
    # it sits in replaceTheme right after the QUEUED label.
    idx = APP_JS.index("needleDropAt(btn);")
    pre = APP_JS[idx - 120:idx]
    assert "QUEUED" in pre


def test_css_overlay_inert_and_present():
    i = APP_CSS.index(".needle-drop {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    assert "pointer-events: none" in block
    assert "position: fixed" in block
    for kf in ("@keyframes nd-fade", "@keyframes nd-spin", "@keyframes nd-arm"):
        assert kf in APP_CSS, f"missing {kf}"
