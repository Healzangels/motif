"""v0.51.287 — Esc-closing the ops drawer drops the IDLE pill's focus ring.

the user: clicking the IDLE pill opens the ops drawer ("info panel"); Esc
closes it but leaves the pill with a stuck focus-visible ring. closeDrawer()
only blurred when focus was INSIDE the drawer — a trigger-opened drawer keeps
focus on the trigger pill, so the Esc keypress flipped Chrome's :focus-visible
heuristic to "keyboard" and painted the ring on close (the v0.51.266 INBOX
class, resurfacing one panel over). closeDrawer now also blurs a focused
[data-ops-trigger] element (IDLE pill / op-mini bar / overflow pill).
"""
from __future__ import annotations

from pathlib import Path

from _slice_helpers import slice_to_next

REPO = Path(__file__).resolve().parent.parent
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()


def _close_drawer_body() -> str:
    return slice_to_next(OPS_JS, "function closeDrawer()", "\n  function ")


def test_close_drawer_blurs_a_focused_trigger():
    body = _close_drawer_body()
    # the blur must cover BOTH a focused drawer descendant and a focused
    # trigger pill — the trigger is the case the drawer-contains check missed.
    assert "drawer.contains(_ae)" in body
    assert "closest && _ae.closest('[data-ops-trigger]')" in body
    assert "_ae.blur()" in body
    assert "v0.51.287" in body


def test_trigger_selector_matches_the_idle_pill_markup():
    # the selector is only as good as the markup it targets: the IDLE pill and
    # the op-mini bar both carry data-ops-trigger in base.html.
    base = (REPO / "app" / "web" / "templates" / "base.html").read_text()
    assert base.count("data-ops-trigger") >= 2
    idle = base[base.index('id="op-status-idle"') - 200:
                base.index('id="op-status-idle"') + 200]
    assert "data-ops-trigger" in idle


def test_escape_path_still_routes_through_close_drawer():
    # the Esc handler closes via closeDrawer() (not an inline hide), so the
    # blur above covers the keyboard path the user reported.
    esc = OPS_JS[OPS_JS.index("e.key !== 'Escape'"):]
    esc = esc[:esc.index("});")]
    assert "closeDrawer()" in esc


def test_v0_51_287_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.287: Esc-closing the ops drawer" in init_py
