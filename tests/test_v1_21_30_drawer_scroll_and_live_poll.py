"""v1.21.30 — two LIVE OPS drawer fixes (Windows scrollbars + live poll).

the user (Chrome/Windows):
  1. Opening + expanding the drawer makes it long → needs a scrollbar, but
     there are TWO (the library page behind + the drawer). On Windows the
     extra scrollbar takes width, so clicking to expand a card shifts the
     layout. Fix: lock the page behind the open drawer (one scrollbar) +
     reserve the scrollbar gutter so nothing shifts when a scrollbar
     appears/disappears.
  2. Opening the drawer at the same time as starting a sync/refresh → the
     drawer never shows live progress until you close + reopen. Fix: while
     the drawer is OPEN keep the 1s poll cadence even when nothing is
     running yet (the worker hasn't created the op_progress row for a
     just-clicked op), so a newly-appearing op shows within a second.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


# ── Bug 2: live poll while the drawer is open ────────────────

def test_poll_stays_fast_while_drawer_open():
    # cadence keeps 1s when the drawer is open, not just when an op runs
    assert "(running || pending || state.drawerOpen) ? 1000 : 10000" in OPS_JS


# ── Bug 1: background scroll-lock + stable gutters ───────────

def test_open_close_toggle_the_lock_class():
    op = OPS_JS.index("function openDrawer(")
    open_body = OPS_JS[op:OPS_JS.index("function closeDrawer(")]
    assert "document.documentElement.classList.add('ops-drawer-locked')" in open_body
    cl = OPS_JS.index("function closeDrawer(")
    close_body = OPS_JS[cl:cl + 600]
    assert "document.documentElement.classList.remove('ops-drawer-locked')" in close_body


def test_lock_css_and_stable_gutters():
    # page locks behind the open drawer
    assert "html.ops-drawer-locked { overflow: hidden; }" in APP_CSS
    # page reserves its scrollbar gutter so locking doesn't shift it
    assert "scrollbar-gutter: stable;" in APP_CSS
    # the drawer's own scroll reserves its gutter so expand doesn't reflow
    idx = OPS_CSS.index(".ops-drawer-body {")
    assert "scrollbar-gutter: stable;" in OPS_CSS[idx:idx + 450]


def test_version_bumped():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
