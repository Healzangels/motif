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

def test_background_lock_removed():
    # v0.51.35 (the user): the ops-drawer-locked `html{overflow:hidden}` lock was
    # REMOVED — it unstuck the position:sticky .topbar (dropped it off the top,
    # exposing the page hero + buttons behind the open drawer). No add/remove of
    # the class anywhere (a comment may still name it); the CSS rule is gone too.
    assert "classList.add('ops-drawer-locked')" not in OPS_JS
    assert "classList.remove('ops-drawer-locked')" not in OPS_JS
    assert "html.ops-drawer-locked { overflow: hidden; }" not in APP_CSS


def test_stable_gutters_survive():
    # the anti-shift halves of v1.21.30 STAY — they were never the problem and
    # keep the width identical (the page scrollbar hides behind the right-pinned
    # drawer, so no double-scrollbar despite dropping the lock).
    assert "scrollbar-gutter: stable;" in APP_CSS          # page reserves its gutter
    idx = OPS_CSS.index(".ops-drawer-body {")
    assert "scrollbar-gutter: stable;" in OPS_CSS[idx:idx + 450]  # drawer reserves its own


def test_version_bumped():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
