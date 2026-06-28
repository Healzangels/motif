"""v1.24.62 — hideable pinned dashboard sections.

the user: "can we make a customization on the dashboard the option to hide a
section." The // customize layout feature already hides any section inside
#dash-sections; the two sections added this session — RECENTLY ADDED (carousel)
and SERVICES — sit OUTSIDE #dash-sections as fixed strips, so they couldn't be
hidden. This tag marks them `data-dash-pinned`: hideable via a hide-only toggle
(no reorder — they keep their fixed top/bottom position), sharing the same
dashboard_layout persistence + sanitizer.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.core.db import init_db
from app.web.api import _clean_dashboard_sections

REPO = Path(__file__).resolve().parent.parent
DASH_HTML = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
CUSTOMIZE_JS = (REPO / "app" / "web" / "static" / "dashboard-customize.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


# ── template: the two pinned blocks carry the markers ────────────────────────


@pytest.mark.parametrize("block_id,sid,label", [
    ("recently-added-block", "recently-added", "RECENTLY ADDED"),
    ("services-block", "services", "SERVICES"),
])
def test_pinned_blocks_are_marked(block_id, sid, label):
    idx = DASH_HTML.index(f'id="{block_id}"')
    block = DASH_HTML[idx:idx + 220]
    assert f'data-dash-section="{sid}"' in block
    assert f'data-dash-label="{label}"' in block
    assert "data-dash-pinned" in block


def test_pinned_blocks_stay_outside_dash_sections():
    # They keep their fixed positions: recently-added before the container,
    # services after it.
    open_idx = DASH_HTML.index('<div id="dash-sections">')
    close_idx = DASH_HTML.index("/#dash-sections")
    assert DASH_HTML.index('id="recently-added-block"') < open_idx
    assert DASH_HTML.index('id="services-block"') > close_idx


# ── customize JS: pinned discovery + apply + persist + toggle ────────────────


def test_js_has_pinned_helpers():
    assert "function pinnedSections()" in CUSTOMIZE_JS
    assert "[data-dash-section][data-dash-pinned]" in CUSTOMIZE_JS
    assert "function isPinnedId(id)" in CUSTOMIZE_JS


def test_applylayout_keeps_pinned_entries_through_filter():
    # the drop-filter must keep pinned ids (they're not in `present`).
    assert "present.has(e.id) || isPinnedId(e.id)" in CUSTOMIZE_JS


def test_rebuild_preserves_pinned_entries():
    # rebuildLayoutFromDOM rebuilds from container-only children; pinned entries
    # must be captured + re-appended or a reorder would drop them.
    idx = CUSTOMIZE_JS.index("function rebuildLayoutFromDOM()")
    body = CUSTOMIZE_JS[idx:CUSTOMIZE_JS.index("\n  function ", idx + 1)]
    assert "const pinnedEntries = LAYOUT.sections.filter((e) => isPinnedId(e.id))" in body
    assert "pinnedEntries.forEach" in body


def test_pinned_controls_are_hide_only_and_wired():
    assert "function injectPinnedControls()" in CUSTOMIZE_JS
    assert "function removePinnedControls()" in CUSTOMIZE_JS
    assert "function onPinnedToggleClick(ev)" in CUSTOMIZE_JS
    # enter/exit customize wire them.
    enter = CUSTOMIZE_JS[CUSTOMIZE_JS.index("function enterCustomize()"):]
    enter = enter[:enter.index("\n  function ")]
    assert "injectPinnedControls()" in enter
    exit_ = CUSTOMIZE_JS[CUSTOMIZE_JS.index("function exitCustomize()"):]
    exit_ = exit_[:exit_.index("\n  function ")]
    assert "removePinnedControls()" in exit_
    # hide-only: pinned control markup has the toggle but NOT reorder arrows.
    inj = CUSTOMIZE_JS[CUSTOMIZE_JS.index("function injectPinnedControls()"):]
    inj = inj[:inj.index("function removePinnedControls()")]
    assert "data-dash-pinned-toggle" in inj
    assert "data-dash-move" not in inj  # no reorder arrows
    assert "draggable" not in inj       # no drag


# ── CSS: a hidden pinned section reveals in customize mode ───────────────────


def test_css_reveals_pinned_in_customize_mode():
    assert "body.dash-customize-mode [data-dash-pinned] {" in APP_CSS
    idx = APP_CSS.index("body.dash-customize-mode [data-dash-pinned] {")
    assert "display: block !important" in APP_CSS[idx:idx + 120]


# ── backend: the sanitizer round-trips a hidden pinned section ───────────────


def test_sanitizer_accepts_pinned_section_hidden(tmp_path):
    init_db(tmp_path / "t.db")  # sanity: schema importable
    out = _clean_dashboard_sections([
        {"id": "top-stats", "hidden": False},
        {"id": "recently-added", "hidden": True},
        {"id": "services", "hidden": True},
    ])
    assert {"id": "recently-added", "hidden": True} in out
    assert {"id": "services", "hidden": True} in out
