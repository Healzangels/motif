"""v1.20.47 — dead-CSS removal (CSS audit bundle 2/4).

The 2026-05-30 audit found ~20 orphan rules + 1 orphan @keyframes, each
verified to have ZERO references in templates / JS / py (so deleting
them is a pure no-op). They were removed-feature remnants (the v1.19.68
topbar badge removal, the v1.14.x coverage-legend / src-key-compact /
tdb-key-compact refactors that replaced them with .pill-filter-row, the
v1.14.53 topbar-status click-handler removal, etc.).

These pins assert the dead rules stay gone AND that the live siblings
they sat next to survive (the deletion mustn't have nicked them).
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()


# Each entry: (class-or-keyframe selector head, which file).
DELETED = [
    (".topbar-status-clickable {", APP_CSS),
    (".topbar-status-clickable:hover #topbar-status-text {", APP_CSS),
    (".lib-col-num ", APP_CSS),
    (".src-key-compact {", APP_CSS),
    (".src-key-compact .link-badge", APP_CSS),
    (".src-key-label {", APP_CSS),
    (".ed-pill-btn.state-pill-btn-active {", APP_CSS),
    (".title-cell-meta ", APP_CSS),
    (".event-level-DEBUG ", APP_CSS),
    (".dlg-actions ", APP_CSS),
    (".dlg-form ", APP_CSS),
    (".link-badge-symlink {", APP_CSS),
    (".link-glyph-ps {", APP_CSS),
    (".link-badge-orphan {", APP_CSS),
    (".pending-reason-warn ", APP_CSS),
    (".tdb-key-compact {", APP_CSS),
    (".tdb-key-label {", APP_CSS),
    (".topbar-badge {", APP_CSS),
    (".topbar-badge-warn {", APP_CSS),
    (".coverage-legend-item ", APP_CSS),
    (".coverage-legend-swatch {", APP_CSS),
    (".op-pill.op-tone-cookies ", OPS_CSS),
    (".op-pill.op-pill-live {", OPS_CSS),
    ("@keyframes op-pill-live-pulse", OPS_CSS),
]


def test_deleted_rules_are_gone():
    for head, src in DELETED:
        assert head not in src, (
            f"v1.20.47 removed `{head}` as dead CSS — it must not reappear"
        )


# Live siblings that sat adjacent to the deleted rules — the surgery
# must not have nicked them.
LIVE = [
    (".src-key-btn {", APP_CSS),
    # v0.51.79: .src-key-clear was dropped here — the CSS audit found it dead
    # (renamed to .pill-filter-clear; zero live emitters) and deleted it, so it's
    # no longer a live sibling. Its removal is guarded by
    # test_v0_51_79_css_a11y_deadcode::test_dead_css_clusters_stay_removed.
    (".pill-filter-row {", APP_CSS),
    (".state-pill-btn-active {", APP_CSS),
    (".tdb-pill-btn {", APP_CSS),
    (".link-glyph {", APP_CSS),
    (".link-glyph-pu {", APP_CSS),
    # v0.51.31: .coverage-bar-seg-no-tdb was dropped here — it's no longer a
    # live sibling (the // COVERAGE COMPARISON block that used it was removed;
    # its co-removal is guarded by test_v1_14_61).
    (".op-pill.op-tone-fail ", OPS_CSS),
    (".op-pill.op-pill-idle {", OPS_CSS),
    ("@keyframes op-pill-fail-pulse", OPS_CSS),
]


def test_live_siblings_survive():
    for head, src in LIVE:
        assert head in src, (
            f"v1.20.47 must keep the live rule `{head}` — the dead-CSS "
            f"deletion sat next to it"
        )


def test_css_braces_balanced():
    for src in (APP_CSS, OPS_CSS):
        assert src.count("{") == src.count("}"), "unbalanced CSS braces"


def test_no_dangling_keyframe_users():
    """op-pill-live-pulse is gone, so no rule may still animate with it
    (a dangling animation name renders nothing — silent break)."""
    assert "op-pill-live-pulse" not in APP_CSS
    assert "op-pill-live-pulse" not in OPS_CSS
    # The surviving fail-pulse keyframe must still have its user.
    assert re.search(r"animation:\s*op-pill-fail-pulse", OPS_CSS)


def test_v1_20_47_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
