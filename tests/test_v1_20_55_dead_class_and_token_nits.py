"""v1.20.55 — dead interpolated class + token-discipline nits.

Design-system audit (2026-05-31):
- menuButtonHtml built `row-menu-${label.toLowerCase()}` →
  .row-menu-source / .row-menu-place / .row-menu-remove, which have NO
  CSS rule (dead since v1.12.25), in the JS-lint blind spot
  (interpolated class). The comment wrongly credited them with the
  constant per-button width — that lives on `.row-menu > summary`.
- Three ops.css spacing regressions from the v1.20.41 accordion block
  (12px ×2, 8px) matched --gap-3 / --gap-2 but carried raw px; plus two
  JS inline styles (gap:4px, margin-left:8px).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()
DASH_JS = (REPO / "app" / "web" / "static" / "dashboard-customize.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_dead_row_menu_label_class_removed():
    """menuButtonHtml must NOT build the per-label class anymore."""
    anchor = APP_JS.index("function menuButtonHtml(")
    body = APP_JS[anchor:anchor + 700]
    assert "row-menu-${label.toLowerCase()}" not in body
    assert "labelClass" not in body
    # The base + kind classes survive.
    assert "const cls = `row-menu ${kindClass || ''}`.trim();" in body
    # And the dead classes have no CSS rule (still true).
    for dead in (".row-menu-source", ".row-menu-place", ".row-menu-remove"):
        assert dead not in APP_CSS, f"{dead} must have no CSS rule"


def test_min_width_lives_on_summary_primitive():
    """The constant per-button width the comment now credits correctly."""
    assert ".row-menu > summary" in APP_CSS
    anchor = APP_CSS.index(".row-menu > summary {")
    block = APP_CSS[anchor:APP_CSS.index("}", anchor)]
    assert "min-width: 78px" in block


def test_ops_card_detail_uses_gap_tokens():
    anchor = OPS_CSS.index(".op-card-detail {")
    block = OPS_CSS[anchor:OPS_CSS.index("}", anchor)]
    assert "margin-top: var(--gap-3)" in block
    assert "padding-top: var(--gap-3)" in block
    assert "12px" not in block


def test_ops_card_runlog_padding_token():
    assert "padding-top: var(--gap-2)" in OPS_CSS
    # the raw 8px padding is gone from the runlog block
    anchor = OPS_CSS.index("padding-top: var(--gap-2)")
    assert "padding-top: 8px" not in OPS_CSS


def test_js_inline_spacing_uses_tokens():
    assert "gap:var(--gap-1)" in DASH_JS
    assert "gap:4px" not in DASH_JS
    assert "margin-left:var(--gap-2)" in APP_JS
    assert 'style="margin-left:8px"' not in APP_JS


def test_v1_20_55_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
