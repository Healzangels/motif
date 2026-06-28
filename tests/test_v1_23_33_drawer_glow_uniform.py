"""v1.23.33 — the LIVE OPS drawer glow matches the // MOTIF INFO card.

the user wants platform-wide uniformity: the drawer carried only a neutral border
+ dark drop shadow, while the .dlg (info card) has a green edge + green ambient
glow. The drawer now mirrors the SAME `0 0 40px rgba(green,0.15)` glow + a
green-deep edge (with a leftward dark drop suited to a right-pinned panel).
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()


def _rule(css: str, selector: str) -> str:
    m = re.search(re.escape(selector) + r"\s*\{([^}]*)\}", css)
    assert m, f"{selector} rule not found"
    return m.group(1)


def test_drawer_carries_the_info_card_green_glow():
    panel = _rule(OPS_CSS, ".ops-drawer-panel")
    # the IDENTICAL ambient glow the .dlg uses.
    assert "0 0 40px rgba(var(--green-rgb), 0.15)" in panel
    # a green edge, like the card (was --line-bright).
    assert "border-left: 1px solid var(--green-deep)" in panel


def test_matches_the_dlg_glow_value():
    dlg = _rule(APP_CSS, ".dlg")
    panel = _rule(OPS_CSS, ".ops-drawer-panel")
    glow = "0 0 40px rgba(var(--green-rgb), 0.15)"
    assert glow in dlg and glow in panel, "drawer + card must share the glow"
    assert "var(--green-deep)" in dlg and "var(--green-deep)" in panel
