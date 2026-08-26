"""v0.51.291 — walker guard over the inbox kind mirror (design audit).

The notification kind vocabulary renders on FIVE surfaces: the Python
INBOX_EVENT_KINDS allowlist (notify_inbox), the config defaults
(_DEFAULT_INBOX_EVENTS — which drive the settings IN-APP INBOX grid), the
severity table (_EVENT_NOTIFY_TYPE), and the JS TIER + GROUP maps (drawer
accent + burst grouping). Nothing pinned them together: a kind added to the
Python set but forgotten in TIER renders with a TRANSPARENT accent (silent
wrong-classification — the SRC-axis bug class) and never groups (bursts
flood the drawer). Same contract-drift shape v1.18.53 walker-guards for the
status-bar maps.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _js_map_keys(const_name: str) -> set[str]:
    i = APP_JS.index(f"const {const_name} = {{")
    body = APP_JS[i:APP_JS.index("};", i)]
    return set(re.findall(r"^\s*([a-z_]+):", body, re.M))


def test_tier_and_group_cover_exactly_the_inbox_kinds():
    from app.core.notify_inbox import INBOX_EVENT_KINDS
    tier = _js_map_keys("TIER")
    group = _js_map_keys("GROUP")
    assert tier == set(INBOX_EVENT_KINDS), (
        f"TIER drift — missing {set(INBOX_EVENT_KINDS) - tier}, "
        f"phantom {tier - set(INBOX_EVENT_KINDS)}: an unmapped kind renders "
        f"a transparent accent (silent wrong-classification)")
    assert group == set(INBOX_EVENT_KINDS), (
        f"GROUP drift — missing {set(INBOX_EVENT_KINDS) - group}, "
        f"phantom {group - set(INBOX_EVENT_KINDS)}: an unmapped kind never "
        f"collapses, so its bursts flood the drawer")


def test_config_defaults_and_severity_cover_the_inbox_kinds():
    from app.core.config_file import _DEFAULT_INBOX_EVENTS
    from app.core.notify import _EVENT_NOTIFY_TYPE
    from app.core.notify_inbox import INBOX_EVENT_KINDS
    assert set(_DEFAULT_INBOX_EVENTS) == set(INBOX_EVENT_KINDS), (
        "the settings IN-APP INBOX grid renders from the config defaults — "
        "a missing key has no toggle, a phantom key toggles nothing")
    missing = set(INBOX_EVENT_KINDS) - set(_EVENT_NOTIFY_TYPE)
    assert not missing, (
        f"{missing} record inbox rows with the 'info' fallback severity "
        f"instead of a declared one")


def test_walker_is_not_vacuous():
    # a broken extractor comparing empty sets passes forever (the .261 rule).
    assert len(_js_map_keys("TIER")) >= 10
    assert len(_js_map_keys("GROUP")) >= 10


def test_drawer_text_buttons_share_the_label_tracking():
    # design audit: .notif-clear-all sat at 0.1em — a third tracking value
    # beside the 0.15em label convention and the drawer title's 0.18em.
    css = (REPO / "app" / "web" / "static" / "ops.css").read_text()
    i = css.index(".notif-clear-all {")
    blk = css[i:css.index("}", i)]
    assert "letter-spacing: 0.15em" in blk


def test_v0_51_291_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.291: " in init_py
