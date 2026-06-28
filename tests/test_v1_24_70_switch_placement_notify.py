"""v1.24.70 — SWITCH PLACEMENT reads as a switch, not a new theme.

the user: a switch-to-API action ("10 (1979)", "10 Cloverfield Lane (2016)") fired
a generic "📤 Theme pushed to Plex — …" notification with the source URL +
thumbnail, so it looked like a brand-new theme. The action queues a place job
with reason='user_switch_placement' (api.py) → worker passes it to the
theme_pushed formatters. That reason now gets its own title verb + via-label.

Also: the auto-scroll checkbox tick is muted (the bright --green accent stood
out too much).
"""
from __future__ import annotations

from pathlib import Path

from app.core.notify_content import (
    _PUSH_REASON_LABEL,
    format_theme_pushed_title,
    format_theme_pushed_item,
)

REPO = Path(__file__).resolve().parent.parent
CTX = {"display_title": "10 (1979)"}


def test_switch_placement_title_is_distinct():
    assert format_theme_pushed_title(CTX, reason="user_switch_placement") == (
        "📤 Placement switched — 10 (1979)")
    # NOT the generic new-ish push title.
    assert "Theme pushed to Plex" not in format_theme_pushed_title(
        CTX, reason="user_switch_placement")


def test_switch_placement_label_registered():
    assert _PUSH_REASON_LABEL["user_switch_placement"] == "SWITCH PLACEMENT"


def test_switch_placement_item_label():
    assert format_theme_pushed_item(CTX, reason="user_switch_placement") == (
        "10 (1979) — via SWITCH PLACEMENT")


def test_generic_push_title_unchanged():
    # a plain push (no reason) still reads "Theme pushed to Plex".
    assert format_theme_pushed_title(CTX, reason=None) == (
        "📤 Theme pushed to Plex — 10 (1979)")


def test_autoscroll_checkbox_tick_muted():
    css = (REPO / "app" / "web" / "static" / "app.css").read_text()
    assert "#recent-autoscroll { accent-color: var(--green-deep); }" in css
