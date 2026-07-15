"""v0.51.149 — the notification drawer renders stored titles verbatim.

Every inbox notification's stored `title` already carries its own emoji +
descriptor (the notify_content formatters: "🎵 Theme added — <item>", "💔 Theme
lost — <item>", "✨ Theme available — <item>", …). The v0.51.148 drawer ALSO
prepended a KIND-map emoji + a phrase sub-line, so against real data each row
double-rendered the emoji and the descriptor (the Phase-2 harness used bare
sample titles, so it never surfaced).

v0.51.149 makes the drawer's KIND map tier-only (event_kind → stripe class) and
renders the stored title verbatim: the row is [title][time + ×]. This pins that
shape + the removal of the dead emoji/sub CSS. The tier stripes (the FIXED
semantic tones) and the mark-seen behaviour are unchanged (still guarded by
test_v0_51_148).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()


def _bind_body() -> str:
    i = APP_JS.index("function bindNotifInbox()")
    return APP_JS[i:i + 5200]


def test_kind_map_is_tier_only():
    body = _bind_body()
    # the map now yields a bare tier class per kind (no emoji/phrase tuple).
    assert "const TIER = {" in body
    assert "plex_item_arrived_themed:     'tier-add'," in body
    assert "new_tdb_theme_available:      'tier-avail'," in body
    assert "plex_theme_lost:              'tier-fyi'," in body


def test_row_renders_title_verbatim_no_emoji_or_phrase():
    body = _bind_body()
    # the stored title is rendered as-is …
    assert "notif-title" in body and "htmlEscape(n.title" in body
    # … and the row no longer synthesises its own emoji span or phrase sub-line.
    assert "notif-emoji" not in body
    assert 'class="notif-sub"' not in body
    assert "notif-sub" not in body


def test_ops_css_two_column_grid_and_dead_rules_removed():
    # the row grid dropped the 20px emoji column.
    row_rule = OPS_CSS[OPS_CSS.index(".notif-row {"):]
    grid_line = row_rule[:row_rule.index("}")]
    assert "grid-template-columns: 1fr auto;" in grid_line
    assert "20px 1fr auto" not in grid_line
    # the emoji/sub rules are gone (`.notif-empty-sub` stays — different class).
    assert ".notif-emoji" not in OPS_CSS
    assert ".notif-sub {" not in OPS_CSS
    assert ".notif-empty-sub" in OPS_CSS  # empty-state hint kept


def test_tier_stripes_still_present():
    # the FIXED tier stripes are unchanged by the render simplification.
    assert ".notif-row.tier-add" in OPS_CSS
    assert ".notif-row.tier-avail" in OPS_CSS
    assert ".notif-row.tier-fyi" in OPS_CSS
