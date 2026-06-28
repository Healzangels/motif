"""v1.20.49 (→ partially reverted v1.20.51) — row-menu + orphans button
label voice (CSS/button audit bundle 4/4).

v1.20.49 added the `// `/`× ` prefix to the per-row SOURCE/PLACE/REMOVE
menu + the orphans page to match the otherwise-universal motif voice.
The LIBRARY row-menu half broke the layout — `// SOURCE ▾` overran the
`.row-menu > summary` 78px min-width and the right-anchored .row-actions
cluster overflowed into the IMDB column (the user's repro). v1.20.51
reverted the library row menu to BARE labels; the bulk bar (has room)
and the orphans page (roomy findings table, no overlap) keep their
prefixes + the red DELETE SIDECAR tone.

These pins now assert the library row menu stays BARE (regression guard
so a future audit doesn't re-add the overflow) and the orphans buttons
keep their voice. JS has no execution harness here, so they're source
pins on the render logic (the contract this project uses for app.js).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
ORPHANS = (REPO / "app" / "web" / "templates" / "orphans.html").read_text()


# ── library row menu stays BARE (v1.20.51 revert) ────────────


def test_menu_item_renders_bare_label():
    """menuItemHtml must render the bare label — NO `// `/`× ` prefix
    logic — so the row-menu buttons stay compact and don't overflow the
    actions cell into the IMDB column."""
    anchor = APP_JS.index("function menuItemHtml(")
    body = APP_JS[anchor:anchor + 8200]
    assert ">${label}</button>" in body
    # The v1.20.49 prefix machinery must be gone.
    assert "extras.danger ? '× ' : '// '" not in body
    assert "menuLabel" not in body


def test_menu_summaries_are_bare():
    """SOURCE/PLACE/REMOVE ▾ summaries render bare (no `// ` prefix) —
    re-adding it overruns the 78px summary min-width."""
    anchor = APP_JS.index("function menuButtonHtml(")
    body = APP_JS[anchor:anchor + 1200]
    assert "${htmlEscape(label)} ▾</summary>" in body
    assert "// ${htmlEscape(label)} ▾</summary>" not in body


# ── orphans page buttons ─────────────────────────────────────


def test_orphans_normal_buttons_prefixed():
    for label in ("// RUN SCAN", "// RE-PUSH", "// LET PLEX SERVE", "// PROBE"):
        assert f">{label}</button>" in ORPHANS, f"orphans button {label!r}"


def test_orphans_destructive_buttons_use_glyph():
    assert ">× PURGE</button>" in ORPHANS
    assert ">× DELETE SIDECAR</button>" in ORPHANS


def test_orphans_delete_sidecar_is_danger_not_warn():
    """A filesystem delete must read as red danger, not amber warn."""
    anchor = ORPHANS.index("data-act=\"delete-sidecar\"")
    # Walk back to the <button ...> that opens this action.
    btn_open = ORPHANS.rfind("<button", 0, anchor)
    btn = ORPHANS[btn_open:anchor]
    assert "btn-danger" in btn
    assert "btn-warn" not in btn


def test_v1_20_49_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
