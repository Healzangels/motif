"""v0.51.8 — LOGS mobile polish: jobs-grid scroll affordance + live-indicator fit.

Mobile audit findings A5 + A8 (P2/P3), both on /queue at 375px:

A5: the .jobs-grid scrolls horizontally (min-width:760px) so the ACTION column
(cancel / // ACK) sits off the right edge, but on touch the native overlay
scrollbar is invisible until you scroll — the column read as absent. Added a thin
persistent scroll track (same treatment as the topbar nav) as the affordance.

A8: the LOGS .block-head (// JOBS + // EVENT STREAM toggle chips + the `● live`
indicator) is space-between + flex-wrap on mobile; the chips (~238px) + live (~45px)
exceeded the ~281px content width by a hair, so `live` wrapped to its own line,
detached from the // EVENT STREAM chip. Trimmed the block-head's 18px side padding
to 12px on a phone (a sensible density gain everywhere) — recovers enough room for
live to stay on the chip row.

Verified in-browser at 375px: live no longer wraps (block-head back to 50px),
jobs-grid scrollbar-width:thin, still scrolls, no page overflow.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _mobile_block(css: str) -> str:
    i = css.index("@media (max-width: 600px) {")
    depth = 0
    j = i
    while True:
        if css[j] == "{":
            depth += 1
        elif css[j] == "}":
            depth -= 1
            if depth == 0:
                return css[i:j + 1]
        j += 1


MOBILE = _mobile_block(APP_CSS)


def test_jobs_grid_has_scroll_affordance():
    assert ".jobs-grid { scrollbar-width: thin; scrollbar-color: var(--green-deep) transparent; }" in MOBILE
    assert ".jobs-grid::-webkit-scrollbar { height: 3px; }" in MOBILE
    # v0.51.139: border-radius 2px → var(--radius) (token hygiene; --radius is 2px).
    assert ".jobs-grid::-webkit-scrollbar-thumb { background: var(--green-deep); border-radius: var(--radius); }" in MOBILE


def test_block_head_padding_trimmed_on_mobile():
    # v0.51.139: 12px → var(--gap-3) (token hygiene; --gap-3 is 12px).
    assert ".block-head { flex-wrap: wrap; gap: var(--gap-2); padding-left: var(--gap-3); padding-right: var(--gap-3); }" in MOBILE


def test_desktop_block_head_padding_unchanged():
    # the base block-head keeps its 18px side padding on desktop.
    base = APP_CSS[APP_CSS.index("\n.block-head {"):]
    base = base[:base.index("}")]
    assert "padding: 12px 18px;" in base
