"""v0.51.85 — the op-mini overflow pill carries no stale tone class (UI audit).

The #op-mini-overflow pill was server-rendered `class="op-pill op-tone-warn"`,
but renderTopbar always overwrites its className to a computed tone
(op-tone-plex / op-tone-tdb / op-tone-queue) before it un-hides, and no op-mini
renders the warn tone anymore (retired in ops.css). The stale warn default was a
misleading second source of truth. Dropped to a neutral bare `.op-pill`.

Counter-guard: the #topbar-disk-badge legitimately KEEPS op-tone-warn — it's a
real disk-space warning, not an always-overwritten op-mini.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()


def _tag_with_id(el_id: str) -> str:
    m = re.search(rf'<a\b[^>]*\bid="{re.escape(el_id)}"[^>]*>', BASE_HTML, re.DOTALL)
    assert m, f"#{el_id} anchor not found"
    return m.group(0)


def test_overflow_pill_has_no_stale_tone_class():
    tag = _tag_with_id("op-mini-overflow")
    assert "op-pill" in tag, "still an op-pill"
    assert "op-tone-warn" not in tag, (
        "v0.51.85: #op-mini-overflow must not carry op-tone-warn — the tone is "
        "computed in JS before it shows; warn is retired from op-minis")


def test_disk_badge_keeps_its_warn_tone():
    # counter-guard so the cleanup didn't strip the LEGIT warn on the disk badge.
    tag = _tag_with_id("topbar-disk-badge")
    assert "op-tone-warn" in tag, (
        "the disk-space badge is a real warning — it must keep op-tone-warn")
