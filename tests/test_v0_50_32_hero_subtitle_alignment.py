"""v0.50.32 — hero descriptions line up on Y across every tab + the dashboard.

The page description (the muted line under the title) sat at a different Y on
each page: only #library-subtitle had an explicit margin-top, and the title's
margin-bottom behaved differently in the flex .hero-row (dashboard/library — it
ADDS) vs a plain block hero (logs/settings/orphans — it COLLAPSES). v0.50.32
zeroes the title's bottom margin and moves the whole title→subtitle gap onto a
single shared .hero-sub class, so the gap is one value everywhere.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
TPL = REPO / "app" / "web" / "templates"

# Every page whose hero carries a description line.
PAGES = ["dashboard.html", "library.html", "queue.html", "settings.html", "orphans.html"]


def test_title_has_no_bottom_margin():
    i = APP_CSS.index(".title {")
    block = APP_CSS[i:APP_CSS.index("}", i)]
    assert "margin-bottom: 0;" in block
    # The old gap-2 bottom margin (which differed flex-vs-block) is gone.
    assert "margin-bottom: var(--gap-2)" not in block


def test_hero_sub_owns_the_gap():
    assert ".hero-sub { margin-top: var(--gap-5); }" in APP_CSS
    # The old per-page id rule must not linger and re-introduce the divergence.
    assert "#library-subtitle { margin-top:" not in APP_CSS


def test_every_hero_description_carries_hero_sub():
    for page in PAGES:
        html = (TPL / page).read_text()
        # The description is the first muted <p> in the hero; it must carry hero-sub.
        assert 'class="muted hero-sub"' in html, f"{page} hero description missing .hero-sub"
