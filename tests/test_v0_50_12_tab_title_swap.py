"""v0.50.12 — library hero <h1> title updates on client-side tab switch.

the user: the big title read ANIME while on the TV tab, only correcting on a
hard refresh; navigating tabs kept showing the last hard-loaded tab's name.

switchLibraryTab (the client-side tab nav) swapped document.title + the
#library-subtitle + the REFRESH button label from the fetched page, but never
the visible `.hero h1.title`, so it kept the originally-loaded tab's name. The
fix swaps the <h1> too, mirroring the adjacent subtitle swap.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
LIBRARY = (REPO / "app" / "web" / "templates" / "library.html").read_text()


def test_hero_title_is_h1_title_in_template():
    """The selector the swap targets matches the SSR'd header."""
    assert '<h1 class="title">{{ title|upper }}</h1>' in LIBRARY


def test_switch_tab_swaps_the_hero_h1():
    i = APP_JS.index("async function switchLibraryTab(")
    # v0.50.92: widened for the pre-swap variant block; v0.51.12: again for the
    # collections-boundary full-nav guard inserted at the top of the function.
    body = APP_JS[i:i + 6200]  # v0.51.22: widened for the ALL-default pre-apply block
    assert "doc.querySelector('.hero h1.title')" in body
    assert "document.querySelector('.hero h1.title')" in body
    # swapped by textContent like the subtitle right above it.
    assert "curH1.textContent = newH1.textContent" in body
