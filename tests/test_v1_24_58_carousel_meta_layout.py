"""v1.24.58 — RECENTLY ADDED carousel meta layout.

the user's follow-ups after v1.24.57:
  - center the title + info under the poster (was left-aligned);
  - put the year in (brackets) like a normal movie/show year;
  - drop the "·" separator — year and date on their own lines;
  - show fewer cards at once (bigger posters + more spacing);
  - single-line title (the mixed 1-line/2-line look read as ragged).

Client-side surfaces (JS/CSS source pins).
"""
from __future__ import annotations

from pathlib import Path

from _slice_helpers import slice_to_next

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _load_recent_body():
    # v0.51.285: was a fixed `[idx:idx + 3300]` window — the transcode comment
    # above the dataset.src line pushed recent-meta-date past the edge (the
    # .261 bug class). Anchored now to the next sibling function.
    return slice_to_next(
        APP_JS,
        "async function loadRecentlyAdded()",
        "\n  function ", "\n  async function ")


def test_year_is_bracketed():
    body = _load_recent_body()
    assert "(${it.year})" in body, "year should render in (brackets)"


def test_year_and_date_are_separate_lines_no_dot_separator():
    body = _load_recent_body()
    assert "recent-meta-year" in body
    assert "recent-meta-date" in body
    # the old "year · date" join is gone.
    assert "' · '" not in body


def test_meta_is_centered_column():
    block = APP_CSS[APP_CSS.index(".recent-meta {"):APP_CSS.index(".recent-meta {") + 220]
    assert "flex-direction: column" in block
    assert "align-items: center" in block


def test_card_centers_text():
    block = APP_CSS[APP_CSS.index(".recent-card {"):APP_CSS.index(".recent-poster {")]
    assert "text-align: center" in block
    assert "text-align: left" not in block


def test_cards_are_larger_and_more_spaced():
    card = APP_CSS[APP_CSS.index(".recent-card {"):APP_CSS.index(".recent-poster {")]
    assert "width: 150px" in card
    strip = APP_CSS[APP_CSS.index(".recent-strip {"):APP_CSS.index(".recent-card {")]
    assert "gap: var(--gap-5)" in strip
