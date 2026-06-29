"""v0.50.54 — results-table column priority on narrow viewports.

Below full-desktop width the fixed columns (ACTIONS 320 + ED 160 + IMDB 120 + …)
summed past the viewport and squeezed the no-width TITLE column. Drop the two
lowest-value columns — ED (empty for ~all rows) and IMDB (raw tt id) — at ≤1280px
so TITLE keeps its room without scrolling down to ~1080px; below 1080 the trimmed
table pins a min-width and swipes (TITLE holds ~150px).
"""
from __future__ import annotations

import re
from pathlib import Path

CSS = (Path(__file__).resolve().parent.parent / "app" / "web" / "static" / "app.css").read_text()


def test_ed_and_imdb_columns_hidden_below_1280():
    block = re.search(r"@media \(max-width: 1280px\) \{.*?\n\}", CSS, re.S).group(0)
    assert "#library-table td.col-edition" in block
    assert "#library-table td.col-imdb" in block
    assert "display: none;" in block
    # scoped to the library table only (other tables keep these columns)
    assert ".col-edition," in block and "#library-table" in block


def test_trimmed_table_pins_min_width_below_1080():
    block = re.search(r"@media \(max-width: 1080px\) \{.*?\n\}", CSS, re.S).group(0)
    assert "#library-table { min-width: 1080px; }" in block


def test_phone_block_no_longer_carries_the_table_min_width():
    phone = re.search(r"@media \(max-width: 600px\) \{.*?\n\}", CSS, re.S).group(0)
    assert "#library-table { min-width" not in phone
