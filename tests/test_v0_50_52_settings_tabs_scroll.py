"""v0.50.52 — settings tab bar scrolls as one row on a phone.

The 13 settings tabs (.tabs flex-wrap:wrap) wrapped into a ragged multi-row block
that read as "one big list stacked on itself" (the user). On a phone they now
scroll as a single compact row — the same treatment as the topbar nav.
"""
from __future__ import annotations

import re
from pathlib import Path

CSS = (Path(__file__).resolve().parent.parent / "app" / "web" / "static" / "app.css").read_text()


def test_settings_tabs_scroll_single_row_on_phone():
    block = re.search(r"@media \(max-width: 600px\) \{.*?\n\}", CSS, re.S).group(0)
    i = block.index(".tabs {")
    rule = block[i:block.index("}", i) + 1]
    assert "flex-wrap: nowrap;" in rule
    assert "overflow-x: auto;" in rule
    assert ".tab { flex-shrink: 0; }" in block
