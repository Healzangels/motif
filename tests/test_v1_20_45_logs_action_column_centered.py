"""v1.20.45 — center the LOGS // JOBS ACTION column.

the user 2026-05-30 (screenshot): "also the action head is but lined up
over the action below would like to make it centered under the header."

The // JOBS pane is a 7-column CSS grid (ID/TYPE/ITEM/STATE/TIME/NOTE/
ACTION) shared by the header row (.jobs-grid-header) and every data row
(.jobs-grid-row). Cells default to left-align, so the "ACTION" header
label sat left while the // ACK / × CANCEL buttons below read as offset.
ACTION is the LAST cell in both, so a :last-child text-align:center rule
centers the label AND each row's button in the column together.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
QUEUE_HTML = (REPO / "app" / "web" / "templates" / "queue.html").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_action_column_centered_rule_present():
    """Both the header label and the data-row button cell must be
    centered via the shared :last-child selector."""
    anchor = APP_CSS.index("v1.20.45")
    block = APP_CSS[anchor:anchor + 700]
    assert ".jobs-grid-header > span:last-child" in block
    assert ".jobs-grid-row > span:last-child" in block
    assert "text-align: center" in block


def test_action_is_last_header_cell():
    """The selector relies on ACTION being the LAST span in the header
    row — guard the markup so a future column insertion doesn't silently
    move the centering onto a different column."""
    anchor = QUEUE_HTML.index("jobs-grid-header")
    block = QUEUE_HTML[anchor:anchor + 260]
    last_span = block.rfind("<span>")
    assert block[last_span:].startswith("<span>ACTION</span>"), (
        "ACTION must remain the last header <span> for the :last-child "
        "centering rule to target the right column"
    )


def test_action_is_last_data_row_cell():
    """The data-row template must keep the actionCell as the last span
    so :last-child targets it (not the NOTE cell)."""
    cell = "<span>${actionCell}</span>"
    action_idx = APP_JS.index(cell)
    li_end = APP_JS.index("</li>", action_idx)
    # No further <span> may sit between the action cell and the row's
    # close — i.e. actionCell is the LAST cell, so :last-child hits it.
    between = APP_JS[action_idx + len(cell):li_end]
    assert "<span>" not in between, (
        "the actionCell span must remain the last cell in the jobs-grid-row "
        "template so the :last-child centering rule targets the ACTION column"
    )


def test_v1_20_45_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
