"""v0.51.26 — two mobile display fixes (the user).

1. EXISTING BACKUPS rows: the flex row's 3 flex-shrink:0 action buttons crushed
   the min-width:0 name column to ~one char, so word-break:break-all stacked the
   filename vertically one letter per line. Stack the row on a phone.
2. The horizontal-scroll data regions (dash-pair cards / library .table-scroll /
   sync-history) fell through to the global 10px muted-gray scrollbar instead of
   the nav's thin-green treatment. Unify them.

Both scoped to the @media (max-width: 600px) phone tier.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _mobile_block() -> str:
    i = APP_CSS.index("@media (max-width: 600px) {")
    j = i + APP_CSS[i:].index("{")
    depth = 0
    for k in range(j, len(APP_CSS)):
        if APP_CSS[k] == "{":
            depth += 1
        elif APP_CSS[k] == "}":
            depth -= 1
            if depth == 0:
                return APP_CSS[j:k + 1]
    raise AssertionError("unterminated @media (max-width: 600px)")


MOBILE = _mobile_block()


def test_backup_row_stacks_on_mobile():
    assert ".backup-row {" in MOBILE and "flex-direction: column" in \
        MOBILE[MOBILE.index(".backup-row {"):MOBILE.index(".backup-row {") + 120], (
        "v0.51.26: .backup-row must stack (flex-direction:column) on a phone so "
        "the filename gets full width instead of one-char-per-line")


def test_mobile_scroll_regions_use_thin_green_scrollbar():
    # the dash-pair / table-scroll / sync-history scrollbar tinted green-deep.
    assert "scrollbar-color: var(--green-deep) transparent;" in MOBILE
    # the webkit thumb is green-deep for those regions.
    assert ".dash-pair-col::-webkit-scrollbar-thumb" in MOBILE
    thumb_i = MOBILE.index(".dash-pair-col::-webkit-scrollbar-thumb")
    thumb_block = MOBILE[thumb_i:thumb_i + 200]
    assert "background: var(--green-deep)" in thumb_block, (
        "v0.51.26: the mobile data-region scrollbar thumb must be green-deep to "
        "match the nav / header instead of the global muted-gray bar")
