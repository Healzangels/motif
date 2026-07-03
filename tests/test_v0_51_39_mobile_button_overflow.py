"""v0.51.39 — mobile: DB-backup + DRY-RUN button rows no longer overflow.

The user (settings#database at ~361px): the backup row's DOWNLOAD / RESTORE /
DELETE spilled off the LEFT edge. Root: `.backup-row-actions` was
`justify-content: flex-end` with NO flex-wrap, so on overflow the right-anchored
group pushed off-screen left. Fix: wrap + centre (same as the v0.51.38 pager).

Audit sibling: the DRY-RUN `.control-actions` (ENABLE DRY-RUN + DISABLE) was
nowrap; its ~335px min-content forced the whole settings page to ≥373px wide.
Fix: flex-wrap (kept LEFT-aligned under its label per v0.51.7 — NOT centred,
which would detach the buttons from the left label).

All scoped to @media (max-width: 600px).
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


def test_backup_row_actions_wrap_and_centre():
    # the standalone 3-button group (no adjacent label) wraps + centres like the
    # pager — a button that can't share the row centres, not spills off the left.
    assert ".backup-row-actions { flex-wrap: wrap; justify-content: center; }" in MOBILE


def test_control_actions_wrap_left_aligned():
    # the labelled control row stays LEFT-aligned under its label (v0.51.7) but
    # now wraps so the one 2-button row (DRY-RUN) can't force a page-wide overflow.
    assert ".control-row .control-actions { margin-left: 0; flex-wrap: wrap; }" in MOBILE
    # must NOT be centred — it sits under a left-aligned label.
    assert ".control-row .control-actions { margin-left: 0; justify-content: center" not in MOBILE
