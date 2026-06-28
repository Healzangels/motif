"""v1.15.102 — failure_tab_row ORDER BY (closes AUDIT_API.md M7 partial).

Walking back through the May 13 audit doc revealed M7 was only
half-closed. `update_tab_row` (api.py:~5288) gained the
`ORDER BY ps.is_anime, ps.type, ps.is_4k` deterministic-pick
in some prior commit, but `failure_tab_row` (api.py:~5197)
still had a bare `LIMIT 1` with no ORDER BY.

## The bug

The static fallback href for the topbar FAIL pill (used on
the very first click before tabs[] hydrates from the
breakdown query) picks "the first failing tab" via LIMIT 1.
Without ORDER BY, SQLite is free to return any matching row
— across SQLite version upgrades, autovacuum, ANALYZE runs,
the choice may shift. The breakdown query that backs the
click-cycle IS well-ordered (api.py:~5269 and ~5340 in the
update side), so subsequent clicks rotate deterministically.
But the first click was non-deterministic.

User-visible: "first click on FAIL pill goes anywhere" —
small UX flake. the user never reported this directly; caught
by walking back through the audit doc.

## Fix

Add `ORDER BY ps.is_anime, ps.type, ps.is_4k` before the
`LIMIT 1`. Same ordering update_tab_row uses → consistent
"first failure surfaces in the same tab order as first
update."

## Tests

Static guard that the query has the ORDER BY clause.
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


def test_failure_tab_row_has_order_by():
    """`failure_tab_row` LIMIT 1 must have a deterministic
    ORDER BY. Without it the first click on the topbar FAIL
    pill lands on a non-deterministic tab — small UX flake
    flagged in the May 13 AUDIT_API.md M7."""
    src = API_PY.read_text()
    # Find the failure_tab_row query.
    anchor = src.index("failure_tab_row = conn.execute(")
    block = src[anchor:anchor + 1500]
    # The ORDER BY must appear before LIMIT 1.
    order_idx = block.find("ORDER BY ps.is_anime")
    limit_idx = block.find("LIMIT 1")
    assert order_idx > 0, (
        "v1.15.102: failure_tab_row must include `ORDER BY "
        "ps.is_anime, ps.type, ps.is_4k` before the LIMIT 1. "
        "AUDIT_API.md M7 — without it the first-click target is "
        "non-deterministic."
    )
    assert limit_idx > order_idx, (
        "v1.15.102: the ORDER BY must precede the LIMIT 1 in "
        "the same query (not separated by another statement). "
        "Found ORDER BY but it's positioned wrong."
    )


def test_update_tab_row_still_has_order_by():
    """Regression-guard: `update_tab_row` (api.py:~5288) had
    the ORDER BY clause before v1.15.102 too. Pin so a future
    edit doesn't remove it from BOTH sides while ostensibly
    fixing one."""
    src = API_PY.read_text()
    anchor = src.index("update_tab_row = conn.execute(f")
    # v1.20.62: widened 3500→5500 — the section-scoped decision rewrite
    # + its mirror-drift comment block pushed the trailing ORDER BY
    # further from the anchor. The clause is unchanged, just later.
    block = src[anchor:anchor + 5500]
    order_idx = block.find("ORDER BY ps.is_anime, ps.type, ps.is_4k")
    limit_idx = block.find("LIMIT 1")
    assert order_idx > 0 and order_idx < limit_idx, (
        "Regression: update_tab_row's ORDER BY clause was "
        "removed. Pre-v1.15.102 this query was the only one of "
        "the failure/update tab-row pair with deterministic "
        "ordering; both must stay aligned."
    )
