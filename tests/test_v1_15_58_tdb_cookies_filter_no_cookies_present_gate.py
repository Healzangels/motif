"""v1.15.58 — TDB pill `cookies` filter no longer gated on
`cookies_present`.

the user (screenshot of /anime with the TDB ⚠ chip selected,
no results filtered): "filter not applying it appears."

## Bug

The TDB pill `cookies` branch at api.py:~1254 had:
    elif p == "cookies":
        if not cookies_present:
            branches.append("(t.failure_kind = 'cookies_expired')")

With cookies.txt present in /config (the user's setup), the SQL
branch was never appended → tdb_pills filter list stays empty
for that pill → where_extra doesn't get an AND clause → the
filter is effectively a no-op (all rows shown).

Mirror-principle leak from v1.15.38, which fixed the same bug
on the ATTN pill axis (attn_pills=cookies, line ~1383) with the
correct rationale:
    "the row remains in the cookies-expired state until a probe
    / download runs against it with cookies present to clear
    failure_kind, regardless of whether cookies.txt is currently
    configured."

The TDB pill axis was overlooked in that pass — the same fix
applies. Same the user complaint, two tags later, different axis.

## Fix

Drop the `if not cookies_present:` gate. The branch now always
appends. Matches the ATTN pill behavior + the JS computeTdbPill
render rule (yellow ⚿ on every cookies_expired row regardless
of cookies_present).

Static-text guards consistent with v1.15.38 filter-axis test
patterns.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


def test_tdb_pill_cookies_branch_not_gated_on_cookies_present():
    """The TDB pill `cookies` branch must NOT gate on
    `cookies_present`. Pre-fix it did, making the filter a no-op
    for the user's cookies-present setup."""
    src = API_PY.read_text()
    # Find the TDB pill cookies branch. There are 3 cookies
    # branches in api.py (TDB pill, ATTN pill, post-stat ATTN
    # matcher). Distinguish the TDB pill branch by anchoring on
    # the preceding `elif p == "update":` line (only the TDB
    # pill loop has tdb/update/cookies/dead/none/dropped in
    # that order; ATTN pill goes fail/cookies/update/mismatch).
    update_anchor = src.index('elif p == "update":')
    # Find the next `elif p == "cookies":` after `update`.
    cookies_anchor = src.index('elif p == "cookies":', update_anchor)
    # Slice until the next `elif p == "dead":` (which only
    # exists in the TDB pill loop).
    branch_end = src.index('elif p == "dead":', cookies_anchor)
    branch = src[cookies_anchor:branch_end]
    # Strip Python comment lines (those starting with '#').
    code_only = "\n".join(
        line for line in branch.splitlines()
        if not line.lstrip().startswith("#")
    )
    assert "branches.append(\"(t.failure_kind = 'cookies_expired')\")" in code_only, (
        "v1.15.58: TDB pill cookies branch must unconditionally "
        "append the (t.failure_kind = 'cookies_expired') SQL — "
        "pre-fix it was gated on `if not cookies_present:` which "
        "made the filter a no-op for cookies-present setups"
    )
    assert "if not cookies_present:" not in code_only, (
        "v1.15.58: the gate `if not cookies_present:` must be GONE "
        "from the live TDB pill cookies branch — only mentioned in "
        "the rationale comment"
    )


def test_tdb_pill_cookies_matches_attn_pill_cookies_predicate():
    """Cross-source consistency: the TDB pill cookies branch
    (this tag's fix) and the ATTN pill cookies branch
    (v1.15.38's fix) must use the SAME SQL predicate. Drift =
    one axis shows different rows than the other, same complaint
    returns on whichever axis diverges."""
    src = API_PY.read_text()
    # ATTN pill cookies branch (v1.15.38) — anchor on the marker.
    attn_anchor = src.index(
        "v1.15.17: cookies-needed STATUS pill. Filters to"
    )
    attn_body = src[attn_anchor:attn_anchor + 1500]
    attn_sql_present = (
        "(t.failure_kind = 'cookies_expired')" in attn_body
    )
    # TDB pill cookies branch (v1.15.58 fix).
    tdb_anchor = src.index(
        "v1.15.58: dropped the `if not cookies_present:`"
    )
    tdb_body = src[tdb_anchor:tdb_anchor + 1500]
    tdb_sql_present = (
        "(t.failure_kind = 'cookies_expired')" in tdb_body
    )
    assert attn_sql_present and tdb_sql_present, (
        "v1.15.58: both axes must use the identical "
        "(t.failure_kind = 'cookies_expired') predicate so the "
        "TDB ⚠ chip + the ATTN ⚿ chip show the SAME row set"
    )


def test_v1_15_38_attn_pill_cookies_still_unrgated():
    """Regression guard: the v1.15.38 fix for the ATTN pill axis
    must stay in place. If a future refactor reintroduces the
    cookies_present gate on the ATTN branch, the same the user
    complaint returns on that axis too."""
    src = API_PY.read_text()
    anchor = src.index(
        "v1.15.17: cookies-needed STATUS pill. Filters to"
    )
    # Slice the immediate ATTN pill cookies branch (next ~30
    # lines until the next `elif p == "mismatch":`).
    branch_end = src.index('elif p == "mismatch":', anchor)
    branch = src[anchor:branch_end]
    # Strip comment lines so the gate-check only looks at code.
    code_only = "\n".join(
        line for line in branch.splitlines()
        if not line.lstrip().startswith("#")
    )
    assert "if not cookies_present:" not in code_only, (
        "v1.15.58: regression guard — ATTN pill cookies branch "
        "must STAY ungated (the v1.15.38 fix)"
    )
    assert "(t.failure_kind = 'cookies_expired')" in branch
