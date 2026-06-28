"""v1.14.91 — THEMED + ↺ (broken) attn pill no longer no-ops.

the user: "filtering on the conical missing filter is showing
all results"

Repro: /movies, click THEMED chip, click ↺ (canonical missing)
attn pill. Expected: just rows where motif's local file was
deleted but the Plex copy is intact. Actual (pre-fix): all
3,295 themed rows.

## Root cause

The route handler at api.py:6857-6868 has special routing for
the broken-only pill case:

    if dl_set == {"broken"} and status == "all":
        effective_status = "dl_missing"
        dl_set = set()
    if attn_set == {"broken"} and status == "all":
        effective_status = "dl_missing"
        attn_set = set()

The override translates `attn_pills={broken}` into a
`status=dl_missing` query (which has the SQL pre-filter +
post-stat narrowing the broken pill needs). But the override
ONLY fires when `status == "all"`. With THEMED active
(status='has_theme'), the override skipped.

Once the override skipped, attn_pills={broken} fell through
to the SQL loop. The loop has branches for fail / update /
mismatch / await — but NO branch for "broken" (the comment
acknowledges this: "Mixed-with-broken would need post-SQL
union; out of scope for v1.13.68"). So `attn_branches` came
out empty → no SQL filter for broken → result: every themed
row, the broken pill silently dropped.

## Fix

Expand the routing condition to include `status='has_theme'`.
This is semantically safe because dl_missing → has_theme:
a tracked-but-missing canonical means a theme source IS
present, so the row is themed by definition. The override
narrows the result correctly.

Same fix for dl_pills broken (same shape, same blind spot).

Mixed-with-broken (e.g. attn_pills={broken, fail}) is still
out of scope per the v1.13.68 comment — that needs a SQL UNION
or a post-stat OR-narrowing pass. the user's report is the solo
case so this fix is targeted.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


def _routing_block() -> str:
    """The route handler block that does the broken-pill routing."""
    src = API_PY.read_text()
    # Anchor on the v1.12.23 marker that opens the routing block.
    anchor = src.index(
        "v1.12.23: 'broken' DL pill alone routes through the existing"
    )
    return src[anchor:anchor + 2500]


def test_dl_pills_broken_routing_includes_has_theme():
    """The dl_pills broken-only override must fire for both
    status='all' AND status='has_theme'. Pre-fix only 'all'
    was checked, so a THEMED + DL=broken combo silently
    dropped the broken filter."""
    block = _routing_block()
    assert (
        'if dl_set == {"broken"} and status in ("all", "has_theme"):'
        in block
    )


def test_attn_pills_broken_routing_includes_has_theme():
    """Same fix for the attn_pills broken pill (the ↺ chip
    the user reported). The override must fire for THEMED too,
    since dl_missing implies has_theme."""
    block = _routing_block()
    assert (
        'if attn_set == {"broken"} and status in ("all", "has_theme"):'
        in block
    )


def test_v1_14_91_marker_explains_the_has_theme_expansion():
    """A v1.14.91 marker on the routing block documents WHY the
    has_theme expansion is safe (semantic equivalence) so a
    future 'tighten the routing' refactor sees the rationale."""
    block = _routing_block()
    assert "v1.14.91" in block
    # The rationale must mention the implication / equivalence.
    assert (
        "implies has_theme" in block
        or "implies" in block.lower()
        or "themed by definition" in block
    )


def test_routing_does_not_fire_for_untracked_status():
    """status='untracked' (UNTHEMED chip) + broken should NOT
    route through dl_missing. dl_missing requires file_path
    NOT NULL — untracked rows have file_path NULL — empty
    intersection. Overriding to dl_missing would break the
    user's explicit untracked filter."""
    block = _routing_block()
    # The conditions must be exactly status in ("all", "has_theme") —
    # NOT a wildcard. Pin both via the literal substring above.
    # And confirm 'untracked' isn't in any of the override conditions.
    # Slice just the override conditions (lines starting with
    # `if dl_set` / `if attn_set`).
    cond_lines = [
        line.strip() for line in block.splitlines()
        if line.strip().startswith("if dl_set ==")
        or line.strip().startswith("if attn_set ==")
    ]
    assert len(cond_lines) == 2, (
        f"Expected exactly 2 routing conditions; found "
        f"{len(cond_lines)}: {cond_lines}"
    )
    for line in cond_lines:
        assert "untracked" not in line, (
            f"Routing condition must not include untracked: {line}"
        )
        assert "manual" not in line, (
            f"Routing condition must not include manual: {line}"
        )


def test_attn_pills_broken_in_sql_loop_remains_unhandled():
    """The SQL loop in _library_main_query (api.py ~1196-1305)
    intentionally has no branch for `p == "broken"` — the
    routing override consumes it. This test pins the v1.13.68
    architectural decision: solo broken is route-overridden;
    mixed broken is documented as out-of-scope.

    If a future change adds a SQL branch for broken, the
    routing override should also be re-evaluated (might no
    longer be needed)."""
    src = API_PY.read_text()
    # Slice the attn_pills loop body.
    loop_anchor = src.index(
        "v1.13.68: ATTN (attention) pill axis"
    )
    # The loop ends where the next major block (dl_pills handling)
    # begins.
    loop_end = src.index(
        "v1.12.81: when 'broken' is in dl_pills or pl_pills",
        loop_anchor,
    )
    loop_block = src[loop_anchor:loop_end]
    # The branches present.
    assert 'p == "fail"' in loop_block
    assert 'p == "update"' in loop_block
    assert 'p == "mismatch"' in loop_block
    assert 'p == "await"' in loop_block
    # Broken intentionally absent — handled at route level.
    assert 'p == "broken"' not in loop_block, (
        "If a SQL branch for broken got added, re-check whether "
        "the route-level override is still needed."
    )
