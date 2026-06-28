"""v1.15.39 — systematic filter-logic audit.

the user: "lets do a logic pass of all the different filter
conditions and make sure they all pass and match the logic
we want I don't want to have to keep troubleshooting when we
add filters and they don't work."

## What this tag ships

1. **`attn_pills=broken` mixed-selection bug fix.** The
   v1.13.68 comment explicitly punted on mixed-with-broken
   ("would need post-SQL union; out of scope"). Result: a
   user clicking BROKEN + COOKIES (or any combo) got
   COOKIES-only results — broken silently dropped. Now: when
   broken is in attn_pills, the SQL skips the attn_branches
   filter entirely (same pattern as dl_pills/pl_pills
   broken) and a Python attn-matcher post-stat ORs every
   selected attn pill including broken (canonical_missing
   stat-check). Costs a larger unbounded SQL fetch for the
   broken-mixed case but matches user-expected OR semantics.

2. **SQL-backend ↔ pillAxes cross-reference test.** Extends
   v1.14.58's template/pillAxes/deep-link triangle by adding
   a fourth source: the api.py SQL backend. Every pill value
   declared in the JS `pillAxes` allowlist must have either
   (a) a corresponding SQL branch in api.py's `for p in
   <axis>:` loop, OR (b) be explicitly routed via the
   handler (broken → effective_status='dl_missing' for
   sole-broken / post-stat path for mixed-broken). Test
   fails when a new value lands on three surfaces but
   silently no-ops on the fourth.

## False-positive findings from the round-6 audit

The audit agent flagged several "divergences" that turned
out to be by-design or already-fixed. Documenting here so
future audits don't re-flag them:

- **tdb_pills=cookies**: Both SQL filter AND JS pill gate on
  `cookies_present`. When cookies present, SQL adds no
  branch + JS hides the pill → consistent.
- **attn_pills=cookies**: Already aligned in v1.15.38.
- **attn_pills=update vs tdb_pills=update**: Two separate
  axes; each is internally consistent (attn=actionable,
  tdb=loose pending). Agent conflated them.
- **attn_pills=fail**: By-design ack-aware (documented in
  v1.15.38 marker). Acking is the UX mechanism for tuning
  the FAIL count.
- **link_pills=hl/c mismatch gate**: JS render also excludes
  mismatch via precedence (mismatch wins over hl/c). SQL +
  JS consistent.
- **pl_pills=off**: Agent re-assessed as no divergence.

Real codebase shape: filter axes are mostly clean. The
v1.15.34→38 audit cycle + v1.14.58 cross-ref test already
plugged most gaps. This tag fills the last documented hole
(broken-mixed) and adds the regression scaffold.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── 1. attn_pills=broken mixed-selection fix ──────────────────


def test_attn_needs_post_stat_flag_defined():
    """The new `attn_needs_post_stat` flag must fire when
    broken is in attn_pills (alone or mixed). Pre-fix the
    mixed case silently dropped broken; this flag triggers
    the unbounded-load + post-stat path that handles broken
    correctly."""
    src = API_PY.read_text()
    anchor = src.index(
        "v1.15.39: mixed-with-broken (broken + cookies"
    )
    block = src[anchor:anchor + 1500]
    assert 'attn_needs_post_stat = (' in block
    assert '"broken" in attn_pills' in block


def test_attn_post_stat_triggers_unbounded_pagination():
    """`needs_post_stat_pagination` must include the new
    attn-broken trigger so the SQL fetch comes back unbounded
    (no LIMIT), letting post-stat see every canonical_missing
    candidate before paginating."""
    src = API_PY.read_text()
    anchor = src.index("needs_post_stat_pagination = (")
    block = src[anchor:anchor + 1500]
    assert "attn_needs_post_stat" in block


def test_row_matches_attn_implements_all_six_pills():
    """The post-stat `_row_matches_attn` helper must
    implement every attn pill value (fail, cookies, update,
    mismatch, await, broken) so the OR-filter mirrors the
    SQL branches exactly. Pin each value's matcher so a new
    attn pill addition that misses the post-stat helper
    breaks here."""
    src = API_PY.read_text()
    anchor = src.index("def _row_matches_attn(it):")
    fn_end = src.index("return False", anchor)
    fn_body = src[anchor:fn_end]
    # All six pill values present.
    for value in (
        '"broken"',
        '"fail"',
        '"cookies"',
        '"update"',
        '"mismatch"',
        '"await"',
    ):
        assert value in fn_body, (
            f"v1.15.39: _row_matches_attn must handle {value}"
        )
    # The broken value uses the post-stat canonical_missing
    # field (the whole reason this path exists).
    assert 'it.get("canonical_missing")' in fn_body
    # The "fail" matcher mirrors the SQL (failure_kind +
    # not failure_acked_at).
    assert 'it.get("failure_kind")' in fn_body
    assert 'not it.get("failure_acked_at")' in fn_body
    # The "cookies" matcher matches v1.15.38 (cookies_expired,
    # no ack).
    assert 'it.get("failure_kind") == "cookies_expired"' in fn_body
    # The "update" matcher reads actionable_update (matches
    # the SQL's tight predicate).
    assert 'it.get("actionable_update")' in fn_body
    # The "mismatch" matcher.
    assert 'it.get("mismatch_state") == "pending"' in fn_body
    # The "await" matcher includes the LPS exclusion.
    assert "is_lps" in fn_body


def test_sql_attn_branches_skipped_when_broken_present():
    """When broken is in attn_pills, the SQL `attn_branches`
    loop must be skipped entirely — otherwise the SQL
    pre-narrows to non-broken values and the canonical_missing
    rows that broken should match never come back. Pin the
    `if attn_pills and not attn_needs_post_stat:` guard."""
    src = API_PY.read_text()
    anchor = src.index("v1.15.39: mixed-with-broken")
    block = src[anchor:anchor + 1500]
    assert "if attn_pills and not attn_needs_post_stat:" in block


# ── 2. SQL-backend ↔ pillAxes cross-reference ────────────────


def _extract_deep_link_values(param: str) -> set[str]:
    """Pull the `values: new Set([...])` for a PILL_DEEP_LINKS
    entry by its `param:` name. Mirrors the helper in
    test_v1_14_58_pill_axis_cross_reference.py so the two
    tests stay aligned on the same parsing shape."""
    src = APP_JS.read_text()
    needle = f"param: '{param}',"
    idx = src.find(needle)
    if idx == -1:
        return set()
    block = src[idx:idx + 1500]
    m = re.search(
        r"values:\s*new Set\(\[\s*([^\]]+?)\s*\]\)", block,
        flags=re.DOTALL,
    )
    if not m:
        return set()
    return set(re.findall(r"'([^']+)'", m.group(1)))


def _extract_pill_branches_from_api(axis_param: str) -> set[str]:
    """For each `for p in <axis>:` loop in api.py, pull the
    literal value tokens from `if p == "<value>":` /
    `elif p == "<value>":` branches. Walks the loop body by
    indentation so we stop at the right boundary."""
    src = API_PY.read_text()
    values: set[str] = set()
    for m in re.finditer(
        rf"^(\s*)for p in {re.escape(axis_param)}\s*:\s*$",
        src, flags=re.MULTILINE,
    ):
        loop_indent = len(m.group(1))
        # Walk lines after the for-loop until the indent pops
        # back to ≤ loop_indent.
        rest = src[m.end():]
        block_lines = []
        for line in rest.split('\n'):
            if not line.strip():
                block_lines.append(line)
                continue
            line_indent = len(line) - len(line.lstrip())
            if line_indent <= loop_indent:
                break
            block_lines.append(line)
        block = '\n'.join(block_lines)
        for vm in re.finditer(
            r'(?:if|elif)\s+p\s*==\s*"([^"]+)"', block,
        ):
            values.add(vm.group(1))
    return values


def test_pill_axes_have_sql_branch_coverage():
    """Every pill value the user can deep-link to (declared in
    the JS PILL_DEEP_LINKS parser) must have either (a) a
    corresponding `if p == "<value>":` branch in api.py's
    `for p in <axis>:` loop, OR (b) be explicitly routed via
    the handler (the by-design exception list below).

    Extends v1.14.58's template/pillAxes/deep-link triangle
    by adding a fourth source-of-truth: the SQL backend.
    Catches the exact failure mode the user flagged ("filter
    doesn't filter") when a new pill value lands on three
    surfaces but silently no-ops on the fourth."""
    # Per-axis exceptions: values handled outside the SQL
    # for-loop (route-level routing or post-stat). Each entry
    # points at the specific handler so a future maintainer
    # can audit the routing.
    BY_DESIGN_EXCEPTIONS = {
        # dl_pills broken/on → post-stat canonical_missing
        # check (see _row_matches_dl in api.py).
        "dl_pills": {"broken", "on"},
        # pl_pills broken/on → post-stat canonical_missing
        # / placement_missing check.
        "pl_pills": {"broken", "on"},
        # attn_pills broken → v1.15.39: post-stat
        # _row_matches_attn helper (mixed mode) + the
        # effective_status='dl_missing' routing (sole mode).
        "attn_pills": {"broken"},
    }
    # src_pills uses a different SQL shape (_SRC_LETTER_SQL
    # CASE expression, not a per-value for-loop) — skip it
    # from the per-value coverage check.
    AXES_WITH_FOR_LOOP = (
        "tdb_pills", "attn_pills", "dl_pills",
        "pl_pills", "link_pills", "ed_pills",
    )
    failures = []
    for axis in AXES_WITH_FOR_LOOP:
        declared = _extract_deep_link_values(axis)
        assert declared, (
            f"v1.15.39: PILL_DEEP_LINKS missing entry for {axis} "
            f"(or the helper regex needs updating)"
        )
        sql_values = _extract_pill_branches_from_api(axis)
        exceptions = BY_DESIGN_EXCEPTIONS.get(axis, set())
        missing = declared - sql_values - exceptions
        if missing:
            failures.append(
                f"{axis}: {sorted(missing)} declared in JS but "
                f"missing from api.py SQL branches "
                f"(neither a `if p == \"X\":` branch nor in "
                f"the by-design exception list)"
            )
    assert not failures, (
        "v1.15.39: filter-axis SQL coverage gap — "
        + "; ".join(failures)
    )


def test_sql_branches_dont_have_orphan_values():
    """Inverse: every SQL branch value should be declared in
    the JS PILL_DEEP_LINKS allowlist. An orphan SQL branch is
    a dead branch (the JS can never produce that value, so
    the SQL is unreachable). Catches `if p == "<typo>":`
    branches that no caller hits."""
    AXES_WITH_FOR_LOOP = (
        "tdb_pills", "attn_pills", "dl_pills",
        "pl_pills", "link_pills", "ed_pills",
    )
    failures = []
    for axis in AXES_WITH_FOR_LOOP:
        declared = _extract_deep_link_values(axis)
        sql_values = _extract_pill_branches_from_api(axis)
        orphans = sql_values - declared
        if orphans:
            failures.append(
                f"{axis}: {sorted(orphans)} have SQL branches "
                f"but aren't in JS PILL_DEEP_LINKS — dead code "
                f"or missing JS allowlist entry"
            )
    assert not failures, (
        "v1.15.39: SQL has branches the JS can never produce — "
        + "; ".join(failures)
    )


# ── 3. Documented by-design divergences (regression guards) ──


def test_attn_pills_fail_intentionally_ack_aware():
    """The FAIL filter is intentionally NOT broadened (unlike
    cookies in v1.15.38). Documenting via the v1.15.38 marker
    comment so a future maintainer doesn't 'fix' it. Test pins
    the comment so it can't be accidentally removed."""
    src = API_PY.read_text()
    fail_anchor = src.index("v1.15.38: kept ack-aware")
    fail_block = src[fail_anchor:fail_anchor + 1500]
    assert "FAIL chip" in fail_block
    assert "needs-action count" in fail_block
    # "the user's explicit complaint" wraps over a newline in the
    # source; just check the on-one-line tail.
    assert "explicit complaint was COOKIES, not FAIL" in fail_block


def test_link_pills_hl_c_intentionally_exclude_mismatch():
    """The link_pills=hl / link_pills=c SQL filters exclude
    rows with mismatch_state set. The JS row pill render
    has the SAME precedence (mismatch wins over hl/c via the
    if/else-if chain in linkCell computation). Documenting
    here so audit-found "divergence" isn't refixed."""
    src = API_PY.read_text()
    js_src = APP_JS.read_text()
    # SQL excludes mismatch.
    sql_idx = src.index('COALESCE(p_e.placement_kind, p_g.placement_kind) = \'hardlink\' AND COALESCE(lf_e.mismatch_state, lf_g.mismatch_state) IS NULL')
    assert sql_idx > 0
    # JS render: mismatch wins.
    js_idx = js_src.index("if (isMismatch && placed) {")
    hl_idx = js_src.index("it.placement_kind === 'hardlink'")
    assert js_idx < hl_idx, (
        "v1.15.39: JS linkCell render must check mismatch "
        "BEFORE hardlink (mismatch precedence mirrors SQL "
        "filter's mismatch_state IS NULL gate)"
    )
