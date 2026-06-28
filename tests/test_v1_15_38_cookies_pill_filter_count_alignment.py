"""v1.15.38 — yellow TDB ⚿ pill: filter + chip count both match
the row-level pill render (drop ack predicates).

the user: "the yellow TDB Pill filter wasn't filtering properly."

## Pre-fix

Three layers diverged:

1. **Row pill (canonical)** — `app/web/static/app.js:6089-6101`:
   `if (it.failure_kind === 'cookies_expired') { ...
    return ` <span class="tdb-pill tdb-pill-cookies">TDB ⚿`
    ... }`. NO ack check.
2. **Filter SQL** — `app/web/api.py` `attn_pills=cookies` branch:
   `failure_kind = 'cookies_expired' AND failure_acked_at IS
    NULL AND sfa.acked_at IS NULL`. ACK-AWARE → fewer rows
   than the visible pills.
3. **Topbar count SQL** — `_topbar_ssr_state` cookies_count +
   `/api/stats` failures_cookies: same ack-aware shape →
   chip count diverged from filter results AFTER fix #2.

## Post-fix

All three aligned to `failure_kind = 'cookies_expired'` (the
predicate the row pill uses, no ack):
- New `_COOKIES_PILL_WHERE_SQL` constant captures the
  shared predicate.
- The `attn_pills=cookies` filter uses it directly.
- The topbar SSR `cookies_count` + `/api/stats`
  `failures_cookies` use it via `_FAILURES_SFA_FROM_SQL`
  (per-(title, section) FROM clause for count parity with
  the existing FAIL-pill counts; only the WHERE changes).

The FAIL pill filter was NOT broadened — the user's complaint
was specifically yellow ⚿. The FAIL chip's historical
ack-aware semantic ("acked failures don't count toward
needs-action") stays intact; the FAIL count + filter remain
aligned to each other (just to a different shape than the
row red ✗ pill, which is by design — acking is a UX
mechanism for tuning the FAIL count).

## Audit findings shipped alongside

The audit pass that surfaced this also found:
- Topbar count divergence (this fix's #3 above) — would
  have been an instant follow-up tag if missed.

No other HIGH/MED bugs found in the round-5 audit beyond
what the v1.15.34/35/36/37 sweep already shipped.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


# ── 1. Cookies filter no longer gates on ack ──────────────────


def test_attn_pills_cookies_filter_drops_ack_predicates():
    """The `attn_pills=cookies` filter must match the row's
    yellow ⚿ pill render: `failure_kind = 'cookies_expired'`,
    no ack predicates. Pre-fix the SQL had `AND
    failure_acked_at IS NULL AND sfa.acked_at IS NULL` which
    excluded acked rows the user could see in the table.
    Note: there are two `elif p == "cookies":` blocks in api.py
    — the first (older, in tdb_pills axis) is unchanged; the
    second (newer, in attn_pills axis) is the v1.15.17 STATUS
    pill which v1.15.38 broadened. Anchor on the v1.15.38 marker
    comment to find the correct branch."""
    src = API_PY.read_text()
    anchor = src.index(
        "v1.15.38: dropped the ack predicates (same fix as"
    )
    branch_block = src[anchor:anchor + 1500]
    # The new minimal predicate.
    assert "(t.failure_kind = 'cookies_expired')" in branch_block
    # Ack predicates are GONE from the cookies branch body.
    # (The marker comment explains why; check just the SQL clause.)
    sql_anchor = branch_block.index("attn_branches.append(")
    sql_block = branch_block[sql_anchor:sql_anchor + 400]
    assert "failure_acked_at IS NULL" not in sql_block, (
        "v1.15.38: the cookies filter SQL must not gate on title-ack — "
        "row yellow ⚿ pill renders regardless of ack state"
    )
    assert "sfa.acked_at IS NULL" not in sql_block, (
        "v1.15.38: the cookies filter SQL must not gate on per-section-ack"
    )


# ── 2. FAIL filter intentionally NOT broadened ───────────────


def test_attn_pills_fail_filter_keeps_ack_predicates():
    """v1.15.38 considered broadening the FAIL filter the same
    way as cookies but DIDN'T — the user only flagged cookies,
    and the FAIL chip's historical semantic is "needs-action
    count" where acking intentionally removes a row from the
    chip + filter. The FAIL count and the FAIL filter stay
    aligned to each other; their joint divergence from the
    row-level red ✗ pill is by design."""
    src = API_PY.read_text()
    # Anchor on the FAIL branch's v1.15.38 marker comment to
    # avoid grabbing some other `if p == "fail":` further up
    # the file.
    anchor = src.index(
        "v1.15.38: kept ack-aware (NOT broadened, unlike"
    )
    branch_block = src[anchor:anchor + 1500]
    # Ack predicates stay.
    assert "failure_acked_at IS NULL" in branch_block
    assert "sfa.acked_at IS NULL" in branch_block
    # Marker comment documents the deliberate non-change.
    assert "explicit complaint was COOKIES, not FAIL" in branch_block


# ── 3. Cookies topbar count + stats payload align ─────────────


def test_cookies_pill_where_sql_constant_defined():
    """The new `_COOKIES_PILL_WHERE_SQL` constant must capture
    the shared predicate (just `failure_kind = 'cookies_expired'`,
    no ack) so count + filter stay in lockstep going forward.
    Pin the exact value so a future "tighten this back to
    acked" refactor breaks here."""
    src = API_PY.read_text()
    assert (
        "_COOKIES_PILL_WHERE_SQL = \"t.failure_kind = 'cookies_expired'\""
        in src
    )


def test_topbar_ssr_cookies_count_uses_new_predicate():
    """`_topbar_ssr_state`'s `cookies_count` SELECT must use
    `_COOKIES_PILL_WHERE_SQL` (not the ack-aware
    `_FAILURES_SFA_WHERE_SQL`)."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _topbar_ssr_state(")
    fn_end = src.index('templates.env.globals["topbar_ssr_state"]', fn_anchor)
    fn_body = src[fn_anchor:fn_end]
    # The cookies_count SELECT references the new constant.
    cookies_select_idx = fn_body.index("AS cookies_count")
    cookies_select_block = fn_body[
        max(0, cookies_select_idx - 600):cookies_select_idx
    ]
    assert "_COOKIES_PILL_WHERE_SQL" in cookies_select_block
    # And does NOT use the old `_FAILURES_SFA_WHERE_SQL AND
    # failure_kind = 'cookies_expired'` shape.
    assert (
        "_FAILURES_SFA_WHERE_SQL}\n                         AND t.failure_kind = 'cookies_expired'"
        not in cookies_select_block
    )


def test_api_stats_failures_cookies_uses_new_predicate():
    """`/api/stats` `failures_cookies` SELECT must use
    `_COOKIES_PILL_WHERE_SQL`. Same intent as the topbar
    count — chip count must match what the user clicks-through
    to."""
    src = API_PY.read_text()
    # Locate the failures_cookies SELECT.
    cookies_idx = src.index("AS failures_cookies")
    cookies_block = src[max(0, cookies_idx - 600):cookies_idx]
    assert "_COOKIES_PILL_WHERE_SQL" in cookies_block
    # Old ack-aware shape is gone.
    assert (
        "_FAILURES_SFA_WHERE_SQL}\n                     AND t.failure_kind = 'cookies_expired'"
        not in cookies_block
    )


# ── 4. The canonical FAIL constants stay intact ──────────────


def test_failures_sfa_where_sql_unchanged():
    """The canonical FAIL predicate constant must keep its
    ack-aware shape (used by FAIL count, FAIL filter, sub-
    counts, dashboard insights). v1.15.38 didn't touch it —
    only the COOKIES count switched to the new constant."""
    src = API_PY.read_text()
    assert "_FAILURES_SFA_WHERE_SQL = (" in src
    anchor = src.index("_FAILURES_SFA_WHERE_SQL = (")
    block = src[anchor:anchor + 400]
    assert '"t.failure_kind IS NOT NULL "' in block
    assert '"AND t.failure_acked_at IS NULL "' in block
    assert '"AND sfa.acked_at IS NULL"' in block


def test_failures_sfa_from_sql_unchanged():
    """The canonical FROM clause (per-(title, section)
    multiplicity) survives — both the FAIL count and the
    new COOKIES count use it. Pin so a future refactor doesn't
    accidentally drop the LEFT JOIN sfa (which is still needed
    for the FAIL ack-aware predicate even though COOKIES
    doesn't use it)."""
    src = API_PY.read_text()
    assert "_FAILURES_SFA_FROM_SQL = (" in src
    anchor = src.index("_FAILURES_SFA_FROM_SQL = (")
    # v1.15.61 widened the constant with explanatory comment;
    # bumped slice 600 → 2500 to keep the JOIN clauses in scope.
    block = src[anchor:anchor + 2500]
    assert "JOIN plex_items pi" in block
    assert "JOIN plex_sections ps" in block
    assert "ps.included = 1" in block
    assert "LEFT JOIN section_failure_acks sfa" in block
