"""v1.14.30 — public-stats + secondary stats surfaces sync.

Closes audit findings api H1 + H3 + H4 (mirror-principle drift,
class P). Four secondary stats surfaces had drifted back to the
pre-v1.14.8 title-global `WHERE failure_kind IS NOT NULL` shape,
so they disagreed with the topbar FAIL pill + library filter
(`attn_pills=fail`) by exactly the count of sfa-acked sections.

## Scope

1. Module-level constants `_FAILURES_SFA_FROM_SQL` +
   `_FAILURES_SFA_WHERE_SQL` codify the canonical per-(title,
   section) sfa-aware predicate the v1.14.8 rewrite established.
2. /api/stats `failures_total` (kept) + `failures_unavailable` +
   `failures_cookies` (REWRITTEN) consume the constants.
3. /api/public/stats `unacked_failures` + `failures_total`
   (REWRITTEN) consume the constants.
4. Dashboard insights `failure_kinds` chart (REWRITTEN) consumes
   the constants and groups by `t.failure_kind`.

## Why constants over per-site rewrites

Per-site rewrites would close H1+H3+H4 today, but the audit's
Theme 2 (mirror-principle drift class P) keeps recurring because
the SAME predicate lives in many SQL surfaces written at different
times. The class-fix recommendation is "single Python function
returning the SQL fragment, called by every consumer". v1.14.30
does the smaller version of that fix: two module constants
holding the FROM/JOIN and WHERE chunks, all six current consumers
wired to them. Adding a 7th consumer in a future tag now requires
ONE place to update, not six.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Canonical sfa-aware predicate constants ──────────────────


def test_failures_sfa_from_constant_defined():
    """The _FAILURES_SFA_FROM_SQL module constant must hold the
    canonical FROM/JOIN sequence — themes JOIN plex_items JOIN
    plex_sections LEFT JOIN section_failure_acks. Pin every
    structural piece so a refactor can't silently drop the
    plex_sections include-gate or the sfa join."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "_FAILURES_SFA_FROM_SQL = (" in src
    anchor = src.index("_FAILURES_SFA_FROM_SQL = (")
    # v1.15.61 widened the constant with explanatory comment;
    # bumped slice 1000 → 2500 to keep the join clauses in scope.
    body = src[anchor:anchor + 2500]
    # v1.15.62: simplified the join from (guid_tmdb + media_type)
    # OR theme_id back to just `pi.theme_id = t.id`. The OR caused
    # SQLite's optimizer to bail and degraded perf to 60s+ page
    # loads. theme_id covers both TDB rows + orphans.
    assert "FROM themes t" in body
    assert "JOIN plex_items pi" in body
    assert "pi.theme_id = t.id" in body
    # plex_sections include-gate (excluded sections don't count).
    assert "JOIN plex_sections ps" in body
    assert "ps.included = 1" in body
    # sfa LEFT JOIN keyed on (media_type, tmdb_id, section_id).
    assert "LEFT JOIN section_failure_acks sfa" in body
    assert "sfa.media_type = t.media_type" in body
    assert "sfa.tmdb_id = t.tmdb_id" in body
    assert "sfa.section_id = pi.section_id" in body


def test_failures_sfa_where_constant_defined():
    """The _FAILURES_SFA_WHERE_SQL constant holds the WHERE
    clause: the row's failure_kind set, NOT title-acked, NOT
    section-acked. Same predicate the library filter uses."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "_FAILURES_SFA_WHERE_SQL = (" in src
    anchor = src.index("_FAILURES_SFA_WHERE_SQL = (")
    body = src[anchor:anchor + 400]
    assert "t.failure_kind IS NOT NULL" in body
    assert "t.failure_acked_at IS NULL" in body
    assert "sfa.acked_at IS NULL" in body


# ── /api/stats consumers ────────────────────────────────────


def test_stats_failures_unavailable_uses_sfa_constants():
    """`failures_unavailable` (the dead-URL kind sub-total
    rendered next to the topbar FAIL pill) must use the sfa-
    aware predicate so it agrees with failures_total. Pre-fix
    it was title-global — over-counted by the sfa-acked count."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # Find the failures_unavailable subquery alias.
    anchor = src.index("AS failures_unavailable")
    # Walk back to the SELECT COUNT( that opens its subquery.
    select_open = src.rfind("(SELECT COUNT(*)", 0, anchor)
    block = src[select_open:anchor + 30]
    assert "_FAILURES_SFA_FROM_SQL" in block
    assert "_FAILURES_SFA_WHERE_SQL" in block
    # Constraint on the kind set is preserved.
    assert "video_private" in block
    assert "video_removed" in block
    assert "video_age_restricted" in block
    assert "geo_blocked" in block


def test_stats_failures_cookies_uses_sfa_constants():
    """`failures_cookies` sibling — same FROM-clause shape as
    failures_total (per-(title, section) multiplicity).
    v1.15.38: WHERE clause switched from
    `_FAILURES_SFA_WHERE_SQL` (ack-aware) to
    `_COOKIES_PILL_WHERE_SQL` (no ack predicates) to match the
    v1.15.38 attn_pills=cookies filter + the row-level yellow
    ⚿ pill render. The FROM clause stays identical so the
    per-section count semantics are preserved."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    anchor = src.index("AS failures_cookies")
    select_open = src.rfind("(SELECT COUNT(*)", 0, anchor)
    block = src[select_open:anchor + 30]
    # FROM clause unchanged.
    assert "_FAILURES_SFA_FROM_SQL" in block
    # v1.15.38: WHERE switched to the cookies-pill predicate.
    assert "_COOKIES_PILL_WHERE_SQL" in block
    # The old ack-aware WHERE constant is GONE from this SELECT.
    assert "_FAILURES_SFA_WHERE_SQL" not in block, (
        "v1.15.38: failures_cookies must use the no-ack "
        "_COOKIES_PILL_WHERE_SQL — see test_v1_15_38_cookies_"
        "pill_filter_count_alignment for the rationale"
    )


def test_stats_failures_total_still_wires_constants():
    """The v1.14.8 failures_total query — already sfa-aware —
    now consumes the constants too. Belt-and-suspenders pin so
    a future refactor can't decouple the canonical query from
    the constant."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    anchor = src.index("AS failures_total,")  # comma → /api/stats site
    select_open = src.rfind("(SELECT COUNT(*)", 0, anchor)
    block = src[select_open:anchor + 30]
    assert "_FAILURES_SFA_FROM_SQL" in block
    assert "_FAILURES_SFA_WHERE_SQL" in block


# ── /api/public/stats consumers ─────────────────────────────


def _public_stats_body(src: str) -> str:
    """Slice from the api_public_stats handler def through to the
    next handler def (the next `async def api_...`)."""
    fn_anchor = src.index("async def api_public_stats(")
    next_handler = src.index("\n    async def ", fn_anchor + 1)
    return src[fn_anchor:next_handler]


def test_public_stats_unacked_failures_uses_sfa_constants():
    """The Homepage widget's unacked_failures must agree with the
    in-app /api/stats unacked_failures. Pre-fix the public widget
    was title-global; a U-resolved 4K row stayed in the public
    count even though the topbar dot had cleared."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    body = _public_stats_body(src)
    alias_anchor = body.index("AS unacked_failures")
    select_open = body.rfind("(SELECT COUNT(*)", 0, alias_anchor)
    block = body[select_open:alias_anchor + 30]
    assert "_FAILURES_SFA_FROM_SQL" in block
    assert "_FAILURES_SFA_WHERE_SQL" in block


def test_public_stats_failures_total_uses_sfa_constants():
    """Same fix shape for failures_total in /api/public/stats —
    pre-fix it was the WORST of the bunch (no failure_acked_at
    gate at all, so it counted ACKed rows on top of the sfa drift)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    body = _public_stats_body(src)
    alias_anchor = body.index("AS failures_total")
    select_open = body.rfind("(SELECT COUNT(*)", 0, alias_anchor)
    block = body[select_open:alias_anchor + 30]
    assert "_FAILURES_SFA_FROM_SQL" in block
    assert "_FAILURES_SFA_WHERE_SQL" in block


def test_public_stats_no_longer_has_pre_v1_14_30_title_global_form():
    """Pin the regression: the bare `FROM themes WHERE failure_kind
    IS NOT NULL` form (no acked gate, no sfa join) must NOT
    reappear in /api/public/stats. The constant-driven form is
    the only acceptable shape."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    body = _public_stats_body(src)
    assert "FROM themes WHERE failure_kind IS NOT NULL) AS failures_total" not in body
    assert "FROM themes WHERE failure_kind IS NOT NULL) AS unacked_failures" not in body


# ── Dashboard insights failure_kinds chart ──────────────────


def test_dashboard_insights_failure_kinds_uses_sfa_constants():
    """The // FAILURE BREAKDOWN chart — pre-fix users saw a kind
    claim N rows in the chart, clicked through to library, found
    fewer (the library filter is sfa-aware). v1.14.30 wires the
    chart to the same canonical predicate. GROUP BY t.failure_kind
    so the rollup keys align with the kind labels rendered next
    to the bars."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_dashboard_insights(")
    body = src[fn_anchor:fn_anchor + 5000]
    # Find the failure_rows = conn.execute( query.
    q_anchor = body.index("failure_rows = conn.execute(")
    block = body[q_anchor:q_anchor + 1500]
    assert "_FAILURES_SFA_FROM_SQL" in block
    assert "_FAILURES_SFA_WHERE_SQL" in block
    assert "GROUP BY t.failure_kind" in block
    # The pre-v1.14.30 title-global shape must NOT survive — the
    # chart used to query `FROM themes` directly.
    assert "FROM themes\n" not in block or "GROUP BY failure_kind" not in block


# ── Constant consumer sweep — count the call sites ──────────


def test_failures_sfa_from_sql_has_at_least_six_consumers():
    """v1.14.30 wires the constant into:
      1. /api/stats failures_total
      2. /api/stats failures_unavailable
      3. /api/stats failures_cookies
      4. /api/public/stats unacked_failures
      5. /api/public/stats failures_total
      6. dashboard insights failure_kinds chart

    Pin the lower bound — a refactor that drops one site below 6
    means a consumer drifted off the canonical predicate. Count
    references in the SQL site fragments only (excludes the
    constant's own definition)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    n = src.count("{_FAILURES_SFA_FROM_SQL}")
    assert n >= 6, (
        f"Expected ≥6 consumers of _FAILURES_SFA_FROM_SQL, found {n} — "
        "a stats surface drifted back to title-global, OR a future "
        "refactor consolidated sites in a way that needs this "
        "lower-bound bumped down with care."
    )


# ── Reuse pin ────────────────────────────────────────────────


def test_v14_8_failures_total_marker_preserved():
    """The v1.14.8 archaeology comment that explains WHY the SQL
    is sfa-aware must survive the v1.14.30 DRY refactor.
    Per CLAUDE.md: never delete a # vX.Y.Z marker unless deleting
    the line it guards. The line still exists; the marker stays."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "v1.14.8: rewrote from a title-global COUNT" in src
