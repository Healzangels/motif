"""v1.15.45 — dashboard stat-card SSR (flash-bug sweep, phase 1).

the user: "lets do a flash bug sweep" — closing the SSR follow-up
flagged repeatedly across v1.15.27/29/31/33/40/41/42/44 open-
threads sections. The dashboard's ~10 stat cards rendered "—" on
first paint and JS swapped to the real values on the next
/api/stats tick (1s TTL cache). The flash got more annoying after
v1.15.42 + v1.15.44 made several of them clickable — "—" → number
on a card you want to click is jarring.

This tag SSR-bakes 11 of the most prominent stat-num values via
a new `_dashboard_ssr_state()` helper registered as a Jinja
global (mirrors the v1.15.22 `_topbar_ssr_state` pattern). The
JS poll still runs and overwrites with the live /api/stats value
on the next tick, idempotent when SSR matched.

## What's SSR'd (11 fields)

* COVERAGE (2):     movies.total, tv.total
* OPERATIONS (3):   queue.pending, queue.running, queue.failed
* ACTIVITY (2):     placements_today, placements_week
* STORAGE (4):      hardlinks, copies, wasted (formatted), orphans

## What's deferred to v1.15.46+

* 4 COVERAGE footers (tdb-{movies,tv}-{in-library,themed}) —
  sourced from /api/sections/coverage per-section aggregates,
  not /api/stats; bigger SQL lift.
* INSIGHT charts + SECTION COVERAGE table — JS-rendered SVG/
  HTML, less benefit from pre-baked values.
* Topbar UPD chip — complex per-section URL-diff SQL, deferred
  in v1.15.22 with "ship when the user reports a visible flash."

Static-text guards (consistent with v1.15.22 SSR-test patterns).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
DASH_HTML = REPO / "app" / "web" / "templates" / "dashboard.html"


# ── 1. Helper exists + registered as Jinja global ──────────


def test_dashboard_ssr_state_helper_defined():
    """The `_dashboard_ssr_state` function must exist in api.py
    and be registered as a Jinja global so dashboard.html can
    call it. Mirrors the v1.15.22 _topbar_ssr_state pattern."""
    src = API_PY.read_text()
    assert "def _dashboard_ssr_state(" in src, (
        "v1.15.45: missing _dashboard_ssr_state helper — required "
        "to SSR the 11 dashboard stat-num values"
    )
    assert 'templates.env.globals["dashboard_ssr_state"] = _dashboard_ssr_state' in src, (
        "v1.15.45: _dashboard_ssr_state must be registered as a "
        "Jinja global so dashboard.html can call it without an "
        "explicit route-context plumb"
    )


def test_dashboard_ssr_state_returns_all_11_fields():
    """The state dict must declare all 11 SSR'd field names so a
    refactor that drops one doesn't silently regress the
    template's `_ssr_dash.foo` accesses to None → "—" flash."""
    src = API_PY.read_text()
    anchor = src.index("def _dashboard_ssr_state(")
    body = src[anchor:anchor + 8000]
    required_fields = [
        "movies_tdb_total",
        "tv_tdb_total",
        "queue_pending",
        "queue_running",
        "queue_failed",
        "placements_today",
        "placements_week",
        "storage_hardlinks",
        "storage_copies",
        "storage_wasted_pretty",
        "orphans",
    ]
    missing = [f for f in required_fields if f"\"{f}\"" not in body]
    assert not missing, (
        f"v1.15.45: _dashboard_ssr_state missing field(s) {missing} "
        "— dashboard.html will render '—' on first paint for these"
    )


def test_dashboard_ssr_state_sql_matches_stats_sync_predicates():
    """SQL predicates in _dashboard_ssr_state MUST match the
    canonical _stats_sync predicates exactly. Divergence = the
    SSR-baked number disagrees with the post-poll /api/stats
    number → visible flicker on every dashboard load (the very
    bug we're trying to eliminate). Test pins the load-bearing
    predicates that the author got wrong on the first draft."""
    src = API_PY.read_text()
    anchor = src.index("def _dashboard_ssr_state(")
    body = src[anchor:anchor + 8000]
    # media_type='tv' (not 'show') — schema uses 'tv' but it's
    # easy to mistype as 'show' since the UI labels say "TV SERIES".
    assert "media_type = 'tv'" in body, (
        "v1.15.45: tv_tdb_total predicate must use media_type='tv', "
        "not 'show' — schema convention"
    )
    # failed gates on acked_at IS NULL (matches /queue FAILED chip).
    assert "status='failed' AND acked_at IS NULL" in body, (
        "v1.15.45: queue_failed must filter acked_at IS NULL to "
        "match _stats_sync — pre-fix the SSR'd FAILURES count "
        "would include acked failures, then JS swaps to the lower "
        "ack-aware count = downward flash"
    )
    # orphans counts plex_orphan THEMES (not placement-row nulls).
    assert "upstream_source = 'plex_orphan'" in body, (
        "v1.15.45: orphans must query themes WHERE upstream_source="
        "'plex_orphan' to match _stats_sync (line 4764) — NOT "
        "placements WHERE rating_key IS NULL (different semantic)"
    )
    # storage_wasted_bytes JOINs local_files with section_id
    # (the v1.14.36 anti-cartesian fix).
    assert "AND lf.section_id = p.section_id" in body, (
        "v1.15.45: storage_wasted_bytes JOIN must include section_id "
        "to avoid the v1.14.36 cross-section cartesian-product "
        "inflation bug on multi-section libraries"
    )


def test_dashboard_ssr_state_safe_fallback_on_error():
    """Any DB / runtime error must log WARNING + return the safe-
    default dict (None values → template renders '—'). Pattern
    matches the v1.15.34 _topbar_ssr_state hardening.

    v1.15.50: widened slice 8000 → 14000 since phase-2 added a
    second SELECT block. The except/log pair lives at the
    function tail.

    v1.15.65: anchor on the next top-level function instead of a
    char-count slice — the function keeps growing (phase-3 added
    EXISTS visibility predicates) and char-count slices need
    bumping every release."""
    src = API_PY.read_text()
    anchor = src.index("def _dashboard_ssr_state(")
    fn_end = src.index('templates.env.globals["dashboard_ssr_state"]',
                        anchor)
    body = src[anchor:fn_end]
    assert "except Exception as e:" in body, (
        "v1.15.45: must catch broad Exception so a single bad "
        "query can't 500 the entire dashboard route"
    )
    assert 'log.warning("dashboard_ssr_state failed:' in body, (
        "v1.15.45: must log WARNING on failure — pre-v1.15.34 the "
        "_topbar_ssr_state bare-pass meant silent flash-on-error; "
        "match that lesson here"
    )


# ── 2. Template uses _ssr_dash for all 11 fields ────────────


SSR_TEMPLATE_BINDINGS = [
    # v1.23.34: top cards lead with coverage % (ssr_pct on the plex
    # aggregates), not the old TDB-catalogue data-stat headlines.
    ('id="cov-movies-pct"', "_ssr_dash.plex_movies_with_theme"),
    ('id="cov-tv-pct"', "_ssr_dash.plex_tv_with_theme"),
    ('data-stat="queue.pending"', "_ssr_dash.queue_pending"),
    ('data-stat="queue.running"', "_ssr_dash.queue_running"),
    ('data-stat="queue.failed"', "_ssr_dash.queue_failed"),
    ('id="activity-placements-today"', "_ssr_dash.placements_today"),
    ('id="activity-placements-week"', "_ssr_dash.placements_week"),
    ('id="storage-hardlinks"', "_ssr_dash.storage_hardlinks"),
    ('id="storage-copies"', "_ssr_dash.storage_copies"),
    ('id="storage-wasted"', "_ssr_dash.storage_wasted_pretty"),
    ('id="orphan-count"', "_ssr_dash.orphans"),
]


def test_dashboard_template_calls_ssr_state_helper():
    """Template must `{% set _ssr_dash = dashboard_ssr_state() %}`
    near the top so the SSR'd values are available to the cards
    below. Without this set, every `{{ _ssr_dash.foo }}` access
    becomes None → "—" flash."""
    html = DASH_HTML.read_text()
    assert "{% set _ssr_dash = dashboard_ssr_state() %}" in html, (
        "v1.15.45: dashboard.html must call dashboard_ssr_state() "
        "to populate _ssr_dash for the stat-card SSR"
    )


def test_dashboard_template_ssr_bakes_all_11_fields():
    """Each of the 11 SSR'd cards must reference its
    `_ssr_dash.<field>` value. Cross-source check: template
    binding ↔ helper field name. Drift here = silent flash.

    v1.15.50: the v1.15.45 inline `if X is not none else "—"`
    conditionals migrated to a single `ssr_num()` macro defined
    at the top of dashboard.html. The "—" fallback now lives
    inside the macro, so per-cell `"—"` check is dropped here —
    covered globally by the v1.15.50 macro-presence test."""
    html = DASH_HTML.read_text()
    failures = []
    for anchor_attr, expected_ref in SSR_TEMPLATE_BINDINGS:
        idx = html.find(anchor_attr)
        if idx == -1:
            failures.append(
                f"{anchor_attr}: element missing from template — "
                "SSR_TEMPLATE_BINDINGS may be stale"
            )
            continue
        block = html[idx:idx + 400]
        # Post-v1.15.50: cells call ssr_num(_ssr_dash.foo) instead
        # of the verbose conditional. Either form is acceptable —
        # the underlying contract (field reference present, with
        # fallback handling) holds via the macro.
        if expected_ref not in block:
            failures.append(
                f"{anchor_attr}: missing '{{{{ {expected_ref} }}}}' "
                f"reference — flash bug returns for this card"
            )
    assert not failures, "v1.15.45: " + "; ".join(failures)


def test_dashboard_template_storage_wasted_uses_pre_formatted_string():
    """storage-wasted MUST use the pre-formatted `_ssr_dash.storage_
    wasted_pretty` (e.g. "1.2 GB"), NOT a raw byte count. Otherwise
    SSR renders "1234567" and JS swaps to "1.2 MB" = visible
    textual flash on every page load. The Python format helper
    mirrors fmtBytes() in app.js for parity."""
    html = DASH_HTML.read_text()
    wasted_idx = html.index('id="storage-wasted"')
    block = html[wasted_idx:wasted_idx + 400]
    assert "_ssr_dash.storage_wasted_pretty" in block, (
        "v1.15.45: storage-wasted must use the pre-formatted pretty "
        "string so SSR + JS agree on the display text (mirror of "
        "fmtBytes() in app.js)"
    )
    # The Python formatter logic must agree with fmtBytes — pin
    # the units array so a future drift on either side trips a test.
    api_src = API_PY.read_text()
    fn_anchor = api_src.index("def _dashboard_ssr_state(")
    # v1.18.20: helper grew again (collection SSR aggregates +
    # rationale comments pushed the units array past the prior
    # 14000-char window). Bump to 18000 with comfortable margin.
    fn_body = api_src[fn_anchor:fn_anchor + 22000]
    assert '["B", "KB", "MB", "GB", "TB"]' in fn_body, (
        "v1.15.45: SSR byte-formatter units array must match "
        "fmtBytes() in app.js so SSR + post-poll agree"
    )
