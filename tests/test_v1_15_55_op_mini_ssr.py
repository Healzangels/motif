"""v1.15.55 — SSR op-mini bar so the topbar live-ops status
persists across tab navs.

the user: "Also going to the libraries and settings the status bar
flickers and reloads vs dash and logs it appears to stay the
whole time I want it to remain consistant on changing to
different tabs in motif."

## Bug

Pre-v1.15.55 base.html rendered the op-mini bar as:
    <a class="op-mini" id="op-mini" hidden ...></a>
On every page nav the bar started hidden + empty; ops.js's
init() called poll() which fetched /api/progress, computed
renderTopbar(), and only then revealed the bar. Network RTT
+ async poll timing meant a 50ms-1s gap between paint and
reveal — visible as a hidden→visible flash on every nav.

DASH/LOGS may have felt less flickery because their JS init
paths complete faster, but the underlying race was identical.

## Fix

Extend _topbar_ssr_state to include op-mini fields:
* op_running — bool, True when any op_progress row has status
  IN ('running', 'cancelling')
* op_tone — string (tdb / plex / warn / etc), mirrors
  TONE_BY_KIND in ops.js
* op_label — stage_label (the human-readable in-flight phase)
  or kind-based default mirroring KIND_LABEL in ops.js

base.html renders the bar VISIBLE with class + label baked in
when op_running, HIDDEN otherwise. Bar is indeterminate at
SSR-time (no pct baked) so JS pct refinement on the next poll
doesn't cause a visible jump. JS renderTopbar still owns live
updates — SSR just provides a non-flashing starting frame.

Static-text guards consistent with v1.15.45/50 SSR patterns.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
BASE_HTML = REPO / "app" / "web" / "templates" / "base.html"
OPS_JS = REPO / "app" / "web" / "static" / "ops.js"


# ── 1. Helper exposes op-mini fields ─────────────────────────


def test_topbar_ssr_state_declares_op_mini_fields():
    """`_topbar_ssr_state` must declare op_running / op_tone /
    op_label in its default-dict so the template's _ssr_top.foo
    access never falls through to None (would break the Jinja
    conditional + leave the bar permanently hidden)."""
    src = API_PY.read_text()
    anchor = src.index("def _topbar_ssr_state()")
    body = src[anchor:anchor + 8000]
    for field in ('"op_running": False', '"op_tone": "tdb"', '"op_label": ""'):
        assert field in body, (
            f"v1.15.55: _topbar_ssr_state must declare {field}"
        )


def test_topbar_ssr_queries_op_progress_for_running_op():
    """The helper must SELECT from op_progress to find the most-
    recent running op. Without this query op_running stays False
    even when an op IS active = no SSR fix."""
    src = API_PY.read_text()
    anchor = src.index("def _topbar_ssr_state()")
    body = src[anchor:anchor + 8000]
    assert "FROM op_progress" in body, (
        "v1.15.55: helper must SELECT FROM op_progress"
    )
    assert "WHERE status IN ('running', 'cancelling')" in body, (
        "v1.15.55: query must gate on running+cancelling statuses "
        "(matches ops.js renderTopbar's runningOnly filter)"
    )
    # v1.15.82: updated_at moved from primary sort to tiebreaker
    # after a CASE-on-kind priority order. The query must still
    # ORDER BY updated_at (now DESC as the secondary key) — pin
    # the substring without anchoring to its position.
    assert "updated_at DESC" in body, (
        "v1.15.55: query must include updated_at DESC (now as the "
        "tiebreaker after the v1.15.82 priority-CASE primary sort)"
    )


def test_topbar_ssr_tone_mapping_mirrors_ops_js():
    """The Python tone_by_kind dict in _topbar_ssr_state must
    mirror TONE_BY_KIND in ops.js. Drift = SSR tone disagrees
    with the post-poll JS tone, causing a visible color flash."""
    src = API_PY.read_text()
    anchor = src.index("def _topbar_ssr_state()")
    body = src[anchor:anchor + 8000]
    # Pin the canonical kind→tone pairs (subset that matters).
    for line in (
        '"tdb_sync": "tdb"',
        '"plex_enum": "plex"',
        '"reprobe_plex_themes": "plex"',
        '"bulk_probe_tdb": "tdb"',
    ):
        assert line in body, (
            f"v1.15.55: tone mapping must include {line!r} — "
            "mirror of TONE_BY_KIND in ops.js"
        )
    # Also verify the JS-side map still has these — cross-source
    # contract pin so a future ops.js rename catches a test failure.
    js = OPS_JS.read_text()
    js_map_anchor = js.index("const TONE_BY_KIND = {")
    js_map_end = js.index("};", js_map_anchor)
    js_map_block = js[js_map_anchor:js_map_end]
    for js_pair in (
        "tdb_sync:            'tdb'",
        "plex_enum:           'plex'",
        "reprobe_plex_themes: 'plex'",
    ):
        assert js_pair in js_map_block, (
            f"v1.15.55: JS TONE_BY_KIND must keep {js_pair!r} — "
            "Python SSR mirror depends on this binding"
        )


# ── 2. base.html SSR rendering ──────────────────────────────


def test_base_html_op_mini_uses_ssr_visibility_gate():
    """The op-mini <a> must conditionally hide via `{% if not
    _ssr_top.op_running %}hidden{% endif %}` so an active op
    renders the bar visible on first paint."""
    html = BASE_HTML.read_text()
    op_mini_anchor = html.index('id="op-mini"')
    # Walk to enclosing <a> opening tag (look back a few hundred chars).
    a_open = html.rfind('<a', 0, op_mini_anchor)
    a_close = html.index('>', op_mini_anchor)
    a_tag = html[a_open:a_close + 1]
    assert "{% if not _ssr_top.op_running %}hidden{% endif %}" in a_tag, (
        "v1.15.55: op-mini must SSR-gate the hidden attr on "
        "_ssr_top.op_running — pre-fix it was always hidden + JS "
        "revealed after first poll = visible flash on every nav"
    )


def test_base_html_op_mini_ssr_classes_and_content():
    """When op_running, the SSR'd bar must carry:
    * `op-tone-{{ tone }}` class (color)
    * `op-mini-indet` class (indeterminate bar — JS refines pct
      on next poll without a visible jump)
    * label span with {{ _ssr_top.op_label }}
    """
    html = BASE_HTML.read_text()
    op_mini_anchor = html.index('id="op-mini"')
    # Inspect the full <a> opening + body up to </a>.
    a_open = html.rfind('<a', 0, op_mini_anchor)
    a_close = html.index('</a>', a_open) + len('</a>')
    block = html[a_open:a_close]
    assert "op-tone-{{ _ssr_top.op_tone }}" in block, (
        "v1.15.55: SSR bar must include op-tone-{{ tone }} class"
    )
    assert "op-mini-indet" in block, (
        "v1.15.55: SSR bar must use op-mini-indet (full-width fill) "
        "so JS pct refinement doesn't cause a visible jump"
    )
    assert "{{ _ssr_top.op_label }}" in block, (
        "v1.15.55: SSR bar must render the label so the operator "
        "reads the in-flight phase across navs"
    )


def test_base_html_op_mini_hidden_when_no_running_op():
    """Counter-guard: when op_running is False, the bar must
    render with the `hidden` attribute (otherwise empty bars
    flash on every page load with no work happening)."""
    html = BASE_HTML.read_text()
    op_mini_anchor = html.index('id="op-mini"')
    a_open = html.rfind('<a', 0, op_mini_anchor)
    a_close = html.index('>', op_mini_anchor)
    a_tag = html[a_open:a_close + 1]
    # The conditional must wrap `hidden` so the attr only renders
    # when op_running is falsy.
    assert "{% if not _ssr_top.op_running %}hidden{% endif %}" in a_tag


# ── 3. SSR survives DB error safely ─────────────────────────


def test_topbar_ssr_op_query_inside_try_except():
    """The op_progress query must live inside the existing
    try/except so a DB error (e.g. table not yet migrated) falls
    back to the default state (op_running=False) instead of
    500-ing the entire page render."""
    src = API_PY.read_text()
    anchor = src.index("def _topbar_ssr_state()")
    fn_end = src.index("templates.env.globals", anchor)
    fn_body = src[anchor:fn_end]
    # The op_row query must come BEFORE the except clause.
    op_query_idx = fn_body.index("FROM op_progress")
    except_idx = fn_body.index("except Exception")
    assert op_query_idx < except_idx, (
        "v1.15.55: op_progress query must live inside the try "
        "block so DB errors fall through to the safe default"
    )
