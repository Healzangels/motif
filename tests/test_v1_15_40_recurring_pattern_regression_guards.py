"""v1.15.40 — regression guards for recurring patterns identified
in the v1.15.34-39 audit cycle.

the user: "lets check if there are any other tests worth creating
for frequently repeated asks or issues to prevent issues in the
future."

The v1.15.34-39 audit cycle exposed a few patterns the codebase
keeps recurring on, each well-understood by now but fragile to
future maintenance. This tag pins three of them as static-text
regression guards — no behavior change, just guardrails so:
  - A future refactor can't silently break a known-good pattern
  - A future audit doesn't waste time re-flagging a documented
    by-design divergence

## What's pinned

### 1. Background-op log_event-before-Thread-spawn

The 4 singleton background-op routes (bulk-probe-tdb,
reprobe-plex-themes, reprobe-tdb-failures, bulk-lps) all spawn
a daemon thread. the user's recurring "deploy looked fine but
nothing happened" reports trace to threads that crashed
silently — the log_event call BEFORE the thread spawn is the
docker-log beacon proving the route was hit. v1.15.34/35/37
addressed each instance; this test pins the ordering so a
future route addition that misses log_event-first is caught.

### 2. Optimistic placeholder clears at v1.15.35/36 call sites

v1.15.35 (download-backup) + v1.15.36 (redownload) added
`clearOptimisticPlaceholder('download_queue')` in the error
paths so the placeholder doesn't linger 5s after the action
fails + the user dismisses the alert. The pattern was bitten
twice in two tags; pin the fixes here so a future refactor
doesn't silently drop them.

### 3. By-design divergence markers

Three documented-as-intentional divergences from v1.15.38/39
that audits keep re-flagging:
  - `attn_pills=fail` SQL filter is ack-aware on purpose
    (v1.15.38). the user's mental model for the FAIL chip is
    "needs-action count" — acking removes the row from chip +
    filter. Different from cookies (where v1.15.38 broadened).
  - `link_pills=hl/c` SQL excludes `mismatch_state IS NOT
    NULL`; JS linkCell render gives mismatch precedence over
    hl/c via the if/else-if order (v1.15.39 audit verified).
  - `attn_pills=broken` mixed-mode handled via post-stat
    Python matcher, not a SQL branch (v1.15.39 — the SQL
    union refactor was out-of-scope per v1.13.68).

Each marker comment lives at the divergence site; this test
pins them so a future refactor that removes the comment fails
here, prompting the maintainer to re-read the audit notes
before changing.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── 1. Background-op log_event-before-Thread-spawn ────────────


# (handler-fn-name, op_id) for each singleton background route.
# When adding a new background route, add its (fn_name, op_id) tuple
# here so this test catches log-ordering regressions.
BACKGROUND_OP_ROUTES = [
    ("api_admin_bulk_probe_tdb", "bulk-probe-tdb"),
    ("api_admin_reprobe_tdb_failures", "bulk-probe-tdb"),
    ("api_admin_reprobe_plex_themes", "reprobe-plex-themes"),
    ("api_admin_bulk_let_plex_serve", "bulk-lps"),
]


def test_every_background_op_route_logs_before_thread_spawn():
    """Every singleton background-op route MUST call `log_event(...)`
    BEFORE `threading.Thread(...).start()` so the docker log
    always shows the operation was attempted, even if the
    spawned thread crashes before `start_progress` runs.

    Recurring symptom: "deploy looked fine but nothing
    happened" — addressed per-route in v1.15.34/35/37, pinned
    here as a class-level guard."""
    src = API_PY.read_text()
    failures = []
    for fn_name, op_id in BACKGROUND_OP_ROUTES:
        anchor = src.find(f"async def {fn_name}(")
        assert anchor != -1, (
            f"v1.15.40: route handler {fn_name} missing — update "
            f"BACKGROUND_OP_ROUTES if the function was renamed"
        )
        # Walk to next route or function definition.
        end = src.find("\n    @app.", anchor)
        body = src[anchor:end] if end > 0 else src[anchor:anchor + 8000]
        log_idx = body.find("log_event(")
        thread_idx = body.find("threading.Thread(")
        if log_idx == -1:
            failures.append(
                f"{fn_name}: no log_event() call — every "
                f"background-op route must log a structured "
                f"event before spawning its thread"
            )
        elif thread_idx == -1:
            failures.append(
                f"{fn_name}: no threading.Thread() spawn found "
                f"— BACKGROUND_OP_ROUTES list may be stale"
            )
        elif log_idx > thread_idx:
            failures.append(
                f"{fn_name}: log_event() at offset {log_idx} "
                f"comes AFTER threading.Thread() at offset "
                f"{thread_idx} — must log BEFORE spawning so "
                f"docker logs show the attempt even if the "
                f"thread dies"
            )
    assert not failures, (
        "v1.15.40: background-op log-ordering regression — "
        + "; ".join(failures)
    )


# ── 2. Optimistic placeholder clears at v1.15.35/36 call sites ─


def test_download_backup_error_path_clears_placeholder():
    """v1.15.35 added `clearOptimisticPlaceholder('download_queue')`
    in the download-tdb-backup catch block so the placeholder
    doesn't linger 5s after a failed action. Pin so a future
    refactor doesn't silently drop the cleanup."""
    src = APP_JS.read_text()
    # Anchor on the v1.15.35 marker.
    anchor = src.index(
        "v1.15.35: clear the optimistic placeholder on"
    )
    block = src[anchor:anchor + 1500]
    assert "clearOptimisticPlaceholder('download_queue')" in block, (
        "v1.15.40: v1.15.35's download-backup error-path "
        "placeholder cleanup must stay intact"
    )


def test_redownload_error_path_clears_placeholder():
    """v1.15.36 added the same cleanup in the redownload catch
    block (mirrors the v1.15.35 download-backup fix). Pin so a
    future refactor doesn't silently drop it."""
    src = APP_JS.read_text()
    anchor = src.index(
        "v1.15.36: clear the optimistic placeholder on failure"
    )
    block = src[anchor:anchor + 1500]
    assert "clearOptimisticPlaceholder('download_queue')" in block, (
        "v1.15.40: v1.15.36's redownload error-path placeholder "
        "cleanup must stay intact"
    )


def test_motifops_exports_clear_optimistic_placeholder():
    """The `clearOptimisticPlaceholder` helper exported on
    `motifOps` by v1.15.35 — both the function definition and
    the export. Without the export, the app.js callers fail
    silently (the `if (window.motifOps.clearOptimisticPlaceholder)`
    guard short-circuits)."""
    src = (REPO / "app" / "web" / "static" / "ops.js").read_text()
    # Function definition.
    assert "function clearOptimisticPlaceholder(" in src
    # Exported on the motifOps object.
    export_anchor = src.index("init,")
    export_block = src[export_anchor:export_anchor + 500]
    assert "clearOptimisticPlaceholder," in export_block, (
        "v1.15.40: clearOptimisticPlaceholder must be exported "
        "on motifOps so app.js callers can reach it"
    )


# ── 3. By-design divergence markers ──────────────────────────


# Each entry pins a (marker substring, file_path, rationale) so
# a future audit re-finding the "divergence" sees the marker and
# stops. Removing the marker fails this test.
BY_DESIGN_MARKERS = [
    (
        "v1.15.38: kept ack-aware",
        API_PY,
        "attn_pills=fail intentionally ack-aware (chip count = "
        "needs-action; acking removes a row from chip + filter)",
    ),
    (
        "v1.15.39: mixed-with-broken",
        API_PY,
        "attn_pills=broken mixed-mode handled via post-stat "
        "Python matcher (v1.15.39 — SQL union refactor was "
        "out-of-scope per v1.13.68)",
    ),
    (
        "v1.15.39: attn_pills broken needs post-stat",
        API_PY,
        "needs_post_stat_pagination extended to include attn-broken "
        "trigger (mirrors dl_pills/pl_pills broken handling)",
    ),
]


def test_by_design_divergence_markers_intact():
    """Every documented by-design divergence from v1.15.38/39
    audits has a marker comment at its site. This test pins
    each marker substring so a future refactor that removes
    the comment fails here — prompting the maintainer to
    re-read the audit notes before "fixing" the documented
    intentional divergence."""
    failures = []
    for marker, path, rationale in BY_DESIGN_MARKERS:
        src = path.read_text()
        if marker not in src:
            failures.append(
                f"missing marker {marker!r} in {path.name} — "
                f"rationale: {rationale}"
            )
    assert not failures, (
        "v1.15.40: by-design marker comment removed without "
        "re-reading audit notes: " + "; ".join(failures)
    )


def test_attn_pills_fail_sql_branch_still_ack_aware():
    """Pin the v1.15.38 design decision at the SQL site itself
    (not just the marker comment): the attn_pills=fail SQL
    branch must continue to gate on both `failure_acked_at IS
    NULL` AND `sfa.acked_at IS NULL`. Catches a future
    "consistent with cookies" refactor that broadens the
    filter without re-reading the v1.15.38 design notes."""
    src = API_PY.read_text()
    # Find the FAIL branch (not the marker comment block above).
    marker_anchor = src.index("v1.15.38: kept ack-aware")
    branch_block = src[marker_anchor:marker_anchor + 2000]
    sql_anchor = branch_block.index("attn_branches.append(")
    sql_block = branch_block[sql_anchor:sql_anchor + 500]
    assert "t.failure_kind IS NOT NULL" in sql_block
    assert "t.failure_acked_at IS NULL" in sql_block
    assert "sfa.acked_at IS NULL" in sql_block


def test_link_pills_hl_mismatch_precedence_intact():
    """Pin the v1.15.39 verified design: link_pills=hl/c SQL
    excludes mismatch_state, and the JS linkCell render gives
    mismatch precedence over hardlink/copy via the if/else-if
    order. Both must stay in sync — changing one side without
    the other breaks the consistency."""
    # SQL side: filter excludes mismatch.
    api_src = API_PY.read_text()
    assert "COALESCE(p_e.placement_kind, p_g.placement_kind) = 'hardlink' AND COALESCE(lf_e.mismatch_state, lf_g.mismatch_state) IS NULL" in api_src
    assert "COALESCE(p_e.placement_kind, p_g.placement_kind) = 'copy' AND COALESCE(lf_e.mismatch_state, lf_g.mismatch_state) IS NULL" in api_src
    # JS side: mismatch check comes BEFORE hardlink check.
    js_src = APP_JS.read_text()
    mismatch_idx = js_src.index("if (isMismatch && placed) {")
    hardlink_idx = js_src.index("it.placement_kind === 'hardlink'")
    assert mismatch_idx < hardlink_idx, (
        "v1.15.40: JS linkCell render must check mismatch BEFORE "
        "hardlink (mismatch precedence mirrors the SQL filter's "
        "mismatch_state IS NULL gate)"
    )
