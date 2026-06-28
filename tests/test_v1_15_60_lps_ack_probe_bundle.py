"""v1.15.60 — four fixes bundled per the user's feedback round:

1. LPS button visibility safety net (re-add +P-filter fallback)
2. ACK button green tone (drop btn-warn)
3. ACK endpoint that acks BOTH job + theme (was theme-only,
   left job row red — "clicking htem actually doesn't do
   anything")
4. New // PROBE TDB SELECTED bulk action (btn-info color,
   uses /api/admin/bulk-probe-tdb existing scope_items support)

## the user's prompts (verbatim)

* "can we make this ack button green"
* "clicking htem actually doesn't do anything currently"
* "attempted a bulk let plex server but it didn't do anything."
  (with screenshot showing 7 U+P composites selected but no
  LPS button in the bulk bar)
* "also can we add a bulk action on selected that are in tdb
  to bulk probe the selected, make sure it follows the style
  of other bulk buttons and uses color of info probe button"

## Fixes

### Fix 1: LPS visibility safety net

v1.15.49 narrowed LPS visibility from "+P SRC filter active"
to `lpsOnlyCount > 0`. For the user's U+P selection, lpsOnlyCount
should compute > 0 — but the button was missing. v1.15.60 adds
back the +P-filter-active path as a safety net (`lpsOnlyCount
> 0 || onPlusPFilter`). The click handler's M-sidecar skip
block (v1.15.49) still enforces the no-footgun contract, so
restoring the filter gate doesn't reintroduce LPS-on-M.

### Fix 2 + 3: ACK button green tone + working clicks

v1.15.54 added the per-job ACK button using `btn-warn`
(amber). the user wanted green. Drop the tone class — default
`.btn` is the theme's green.

The click "doing nothing" traced to a different bug: the
handler POSTed `/api/items/{mt}/{id}/clear-failure` which only
acks the THEME (`themes.failure_acked_at`). The JOB row's
green "failed (acknowledged)" tone is gated on
`jobs.acked_at` (v1.12.12) — the handler never touched it, so
the row stayed red and the button stayed visible. From
the user's POV: nothing happened.

Fix: new `/api/jobs/{job_id}/ack-failure` endpoint that acks
BOTH:
  * `jobs.acked_at` (flips /queue row green)
  * `themes.failure_acked_at` (drops topbar FAIL pill)

Atomic in one transaction. Sync/enum jobs without a
media_type/tmdb_id binding skip the theme ack (no theme to
ack).

### Fix 4: bulk PROBE TDB SELECTED

New btn-info (cyan) button in the bulk bar. Reuses the
existing `/api/admin/bulk-probe-tdb` endpoint's `scope_items`
support (v1.15.1) so the probe runs scoped to the selection
instead of the whole library. Visibility gates on
`probeTdbCount > 0` (effectiveCount of themed rows).

Static-text guards consistent with v1.15.46/49/59 bulk-button
test patterns.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
LIB_HTML = REPO / "app" / "web" / "templates" / "library.html"


# ── Fix 1: LPS visibility safety net ─────────────────────────


def test_lps_visibility_no_longer_uses_plus_p_safety_net():
    """v1.15.60 added a +P-SRC-filter safety net to keep the LPS
    button visible when lpsOnlyCount under-counted due to data-
    attribute drift OR (the real cause) visible-page-only walking.

    v1.16.10 fixed the actual undercount via the selectedRows
    cache (lpsOnlyCount is now selection-wide), and dropped the
    safety net along with its bare-label-no-count branch —
    the user's complaint "let plex server isn't displaying a number
    at all anytime" was that branch. The LPS visibility gate is
    back to a pure `lpsOnlyCount > 0` check with a count badge
    on every visible label."""
    js = APP_JS.read_text()
    # v1.16.10: marker header trimmed back to v1.14.27 / v1.15.49.
    marker = "v1.14.27 / v1.15.49: bulk LET PLEX SERVE"
    anchor = js.index(marker)
    # Narrow to just the LPS block (ends at the probe-tdb marker).
    block = js[anchor:js.index("// v1.15.60: bulk PROBE TDB SELECTED", anchor)]
    assert "onPlusPFilter" not in block, (
        "v1.16.10: the v1.15.60 onPlusPFilter safety net must be "
        "removed — the selectedRows cache makes the lpsOnlyCount "
        "undercount it guarded against impossible"
    )
    assert "letPlexServeBtn.style.display = lpsOnlyCount > 0 ? '' : 'none';" in block, (
        "v1.16.10: LPS visibility must reduce to a simple "
        "lpsOnlyCount > 0 ternary"
    )


def test_lps_handler_still_filters_m_sidecars():
    """Regression guard: even though v1.15.60 made the button
    more permissive (filter-based fallback), the click handler
    still skips M sidecars (the v1.15.49 footgun guard). The
    button can be reachable for the safe cases without
    reintroducing the LPS-on-M-deletes-only-theme bug."""
    js = APP_JS.read_text()
    handler_anchor = js.index("library-let-plex-serve-btn')?.addEventListener")
    body = js[handler_anchor:handler_anchor + 3000]
    assert "v1.15.49: exclude M sidecars" in body, (
        "v1.15.60: regression — v1.15.49's M-sidecar skip must "
        "stay in the handler even with the relaxed visibility gate"
    )


# ── Fix 2: ACK button green tone ─────────────────────────────


def test_ack_button_uses_default_green_tone():
    """The ACK button must use the default `.btn` class (theme
    green), NOT btn-warn (amber). the user: 'can we make this
    ack button green'."""
    js = APP_JS.read_text()
    ack_btn_idx = js.index('data-act="ack-job-failure"')
    # Walk back to the surrounding button tag.
    btn_open = js.rfind('<button', 0, ack_btn_idx)
    btn_close = js.index('>', ack_btn_idx)
    btn_tag = js[btn_open:btn_close + 1]
    assert 'btn-warn' not in btn_tag, (
        "v1.15.60: ACK button must NOT use btn-warn (amber) — "
        "the user wants green tone (default .btn class)"
    )
    assert 'class="btn btn-tiny"' in btn_tag, (
        "v1.15.60: ACK button must use the bare 'btn btn-tiny' "
        "class for default green theme tone"
    )


# ── Fix 3: ACK endpoint acks BOTH job + theme ───────────────


def test_ack_job_failure_endpoint_exists():
    """The new /api/jobs/{id}/ack-failure endpoint must exist —
    it's the single-call endpoint that flips both jobs.acked_at
    (row tone) AND themes.failure_acked_at (topbar count)."""
    src = API_PY.read_text()
    assert '@app.post("/api/jobs/{job_id}/ack-failure")' in src, (
        "v1.15.60: /api/jobs/{job_id}/ack-failure route required — "
        "v1.15.54's theme-only ACK left jobs.acked_at NULL"
    )
    assert "async def api_ack_job_failure(" in src


def test_ack_endpoint_updates_both_jobs_and_themes():
    """The endpoint MUST update both surfaces:
    * jobs.acked_at — drives /queue row green tone (v1.12.12)
    * themes.failure_acked_at — drives topbar FAIL pill count
    Without both updates the user sees 'nothing happens' from
    one angle or another."""
    src = API_PY.read_text()
    fn_anchor = src.index("async def api_ack_job_failure(")
    fn_end = src.index("\n    @app.post(", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    assert "UPDATE jobs SET acked_at = ? WHERE id = ?" in fn_body, (
        "v1.15.60: ACK endpoint must stamp jobs.acked_at — "
        "the /queue row tone gate (v1.12.12)"
    )
    assert "UPDATE themes SET failure_acked_at = ?" in fn_body, (
        "v1.15.60: ACK endpoint must stamp themes.failure_acked_at "
        "— the topbar FAIL pill count gate"
    )


def test_ack_handler_uses_new_endpoint():
    """The JS click handler must POST to the new endpoint, not
    the legacy /clear-failure path."""
    js = APP_JS.read_text()
    # Find the handler's API call.
    handler_anchor = js.index("data-act=\"ack-job-failure\"]')")
    body = js[handler_anchor:handler_anchor + 2000]
    assert "/api/jobs/${encodeURIComponent(jobId)}/ack-failure" in body, (
        "v1.15.60: ACK handler must POST to /api/jobs/{id}/ack-failure "
        "(NEW v1.15.60 endpoint) — pre-fix it POSTed /clear-failure "
        "which only acked the theme, leaving the job row red"
    )
    # Legacy /clear-failure path must be GONE from this handler.
    assert "/clear-failure" not in body, (
        "v1.15.60: legacy /clear-failure call must be removed from "
        "the ACK handler — it left jobs.acked_at unset"
    )


# ── Fix 4: bulk PROBE TDB SELECTED button ────────────────────


def test_bulk_probe_tdb_button_exists_in_template():
    """New bulk button rendered in library.html with btn-info
    tone (matches the per-row INFO PROBE button color family)
    + display:none default + canonical label."""
    html = LIB_HTML.read_text()
    assert 'id="library-bulk-probe-tdb-btn"' in html, (
        "v1.15.60: bulk PROBE TDB button required in template"
    )
    btn_anchor = html.index('id="library-bulk-probe-tdb-btn"')
    btn_open = html.rfind('<button', 0, btn_anchor)
    btn_close = html.index('>', btn_anchor)
    btn_tag = html[btn_open:btn_close + 1]
    assert 'btn-info' in btn_tag, (
        "v1.15.60: bulk PROBE TDB button must use btn-info "
        "(cyan) — matches the per-row INFO PROBE color"
    )
    assert 'style="display:none"' in btn_tag, (
        "v1.15.60: default hidden; updateLibrarySelectionUi reveals"
    )
    # Label between > and </button>.
    text_start = html.index('>', btn_anchor) + 1
    text_end = html.index('</button>', text_start)
    label = html[text_start:text_end].strip()
    assert label == "// PROBE TDB SELECTED", (
        f"v1.15.60: bulk PROBE TDB label must be '// PROBE TDB "
        f"SELECTED', got {label!r}"
    )


def test_bulk_probe_tdb_visibility_wired():
    """Counter + visibility gate + count-badge label, mirroring
    every other bulk button's convention (v1.15.59 sweep)."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function updateLibrarySelectionUi()")
    # v1.19.38: widened from 30000 → 40000 chars. The function grew
    # past the original window via the v1.19.38 comment block
    # documenting the SRC-axis sixth-site widening at the
    # pushableCount predicate. The withCount line at ~line 10109
    # sat just past 30000 chars after v1.19.38; the larger window
    # gives headroom.
    fn_body = js[fn_anchor:fn_anchor + 40000]
    assert "const probeTdbCount = effectiveCount(themedPred);" in fn_body, (
        "v1.15.60: probeTdbCount counter must be declared via "
        "effectiveCount with the themedPred predicate"
    )
    assert "library-bulk-probe-tdb-btn" in fn_body
    assert "withCount('// PROBE TDB SELECTED', probeTdbCount)" in fn_body, (
        "v1.15.60: bulk PROBE button must use the withCount() "
        "helper for the (N) badge (v1.15.59 convention)"
    )


def test_bulk_probe_tdb_handler_posts_to_existing_endpoint():
    """Handler POSTs `{items: [{media_type, tmdb_id}, ...]}` to
    /api/admin/bulk-probe-tdb — reuses the v1.15.1 scope_items
    support (no new endpoint needed)."""
    js = APP_JS.read_text()
    handler_anchor = js.index(
        "library-bulk-probe-tdb-btn')?.addEventListener"
    )
    body = js[handler_anchor:handler_anchor + 3000]
    assert "/api/admin/bulk-probe-tdb" in body, (
        "v1.15.60: handler must POST to /api/admin/bulk-probe-tdb "
        "(existing endpoint, scope_items support v1.15.1)"
    )
    assert "{ items: targets }" in body or "{items: targets}" in body, (
        "v1.15.60: POST body must wrap items in {items: [...]}"
    )
