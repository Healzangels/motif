"""v1.18.73 — v1.18.61 audit roadmap rollover.

Closes out the HIGH-severity items v1.18.61's full-application
audit catalogued for v1.18.62 → v1.18.66 but the user's bug
reports drove those tags elsewhere. Re-verified each item in
current code; the real ones get fixed here.

## Items shipped

- **HIGH-B**: PURGE / UNPLACE / DELETE didn't cancel in-flight
  jobs. Pre-fix a place/download/refresh job running against
  (mt, tmdb) could land bytes onto a row being destroyed,
  creating ghost placement rows or NULL-column crashes. Added
  the shared `_cancel_jobs_for_row` helper + invocations in
  api_unplace_item, api_forget_item, api_delete_item.

- **HIGH-C**: `_bulk_lps_run` didn't handle plex_upload
  placements. Pre-fix `Path(media_folder='') / 'theme.mp3'`
  silently no-op'd for those rows — placements row got deleted
  but motif's API-uploaded theme stayed serving in Plex. Now
  invokes the v1.18.60 shared helper for plex_upload
  placements before the sidecar unlink loop.

- **HIGH-D**: bulk-probe-tdb body parse failure silently fell
  back to "all rows" scope. A malformed UI request would
  expand a selected-items probe to a full-library probe with
  no breadcrumb. Now `log.warning` surfaces the fall-through.

- **HIGH-E**: bulk-lps body parse same shape. Downstream 400
  already catches the empty-targets case, but the confusing
  "items must be non-empty" error masked the true parse-failure
  cause. Now `log.warning` surfaces the parse failure too.

- **HIGH-F**: `_tab_availability_for_nav` silently returned
  all-False on any DB error. Operator would see "no nav tabs"
  with no idea why. Now `log.warning` surfaces the query
  failure; the all-False fallback preserves page rendering.

## Items not changed

- **HIGH-G** (scanner.py:431 `except OSError: pass` on
  `local_path.stat().st_ino`) — re-verified as genuinely
  benign. Failure to read st_ino just causes the code to fall
  through to `return "hash_match"` instead of `"exact_match"`,
  which means "adoptable" instead of "already-hardlinked".
  Worst case: an unneeded re-hardlink. No data loss, no state
  drift. Left as-is; the audit report catalogues why.

- **HIGH-A** (worker payload-parse cluster) — re-classified as
  LOW in v1.18.72's audit. The catches handle the documented
  "payload absent → use defaults" path. Inline comments would
  aid readability but no behavior change is warranted.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def _api_py() -> str:
    return (REPO / "app" / "web" / "api.py").read_text()


# ── HIGH-B: cancel-jobs-for-row helper + invocations ────────


def test_cancel_jobs_for_row_helper_exists():
    """The shared cancellation helper must exist at module
    level so all three destructive endpoints share the same
    cancellation semantics."""
    src = _api_py()
    assert "def _cancel_jobs_for_row(" in src, (
        "v1.18.73 (HIGH-B): shared helper required"
    )


def test_cancel_jobs_for_row_covers_every_per_row_kind():
    """The helper must cancel every per-row job kind. Missing
    one would leave that handler still able to write into a
    destroyed row. Pin the canonical list so a future kind
    addition is forced through the helper."""
    src = _api_py()
    fn_idx = src.index("def _cancel_jobs_for_row(")
    fn_end = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "PER_ROW_JOB_KINDS = (" in body
    # The full set of per-row workers.
    for kind in ("place", "download", "refresh",
                 "relink", "adopt", "scan"):
        assert f'"{kind}"' in body, (
            f"v1.18.73 (HIGH-B): job kind {kind!r} missing "
            "from cancellation set"
        )


def test_cancel_jobs_for_row_guards_on_running_or_pending():
    """The UPDATE must filter `status IN ('pending', 'running')`
    so a previously-cancelled or already-done job isn't
    re-touched. The worker's status='running' WHERE guard
    handles the race (per v1.14.25 H1)."""
    src = _api_py()
    fn_idx = src.index("def _cancel_jobs_for_row(")
    fn_end = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "status IN ('pending', 'running')" in body


def test_cancel_jobs_for_row_supports_section_id_or_global():
    """When section_id is passed, only that section's jobs are
    cancelled (multi-section title isolation). When None, all
    sections for the (mt, tmdb) are cancelled (full destruction
    case)."""
    src = _api_py()
    fn_idx = src.index("def _cancel_jobs_for_row(")
    fn_end = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "if section_id:" in body
    assert "AND section_id = ?" in body
    # And the else branch (None) omits the section filter.
    assert body.count("WHERE job_type IN") == 2


def test_unplace_item_calls_cancel_jobs_for_row():
    """api_unplace_item must invoke the cancel helper inside
    its txn block, BEFORE the DELETE FROM placements sweep."""
    src = _api_py()
    fn_idx = src.index("async def api_unplace_item(")
    fn_end = src.index("@app.post", fn_idx + 1)
    body = src[fn_idx:fn_end]
    cancel_idx = body.index("_cancel_jobs_for_row(")
    delete_idx = body.index("DELETE FROM placements")
    assert cancel_idx < delete_idx, (
        "v1.18.73 (HIGH-B): cancel must precede the placements "
        "DELETE — pre-fix a race let a running place job land "
        "bytes onto the row mid-delete"
    )


def test_forget_item_calls_cancel_jobs_for_row():
    """api_forget_item must invoke the cancel helper inside
    its txn block, BEFORE the DELETE FROM placements sweep."""
    src = _api_py()
    fn_idx = src.index("async def api_forget_item(")
    fn_end = src.index("@app.post", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "_cancel_jobs_for_row(" in body
    cancel_idx = body.index("_cancel_jobs_for_row(")
    delete_idx = body.index("DELETE FROM placements")
    assert cancel_idx < delete_idx


def test_delete_item_calls_cancel_jobs_for_row():
    """api_delete_item must invoke the cancel helper BEFORE
    the FK-cascading DELETE FROM themes. Same rationale as
    the other two — a place job mid-transaction would see
    partial state."""
    src = _api_py()
    fn_idx = src.index("async def api_delete_item(")
    fn_end = src.index("@app.post", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "_cancel_jobs_for_row(" in body
    cancel_idx = body.index("_cancel_jobs_for_row(")
    drop_idx = body.index("DELETE FROM themes")
    assert cancel_idx < drop_idx


# ── HIGH-C: bulk_lps plex_upload teardown ───────────────────


def test_bulk_lps_select_includes_placement_kind():
    """`_bulk_lps_run` SELECT must include placement_kind so
    the loop can route plex_upload placements to the API
    teardown helper instead of the sidecar unlink. Pre-fix
    SELECT only read media_folder."""
    src = _api_py()
    fn_idx = src.index("def _bulk_lps_run(")
    # Anchor on the placements query inside the loop.
    select_idx = src.index(
        "SELECT media_folder, placement_kind", fn_idx)
    assert select_idx > 0, (
        "v1.18.73 (HIGH-C): bulk_lps SELECT must include "
        "placement_kind so plex_upload rows are detectable"
    )


def test_bulk_lps_calls_teardown_helper_for_plex_upload():
    """For plex_upload placements, the bulk_lps loop must
    invoke `_teardown_plex_api_artifacts_for_placements` so
    motif's Plex API entry is cleared. Pre-fix the row's API
    upload stayed serving (Plex side) while motif's DB
    tracking showed the placement deleted."""
    src = _api_py()
    fn_idx = src.index("def _bulk_lps_run(")
    fn_end = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "_teardown_plex_api_artifacts_for_placements(" in body, (
        "v1.18.73 (HIGH-C): bulk_lps must call the API "
        "teardown helper for plex_upload placements"
    )
    # And the filter that targets only plex_upload rows.
    assert "plex_upload_placements" in body
    assert '"plex_upload"' in body


def test_bulk_lps_sidecar_loop_skips_plex_upload():
    """The sidecar unlink loop must explicitly `continue` on
    plex_upload placements rather than rely on the
    `Path('') / 'theme.mp3'` silent no-op. Explicit skip
    makes the intent visible + ensures the row counts as
    'unlinked_any' so the downstream placements DELETE
    fires."""
    src = _api_py()
    fn_idx = src.index("def _bulk_lps_run(")
    fn_end = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:fn_end]
    # The sidecar loop has an explicit plex_upload skip.
    skip_idx = body.index(
        'if (pr["placement_kind"] or "") == "plex_upload":')
    # And the unlinked_any = True + continue follow.
    after = body[skip_idx:skip_idx + 200]
    assert "unlinked_any = True" in after
    assert "continue" in after


# ── HIGH-D + HIGH-E: bulk body parse breadcrumbs ────────────


def test_bulk_probe_tdb_body_parse_logs_warning():
    """bulk-probe-tdb body parse failure must log a warning
    so the operator sees the scope-default fall-through
    (selected items → all rows). Pre-fix the silent body={}
    expanded the scope without trace."""
    src = _api_py()
    # The bulk-probe-tdb endpoint's body-parse block.
    fn_idx = src.index("BULK PROBE TDB started by")
    parse_idx = src.rindex("body = await request.json()", 0, fn_idx)
    block = src[parse_idx:fn_idx]
    assert "log.warning(" in block
    assert "BULK PROBE TDB: body parse failed" in block
    # The exception is captured.
    assert "except Exception as e:" in block


def test_bulk_lps_body_parse_logs_warning():
    """bulk-lps body parse failure must log a warning even
    though the downstream 400 catches the empty-targets case
    — operator gets a clearer signal than 'items must be
    non-empty' alone."""
    src = _api_py()
    fn_idx = src.index("BULK LPS started by")
    parse_idx = src.rindex("body = await request.json()", 0, fn_idx)
    block = src[parse_idx:fn_idx]
    assert "log.warning(" in block
    assert "BULK LPS: body parse failed" in block


# ── HIGH-F: tab availability query failure surfaces ─────────


def test_tab_availability_logs_on_query_failure():
    """`_tab_availability_for_nav` must log a warning before
    returning the all-False fallback. Pre-fix DB errors,
    schema mismatches, plex_sections query failures all
    silently returned {movies: False, tv: False, anime: False}
    — operator saw nav tabs disappear with no log signal."""
    src = _api_py()
    fn_idx = src.index("def _tab_availability_for_nav()")
    fn_end = src.index("templates.env.globals[", fn_idx)
    body = src[fn_idx:fn_end]
    assert "log.warning(" in body
    assert "_tab_availability_for_nav" in body
    # Exception captured for the log message.
    assert "except Exception as e:" in body
    # All-False fallback preserved for page-rendering safety.
    assert '"movies": False' in body


# ── Markers ─────────────────────────────────────────────────


def test_v1_18_73_markers_reference_audit_section():
    """Every fix references the v1.18.61 audit's HIGH-letter
    so future code-archaeologists can trace fixes back to the
    catalog entry."""
    src = _api_py()
    # HIGH-B in three sites.
    assert "v1.18.73 (HIGH-B audit)" in src
    assert src.count("v1.18.73 (HIGH-B audit)") >= 3, (
        "v1.18.73: HIGH-B marker required in all three "
        "destructive endpoints"
    )
    # HIGH-C, D, E, F each at least once.
    for letter in ("HIGH-C", "HIGH-D", "HIGH-E", "HIGH-F"):
        assert f"v1.18.73 ({letter} audit)" in src, (
            f"v1.18.73: {letter} marker required"
        )
