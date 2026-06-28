"""v1.15.35 — silent-failure audit: MED-severity batch.

Continues the v1.15.34 sweep with the lower-severity findings
deferred from that tag. Mostly logging-upgrade fixes (bare
`except: pass` → `except Exception as e: log.warning(...)`)
plus a few small behavioral changes (events.py audit-log
retry, extra optimistic-placeholder clear on failure).

## Findings batch (deferred from v1.15.34 audit)

1. **events.py:94** — audit-log batches dropped silently on
   `database is locked`. Fix: bounded retry (3 attempts, linear
   backoff) on the lock string; log.error on persistent
   failure.
2. **events.py:150** — queue-overflow logged at DEBUG. Fix:
   bumped to WARNING.
3. **worker.py:360** — clear_download_progress bare pass. Fix:
   log.debug on failure.
4. **worker.py:1919** — relink temp file unlink bare pass. Fix:
   log.debug on cleanup failure.
5. **sync.py:1504** — git mirror _close() bare pass. Fix:
   log.warning on failure.
6. **sync.py:1742** — git fetch progress-callback parse hiccup
   bare pass. Fix: log.debug with phase context.
7. **sync.py:2065** — read_json returning None silently
   incremented stats.errors. Fix: log.info with the failed path.
8. **downloader.py:504** — yt-dlp progress hook callback bare
   pass. Fix: log.warning.
9. **downloader.py:534** — post-download progress(1.0) callback
   bare pass. Fix: log.debug.
10. **api.py _library_resolution_state** — bare except. Fix:
    log.warning.
11. **api.py _pipeline_in_flight** — bare except. Fix:
    log.warning.
12. **api.py /api/tmdb/test** — JSON coercion to {}. Fix: raise
    HTTPException(400).
13. **api.py /api/libraries/refresh** — JSON coercion to {}.
    Fix: raise HTTPException(400).
14. **plex.py get_item_paths_bulk** — silent {} on batch
    failure. Fix: include first/last rks in the warning so the
    operator can correlate "missing folder_path" reports.
15. **sections.py reassign_themes_subdir + list_sections** —
    silent location_paths JSON parse. Fix: log.warning with
    section_id.
16. **app.js page-load /api/stats probe** — empty .catch. Fix:
    console.error so dev-tools surfaces the failure.
17. **app.js download-backup error path** — placeholder
    lingered after failure. Fix: clear placeholder explicitly
    via new clearOptimisticPlaceholder export.
18. **ops.js postCancel** — silent catch. Fix: console.error
    on the underlying network/auth failure.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
EVENTS_PY = REPO / "app" / "core" / "events.py"
WORKER_PY = REPO / "app" / "core" / "worker.py"
SYNC_PY = REPO / "app" / "core" / "sync.py"
DOWNLOADER_PY = REPO / "app" / "core" / "downloader.py"
API_PY = REPO / "app" / "web" / "api.py"
PLEX_PY = REPO / "app" / "core" / "plex.py"
SECTIONS_PY = REPO / "app" / "core" / "sections.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
OPS_JS = REPO / "app" / "web" / "static" / "ops.js"


# ── 1+2. events.py audit-log retry + queue-overflow log level ─


def test_event_flusher_retries_on_database_locked():
    """The flusher must retry the batch insert on `database is
    locked` rather than dropping silently. Pre-fix a single
    lock-contention spike during a busy sync wiped the whole
    batch with only a WARNING — audit-log gaps invisible
    without log review. Now: 3 attempts with linear backoff."""
    src = EVENTS_PY.read_text()
    fn_anchor = src.index("def _flusher_loop(")
    fn_end = src.index("\ndef ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    # v1.15.35 retry loop.
    assert "v1.15.35: bounded retry" in fn_body
    assert "for attempt in range(3):" in fn_body
    # Lock-string match (motif's canonical retry pattern from
    # CLAUDE.md class 8 — only retry the lock string, not all
    # OperationalErrors).
    assert '"database is locked" in str(e)' in fn_body
    # Backoff.
    assert "threading.Event().wait(0.5 * (attempt + 1))" in fn_body
    # Persistent failure logged at ERROR (audit-log drop is
    # serious).
    assert "log.error(" in fn_body
    assert "DROPPING batch" in fn_body


def test_event_queue_overflow_logged_at_warning():
    """Pre-fix `log.debug("event queue full ...")` meant
    audit-log losses under peak load were invisible without
    `LOG_LEVEL=DEBUG`. v1.15.35 bumps to WARNING."""
    src = EVENTS_PY.read_text()
    # Locate the queue.Full handler.
    handler_anchor = src.index("except queue.Full:")
    handler_block = src[handler_anchor:handler_anchor + 800]
    assert "log.warning(" in handler_block
    assert "log.debug(" not in handler_block
    assert "event queue full" in handler_block


# ── 3-9. cleanup-path bare passes get logging ────────────────


def test_worker_clear_download_progress_failure_logged():
    """Worker loop's clear_download_progress cleanup must log
    failures at DEBUG (was bare pass)."""
    src = WORKER_PY.read_text()
    anchor = src.index("clear_download_progress(job[\"id\"])")
    block = src[anchor:anchor + 800]
    assert "log.debug(" in block
    assert "clear_download_progress" in block


def test_worker_relink_tmp_unlink_failure_logged():
    """Relink temp file cleanup must log unlink failures at
    DEBUG (was bare pass) so stale .relink-tmp files have a
    correlateable trail."""
    src = WORKER_PY.read_text()
    anchor = src.index("v1.15.35: log temp-file cleanup failures")
    block = src[anchor:anchor + 800]
    assert "log.debug(" in block
    assert "tmp file cleanup failed" in block


def test_sync_git_mirror_close_failure_logged():
    """git mirror _close() must log close failures at WARNING
    (was bare pass) so corrupted-HEAD / permission errors
    surface."""
    src = SYNC_PY.read_text()
    anchor = src.index("def _close(self) -> None:")
    fn_end = src.index("\n    # ", anchor)
    fn_body = src[anchor:fn_end]
    assert "log.warning(" in fn_body
    assert "git mirror close failed" in fn_body


def test_sync_git_fetch_progress_parse_logged():
    """git fetch progress-callback parse hiccups must log at
    DEBUG with phase context (was bare pass) so the operator
    can diagnose "frozen progress" reports."""
    src = SYNC_PY.read_text()
    anchor = src.index("v1.15.35: log at DEBUG with phase context")
    block = src[anchor:anchor + 800]
    assert "log.debug(" in block
    assert "phase=" in block


def test_sync_read_json_failure_logged_with_path():
    """sync git: read_json returning None must log the failed
    path so the operator can tell stats.errors aggregate from
    a single rotted file repeating."""
    src = SYNC_PY.read_text()
    anchor = src.index('log.info(\n                "sync git: read_json')
    block = src[anchor:anchor + 400]
    assert "rel_path" in block


def test_downloader_progress_hook_failure_logged():
    """yt-dlp progress hook callback must log at WARNING (was
    bare pass) so stuck-bar reports correlate with the actual
    cause."""
    src = DOWNLOADER_PY.read_text()
    anchor = src.index("v1.15.35: log progress-callback failures")
    block = src[anchor:anchor + 800]
    assert "log.warning(" in block
    assert "yt-dlp progress hook callback failed" in block


def test_downloader_post_download_progress_failure_logged():
    """Post-download progress(1.0) callback must log at DEBUG
    (was bare pass). Lower severity than the in-loop hook
    since the file IS on disk by this point."""
    src = DOWNLOADER_PY.read_text()
    anchor = src.index(
        "v1.15.35: log post-download finalization callback"
    )
    block = src[anchor:anchor + 800]
    assert "log.debug(" in block


# ── 10-11. api.py template helpers ────────────────────────────


def test_library_resolution_state_logs_db_errors():
    """Template helper bare except must log DB hiccups at
    WARNING — pre-fix the 4K/Standard chips silently
    disappeared on every page-load with zero operator
    visibility."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _library_resolution_state(")
    fn_end = src.index(
        'templates.env.globals["library_resolution_state"]', fn_anchor)
    fn_body = src[fn_anchor:fn_end]
    assert "log.warning(" in fn_body
    assert "library_resolution_state" in fn_body
    assert "except Exception as e:" in fn_body


def test_pipeline_in_flight_logs_db_errors():
    """Template helper bare except must log DB hiccups at
    WARNING — pre-fix returned False silently could let the
    user queue duplicate refresh jobs."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _pipeline_in_flight(")
    fn_end = src.index("\n    # ", fn_anchor + 100)
    fn_body = src[fn_anchor:fn_end]
    assert "log.warning(" in fn_body
    assert "_pipeline_in_flight failed" in fn_body
    assert "except Exception as e:" in fn_body


# ── 12-13. api.py JSON coercion routes ───────────────────────


def test_tmdb_test_raises_on_invalid_json():
    """/api/tmdb/test must raise HTTPException(400) on
    malformed JSON — pre-fix coerced to {} and returned the
    generic "no API key configured" message even when the
    actual error was bad JSON."""
    src = API_PY.read_text()
    # Anchor on the v1.15.35 marker inside the route.
    anchor = src.index(
        "v1.15.35: raise on JSON parse failure rather than coercing\n"
        "        # to {}. Pre-fix the silent coerce let the caller see"
    )
    block = src[anchor:anchor + 800]
    assert "raise HTTPException(" in block
    assert "status_code=400" in block
    assert "invalid JSON body" in block


def test_libraries_refresh_raises_on_invalid_json():
    """/api/libraries/refresh must raise HTTPException(400)
    on malformed JSON — pre-fix silently fell through to the
    legacy global-refresh branch when the user thought they
    were narrowing by tab/fourk."""
    src = API_PY.read_text()
    anchor = src.index(
        "v1.15.35: raise on JSON parse failure rather than coercing\n"
        "        # to {}. Pre-fix a malformed body silently fell through to\n"
        "        # the legacy global-refresh branch"
    )
    block = src[anchor:anchor + 800]
    assert "raise HTTPException(" in block
    assert "status_code=400" in block


# ── 14. plex.py get_item_paths_bulk batch context ────────────


def test_get_item_paths_bulk_logs_batch_rks_on_failure():
    """The HTTP and JSON-parse failure logs must include the
    first + last rks of the failed batch so the operator can
    correlate "this row's folder_path is empty" reports with
    a specific batch failure."""
    src = PLEX_PY.read_text()
    fn_start = src.index("def _fetch_batch(batch:")
    fn_end = src.index("\n        ", fn_start + 200)
    # Pull a generous window — the function spans several
    # log.warning calls + the result loop.
    fn_body = src[fn_start:fn_start + 3000]
    assert "v1.15.35: include the first/last batch rks" in fn_body
    assert "batch[0], batch[-1]" in fn_body
    # Both the HTTP-failure and JSON-parse warnings should
    # carry the rks.
    assert fn_body.count("batch[0], batch[-1]") >= 2


# ── 15. sections.py location_paths JSON parse ─────────────────


def test_reassign_themes_subdir_logs_corrupt_location_paths():
    """reassign_themes_subdir must log at WARNING when the
    section's stored location_paths JSON can't be parsed —
    pre-fix the silent-empty fallback risked themes_subdir
    collisions."""
    src = SECTIONS_PY.read_text()
    fn_start = src.index("def reassign_themes_subdir(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "log.warning(" in fn_body
    assert "corrupt" in fn_body.lower()
    assert "location_paths" in fn_body


def test_list_sections_logs_corrupt_location_paths():
    """list_sections (read-only path) must also log corrupt
    location_paths so the operator is aware of data integrity
    issues even though placement isn't directly affected."""
    src = SECTIONS_PY.read_text()
    fn_start = src.index("def list_sections(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "log.warning(" in fn_body
    assert "list_sections" in fn_body
    assert "location_paths" in fn_body


# ── 16. app.js page-load stats probe ──────────────────────────


def test_page_load_stats_probe_logs_on_failure():
    """The page-load `/api/stats` probe's `.catch(()=>{})` was
    a silent swallow — if the first stats call failed (DB
    mid-migration, server start race), the sync watcher never
    armed and the operator had no UI signal. v1.15.35
    console.error so dev-tools surfaces the failure."""
    src = APP_JS.read_text()
    anchor = src.index(
        "v1.15.35: log the page-load stats probe failure"
    )
    block = src[anchor:anchor + 1000]
    assert "console.error(" in block
    assert "/api/stats" in block


# ── 17. app.js download-backup placeholder cleanup ────────────


def test_download_backup_clears_optimistic_placeholder_on_failure():
    """download-tdb-backup error path must explicitly clear
    the optimistic placeholder so the drawer doesn't say
    '// QUEUING DOWNLOAD' for 5s after the action failed."""
    src = APP_JS.read_text()
    anchor = src.index(
        "v1.15.35: clear the optimistic placeholder on"
    )
    block = src[anchor:anchor + 1000]
    assert "clearOptimisticPlaceholder" in block
    assert "'download_queue'" in block


def test_ops_js_exports_clear_optimistic_placeholder():
    """The v1.15.35 clearOptimisticPlaceholder helper must be
    exported on motifOps so app.js (and other callers) can use
    it. Mirrors the v1.13.19 setOptimisticPlaceholder export."""
    src = OPS_JS.read_text()
    # Function defined.
    assert "function clearOptimisticPlaceholder(" in src
    # Exported on the motifOps object.
    assert "clearOptimisticPlaceholder," in src


# ── 18. ops.js postCancel logged ──────────────────────────────


def test_ops_post_cancel_logs_failure():
    """postCancel's silent catch must log at console.error so
    the user / dev-tools knows when the cancel button click
    didn't actually cancel."""
    src = OPS_JS.read_text()
    fn_anchor = src.index("async function postCancel(")
    fn_end = src.index("\n  function ", fn_anchor + 1)
    if fn_end == -1:
        fn_end = src.index("\n  async function ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    assert "console.error(" in fn_body
    assert "postCancel failed" in fn_body
