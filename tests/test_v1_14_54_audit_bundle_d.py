"""v1.14.54 — audit Bundle D: silent-failure / log-hygiene sweep.

From the v1.14.50 holistic audit. Eight findings across api.py +
scheduler.py + worker.py + events.py + plex_enum.py — all
silent-failure / log-hygiene shape:

  • H3: api_probe_tdb runs sync probe + DB writes on the event
    loop. Dispatched via run_in_threadpool.
  • H6: section_refresh cron collides with daily_sync when
    sync_cron hour is non-numeric. Now skipped with a warning.
  • M6: api_release_latest swallows json.loads errors with bare
    pass. Now log.warning.
  • M7: api_jobs accepts garbage status, returns empty silently.
    Now pattern-validated → 400 on invalid status.
  • M8: low-disk guard burns retry attempts. New _JobTransient
    exception class + _mark_transient dispatch path that
    re-queues without consuming attempts.
  • M12: event-flusher thread dies silently on non-sqlite
    exception. Broad catch around loop body.
  • M13: _scrub doesn't recurse into lists; missing
    api_key/bearer/key substrings.
  • M14: _verify_theme_claims swallows broad Exception at
    log.debug. Bumped to log.warning.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── H3: api_probe_tdb dispatched via run_in_threadpool ───────


def test_api_probe_tdb_uses_run_in_threadpool():
    """The async api_probe_tdb handler must dispatch the sync
    probe + DB write block via run_in_threadpool so it doesn't
    stall the event loop for ~0.5-2s per click."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_probe_tdb(")
    body = src[fn_anchor:fn_anchor + 12000]
    # The sync helper exists and is invoked via run_in_threadpool.
    assert "def _probe_sync():" in body
    assert "await run_in_threadpool(_probe_sync)" in body
    # v1.14.54 marker.
    assert "v1.14.54:" in body


def test_api_probe_tdb_propagates_404_and_409_through_threadpool():
    """The sync helper must signal HTTPException-equivalent states
    via a `_status` key (since FastAPI can't raise HTTPException
    cleanly from the threadpool task). The async wrapper re-raises
    as a real HTTPException."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_probe_tdb(")
    body = src[fn_anchor:fn_anchor + 12000]
    assert '"_status": 404' in body
    assert '"_status": 409' in body
    assert 'if "_status" in out:' in body
    assert 'raise HTTPException(status_code=out["_status"], detail=out["detail"])' in body


# ── H6: section_refresh skipped on non-numeric sync hour ─────


def test_section_refresh_skipped_when_sync_hour_not_numeric():
    """The scheduler must gate the section_refresh cron registration
    on `hour.isdigit()`. Pre-fix the ternary parser bug meant a
    non-numeric hour (`*`, `*/N`, `1,13`) silently registered
    section_refresh at the SAME wall-clock as daily_sync."""
    src = (REPO / "app" / "core" / "scheduler.py").read_text()
    # The new gate.
    assert "if hour.isdigit():" in src
    # Warning log on the non-numeric branch.
    assert "section_refresh cron skipped" in src
    assert "BEGIN IMMEDIATE" in src  # rationale referenced in comment


def test_section_refresh_pre_fix_one_liner_gone():
    """Regression guard: the broken one-liner ternary must not
    survive."""
    src = (REPO / "app" / "core" / "scheduler.py").read_text()
    src_no_comments = "\n".join(
        line for line in src.splitlines()
        if not line.lstrip().startswith("#")
    )
    assert "section_minute, section_hour = minute, str((int(hour) - 1)" not in src_no_comments


# ── M6: api_release_latest warn-logs unreadable cache ────────


def test_api_release_latest_warns_on_unreadable_cache():
    """The release-cache read failure path must warn-log instead
    of bare `pass`. Pre-fix a corrupt release.json silently
    returned latest=None forever; operator never knew."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # Anchor on the release-cache read block.
    anchor = src.index("payload = json.loads(cache_path.read_text())")
    block = src[anchor:anchor + 1500]
    # The except branch logs a warning + names the file.
    assert "except (OSError, ValueError) as e:" in block
    assert "log.warning(" in block
    assert "release cache unreadable" in block
    # The pre-fix bare-pass is gone.
    assert "except (OSError, ValueError):\n                pass" not in block


# ── M7: api_jobs status param pattern-validated ──────────────


def test_api_jobs_status_pattern_validates():
    """The `status` query param must use a regex pattern that
    only accepts the canonical job-status values. Pre-fix
    `?status=garbage` returned an empty `{"jobs": []}` instead
    of a 400."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_jobs(")
    body = src[fn_anchor:fn_anchor + 1500]
    # The pattern includes all 6 valid values.
    assert 'pattern=r"^(all|pending|running|done|failed|cancelled)$"' in body


# ── M8: _JobTransient + _mark_transient + low-disk path ──────


def test_job_transient_exception_class_defined():
    """The new _JobTransient exception must exist with the
    retry_after_seconds attribute."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    assert "class _JobTransient(Exception):" in src
    cls_anchor = src.index("class _JobTransient(Exception):")
    block = src[cls_anchor:cls_anchor + 2000]
    assert "retry_after_seconds: int = 3600" in block
    assert "self.retry_after_seconds = retry_after_seconds" in block


def test_dispatch_loop_catches_job_transient():
    """The worker run loop must catch _JobTransient between
    _JobPermanentFailure and the generic Exception so transient
    re-queues don't fall through to _mark_failed's
    attempt-consuming retry."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    # Anchor on the dispatch catch chain.
    chain_anchor = src.index("except _JobCancelled:")
    block = src[chain_anchor:chain_anchor + 3000]
    permanent_idx = block.index("except _JobPermanentFailure as e:")
    transient_idx = block.index("except _JobTransient as e:")
    generic_idx = block.index("except Exception as e:")
    assert permanent_idx < transient_idx < generic_idx, (
        "_JobTransient catch must sit between _JobPermanentFailure "
        "and the generic Exception so it takes precedence."
    )
    # The _mark_transient call is wired.
    assert "self._mark_transient" in block
    assert "e.retry_after_seconds" in block


def test_mark_transient_does_not_consume_attempts():
    """The _mark_transient handler must DECREMENT attempts (to
    undo the claim-phase increment) so transient retries don't
    chip away at the max_attempts budget."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_anchor = src.index("def _mark_transient(self, job_id: int, err: str,")
    body = src[fn_anchor:fn_anchor + 2000]
    # Decrement clamped at 0.
    assert "attempts = MAX(0, attempts - 1)" in body
    # Status='running' guard mirrors _mark_failed (v1.14.25 audit H1).
    assert "WHERE id = ? AND status = 'running'" in body
    # Re-queue as pending.
    assert "SET status = 'pending'" in body


def test_low_disk_guard_raises_job_transient():
    """The low-disk guard in _do_download must raise _JobTransient
    (not RuntimeError) so retries don't burn attempts. Pre-fix
    the ~6-minute max_attempts=3 budget permanently failed
    hundreds of downloads on Unraid `mover` runs.

    v1.17.4: the guard body grew (now also fires the disk_low
    notification with rate-limit dedupe). Widen the search window
    so the `raise _JobTransient(...)` line at the END of the
    guard still falls inside the assertion's window."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    # Anchor on the low-disk guard.
    anchor = src.index("low disk:")
    block = src[anchor - 300:anchor + 3000]
    assert "raise _JobTransient(msg, retry_after_seconds=3600)" in block
    # Pre-fix RuntimeError raise is gone from this guard.
    assert "raise RuntimeError(msg)" not in block


# ── M12: event-flusher loop broad-catches ────────────────────


def test_event_flusher_loop_broad_catches():
    """The flusher loop body must be wrapped in a broad
    `except Exception:` so a non-sqlite exception doesn't kill
    the thread and silently fill the event queue to 10K.
    v1.15.35 added the lock-contention retry loop which expanded
    the function past the original 3500-char slice — bumped to
    5000 chars to cover the full loop body."""
    src = (REPO / "app" / "core" / "events.py").read_text()
    fn_anchor = src.index("def _flusher_loop(db_path: Path) -> None:")
    body = src[fn_anchor:fn_anchor + 5000]
    # The broad catch + log.exception.
    assert "except Exception:" in body
    assert "log.exception(" in body
    # The sqlite-only catch is still there (more specific log line).
    assert "except sqlite3.Error as e:" in body


# ── M13: _scrub recurses into lists, expanded substrings ─────


def test_scrub_recurses_into_lists():
    """The scrub function must recurse into list elements. Pre-fix
    `detail={"keys": [{"token": "x"}]}` silently leaked the inner token.
    v1.22.41 moved the per-element recursion into the shape-aware
    _scrub_value helper, which also redacts string + nested-list elements
    (not just dict-in-list) so URL creds can't hide one level deeper."""
    src = (REPO / "app" / "core" / "events.py").read_text()
    fn_anchor = src.index("def _scrub_value(v: Any) -> Any:")
    body = src[fn_anchor:fn_anchor + 600]
    assert "isinstance(v, (list, tuple))" in body
    assert "_scrub_value(item)" in body
    assert "_scrub(v)" in body


def test_scrub_substring_set_includes_new_secrets():
    """The redact-trigger substrings must include api_key /
    apikey / bearer alongside the v1.11.40 base set.

    v1.14.60 HOTFIX: removed bare `key` from this list — it was
    matching legitimate non-secret fields (rating_key, cache_key,
    etc.) and silently destroying audit-log data. The hotfix added
    explicit `private_key` / `signing_key` / `encryption_key` /
    `master_key` patterns to cover real secret-bearing key fields
    without the false-positive surface."""
    src = (REPO / "app" / "core" / "events.py").read_text()
    # Anchor on the constant.
    anchor = src.index("_SCRUB_SUBSTRINGS = (")
    block = src[anchor:anchor + 500]
    # v1.14.60: dropped bare `key` from the expected set.
    for needle in ("token", "secret", "password", "cookie", "auth",
                   "api_key", "apikey", "bearer",
                   "private_key", "signing_key",
                   "encryption_key", "master_key"):
        assert f'"{needle}"' in block, f"missing scrub substring: {needle}"


def test_scrub_actually_redacts_nested_list_token():
    """End-to-end: _scrub on a list-of-dict containing a token
    must redact the inner token."""
    from app.core.events import _scrub
    out = _scrub({"items": [{"token": "secret123", "label": "ok"}]})
    assert out["items"][0]["token"] == "***REDACTED***"
    # Non-secret values pass through.
    assert out["items"][0]["label"] == "ok"


def test_scrub_redacts_api_key_substring():
    """End-to-end: api_key keys redact (was a gap in pre-fix
    substring set)."""
    from app.core.events import _scrub
    out = _scrub({"api_key": "abc", "Authorization": "Bearer xyz"})
    assert out["api_key"] == "***REDACTED***"
    # 'Authorization' contains 'auth' substring → redacted.
    assert out["Authorization"] == "***REDACTED***"


# ── M14: _verify_theme_claims logs warning, not debug ────────


def test_verify_theme_claims_logs_warning_on_exception():
    """The plex_enum verify-theme-claims per-row except must log
    at WARNING level so persistent failures actually surface to
    operators. Pre-fix log.debug hid every failure mode."""
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    # Anchor on the per-row catch.
    anchor = src.index("verify_theme_claim row %s failed: %s")
    block = src[anchor - 1500:anchor + 200]
    assert "log.warning(" in block
    # The v1.14.54 marker explains the bump.
    assert "v1.14.54:" in block
