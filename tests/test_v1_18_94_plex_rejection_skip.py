"""v1.18.94 — hourly retry sweep stops re-cycling Plex-rejected rows.

the user's repro: an 11.7MB Fate/stay night theme that Plex
consistently rejected with HTTP 500. The v1.18.70 size-rejection
permanent-fail only catches files >=20MB — sub-20MB rejections
fall through to the transient/retry path (max_attempts=3,
exponential backoff 1m→5m). Each job lifecycle is ~7min of amber
"PLACE INTO PLEX QUEUED" UI, then terminal-fail. Then the hourly
`_retry_pending_placements` sweep creates a FRESH place job and
the dance repeats forever, until the user manually cancels or
Plex stops rejecting.

## Fix

Worker side (worker.py:_do_place_collection): stamp
local_files.last_place_attempt_reason = 'plex_rejected:HTTP_500'
(or similar — pattern is plex_rejected:HTTP_<status>) when Plex
returns an error and no fallback fires.

Sweep side (scheduler._retry_pending_placements): widen the
skip filter to ALSO skip rows where last_place_attempt_reason
starts with 'plex_rejected:' AND the row has 2+ failed place
jobs in the last 24h. First failure stays auto-retryable
(might be a transient Plex hiccup); a 2nd failure within 24h
locks the row out until user intervention.

## What's pinned

- Worker stamps last_place_attempt_reason='plex_rejected:HTTP_<n>'
  when Plex rejects an upload (source-level pin on the UPDATE).
- Sweep eligibility honors the 2+ recent failures threshold.
- First failure still re-sweepable (could be transient).
- Failures older than 24h don't count (rolling window).
- Non-plex_rejected reasons unaffected by the new clause.
- Existing v1.13.76 skip reasons (plex_has_theme,
  existing_theme:*) still skip regardless of job count.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.core.db import init_db


REPO = Path(__file__).resolve().parent.parent
WORKER_PY = REPO / "app" / "core" / "worker.py"
SCHEDULER_PY = REPO / "app" / "core" / "scheduler.py"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _iso_minus(hours: float) -> str:
    return (datetime.now(timezone.utc)
            - timedelta(hours=hours)).isoformat(timespec="seconds")


@pytest.fixture
def db(tmp_path: Path) -> Path:
    p = tmp_path / "motif.db"
    init_db(p)
    return p


# ── Source-level pins: worker stamps the reason ──────────────


def test_worker_stamps_plex_rejected_reason():
    """When _do_place_collection raises on Plex rejection, it
    must first UPDATE local_files.last_place_attempt_reason
    with a `plex_rejected:HTTP_<status>` value so the sweep
    has data to filter on."""
    src = WORKER_PY.read_text()
    # The reason-template + the UPDATE both have to be present.
    assert "plex_rejected:HTTP_" in src, (
        "v1.18.94: worker must stamp 'plex_rejected:HTTP_<n>' "
        "as last_place_attempt_reason on Plex upload rejection"
    )
    # Pin the structural anchors — the UPDATE before the raise.
    idx = src.index("plex_rejected:HTTP_")
    block = src[max(0, idx - 800):idx + 800]
    assert "last_place_attempt_reason" in block, (
        "v1.18.94: the rejection stamp must write "
        "last_place_attempt_reason (not just log)"
    )
    assert "UPDATE local_files" in block, (
        "v1.18.94: stamp must hit local_files table"
    )


def test_worker_stamp_is_defensive():
    """If the stamp UPDATE itself fails (DB locked, etc.) the
    worker must still raise the original Plex rejection — we
    want the retry/permanent-fail classification to land even
    if the stamp didn't. Pattern: try/except around the stamp
    with a log.warning. Per CLAUDE.md class-9: defensive
    catches need a log breadcrumb."""
    src = WORKER_PY.read_text()
    idx = src.index("plex_rejected:HTTP_")
    block = src[idx:idx + 1500]
    # An except path must exist in the stamp block.
    assert "except" in block, (
        "v1.18.94: stamp UPDATE must be wrapped in try/except"
    )
    # And it must log so silent failures don't hide drift.
    assert "log.warning" in block, (
        "v1.18.94: stamp failure must log.warning per class-9 "
        "(silent-defensive-catch is a bug)"
    )


# ── Source-level pins: scheduler widens the filter ───────────


def test_scheduler_filter_includes_plex_rejected_clause():
    """_retry_pending_placements SQL must have the v1.18.94
    AND NOT clause that gates on plex_rejected:* + 2+ recent
    failures."""
    src = SCHEDULER_PY.read_text()
    assert "v1.18.94" in src, (
        "v1.18.94: scheduler.py must mark the widening with the tag"
    )
    assert "plex_rejected:%" in src, (
        "v1.18.94: scheduler filter must reference plex_rejected:%"
    )
    # The 24-hour rolling window is part of the contract.
    assert "-24 hours" in src, (
        "v1.18.94: filter must use a 24-hour rolling window"
    )
    # The >=2 threshold is also part of the contract.
    assert ">= 2" in src, (
        "v1.18.94: filter must use the 2+ failures threshold"
    )


# ── Behavioral: sweep eligibility against seeded scenarios ───


def _insert_row(conn, *, tmdb_id: int, last_reason: str | None,
                title: str = "x"):
    """Theme + local_files row with no matching placement —
    the state the retry sweep targets."""
    now = _now_iso()
    conn.execute(
        "INSERT INTO themes ("
        "  media_type, tmdb_id, title, upstream_source,"
        "  last_seen_sync_at, first_seen_sync_at"
        ") VALUES ('movie', ?, ?, 'imdb', ?, ?)",
        (tmdb_id, title, now, now),
    )
    conn.execute(
        "INSERT INTO local_files ("
        "  media_type, tmdb_id, section_id, file_path, source_video_id,"
        "  downloaded_at, last_place_attempt_at, last_place_attempt_reason"
        ") VALUES ('movie', ?, '1', 'x.mp3', 'vid', ?, ?, ?)",
        (tmdb_id, now,
         now if last_reason else None, last_reason),
    )


def _insert_failed_place_job(conn, *, tmdb_id: int,
                              finished_at_iso: str):
    """Add a `place` job row in status='failed' with the given
    finished_at. The sweep filter counts these in its 24h
    rolling window."""
    conn.execute(
        "INSERT INTO jobs ("
        "  job_type, status, media_type, tmdb_id, section_id,"
        "  payload, created_at, finished_at, last_error"
        ") VALUES ('place', 'failed', 'movie', ?, '1', '{}', "
        "          ?, ?, "
        "          'Plex rejected upload (rk=999, HTTP 500, size=11.7MB)')",
        (tmdb_id, finished_at_iso, finished_at_iso),
    )


def _eligible_tmdb_ids(db: Path) -> set[int]:
    """Mirror of scheduler._retry_pending_placements SQL.
    Update alongside scheduler.py edits. v1.18.94 widened the
    filter; this helper carries the new clauses."""
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT lf.tmdb_id
            FROM local_files lf
            LEFT JOIN placements p
              ON p.media_type = lf.media_type
             AND p.tmdb_id = lf.tmdb_id
             AND p.section_id = lf.section_id
            WHERE p.media_folder IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM jobs j
                  WHERE j.job_type = 'place' AND j.media_type = lf.media_type
                    AND j.tmdb_id = lf.tmdb_id
                    AND j.section_id = lf.section_id
                    AND j.status IN ('pending', 'running')
              )
              AND (lf.last_place_attempt_reason IS NULL
                   OR (lf.last_place_attempt_reason != 'plex_has_theme'
                       AND lf.last_place_attempt_reason NOT LIKE 'existing_theme:%'))
              AND NOT (
                  lf.last_place_attempt_reason LIKE 'plex_rejected:%'
                  AND (
                      SELECT COUNT(*) FROM jobs j2
                      WHERE j2.job_type = 'place'
                        AND j2.media_type = lf.media_type
                        AND j2.tmdb_id = lf.tmdb_id
                        AND j2.section_id = lf.section_id
                        AND j2.status = 'failed'
                        AND j2.finished_at IS NOT NULL
                        -- v1.19.5: julianday() to match scheduler.py
                        -- fix for ISO-T vs space-separated format
                        -- lexicographic mismatch.
                        AND julianday(j2.finished_at)
                            > julianday('now', '-24 hours')
                  ) >= 2
              )
            """
        ).fetchall()
    return {r["tmdb_id"] for r in rows}


def test_eligible_with_plex_rejected_and_zero_recent_failures(db):
    """Reason stamped from the FIRST failure but no completed
    failed job exists yet (the v1.18.94 stamp landed but the
    job itself is still pending its first retry, or hasn't
    terminal-failed yet). Sweep should still pick it up —
    we haven't crossed the 2-failure threshold."""
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1,
                    last_reason="plex_rejected:HTTP_500")
    assert _eligible_tmdb_ids(db) == {1}, (
        "First failure must still be auto-retryable (could be "
        "a transient Plex hiccup)"
    )


def test_eligible_with_plex_rejected_and_one_recent_failure(db):
    """One terminal-failed job in last 24h + reason stamped =
    still eligible. The threshold is 2 — first failure gets
    one free re-sweep before the row locks out."""
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1,
                    last_reason="plex_rejected:HTTP_500")
        _insert_failed_place_job(conn, tmdb_id=1,
                                  finished_at_iso=_iso_minus(hours=2))
    assert _eligible_tmdb_ids(db) == {1}, (
        "1 recent failure < 2-failure threshold — sweep still proceeds"
    )


def test_skipped_with_plex_rejected_and_two_recent_failures(db):
    """The v1.18.94 contract trigger: reason stamped +
    2 terminal-failed place jobs in the last 24h = sweep
    locks the row out. User must intervene (REPLACE TDB,
    PURGE, etc.) to re-enqueue."""
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1,
                    last_reason="plex_rejected:HTTP_500")
        _insert_failed_place_job(conn, tmdb_id=1,
                                  finished_at_iso=_iso_minus(hours=2))
        _insert_failed_place_job(conn, tmdb_id=1,
                                  finished_at_iso=_iso_minus(hours=1))
    assert _eligible_tmdb_ids(db) == set(), (
        "2 recent failures + plex_rejected reason → sweep skips"
    )


def test_skipped_with_plex_rejected_and_many_recent_failures(db):
    """3+ failures = still skipped (filter is >= 2, not == 2).
    Pin to make sure a future tightening doesn't regress to
    exact-count semantics."""
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1,
                    last_reason="plex_rejected:HTTP_500")
        for h in (1, 2, 3, 4):
            _insert_failed_place_job(
                conn, tmdb_id=1,
                finished_at_iso=_iso_minus(hours=h),
            )
    assert _eligible_tmdb_ids(db) == set()


def test_eligible_with_plex_rejected_when_failures_are_old(db):
    """Rolling 24h window — failures older than 24h don't
    count. Lets a row that's been failing for a while get
    one fresh sweep attempt per day. Plex configs change,
    transcoder fixes ship, etc."""
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1,
                    last_reason="plex_rejected:HTTP_500")
        # Both failures outside the 24h window.
        _insert_failed_place_job(conn, tmdb_id=1,
                                  finished_at_iso=_iso_minus(hours=25))
        _insert_failed_place_job(conn, tmdb_id=1,
                                  finished_at_iso=_iso_minus(hours=30))
    assert _eligible_tmdb_ids(db) == {1}, (
        "Failures older than 24h don't count — re-sweep allowed"
    )


def test_eligible_when_reason_is_not_plex_rejected(db):
    """The new clause only fires for plex_rejected:* prefix.
    no_match keeps retrying with no count limit. (v1.21.44:
    placement_error:* is now skipped entirely — see
    test_v1_13_76_retry_sweep_skip.py.)"""
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1, last_reason="no_match")
        # Even 5 recent failures shouldn't lock this out.
        for h in range(5):
            _insert_failed_place_job(
                conn, tmdb_id=1,
                finished_at_iso=_iso_minus(hours=h + 1),
            )
    assert _eligible_tmdb_ids(db) == {1}


def test_existing_v1_13_76_skip_reasons_still_win(db):
    """plex_has_theme + existing_theme:* skip regardless of
    job count — they're permanent reasons with no count gate.
    Pin to make sure the v1.18.94 widening didn't loosen
    the v1.13.76 filter accidentally."""
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1, last_reason="plex_has_theme")
        _insert_row(conn, tmdb_id=2,
                    last_reason="existing_theme:custom.m4a")
    assert _eligible_tmdb_ids(db) == set()


def test_mixed_population_v1_18_94(db):
    """Bigger fixture combining v1.13.76 + v1.18.94 conditions.
    Pin the full partition so a future filter edit can't
    silently regress one class."""
    with sqlite3.connect(db) as conn:
        # Eligible: never attempted.
        _insert_row(conn, tmdb_id=1, last_reason=None)
        # Eligible: transient reason.
        _insert_row(conn, tmdb_id=2, last_reason="no_match")
        # Skipped (v1.13.76): permanent.
        _insert_row(conn, tmdb_id=3, last_reason="plex_has_theme")
        # Eligible: 1 recent plex_rejected failure (under threshold).
        _insert_row(conn, tmdb_id=4,
                    last_reason="plex_rejected:HTTP_500")
        _insert_failed_place_job(conn, tmdb_id=4,
                                  finished_at_iso=_iso_minus(hours=2))
        # Skipped (v1.18.94): 2 recent plex_rejected failures.
        _insert_row(conn, tmdb_id=5,
                    last_reason="plex_rejected:HTTP_500")
        _insert_failed_place_job(conn, tmdb_id=5,
                                  finished_at_iso=_iso_minus(hours=2))
        _insert_failed_place_job(conn, tmdb_id=5,
                                  finished_at_iso=_iso_minus(hours=1))
        # Eligible (v1.18.94): plex_rejected but failures are old.
        _insert_row(conn, tmdb_id=6,
                    last_reason="plex_rejected:HTTP_500")
        _insert_failed_place_job(conn, tmdb_id=6,
                                  finished_at_iso=_iso_minus(hours=30))
        _insert_failed_place_job(conn, tmdb_id=6,
                                  finished_at_iso=_iso_minus(hours=40))
    assert _eligible_tmdb_ids(db) == {1, 2, 4, 6}


def test_other_http_status_codes_also_skip(db):
    """The reason prefix is `plex_rejected:HTTP_<n>` for any
    status. 502/503/504/other 5xx should all classify the same
    way — the worker stamps whatever http_status came back."""
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1,
                    last_reason="plex_rejected:HTTP_502")
        _insert_failed_place_job(conn, tmdb_id=1,
                                  finished_at_iso=_iso_minus(hours=2))
        _insert_failed_place_job(conn, tmdb_id=1,
                                  finished_at_iso=_iso_minus(hours=1))
    assert _eligible_tmdb_ids(db) == set(), (
        "v1.18.94: any plex_rejected:HTTP_<n> prefix triggers the "
        "count gate, not just HTTP_500"
    )


def test_plex_rejected_none_status_also_skip(db):
    """Worker stamps 'plex_rejected:HTTP_none' when no
    http_status was captured (network-error fallback). That
    case should also engage the count gate."""
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1,
                    last_reason="plex_rejected:HTTP_none")
        _insert_failed_place_job(conn, tmdb_id=1,
                                  finished_at_iso=_iso_minus(hours=2))
        _insert_failed_place_job(conn, tmdb_id=1,
                                  finished_at_iso=_iso_minus(hours=1))
    assert _eligible_tmdb_ids(db) == set()


# ── Breadcrumb: sweep logs WHY it skipped rows ───────────────


def test_sweep_logs_lockout_summary_when_rows_blocked(
    db, monkeypatch,
):
    """Per CLAUDE.md class-9 (silent-defensive-catch) + v1.18.5
    cold-path-needs-MORE-logging: when the sweep filter blocks a
    row from auto-retry, the operator must see WHY in /logs. A
    single summary event per sweep run with the count + a tmdb_id
    sample — no per-row hourly spam, but no silent lockout
    either."""
    from app.core import scheduler

    # Seed: 1 eligible (never attempted), 2 locked-out.
    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1, last_reason=None)
        _insert_row(conn, tmdb_id=2,
                    last_reason="plex_rejected:HTTP_500")
        _insert_failed_place_job(conn, tmdb_id=2,
                                  finished_at_iso=_iso_minus(hours=2))
        _insert_failed_place_job(conn, tmdb_id=2,
                                  finished_at_iso=_iso_minus(hours=1))
        _insert_row(conn, tmdb_id=3,
                    last_reason="plex_rejected:HTTP_502")
        _insert_failed_place_job(conn, tmdb_id=3,
                                  finished_at_iso=_iso_minus(hours=3))
        _insert_failed_place_job(conn, tmdb_id=3,
                                  finished_at_iso=_iso_minus(hours=2))

    # Capture log_event calls instead of waiting for the async flusher.
    captured: list[dict] = []

    def _fake_log_event(db_path, *, level, component, message,
                        **kwargs):
        captured.append({
            "level": level, "component": component, "message": message,
        })

    monkeypatch.setattr(scheduler, "log_event", _fake_log_event)

    scheduler._retry_pending_placements(db)

    # Two events expected: the existing "enqueued N" message and the
    # new "N row(s) blocked" summary.
    enqueue_msgs = [c for c in captured if "enqueued" in c["message"]]
    blocked_msgs = [c for c in captured
                    if "blocked from auto-retry" in c["message"]]
    assert len(enqueue_msgs) == 1, (
        f"expected 1 enqueue event, got: {captured}"
    )
    assert "1 placement jobs" in enqueue_msgs[0]["message"], (
        f"only the never-attempted row should be enqueued: "
        f"{enqueue_msgs[0]}"
    )
    assert len(blocked_msgs) == 1, (
        f"v1.18.94: expected 1 lockout summary event, got: {captured}"
    )
    blocked = blocked_msgs[0]
    assert blocked["level"] == "INFO"
    assert blocked["component"] == "scheduler"
    assert "2 row(s)" in blocked["message"], (
        f"summary must include the count: {blocked['message']}"
    )
    # Both locked tmdb_ids should appear in the sample.
    assert "(movie,2)" in blocked["message"]
    assert "(movie,3)" in blocked["message"]
    # Actionable suffix (what the operator does about it).
    assert "REPLACE TDB" in blocked["message"] \
        or "PURGE" in blocked["message"]


def test_sweep_does_not_log_lockout_when_no_rows_blocked(
    db, monkeypatch,
):
    """If nothing's locked out, no lockout summary event fires —
    avoids hourly noise on clean installs."""
    from app.core import scheduler

    with sqlite3.connect(db) as conn:
        # One eligible row only — no plex_rejected lockouts.
        _insert_row(conn, tmdb_id=1, last_reason=None)

    captured: list[dict] = []

    def _fake_log_event(db_path, *, level, component, message,
                        **kwargs):
        captured.append({"message": message})

    monkeypatch.setattr(scheduler, "log_event", _fake_log_event)
    scheduler._retry_pending_placements(db)

    blocked_msgs = [c for c in captured
                    if "blocked from auto-retry" in c["message"]]
    assert blocked_msgs == [], (
        f"v1.18.94: no lockout summary when 0 rows blocked, got: "
        f"{captured}"
    )


def test_sweep_logs_lockout_even_when_zero_enqueued(
    db, monkeypatch,
):
    """The lockout summary fires INDEPENDENTLY of the enqueue
    summary. If every row is locked out (nothing eligible to
    enqueue), the operator still gets the lockout signal —
    otherwise the sweep would run silently every hour with no
    log activity at all."""
    from app.core import scheduler

    with sqlite3.connect(db) as conn:
        _insert_row(conn, tmdb_id=1,
                    last_reason="plex_rejected:HTTP_500")
        _insert_failed_place_job(conn, tmdb_id=1,
                                  finished_at_iso=_iso_minus(hours=2))
        _insert_failed_place_job(conn, tmdb_id=1,
                                  finished_at_iso=_iso_minus(hours=1))

    captured: list[dict] = []

    def _fake_log_event(db_path, *, level, component, message,
                        **kwargs):
        captured.append({"message": message})

    monkeypatch.setattr(scheduler, "log_event", _fake_log_event)
    scheduler._retry_pending_placements(db)

    enqueue_msgs = [c for c in captured if "enqueued" in c["message"]]
    blocked_msgs = [c for c in captured
                    if "blocked from auto-retry" in c["message"]]
    assert enqueue_msgs == [], (
        "nothing eligible to enqueue → no enqueue event"
    )
    assert len(blocked_msgs) == 1, (
        f"v1.18.94: lockout summary must fire even when "
        f"enqueue count is 0, got: {captured}"
    )
