"""v1.14.12 — drop the +30s safety-net refresh job after every place.

the user's complaint: "the long nudge plex process we have currently
after a downloading and placing and item in plex... the delayed
and long nudge process which especially when bulk downloading
is very slow".

Investigation revealed the per-item refresh the user was asking
about IS already there: `placement.place_theme` calls
`plex.refresh(rk)` inline at place time, which fires a
`PUT /library/metadata/{rk}/refresh?force=1` (the canonical
"Refresh Metadata" button equivalent).

What was actually slow at bulk scale was the SAFETY-NET refresh
job: `worker._do_place` was enqueueing a SECOND `refresh` job
with `next_run_at = now + 30s` after every successful place.
The +30s job called `plex.refresh(rk)` again — exactly the same
Plex API call the inline refresh just fired.

Net effect at 100-place bulk:
- Pre-fix: 100 inline refreshes (fast) + 100 queued safety-net
  jobs draining serially through rate-limited worker (slow,
  visible in /queue). 200 Plex refresh calls total.
- Post-fix: 100 inline refreshes only. Queue stays clean.

History:
- v1.11.24 added 3 follow-up refreshes (+10/+30/+90s) as belt-
  and-suspenders for Plex's local-media-assets agent debounce.
- v1.11.59 narrowed to a single +30s follow-up.
- v1.14.12 removes it entirely — the inline call is the
  per-item refresh, and the manual `// REFRESH MOVIES` per-
  section enum stays as the recovery fallback for the rare
  case Plex's agent debounces past the inline call.

Tests pin:
- _do_place no longer enqueues `refresh` job after successful place
- The inline `plex.refresh(rk)` call in `placement.place_theme` is unchanged
- The `_do_refresh` handler still exists (explicit refresh ops elsewhere)
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── _do_place no longer enqueues post-place refresh jobs ──────


def test_do_place_no_longer_enqueues_safety_net_refresh():
    """The pre-fix block at worker.py:1528-1550 INSERT'd a
    `refresh` job into `jobs` with `next_run_at = now + 30s`
    after every successful place. v1.14.12 removed that block
    entirely — the inline refresh in placement.place_theme is
    the per-item refresh, no auto-retry needed.

    Static-text guard: the SQL fragment that enqueued the
    safety net must not survive."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    # The pre-fix insert had this exact payload reason marker.
    assert '"reason": "post_place_refresh_+30s"' not in src, (
        "v1.14.12: the +30s safety-net refresh enqueue must not "
        "survive — the inline refresh is the per-item refresh"
    )
    # And the pre-fix INSERT statement marker — pin its absence too.
    assert "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,\n                                                  payload, status, created_at, next_run_at)\n                               VALUES ('refresh'" not in src


def test_do_place_marker_documents_v1_14_12_decision():
    """The rationale comment must be in place so the next person
    reading _do_place understands why no refresh is enqueued
    here (and doesn't add it back as a "missing safety net")."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    assert "v1.14.12: dropped the +30s safety-net refresh" in src
    # Mention of the inline call so the reader knows where the
    # actual per-item refresh lives.
    assert "inline `plex.refresh(rk)` inside" in src
    # Trade-off acknowledgement: manual REFRESH per-section is the
    # recovery for the rare debounce case.
    assert "// REFRESH MOVIES" in src or "// REFRESH PLEX" in src


# ── Inline refresh in placement.place_theme survives ──────────


def test_inline_refresh_at_place_time_unchanged():
    """`placement.place_theme` must still call `plex.refresh(rk)`
    after the file lands. That's the per-item refresh — the
    whole point of v1.14.12 is to keep this and drop the
    redundant safety net."""
    src = (REPO / "app" / "core" / "placement.py").read_text()
    # The call site is gated on plex enabled + analyze_after + rk.
    assert "if plex and plex.cfg.enabled and analyze_after and rk:" in src
    assert "refreshed = plex.refresh(rk)" in src


def test_plex_refresh_still_hits_force_refresh_endpoint():
    """`plex.refresh()` itself must still hit
    `PUT /library/metadata/{rk}/refresh?force=1`. This is the
    canonical Plex "Refresh Metadata" call — the per-item
    refresh request the user asked for in their redirect."""
    src = (REPO / "app" / "core" / "plex.py").read_text()
    fn_anchor = src.index("def refresh(self, rating_key: str) -> bool:")
    body = src[fn_anchor:fn_anchor + 1500]
    # Force-refresh PUT — this is the per-item Plex API call.
    assert 'self._put(self._rk_path(rating_key, "/refresh")' in body
    assert '"force": "1"' in body


# ── _do_refresh handler stays for explicit ops ────────────────


def test_do_refresh_handler_still_exists():
    """The `_do_refresh` handler must remain in worker.py — it's
    used by explicit refresh ops (e.g. RE-DOWNLOAD TDB recovery
    flows still enqueue refresh jobs). v1.14.12 removed only the
    auto-enqueue from _do_place; the handler itself is still
    needed for the surfaces that DO enqueue refresh jobs
    deliberately."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    assert "def _do_refresh(self, job: sqlite3.Row) -> None:" in src
    # And the dispatch from the worker loop must still route
    # 'refresh' job_type into _do_refresh.
    assert "self._do_refresh(job)" in src


def test_refresh_job_type_still_recognized_by_jobs_table():
    """The jobs table still allows 'refresh' as a job_type — pin
    so a future cleanup that drops the type from the CHECK
    constraint doesn't silently break the explicit refresh path."""
    src = (REPO / "app" / "core" / "db.py").read_text()
    # The jobs.job_type CHECK constraint must list 'refresh'.
    assert "'refresh'" in src


# ── No other code path enqueues post-place refresh ────────────


def test_no_other_code_path_enqueues_post_place_refresh():
    """Whole-codebase sweep: no Python file should still enqueue
    a refresh job under the post-place context (the dropped
    safety net). Pin via the unique pre-fix `reason` marker —
    if it reappears anywhere, we've regressed."""
    forbidden = "post_place_refresh_+30s"
    for f in (REPO / "app").rglob("*.py"):
        contents = f.read_text()
        # Allow the marker in test files (this file documents it).
        if "tests" in str(f):
            continue
        assert forbidden not in contents, (
            f"v1.14.12: {f.name} still references the dropped "
            f"safety-net refresh enqueue"
        )
