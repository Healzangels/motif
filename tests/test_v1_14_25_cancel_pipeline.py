"""v1.14.25 — cancel pipeline robustness (worker H1 + H2 bundle).

Wave 1.5 of the codebase audit. Both findings touch the same
surface (worker.py cancel handling) and are mutually reinforcing —
H1 stops natural completions from clobbering force-cancels, H2
gives long-running short-job handlers safe yield points to honor
cooperative cancels.

## H1 — force-cancel race vs `_mark_*` (audit AUDIT_WORKER.md H1)

Pre-fix: `/api/jobs/{id}/cancel?force=true` flips the row
status='cancelled'. If the worker thread's in-flight handler
then completes naturally, `_mark_done` / `_mark_failed` /
`_mark_failed_terminal` UPDATE WHERE id=? with no status
guard — silently flipping the force-cancelled row back to
'done' / 'failed' / 'pending' (worst case: the retry path
schedules the row to run again).

User saw: × FORCE clicked → row went 'cancelled' → handler
finished moments later → row flipped back to green/red without
their action.

Fix: add `WHERE status='running'` guard to all three UPDATE
statements (mirrors the existing pattern in scheduler.py:201-207
stuck-job sweep). Use `cur.rowcount == 0` to log when the
bookkeeping was a no-op (signal the force-cancel landed first).

## H2 — cooperative cancel no-op for download/place/refresh/relink

Pre-fix: only `_do_sync`, `_do_plex_enum`, `_do_scan` thread
`_is_cancelled` checks into their long loops. The general-
worker handlers (download/place/refresh/relink/adopt) never
called `_is_cancelled`. CANCEL clicked on a running download
just sat — the user's only escape was × FORCE.

The CLAUDE.md cancel-protocol contract ("worker handler
notices on the next safe yield") was silently broken for
these handlers.

Fix: add cooperative-cancel checkpoints at safe yield points:
- `_do_download`: between `bucket.acquire()` and the actual
  yt-dlp call (CANCEL during the rate-limit wait now bails)
- `_do_place`: after FolderIndex build / pre-place capture,
  before the place_theme call
- `_do_relink`: at the top of the per-row for-loop (bulk
  relink walks thousands of rows; CANCEL bails early)

yt-dlp itself accepts no cancel callback so mid-download
cancel still requires × FORCE — but the contract for "CANCEL
during pre-yt-dlp wait" + "CANCEL during long bulk loops"
now matches user expectations.

## Why bundle H1 + H2

Both touch the same worker.py surface, both about cancel
correctness, both low-risk. Shipping together keeps the
cancel contract internally consistent across one tag —
the H2 cooperative checks call `_mark_cancelled` which
relies on the H1 status guards staying clean.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── H1: status='running' guard on _mark_done / _mark_failed_* ─


def test_mark_done_has_running_guard():
    """The `_mark_done` UPDATE must include `AND status = 'running'`
    so a force-cancelled row's natural completion can't overwrite
    the cancelled state."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_anchor = src.index("def _mark_done(self, job_id: int) -> None:")
    body = src[fn_anchor:fn_anchor + 1500]
    assert "WHERE id = ? AND status = 'running'" in body
    # Rowcount-aware no-op log so operators can grep for force-
    # cancel-took-precedence cases.
    assert "if cur.rowcount == 0" in body
    assert "_mark_done no-op" in body


def test_mark_failed_terminal_has_running_guard():
    """Same guard on `_mark_failed_terminal` (the v1.10.40
    permanent-failure path)."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_anchor = src.index("def _mark_failed_terminal(self, job_id: int, err: str) -> None:")
    body = src[fn_anchor:fn_anchor + 1500]
    assert "WHERE id = ? AND status = 'running'" in body
    assert "_mark_failed_terminal no-op" in body


def test_mark_failed_has_running_guard_on_both_branches():
    """`_mark_failed` is the most damaging without a guard — its
    retry branch sets status='pending' + schedules a retry. Pre-fix
    a force-cancelled row whose handler subsequently raised would
    get rescheduled. Both the terminal-failure branch AND the
    retry branch need the guard."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_anchor = src.index("def _mark_failed(self, job_id: int, err: str) -> None:")
    # End at the next def / class boundary.
    end = src.index("\n    def ", fn_anchor + 10)
    body = src[fn_anchor:end]
    # Both branches must have the guard.
    assert body.count("WHERE id = ? AND status = 'running'") >= 2, (
        "v1.14.25 H1: both _mark_failed branches (terminal + retry) "
        "need the status='running' guard"
    )
    # No-op log present.
    assert "_mark_failed no-op" in body


def test_mark_done_pre_fix_unguarded_update_gone():
    """Regression guard: the pre-fix unguarded UPDATE shape
    (`WHERE id = ?` with no status check) must not survive in
    `_mark_done`. Comment-stripped to dodge rationale references."""
    src_raw = (REPO / "app" / "core" / "worker.py").read_text()
    fn_anchor = src_raw.index("def _mark_done(self, job_id: int) -> None:")
    end = src_raw.index("\n    def ", fn_anchor + 10)
    body_raw = src_raw[fn_anchor:end]
    body = "\n".join(
        line for line in body_raw.splitlines()
        if not line.lstrip().startswith("#")
    )
    pre_fix = '"UPDATE jobs SET status = \'done\', finished_at = ?, last_error = NULL WHERE id = ?"'
    assert pre_fix not in body


# ── H2: cooperative cancel checkpoints in short-job handlers ──


def test_do_download_has_cancel_checkpoint_after_bucket_acquire():
    """`_do_download` must check `_is_cancelled` between
    `bucket.acquire()` and the yt-dlp call. Pre-fix CANCEL
    during the rate-limit wait was ignored."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    # Anchor on the v1.14.25 marker for download.
    marker = "v1.14.25: cooperative cancel checkpoint between bucket"
    assert marker in src
    block_start = src.index(marker)
    block = src[block_start:block_start + 2000]
    assert "_is_cancelled(self.settings.db_path, job[\"id\"])" in block
    assert "self._mark_cancelled(job[\"id\"])" in block


def test_do_place_has_cancel_checkpoint_after_index_build():
    """`_do_place` must check `_is_cancelled` after FolderIndex
    build / pre-place capture, before `place_theme` fires the
    actual hardlink + Plex nudge."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    marker = "v1.14.25: cooperative cancel checkpoint after FolderIndex"
    assert marker in src
    block_start = src.index(marker)
    block = src[block_start:block_start + 1500]
    assert "_is_cancelled(self.settings.db_path, job[\"id\"])" in block
    assert "self._mark_cancelled(job[\"id\"])" in block
    # The check must precede the actual place_theme call.
    place_call = src.index("outcome = place_theme(", block_start)
    assert block_start < place_call


def test_do_relink_has_cancel_checkpoint_in_loop():
    """`_do_relink` must check `_is_cancelled` at the top of its
    per-row loop. Bulk relink walks thousands of placement rows;
    CANCEL must bail mid-loop, not wait for full traversal."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    marker = "v1.14.25: cooperative cancel checkpoint inside the per-row"
    assert marker in src
    block_start = src.index(marker)
    block = src[block_start:block_start + 1500]
    assert "_is_cancelled(self.settings.db_path, job_id)" in block
    assert "self._mark_cancelled(job_id)" in block


def test_existing_long_job_handlers_still_thread_cancel_check():
    """Regression guard: `_do_sync` / `_do_plex_enum` / `_do_scan`
    already thread `cancel_check=lambda jid=...` into their
    callees. v1.14.25 doesn't touch them; pin so a refactor
    doesn't drop the existing cooperation."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    # The cancel_check kwarg pattern.
    assert "cancel_check=lambda" in src or "cancel_check=lambda " in src


# ── _is_cancelled itself ──────────────────────────────────────


def test_is_cancelled_helper_unchanged():
    """`_is_cancelled` is the helper every cancel checkpoint
    calls. v1.14.25 doesn't touch it; pin its existence + DB
    read shape so a refactor doesn't break the contract."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    assert "def _is_cancelled(db_path: Path, job_id: int) -> bool:" in src
    # The function reads the cancel_requested flag from jobs.
    fn_anchor = src.index("def _is_cancelled(db_path: Path, job_id: int)")
    body = src[fn_anchor:fn_anchor + 1500]
    assert "cancel_requested" in body


# ── Mirror with /api/jobs cancel endpoint ────────────────────


def test_api_jobs_force_cancel_unchanged():
    """The /api/jobs/{id}/cancel?force=true endpoint at
    api.py:~10497 sets status='cancelled' immediately. v1.14.25
    relies on this to be the OUTSIDE-the-worker write that the
    `_mark_*` guards check against. Pin the endpoint shape."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "@app.post(\"/api/jobs/{job_id}/cancel\")" in src
    fn_anchor = src.index("async def api_cancel_job(")
    body = src[fn_anchor:fn_anchor + 3000]
    # Force-cancel branch flips to 'cancelled' status.
    assert "status = 'cancelled'" in body
