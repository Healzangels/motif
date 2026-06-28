"""v1.22.86 (audit round 2, Batch D #3) — worker LOWs.

(1) The v1.22.40 _stale_backup restore ran ONLY in the
`except DownloadError` arm — any other exception out of
download_theme (OSError from the post-download stat/sha, mkdir
failure) bypassed it: theme.mp3 stayed MISSING with theme.mp3.stale
beside it, and a retry's should_unlink check (target gone) never
re-stashed or restored. A second generic except now restores too.

(2) The SUCCESS-path `_mark_done` was the only bookkeeping mark NOT
routed through the v1.11.51 _safe_mark retry ladder — a
database-is-locked on that one UPDATE escaped into run()'s crash
handler, which re-pended the already-successful job → duplicate
download/place execution + duplicate notifications.

(3) The attempts/max_attempts read that gates terminal rollback ran
BARE inside run()'s crash handler — a locked DB there escaped run()
entirely: the job stayed status='running' (reclaimed only by the aged
stuck-job sweep) and the rollback never ran. Now guarded; on read
failure the job is still marked failed and rollback is conservatively
skipped.

(4) _do_relink deleted the placements row for every unreachable
destination with no cap — a flapping /data mount makes EVERY dst look
missing, so one Storage relink during a mount outage could wipe
thousands of placement rows for files that still exist (the v1.18.10
amplifier-sweep class). Now capped at max(50, 25% of rows) per sweep
with a loud once-per-sweep warning.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


def test_stash_restores_on_any_failure():
    i = WORKER_PY.index("v1.22.86: restore the stash on ANY other failure")
    block = WORKER_PY[i - 200:i + 900]
    assert "except Exception:" in block
    assert "_stale_backup.replace(target_mp3)" in block
    # The handler restores then re-raises.
    assert "_stale_backup = None\n            raise" in block


def test_success_mark_rides_the_retry_ladder():
    assert "self._safe_mark(self._mark_done, job[\"id\"])" in WORKER_PY, (
        "v1.22.86: the success mark must use the v1.11.51 ladder"
    )
    # The bare form must be gone from _dispatch (the helper's own def
    # remains, of course).
    assert "        self._mark_done(job[\"id\"])" not in WORKER_PY


def test_crash_handler_attempts_read_is_guarded():
    i = WORKER_PY.index("crash-handler attempts read failed")
    region = WORKER_PY[i - 900:i + 200]
    assert "row = None" in region
    assert "except sqlite3.OperationalError" in region


def test_relink_missing_dst_deletes_are_capped():
    i = WORKER_PY.index("_relink_missing_cap = max(50, len(rows) // 4)")
    block = WORKER_PY[i:i + 2200]
    assert "if _missing_deleted >= _relink_missing_cap:" in block
    assert "leaving" in block and "the rest tracked" in block, (
        "the cap must log loudly once per sweep"
    )
    assert "_missing_deleted += 1" in block
