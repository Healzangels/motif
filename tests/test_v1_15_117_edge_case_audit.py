"""v1.15.117 — holistic edge-case audit findings.

Two unrelated startup / placement edge-cases surfaced by a wide
audit pass. Both were silent-crash risks under specific user
misconfigurations or environmental hiccups.

## 1. Cron content validation

Pre-fix `start_scheduler` only checked `len(MOTIF_SYNC_CRON.split())
== 5`. The COUNT gate passed for content like `"0 13 1 * 99"`
(day_of_week=99, invalid), but `CronTrigger(day_of_week="99", ...)`
raised ValueError, propagating out of `start_scheduler` and
crashing motif startup AFTER the worker thread had already
started. The user saw: motif boots, runs for a few seconds,
crashes on the scheduler step; signal handlers never wired so
shutdown was unclean.

Post-fix the CronTrigger construction is wrapped in try/except.
On ValueError / TypeError, log + fall back to the canonical
`0 13 * * *` default. Boot stays clean even when MOTIF_SYNC_CRON
is mistyped.

## 2. Placement temp-file pre-clean tolerance

`_safe_link_or_copy` pre-cleans a stale `.motif-tmp` file before
the link/copy step. The pre-clean was `dst_tmp.unlink()` with no
exception handling — a PermissionError / lock / FS hiccup would
propagate out of `place_theme`, surfacing as a worker-job
failure even though the subsequent `os.replace` would have
overwritten the temp anyway.

Post-fix the pre-clean is best-effort. OSError → log.warning,
continue. If the stale temp persists, the hardlink step fails
(FileExistsError caught by the existing `except OSError`
handler), falls back to `shutil.copy2` which overwrites, then
`os.replace` lands the placement. Worst case: kind degrades from
'hardlink' to 'copy' for one placement instead of total failure.

## Tests
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SCHEDULER_PY = REPO / "app" / "core" / "scheduler.py"
PLACEMENT_PY = REPO / "app" / "core" / "placement.py"


def test_cron_construction_wrapped_in_try_except():
    """The CronTrigger() call for `daily_sync` must run inside a
    try/except that falls back to the canonical default. Pre-fix
    a bad cron in MOTIF_SYNC_CRON crashed startup."""
    src = SCHEDULER_PY.read_text()
    # Find the daily-sync section.
    anchor = src.index("# Daily sync")
    # Window covers the trigger construction + fallback.
    block = src[anchor:anchor + 2000]
    assert "try:" in block
    assert "CronTrigger(" in block
    assert "except (ValueError, TypeError)" in block
    # Fallback uses the canonical string.
    assert "0 13 * * *" in block


def test_cron_fallback_string_appears_twice():
    """The fallback cron string must appear in both the
    field-count fallback (pre-existing) and the new
    CronTrigger-construction fallback."""
    src = SCHEDULER_PY.read_text()
    anchor = src.index("# Daily sync")
    block = src[anchor:anchor + 2000]
    # Count `0 13 * * *` references — should be at least 2
    # (the fallback_cron literal + the fallback branch usage).
    assert block.count('"0 13 * * *"') >= 2 or \
           block.count("'0 13 * * *'") >= 2 or \
           (block.count('"0 13 * * *"') + block.count("fallback_cron")) >= 3, (
        "The fallback string should be referenced consistently "
        "in both validation gates."
    )


def test_placement_temp_unlink_wrapped_in_try_except():
    """The pre-clean `dst_tmp.unlink()` in `_safe_link_or_copy`
    must be wrapped so an OSError doesn't propagate out of
    place_theme. Pre-fix a stale temp file with permission
    issues crashed the entire job."""
    src = PLACEMENT_PY.read_text()
    fn_start = src.index("def _safe_link_or_copy(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The unlink must be inside a try block.
    unlink_idx = fn_body.index("dst_tmp.unlink()")
    # Walk back for the nearest `try:` opener; must precede the
    # unlink within reasonable distance.
    try_idx = fn_body.rfind("try:", 0, unlink_idx)
    assert try_idx > -1 and (unlink_idx - try_idx) < 200, (
        "v1.15.117: dst_tmp.unlink() pre-clean must run inside a "
        "try/except — pre-fix it propagated OSError out of "
        "place_theme on stale-temp + permission edge cases."
    )
    # And the except must catch OSError (or broader).
    after_unlink = fn_body[unlink_idx:unlink_idx + 400]
    assert "except OSError" in after_unlink or \
           "except Exception" in after_unlink, (
        "The except clause must catch OSError so the placement "
        "can degrade gracefully (hardlink → copy via overwrite)."
    )


def test_placement_temp_unlink_logs_at_warning():
    """The except branch must log at WARNING — operators need
    visibility into stale-temp situations (could indicate FS
    issues), but the placement shouldn't fail."""
    src = PLACEMENT_PY.read_text()
    fn_start = src.index("def _safe_link_or_copy(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "log.warning" in fn_body, (
        "v1.15.117: the stale-temp pre-clean failure should log "
        "at WARNING for operator visibility."
    )
