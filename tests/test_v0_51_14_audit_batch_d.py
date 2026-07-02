"""v0.51.14 — round-4 audit Batch D (Plex/sync correctness).

#4: a first-run / baseline-reset git walk has changeset removed=[] by
  construction, so the changeset drop detector was structurally blind to every
  upstream removal across the reset window — while the full-tree upsert had
  just refreshed last_seen_sync_at on all surviving rows (exactly the full-walk
  detector's precondition). run_sync now routes reset runs to the full-walk
  detector, excluding failed reads per-item (v1.21.44 pattern).
#5: one chronically-malformed upstream blob pinned the git baseline forever
  (errors != 0 → never advance → same ever-growing delta every run). If the
  SAME baseline yields the SAME failed-path set two runs straight, the escape
  advances anyway (runtime key git_chronic_read_failures).
#6: the in-place has_theme 1→0 "backup ready" detector gated only on
  user_overrides.intent='backup' — walker-staged cloud backups (local_files
  source_kind='plex_cloud' / reason='backup_only', NO override row) fired no
  notification in the Plex-Pass-lapse mode the v1.19.42 pipe was built for.
#7: the reaper's tier-2 find_theme_sidecar_path ran INSIDE the reap's BEGIN
  IMMEDIATE txn — a stalled /data mount held the SQLite writer lock
  indefinitely. Now deadline-bounded (v1.22.65 pattern) + skip-after-first-
  stall; pinned in test_v1_19_41 (timeout + shutdown(wait=False) asserts).
"""
from __future__ import annotations

from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
PLEX_ENUM_PY = (REPO / "app" / "core" / "plex_enum.py").read_text()


# ── #4: baseline-reset detection routing ────────────────────────────────

def test_git_mirror_reports_baseline_reset(tmp_path):
    from tests.test_sync_git import (  # reuse the dulwich scaffolding
        _init_remote_at_v1, _advance_remote_to_v2, _new_mirror, _start_op)
    from app.core.db import init_db
    db = tmp_path / "motif.db"
    init_db(db)
    _start_op(db)
    remote, rrepo, sha1 = _init_remote_at_v1(tmp_path)
    m1 = _new_mirror(db, remote)
    m1.acquire(None)
    assert m1.is_baseline_reset() is True, "first run = no old head = reset"
    m1.commit_sync_ok()
    # second sync with a baseline: not a reset.
    _advance_remote_to_v2(rrepo, sha1)
    m2 = _new_mirror(db, remote)
    m2.acquire(None)
    assert m2.is_baseline_reset() is False
    assert m2.baseline_key(), "baseline_key names the diff base"


def test_reset_runs_route_to_full_walk_detector():
    i = SYNC_PY.index("if ran_git_diff and git_mirror.is_baseline_reset():")
    block = SYNC_PY[i:i + 2200]
    assert "_detect_and_stamp_drops_full_walk(" in block
    assert "exclude_by_mt=errored_by_mt" in block, (
        "failed git reads must be excluded per-item (v1.21.44 pattern)")
    # the changeset detector stays the non-reset path, right after.
    assert "elif ran_git_diff:" in block


def test_git_read_failures_feed_the_exclusion_map():
    i = SYNC_PY.index("def _note_read_failure(")
    block = SYNC_PY[i:i + 900]
    assert "errored_by_mt.setdefault(media_type, set()).add(int(resolved))" in block
    assert "unresolved_failures += 1" in block


# ── #5: chronic-pin escape ──────────────────────────────────────────────

def test_chronic_read_failure_escape_present():
    i = SYNC_PY.index('_key = "git_chronic_read_failures"')
    block = SYNC_PY[i - 800:i + 1800]
    # compares baseline + sorted failed paths against the previous run.
    assert "git_mirror.baseline_key()" in block
    assert '"paths": sorted(git_failed_paths)' in block
    # on a chronic match it advances anyway; otherwise it stores and holds.
    assert block.count("git_mirror.commit_sync_ok()") == 1
    i2 = SYNC_PY.index("if ran_git_diff and detection_ok and stats.errors == 0:")
    assert i < i2, "the escape lives in the errors!=0 branch, before the clean gate"


# ── #6: walker-staged cloud backups notify on in-place 1→0 ──────────────

def test_inplace_detector_matches_walker_staged_backups():
    i = PLEX_ENUM_PY.index("_cloud_backup = None")
    block = PLEX_ENUM_PY[i:i + 1800]
    assert "source_kind = 'plex_cloud'" in block
    assert "last_place_attempt_reason = 'backup_only'" in block
    assert "SELECT 1 FROM local_files" in block
    # the widened gate fires on EITHER signal.
    assert ("if (_backup_ovr is not None\n"
            "                                    or _cloud_backup is not None):"
            ) in PLEX_ENUM_PY


def test_inplace_detector_still_prefers_override():
    # the override query runs first; the local_files probe only fires when no
    # intent='backup' override exists (keeps the dispatch's override_url path).
    i = PLEX_ENUM_PY.index("_cloud_backup = None")
    assert "if _backup_ovr is None:" in PLEX_ENUM_PY[i:i + 500]


# ── #7: bounded in-txn fs check (main pins live in test_v1_19_41) ───────

def test_reaper_fs_stall_skips_remaining_candidates():
    i = PLEX_ENUM_PY.index("_reaper_fs_stalled = False")
    assert "for cand in lost_candidates_raw:" in PLEX_ENUM_PY[i:i + 400]
    j = PLEX_ENUM_PY.index("if _folder_path and not _reaper_fs_stalled:")
    block = PLEX_ENUM_PY[j:j + 2600]
    assert "_reaper_fs_stalled = True" in block, (
        "first stall must disarm the remaining fs checks this reap")
