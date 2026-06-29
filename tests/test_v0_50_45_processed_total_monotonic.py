"""v0.50.45 — drawer processed_total stays monotonic across stage boundaries.

Two parked drawer-audit items (D2 + D3), same class as the v0.50.41 bulk_lps fix:
the op-wide processed_total counter (which feeds RUN INSIGHT avg/s + the live
throughput samples) jumped BACKWARD at a stage boundary.

  D2 cloud_themes_backup: the walk stage climbed processed_total to the candidate
     count (~3,883), then the download stage reset it to 0. Now the walk count is
     carried forward (walked_count[0] + i) so it's monotonic.
  D3 sync remote/snapshot per-item fetch: the per-media-type base was stats.X_seen
     (FLUSHED, excludes fetch errors) while the stamp added `completed` (fetched,
     incl errors), so each movie→tv→collection boundary jumped backward by the prior
     type's error count. Now the base is a running `completed` total (_remote_done).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API = (REPO / "app" / "web" / "api.py").read_text()
SYNC = (REPO / "app" / "core" / "sync.py").read_text()


def _cloud_backup_fn() -> str:
    i = API.index("def _cloud_themes_backup_run(")
    return API[i:i + 13000]


# ── D2: cloud_themes_backup walk → download carry-forward ──────────

def test_walk_count_captured_during_walk():
    fn = _cloud_backup_fn()
    assert "walked_count = [0]" in fn
    assert "walked_count[0] = processed" in fn


def test_download_stage_carries_the_walk_count_forward():
    fn = _cloud_backup_fn()
    # init no longer resets to 0
    assert "processed_total=walked_count[0]," in fn
    # every per-target stamp adds the walk base
    assert fn.count("processed_total=walked_count[0] + i,") == 3
    # the backward-jumping forms are gone from the worker
    assert "processed_total=0," not in fn
    assert "processed_total=i," not in fn


# ── D3: sync remote/snapshot per-item fetch base ──────────────────

def test_remote_fetch_base_is_running_completed_not_seen():
    # accumulator declared, used as the base, and advanced by `completed`
    assert "_remote_done = 0" in SYNC
    assert "media_processed_base = _remote_done" in SYNC
    assert "_remote_done += completed" in SYNC
    # the stamp still adds the per-type completed counter to the base
    assert "processed_total=media_processed_base + completed," in SYNC


def test_remote_fetch_base_no_longer_uses_seen_counts():
    # the stats.X_seen base (which excluded fetch errors → backward jump) is gone
    assert "media_processed_base = stats.movies_seen" not in SYNC
    assert "media_processed_base = (\n" not in SYNC
