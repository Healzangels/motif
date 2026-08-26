"""v0.51.294 — holistic review wave 3: worker + sync fixes.

Five confirmed findings:
  1. worker: the adaptive cooldown pre-flight ran AFTER bucket.acquire(),
     so a cooldown bounce blocked on a shared rate token and then threw it
     away on the no-op re-queue. Hoisted above the acquire.
  2. worker: four 'themes_dir not configured' bails raised plain
     RuntimeError, burning the 3-attempt backoff budget and terminal-
     failing in ~6 minutes — while their own log lines promised the job
     would wait for /settings. Now _JobTransient(retry 1h), the low-disk
     guard's seam.
  3. worker: the class-9 breadcrumb in run()'s finally called job.get("id")
     on a sqlite3.Row (no .get) — the breadcrumb itself crashed the worker
     thread.
  4. sync: the _GIT_MIRROR_MAX_CHANGES bail raised from the differential
     upsert where no handler exists, so the designed git→snapshot cascade
     never fired. run_sync now probes list_changes() inside the cascade
     try (memoized — the upsert reuses it).
  5. sync: the snapshot tier persisted its conditional-GET validators at
     download time — a run that died mid-upsert made the retry 304-skip
     the never-applied delta. Validators now stage and persist via
     commit_sync_ok() at the same success point as the git baseline.
"""
from __future__ import annotations

import json
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WORKER = (REPO / "app" / "core" / "worker.py").read_text()
SYNC = (REPO / "app" / "core" / "sync.py").read_text()


# ── 1. cooldown pre-flight precedes the token acquire ────────


def test_cooldown_preflight_runs_before_bucket_acquire():
    i_gate = WORKER.index('self.settings.download_rate_mode == "adaptive"')
    i_acquire = WORKER.index("self.bucket.acquire()")
    assert i_gate < i_acquire, (
        "a cooldown bounce must neither block on nor consume a shared "
        "rate-limit token — the gate's own 'BEFORE anything is touched' "
        "contract")


# ── 2. config bails ride the attempt-free transient seam ─────


def test_config_bails_are_transient_not_terminal():
    assert 'raise RuntimeError("themes_dir not configured")' not in WORKER, (
        "a plain RuntimeError burns the 3-attempt budget and terminal-fails "
        "in ~6min, contradicting the bail's own 'set it on /settings' log")
    assert WORKER.count(
        '_JobTransient("themes_dir not configured — set it on /settings"'
    ) == 4, "download + place + scan + adopt all wait for config now"


# ── 3. the breadcrumb no longer crashes the thread ───────────


def test_finally_breadcrumb_uses_row_subscript():
    i = WORKER.index("clear_download_progress(%s) failed")
    blk = WORKER[i:WORKER.index("Worker loop stopped", i)]
    assert 'job["id"], e,' in blk
    assert 'job.get("id")' not in WORKER, (
        "sqlite3.Row has no .get — the class-9 breadcrumb itself raised "
        "AttributeError out of the finally and restarted the worker thread")


# ── 4. oversized git diffs cascade instead of failing ────────


def test_git_threshold_probed_inside_the_cascade_window():
    i_log = SYNC.index("git mirror \"\n                                    f\"acquired from")
    i_except = SYNC.index("except _GitMirrorError as e:", i_log)
    window = SYNC[i_log:i_except]
    assert "git_mirror.list_changes()" in window, (
        "the _GIT_MIRROR_MAX_CHANGES bail must raise where the "
        "git→snapshot cascade handler can catch it — from the upsert it "
        "failed the whole sync")


# ── 5. snapshot validators persist only on success ───────────


def test_download_stages_validators_instead_of_persisting():
    i = SYNC.index("class _DatabaseSnapshot")
    j = SYNC.index("\nclass ", i + 10)
    cls = SYNC[i:j]
    assert "self._pending_meta = meta_payload" in cls
    # the ONLY meta_path write lives in commit_sync_ok now.
    assert cls.count("self.meta_path.write_text") == 1
    k = cls.index("def commit_sync_ok")
    assert "self.meta_path.write_text" in cls[k:cls.index("def ", k + 10)]


def test_snapshot_commit_writes_staged_meta_once(tmp_path):
    from app.core.sync import _DatabaseSnapshot
    snap = _DatabaseSnapshot(tmp_path / "motif.db", "op", "http://x/t.tar.gz",
                             lambda: False)
    snap.cache_dir.mkdir(parents=True, exist_ok=True)
    snap.commit_sync_ok()                      # nothing staged → no file
    assert not snap.meta_path.exists()
    snap._pending_meta = {"etag": "abc123"}
    snap.commit_sync_ok()
    assert json.loads(snap.meta_path.read_text()) == {"etag": "abc123"}
    assert snap._pending_meta is None, "staged meta consumed"


def test_snapshot_commit_called_at_the_git_gate_shape():
    i = SYNC.index("snapshot.commit_sync_ok()")
    blk = SYNC[max(0, i - 400):i]
    assert "detection_ok and stats.errors == 0" in blk, (
        "the validator commit must share the git baseline's success gate — "
        "persisting on a dirty run recreates the 304-skip hole")


def test_v0_51_294_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.294: " in init_py
