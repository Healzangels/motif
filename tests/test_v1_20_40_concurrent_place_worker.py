"""v1.20.40 — dedicated place worker runs concurrently with downloads.

v1.20.38 made place-as-you-go work by claiming place jobs ahead of
downloads on the single general worker. But the user hit the failure mode:
with a big place backlog (1,082 queued) the worker drained EVERY place
before any download, so 232 queued downloads never started. "bouncing
between placing and downloading with the download never starting."

Fix: place-as-you-go is now structural, not priority-based. The general
pool is split into download worker(s) (_DOWNLOAD_JOB_TYPES) + ONE
dedicated place worker (_PLACE_JOB_TYPES) that runs CONCURRENTLY. A
finished download enqueues a place the idle place worker picks up at
once — themes go live as they download, and neither tier starves the
other. The claim order is back to plain id-ASC FIFO.

Also: the drawer's ACTIVE sort gained a stable kind-priority tiebreak so
the two now-concurrent queue cards don't swap positions every poll.
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()


def _seed_job(db, job_type):
    with sqlite3.connect(db) as c:
        cur = c.execute(
            "INSERT INTO jobs (job_type, status, created_at) "
            "VALUES (?, 'pending', ?)",
            (job_type, "2026-05-30T00:00:00+00:00"))
        c.commit()
        return cur.lastrowid


def _worker(settings, job_types):
    from app.core.worker import Worker, TokenBucket
    bucket = TokenBucket(rate=10_000, capacity=10_000, period=3600.0)
    return Worker(settings=settings, stop_event=threading.Event(),
                  bucket=bucket, job_type_filter=job_types, name="test")


def _settings(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.db import init_db
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    return s


# ── segregation: each worker sees only its tier ─────────────


def test_download_worker_claims_only_downloads(tmp_path, monkeypatch):
    from app.core.worker import _DOWNLOAD_JOB_TYPES
    s = _settings(tmp_path, monkeypatch)
    _seed_job(s.db_path, "download")   # id 1
    _seed_job(s.db_path, "place")      # id 2
    w = _worker(s, _DOWNLOAD_JOB_TYPES)
    assert w._claim_next_job()["job_type"] == "download"
    # the queued place is invisible to the download worker — it can't
    # block or be drained by it.
    assert w._claim_next_job() is None


def test_place_worker_claims_only_places(tmp_path, monkeypatch):
    from app.core.worker import _PLACE_JOB_TYPES
    s = _settings(tmp_path, monkeypatch)
    _seed_job(s.db_path, "download")   # id 1
    _seed_job(s.db_path, "place")      # id 2
    w = _worker(s, _PLACE_JOB_TYPES)
    assert w._claim_next_job()["job_type"] == "place"
    assert w._claim_next_job() is None  # download not in filter


def test_download_starts_even_with_a_place_backlog(tmp_path, monkeypatch):
    """The v1.20.38 regression repro: a queued place must NOT stop the
    download worker from claiming its download. Both claim concurrently."""
    from app.core.worker import _DOWNLOAD_JOB_TYPES, _PLACE_JOB_TYPES
    s = _settings(tmp_path, monkeypatch)
    _seed_job(s.db_path, "place")      # id 1 — a backlog place, enqueued first
    _seed_job(s.db_path, "place")      # id 2
    _seed_job(s.db_path, "download")   # id 3
    dl_w = _worker(s, _DOWNLOAD_JOB_TYPES)
    pl_w = _worker(s, _PLACE_JOB_TYPES)
    # The download worker claims the download even though 2 places sit
    # ahead of it in id order — they're on the other worker.
    assert dl_w._claim_next_job()["job_type"] == "download"
    assert pl_w._claim_next_job()["job_type"] == "place"


# ── wiring ───────────────────────────────────────────────────


def test_place_worker_spawned():
    anchor = WORKER_PY.index("def start_worker(")
    body = WORKER_PY[anchor:anchor + 8000]
    assert "job_type_filter=_PLACE_JOB_TYPES" in body
    assert 'name="motif-worker-place"' in body
    assert "job_type_filter=_DOWNLOAD_JOB_TYPES" in body


def test_claim_is_plain_fifo_again():
    anchor = WORKER_PY.index("def _claim_next_job(self)")
    body = WORKER_PY[anchor:anchor + 1800]
    assert 'sql += " ORDER BY id ASC LIMIT 1"' in body
    assert "CASE WHEN job_type = 'download'" not in body


def test_active_sort_has_stable_kind_tiebreak():
    anchor = OPS_JS.index("const active = ops.filter")
    body = OPS_JS[anchor:anchor + 1500]
    assert "OP_MINI_PRIORITY[a.kind]" in body
    assert "if (pa !== pb) return pa - pb" in body


def test_v1_20_40_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
