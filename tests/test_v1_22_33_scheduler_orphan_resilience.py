"""v1.22.33 (audit) — scheduler enqueue atomicity + orphan-scan resilience.

Tag H (the two reachable items; the other three audit candidates were refuted —
tmdb negative-cache already implemented, _stuck_job_sweep's lexical ISO compare
is correct, placement's partial-temp self-heals via the pre-clean):

1. scheduler._enqueue_sync ran its "don't double-enqueue" SELECT + the INSERT in
   autocommit, so two enqueuers racing in the same instant (cron tick vs a manual
   /api/sync/now) could both pass the check and both insert. Now wrapped in a
   BEGIN IMMEDIATE transaction so the write lock serializes the check-then-insert.

2. orphan_scan.scan_plex_upload_placements walked every plex_upload placement
   calling plex.get_themes per row with NO per-row guard — one rk's unexpected
   raise aborted the whole diagnostic sweep, leaving every later placement
   unscanned. Now get_themes (and the progress callback) are guarded per row.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
SCHEDULER_PY = (REPO / "app" / "core" / "scheduler.py").read_text()
ORPHAN_PY = (REPO / "app" / "core" / "orphan_scan.py").read_text()
NOW = "2026-06-08T00:00:00+00:00"


# ── (1) _enqueue_sync ─────────────────────────────────────────


def test_enqueue_sync_does_not_double_enqueue(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    from app.core.scheduler import _enqueue_sync
    _enqueue_sync(db)
    _enqueue_sync(db)  # second call sees the pending row → no-op
    with sqlite3.connect(db) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_type='sync'").fetchone()[0]
    assert n == 1, "the pending-guard must prevent a second sync job"


def test_enqueue_sync_wrapped_in_transaction():
    i = SCHEDULER_PY.index("def _enqueue_sync(")
    body = SCHEDULER_PY[i:i + 900]
    assert "with get_conn(db_path) as conn, transaction(conn):" in body, (
        "v1.22.33: the check-then-insert must be one BEGIN IMMEDIATE txn")


# ── (2) orphan_scan per-row resilience ────────────────────────


class _FakePlex:
    """get_themes raises for one rk, returns a healthy empty themes body
    for the rest."""
    def __init__(self, raise_for):
        self.raise_for = raise_for

    def get_themes(self, rating_key):
        if rating_key == self.raise_for:
            raise RuntimeError("simulated Plex client hiccup")
        return {"ok": True, "body": {"MediaContainer": {"Metadata": []}}}


def _seed_two_uploads(db):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        for tid, rk in ((201, "RK1"), (202, "RK2")):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " guid_tmdb, title, year, has_theme, folder_path, first_seen_at,"
                " last_seen_at) VALUES (?, '1','movie',?,?,'2020',1,?,?,?)",
                (rk, tid, f"M{tid}", f"/data/M{tid}", NOW, NOW))
            conn.execute(
                "INSERT INTO placements (media_type, tmdb_id, section_id,"
                " media_folder, placed_at, placement_kind, plex_refreshed,"
                " provenance, edition_key) VALUES ('movie',?,'1','',?,"
                "'plex_upload',1,'auto','')", (tid, NOW))
        conn.commit()


def test_orphan_scan_one_raising_rk_does_not_abort_sweep(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    _seed_two_uploads(db)
    from app.core.orphan_scan import scan_plex_upload_placements
    # RK1 raises; pre-fix this aborts the whole loop → RK2 never scanned.
    findings = scan_plex_upload_placements(db, _FakePlex(raise_for="RK1"))
    assert len(findings) == 2, (
        "both placements must produce a finding — a raising rk must not abort "
        "the sweep")
    drift = {f.get("rk"): f.get("drift_type") for f in findings}
    assert drift.get("RK1") == "plex_fetch_failed", (
        "the raising rk must be recorded as a fetch failure, not propagate")


def test_orphan_scan_guards_get_themes():
    i = ORPHAN_PY.index("themes_resp = plex.get_themes(rating_key=rk)")
    block = ORPHAN_PY[i - 200:i + 200]
    assert "try:" in block and "themes_resp = {\"ok\": False," in block, (
        "v1.22.33: get_themes must be wrapped in a per-row try")


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
