"""v1.23.18 — restore hardening (code-review fixes).

Five fixes from the xhigh code review of the v1.23.15-17 backup feature:
1. apply_pending_restore ABORTS the swap if the pre-restore safety
   backup can't be written (was: proceed → destroy live db with no undo).
2. The safety copy uses a distinct motif-prerestore-<ts>.db name —
   collision-proof (no same-second clash with a routine backup) AND
   exempt from retention pruning (the undo must survive).
3. create_backup cleans up a partial dest file if VACUUM INTO fails
   mid-write (disk full) instead of leaving a truncated snapshot.
4. main.py applies the restore before the first log_event (flusher
   connection race — pinned in test_v1_23_17).
5. upload-restore reads in capped chunks, not one unbounded read (OOM).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.core import db_backup
from app.core.db import CURRENT_SCHEMA_VERSION


REPO = Path(__file__).resolve().parent.parent


def _make_db(path: Path, *, marker: str) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE schema_version "
                     "(version INTEGER PRIMARY KEY, applied_at TEXT)")
        conn.execute("INSERT INTO schema_version VALUES (?, '2026-01-01')",
                     (CURRENT_SCHEMA_VERSION,))
        conn.execute("CREATE TABLE marker (v TEXT)")
        conn.execute("INSERT INTO marker VALUES (?)", (marker,))
        conn.commit()


# ── fix 1: safety-backup failure ABORTS the swap ─────────────


def test_safety_backup_failure_aborts_restore_keeping_live(tmp_path, monkeypatch):
    live = tmp_path / "motif.db"
    _make_db(live, marker="LIVE")
    snap = tmp_path / "snap.db"
    _make_db(snap, marker="SNAP")
    db_backup.stage_restore(live, snap)

    # Force the pre-restore safety create_backup to fail.
    def _boom(*a, **k):
        raise OSError("disk full")
    monkeypatch.setattr(db_backup, "create_backup", _boom)

    res = db_backup.apply_pending_restore(
        live, tmp_path, now_stamp="20260612-150000")
    assert res and res["applied"] is False
    assert "safety backup failed" in res["error"]
    # The live db is UNTOUCHED (still LIVE) and the pending file is KEPT
    # for a retry once the fault clears.
    with sqlite3.connect(live) as conn:
        assert conn.execute("SELECT v FROM marker").fetchone()[0] == "LIVE"
    assert db_backup.restore_pending_path(live).exists()


# ── fix 2: prerestore copy — distinct name, prune-exempt ─────


def test_apply_uses_prerestore_name(tmp_path):
    live = tmp_path / "motif.db"
    _make_db(live, marker="LIVE")
    snap = tmp_path / "snap.db"
    _make_db(snap, marker="SNAP")
    db_backup.stage_restore(live, snap)
    res = db_backup.apply_pending_restore(
        live, tmp_path, now_stamp="20260612-150000")
    assert res["applied"] is True
    assert res["safety_backup"] == "motif-prerestore-20260612-150000.db"
    # it's a valid backup name (listable / downloadable / restorable).
    assert db_backup.is_backup_name(res["safety_backup"])


def test_prerestore_copy_collision_proof(tmp_path):
    """A routine backup at the same second must NOT block the safety
    copy (distinct name shape)."""
    live = tmp_path / "motif.db"
    _make_db(live, marker="LIVE")
    # routine backup already exists for this exact second.
    db_backup.create_backup(live, tmp_path, now_stamp="20260612-150000")
    snap = tmp_path / "snap.db"
    _make_db(snap, marker="SNAP")
    db_backup.stage_restore(live, snap)
    res = db_backup.apply_pending_restore(
        live, tmp_path, now_stamp="20260612-150000")
    assert res["applied"] is True  # no FileExistsError collision
    assert res["safety_backup"] == "motif-prerestore-20260612-150000.db"


def test_prerestore_copy_exempt_from_prune(tmp_path):
    live = tmp_path / "motif.db"
    _make_db(live, marker="LIVE")
    # one prerestore copy + several routine backups.
    db_backup.create_backup(live, tmp_path, now_stamp="20260101-000000",
                           prerestore=True)
    for s in ("20260102-000000", "20260103-000000", "20260104-000000"):
        db_backup.create_backup(live, tmp_path, now_stamp=s)
    removed = db_backup.prune_backups(tmp_path, retention=1)
    # retention=1 keeps the newest ROUTINE backup; the two older routine
    # ones go; the prerestore copy is NEVER pruned.
    assert "motif-prerestore-20260101-000000.db" not in removed
    names = [b.name for b in db_backup.list_backups(tmp_path)]
    assert "motif-prerestore-20260101-000000.db" in names
    assert "motif-20260104-000000.db" in names      # newest routine kept
    assert "motif-20260102-000000.db" not in names  # old routine pruned


def test_prerestore_listed_and_resolvable(tmp_path):
    live = tmp_path / "motif.db"
    _make_db(live, marker="LIVE")
    bf = db_backup.create_backup(live, tmp_path, now_stamp="20260612-150000",
                                prerestore=True)
    assert bf.name == "motif-prerestore-20260612-150000.db"
    assert db_backup.resolve_backup(tmp_path, bf.name) is not None
    assert bf.name in [b.name for b in db_backup.list_backups(tmp_path)]


# ── fix 3: partial-file cleanup on VACUUM failure ────────────


def test_create_backup_cleans_partial_on_vacuum_failure(tmp_path, monkeypatch):
    live = tmp_path / "motif.db"
    _make_db(live, marker="LIVE")
    real_connect = sqlite3.connect

    class _FakeConn:
        def __init__(self, inner):
            self._inner = inner
        def execute(self, sql, *a):
            if sql.startswith("VACUUM INTO"):
                # simulate a partial dest written before the failure.
                dest = sql.split("'", 2)[1]
                Path(dest).write_bytes(b"partial garbage")
                raise sqlite3.OperationalError("disk I/O error")
            return self._inner.execute(sql, *a)
        def close(self):
            self._inner.close()

    monkeypatch.setattr(db_backup.sqlite3, "connect",
                        lambda p: _FakeConn(real_connect(p)))
    with pytest.raises(sqlite3.OperationalError):
        db_backup.create_backup(live, tmp_path, now_stamp="20260612-150000")
    # the partial file must have been removed, not left to pollute the list.
    assert not (db_backup.backups_dir(tmp_path)
                / "motif-20260612-150000.db").exists()
    assert db_backup.list_backups(tmp_path) == []


# ── fix 5: upload OOM guard (source shape) ───────────────────


def test_upload_reads_in_capped_chunks_not_one_unbounded_read():
    """The cap must bound peak memory: a chunked read with a running
    size check, not `data = await upload.read()` then a post-hoc len()."""
    api = (REPO / "app" / "web" / "api.py").read_text()
    i = api.index("api_admin_database_restore_upload")
    block = api[i:i + 2000]
    # chunked read with a running cap check inside the loop.
    assert "while True:" in block
    assert "await upload.read(4 * 1024 * 1024)" in block, (
        "upload must stream in bounded chunks, not one unbounded read"
    )
    assert 'raise HTTPException(status_code=413' in block
