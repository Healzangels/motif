"""v1.23.15: on-disk SQLite database backups.

the user: "create a new settings feature to backup the database." This
module produces a CONSISTENT snapshot of /config/motif.db. A raw
shutil.copy can capture a torn write if the worker or web layer is
mid-transaction (especially in WAL mode where the -wal file holds
uncommitted pages), so we use SQLite's `VACUUM INTO`, which holds a
read transaction and writes a fully-valid, compacted database file —
the sqlite.org-recommended way to back up a live database.

Backups land in <config_dir>/backups/ as motif-YYYYMMDD-HHMMSS.db.
That dir is on /config (the appdata mount — same place as motif.db,
motif.yaml, cookies.txt; already writable + on the Unraid array).

This module is clock-free: the caller supplies the timestamp string
so the behavior is deterministic and testable (mirrors the
events.now_iso injection pattern). list/delete/prune operate on the
dir by validated filename; the API layer serves downloads.
"""
from __future__ import annotations

import logging
import os
import re
import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)

BACKUP_SUBDIR = "backups"
# v1.23.17: a staged restore lands here next to motif.db. The swap is
# applied at BOOT (apply_pending_restore, before any connection opens)
# — never live, so open FDs + WAL replay can't corrupt the result.
RESTORE_PENDING_SUFFIX = ".restore-pending"

# motif-YYYYMMDD-HHMMSS.db — the timestamp is the only variable part.
# Anchored so a stray/hostile filename in the dir can't be served or
# deleted through the API: is_backup_name() is the gate every
# name-addressed operation (download, delete) runs first, blocking
# path traversal + arbitrary-file access.
_BACKUP_RE = re.compile(r"^motif-(\d{8}-\d{6})\.db$")
# v1.23.18: pre-restore safety copies carry a distinct prefix. Two
# reasons (code review): (1) a routine same-second backup can't make
# the safety copy collide (different name shape), and (2) prune_backups
# leaves them alone — the restore undo must survive retention. They're
# still listed/downloadable/restorable (is_backup_name accepts both).
_PRERESTORE_RE = re.compile(r"^motif-prerestore-(\d{8}-\d{6})\.db$")


@dataclass
class BackupFile:
    name: str
    size: int
    created_at: str  # ISO-8601 derived from the embedded stamp (UTC)


def backups_dir(config_dir: Path) -> Path:
    return config_dir / BACKUP_SUBDIR


def is_backup_name(name: str) -> bool:
    """True iff `name` is a motif-<ts>.db (or motif-prerestore-<ts>.db,
    v1.23.18) filename with no path separators. The download/delete
    endpoints call this before touching any file by name — without it a
    crafted name could traverse out of the backups dir or address an
    arbitrary file. Both regexes are fully anchored (^…$)."""
    if "/" in name or "\\" in name or name in (".", ".."):
        return False
    return bool(_BACKUP_RE.match(name) or _PRERESTORE_RE.match(name))


def _stamp_of(name: str) -> str | None:
    """The embedded YYYYMMDD-HHMMSS for a routine OR prerestore backup
    name, else None."""
    m = _BACKUP_RE.match(name) or _PRERESTORE_RE.match(name)
    return m.group(1) if m else None


def _iso_from_stamp(stamp: str) -> str:
    """"20260612-143000" -> "2026-06-12T14:30:00+00:00". The stamp is
    minted in UTC (see api.py), so the offset is fixed."""
    d, t = stamp.split("-")
    return (f"{d[0:4]}-{d[4:6]}-{d[6:8]}T"
            f"{t[0:2]}:{t[2:4]}:{t[4:6]}+00:00")


def create_backup(db_path: Path, config_dir: Path, *,
                  now_stamp: str, prerestore: bool = False) -> BackupFile:
    """VACUUM INTO a fresh snapshot. `now_stamp` is the UTC
    YYYYMMDD-HHMMSS string (caller-supplied — this module never reads
    the clock). `prerestore=True` (v1.23.18) writes a
    motif-prerestore-<ts>.db (the restore undo copy — prune-exempt,
    collision-proof). Returns the BackupFile. Raises FileNotFoundError
    if the source DB is missing, FileExistsError if a same-second backup
    already exists (1s resolution; never clobber), and lets sqlite3
    errors propagate."""
    if not db_path.exists():
        raise FileNotFoundError(f"database not found: {db_path}")
    bdir = backups_dir(config_dir)
    bdir.mkdir(parents=True, exist_ok=True)
    prefix = "motif-prerestore-" if prerestore else "motif-"
    name = f"{prefix}{now_stamp}.db"
    if not is_backup_name(name):
        # guards against a malformed caller-supplied stamp.
        raise ValueError(f"invalid backup stamp: {now_stamp!r}")
    dest = bdir / name
    if dest.exists():
        raise FileExistsError(f"backup already exists: {name}")
    conn = sqlite3.connect(str(db_path))
    try:
        # Wait out a concurrent writer rather than failing with
        # "database is locked" — VACUUM INTO takes a read lock.
        conn.execute("PRAGMA busy_timeout = 30000")
        # VACUUM INTO can't bind parameters for its target path. The
        # path is entirely ours (config_dir/backups/motif-<validated
        # stamp>.db — never user input), but single-quote-escape
        # defensively since it interpolates into SQL.
        safe = str(dest).replace("'", "''")
        conn.execute(f"VACUUM INTO '{safe}'")
    except Exception:
        # v1.23.18: VACUUM INTO writes the dest directly (no temp+rename),
        # so a mid-write failure (disk full) leaves a partial file that
        # would pollute list_backups + count toward retention. Remove it
        # before re-raising.
        try:
            if dest.exists():
                dest.unlink()
        except OSError:
            pass
        raise
    finally:
        conn.close()
    st = dest.stat()
    log.info("database backup written: %s (%d bytes)", name, st.st_size)
    return BackupFile(name=name, size=st.st_size,
                      created_at=_iso_from_stamp(now_stamp))


def list_backups(config_dir: Path) -> list[BackupFile]:
    """Every motif-<ts>.db AND motif-prerestore-<ts>.db in the backups
    dir, newest first (by embedded stamp). Non-matching files (and
    subdirs) are ignored, not an error."""
    bdir = backups_dir(config_dir)
    if not bdir.exists():
        return []
    out: list[BackupFile] = []
    for p in bdir.iterdir():
        if not p.is_file():
            continue
        stamp = _stamp_of(p.name)
        if stamp is None:
            continue
        try:
            size = p.stat().st_size
        except OSError as e:
            log.warning("backup stat failed (%s): %s — skipping", p.name, e)
            continue
        out.append(BackupFile(name=p.name, size=size,
                              created_at=_iso_from_stamp(stamp)))
    # Sort by embedded stamp (true chronological across both name
    # shapes), newest first.
    out.sort(key=lambda b: (_stamp_of(b.name) or "", b.name), reverse=True)
    return out


def resolve_backup(config_dir: Path, name: str) -> Path | None:
    """Validated path for a named backup, or None if the name is
    invalid or the file is absent. The download endpoint uses this so
    it never serves anything outside the backups dir."""
    if not is_backup_name(name):
        return None
    p = backups_dir(config_dir) / name
    return p if p.is_file() else None


def delete_backup(config_dir: Path, name: str) -> bool:
    """Delete one backup by validated name. Returns True if a file
    was removed, False if it didn't exist. Raises ValueError on an
    invalid name (caller should 400)."""
    if not is_backup_name(name):
        raise ValueError(f"not a backup filename: {name!r}")
    p = backups_dir(config_dir) / name
    if not p.is_file():
        return False
    p.unlink()
    log.info("database backup deleted: %s", name)
    return True


def prune_backups(config_dir: Path, retention: int) -> list[str]:
    """v1.23.16: keep the newest `retention` ROUTINE backups, delete
    the older ones. `retention <= 0` keeps everything (no prune).
    Returns the names removed. Used by the scheduled-backup job after
    each new snapshot; a per-file OS error is logged + skipped, not
    fatal.

    v1.23.18: pre-restore safety copies (motif-prerestore-*) are
    EXEMPT — they're the undo for a destructive restore and must not be
    auto-deleted by retention. Only motif-<ts>.db routine snapshots
    count toward + are eligible for the prune."""
    if retention <= 0:
        return []
    routine = [b for b in list_backups(config_dir)
               if _BACKUP_RE.match(b.name)]  # excludes prerestore copies
    doomed = routine[retention:]  # newest-first → tail is oldest
    removed: list[str] = []
    bdir = backups_dir(config_dir)
    for b in doomed:
        try:
            (bdir / b.name).unlink()
            removed.append(b.name)
        except OSError as e:
            log.warning("backup prune: couldn't delete %s: %s", b.name, e)
    if removed:
        log.info("backup retention pruned %d old snapshot(s): %s",
                 len(removed), ", ".join(removed))
    return removed


# ── restore (v1.23.17) ───────────────────────────────────────


def restore_pending_path(db_path: Path) -> Path:
    return db_path.with_name(db_path.name + RESTORE_PENDING_SUFFIX)


@dataclass
class RestoreCheck:
    ok: bool
    schema_version: int | None
    error: str | None


def inspect_restore_source(path: Path) -> RestoreCheck:
    """Validate a candidate restore file: real SQLite header, opens
    read-only, passes integrity_check, carries a schema_version that
    is NOT newer than this build (a newer schema means the file came
    from a future motif the current code can't read — refuse the
    downgrade). Older/equal is fine (init_db migrates forward)."""
    from .db import CURRENT_SCHEMA_VERSION
    try:
        with open(path, "rb") as f:
            head = f.read(16)
    except OSError as e:
        return RestoreCheck(False, None, f"unreadable: {e}")
    if head != b"SQLite format 3\x00":
        return RestoreCheck(False, None,
                            "not a SQLite database (bad file header)")
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            ic = conn.execute("PRAGMA integrity_check").fetchone()
            has_sv = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='schema_version'").fetchone()
            sv_row = (conn.execute(
                "SELECT MAX(version) FROM schema_version").fetchone()
                if has_sv else None)
        finally:
            conn.close()
    except sqlite3.Error as e:
        return RestoreCheck(False, None, f"not a valid database: {e}")
    if not ic or ic[0] != "ok":
        return RestoreCheck(False, None,
                            f"integrity check failed: {ic[0] if ic else '?'}")
    sv = sv_row[0] if sv_row else None
    if sv is None:
        return RestoreCheck(False, None,
                            "no schema_version table — not a motif database")
    if sv > CURRENT_SCHEMA_VERSION:
        return RestoreCheck(
            False, sv,
            f"snapshot is schema v{sv}, newer than this build "
            f"(v{CURRENT_SCHEMA_VERSION}) — upgrade motif before restoring")
    return RestoreCheck(True, sv, None)


def stage_restore(db_path: Path, source_path: Path) -> RestoreCheck:
    """Validate `source_path`, then copy it to the restore-pending
    path next to db_path (temp-then-rename so a half-written pending
    file is never applied). The swap happens at the next boot via
    apply_pending_restore. Raises ValueError if validation fails."""
    check = inspect_restore_source(source_path)
    if not check.ok:
        raise ValueError(check.error or "invalid restore source")
    pending = restore_pending_path(db_path)
    pending.parent.mkdir(parents=True, exist_ok=True)
    tmp = pending.with_name(pending.name + ".tmp")
    shutil.copy2(source_path, tmp)
    os.replace(tmp, pending)
    log.info("restore staged → %s (schema v%s); applies on next restart",
             pending.name, check.schema_version)
    return check


def cancel_pending_restore(db_path: Path) -> bool:
    """Drop a staged restore before it's applied. Returns True if one
    was pending."""
    pending = restore_pending_path(db_path)
    if pending.exists():
        pending.unlink()
        log.info("pending restore cancelled")
        return True
    return False


def apply_pending_restore(db_path: Path, config_dir: Path, *,
                         now_stamp: str) -> dict | None:
    """BOOT hook (call before opening any DB connection). If a
    restore-pending file exists: re-validate it, snapshot the CURRENT
    db as a pre-restore safety copy, drop the old db's stale WAL/-shm
    sidecars, then atomically swap the pending file into place. Returns
    a status dict, or None if nothing was pending. NEVER raises — a
    failed restore must not block boot; it leaves the live db untouched
    and logs WHY (cold-path rule).

    v1.23.18: if the pre-restore safety copy can't be written, the swap
    is ABORTED (the live db is never destroyed without an undo) and the
    pending file is kept for a retry once the fault is cleared."""
    pending = restore_pending_path(db_path)
    if not pending.exists():
        return None
    try:
        check = inspect_restore_source(pending)
        if not check.ok:
            # A corrupt pending file must NOT clobber a good live db.
            log.error("pending restore REJECTED at boot (%s) — keeping the "
                      "current database; discarding the bad pending file",
                      check.error)
            try: pending.unlink()
            except OSError: pass
            return {"applied": False, "error": check.error}
        safety = None
        if db_path.exists():
            try:
                # v1.23.18: prerestore=True → motif-prerestore-<ts>.db,
                # which can't collide with a routine same-second backup
                # and is exempt from retention pruning (the undo must
                # survive).
                bf = create_backup(db_path, config_dir,
                                   now_stamp=now_stamp, prerestore=True)
                safety = bf.name
            except Exception as e:
                # v1.23.18 (code review): ABORT the restore rather than
                # destroy the live db with no undo. Pre-fix this proceeded
                # to os.replace, irreversibly clobbering the prior database
                # whenever the safety copy failed (disk full / perms) — the
                # exact data-loss the safety copy exists to prevent. Leave
                # the pending file in place so a retry (free space / fix
                # perms → restart) still works.
                log.error("pre-restore safety backup FAILED (%s) — ABORTING "
                          "restore to protect the current database; the "
                          "pending snapshot is kept for a retry after the "
                          "fault is cleared", e)
                return {"applied": False,
                        "error": f"safety backup failed: {e}"}
        # The old db's WAL/-shm belong to the OUTGOING inode; if left in
        # place SQLite would try to replay them onto the restored file
        # and corrupt it. Remove them around the swap.
        # v0.51.15 (audit #30): errno-aware — ENOENT (no sidecar) is the
        # benign no-op; any OTHER unlink failure means the stale WAL is
        # still in place, and proceeding with os.replace risks the exact
        # corruption the comment above describes. ABORT the swap (the
        # pending snapshot is kept for a retry, mirroring the safety-backup
        # failure branch above); the old bare `except OSError: pass`
        # conflated the two and left no breadcrumb on the dangerous case.
        for sidecar in (db_path.with_name(db_path.name + "-wal"),
                        db_path.with_name(db_path.name + "-shm")):
            try:
                sidecar.unlink()
            except FileNotFoundError:
                pass
            except OSError as e:
                log.error(
                    "apply_pending_restore: could not remove stale %s (%s) "
                    "— ABORTING the swap; a leftover WAL would replay onto "
                    "the restored file and corrupt it. Pending snapshot "
                    "kept for retry.", sidecar.name, e)
                return {"applied": False,
                        "error": f"stale {sidecar.name} unlink failed: {e}"}
        os.replace(pending, db_path)
        log.warning("DATABASE RESTORED from staged snapshot (schema v%s). "
                    "Pre-restore safety backup: %s",
                    check.schema_version, safety or "(none)")
        return {"applied": True, "schema_version": check.schema_version,
                "safety_backup": safety}
    except Exception as e:
        log.error("apply_pending_restore failed (%s) — current database "
                  "left in place", e)
        return {"applied": False, "error": str(e)}
