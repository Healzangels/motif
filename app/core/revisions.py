"""v0.51.277 — feature-brief B: theme revision history (backend).

Every meaningful canonical replacement becomes an auditable revision instead of
an opaque overwrite. The recording seam is the replacement chokepoints
themselves, BEFORE the old bytes are destroyed:

  * worker download-over-existing — the success path used to `unlink()` its
    stale-stash of the old canonical (worker.py v1.22.40); that inode IS the
    outgoing revision, so it is MOVED into the store instead. Zero copies, and
    the failure paths (which restore the stash) are untouched.
  * UPLOAD MP3 over an existing canonical — the old file is moved to the store
    before the new bytes are written (a fresh inode; in the mismatch case this
    replaces the v1.11.99 hardlink-breaking unlink, same effect + history).
  * restore itself — the outgoing current is captured first, so RESTORE is a
    transition, not a destruction, and restoring twice cannot stack duplicates
    (byte-identical content dedupes).

Retention, per the operator's decision (2026-08-24): full metadata history,
last 2 retained binaries per (media_type, tmdb_id, section_id, edition_key).
Rotation unlinks the oldest retained file and NULLs its retained_path — the
row survives as metadata-only, and the API says "re-download required" rather
than promising an impossible rollback (the brief's rule).

Retained binaries live under themes_dir/.revisions/ — INSIDE themes_dir so
moves stay same-filesystem (atomic replace, no copy), with a dot-prefix so the
orphan scan and Plex never see them as sidecars.
"""
from __future__ import annotations

import logging
from pathlib import Path

from .db import get_conn, transaction
from .events import log_event, now_iso

log = logging.getLogger(__name__)

_REV_DIR = ".revisions"
_KEEP_BINARIES = 2


def _rev_rel_path(media_type: str, tmdb_id: int, section_id: str,
                  edition_key: str, sha: str | None, rev_hint: str) -> str:
    tag = (sha or rev_hint or "nosha")[:12]
    ed = (edition_key or "std").replace("/", "_").replace(" ", "_")
    return f"{_REV_DIR}/{media_type}-{tmdb_id}-s{section_id}-{ed}-{tag}.mp3"


def capture_revision(db_path: Path, themes_dir: Path, *, media_type: str,
                     tmdb_id: int, section_id: str, edition_key: str,
                     reason: str, actor: str = "system",
                     stashed_file: Path | None = None,
                     incoming_sha: str | None = None) -> int | None:
    """Record the CURRENT canonical as a revision before it is replaced.

    `stashed_file`: the old canonical already moved aside by the caller (the
    worker's .stale stash) — it is MOVED into the store. None → the current
    canonical at local_files.file_path is COPIED (the upload/restore paths,
    where the active file must keep serving until the new bytes land).

    Returns the revision id, or None when there is nothing to record: no
    local_files row (a first add is not a change), no bytes anywhere, or the
    incoming content is byte-identical (`incoming_sha` matches — a redownload
    of the same video must not mint a meaningless revision; the caller then
    still owns disposing of its stash)."""
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT file_path, file_sha256, file_size, source_kind, "
            "       source_video_id FROM local_files "
            " WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
            "   AND COALESCE(edition_key, '') = ?",
            (media_type, tmdb_id, section_id, edition_key or "")).fetchone()
    if row is None:
        return None
    if incoming_sha and row["file_sha256"] and incoming_sha == row["file_sha256"]:
        return None                       # byte-identical: not a change
    src = stashed_file if stashed_file is not None else (
        (themes_dir / row["file_path"]) if row["file_path"] else None)
    retained_rel: str | None = None
    if src is not None and src.exists():
        retained_rel = _rev_rel_path(media_type, tmdb_id, section_id,
                                     edition_key, row["file_sha256"], now_iso())
        dest = themes_dir / retained_rel
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            if dest.exists():
                # same content already retained (sha-keyed name) — reuse it.
                if stashed_file is not None:
                    stashed_file.unlink()
            elif stashed_file is not None:
                src.replace(dest)
            else:
                import shutil
                shutil.copy2(src, dest)
        except OSError as e:
            log.warning("revisions: could not retain binary for %s/%s (%s) — "
                        "recording metadata-only: %s",
                        media_type, tmdb_id, retained_rel, e)
            retained_rel = None
    with get_conn(db_path) as conn, transaction(conn):
        cur = conn.execute(
            """INSERT INTO theme_revisions
                 (media_type, tmdb_id, section_id, edition_key, created_at,
                  source_kind, source_video_id, content_sha256, file_size,
                  reason, actor, retained_path)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (media_type, tmdb_id, section_id, edition_key or "", now_iso(),
             row["source_kind"], row["source_video_id"], row["file_sha256"],
             row["file_size"], reason, actor, retained_rel))
        rev_id = cur.lastrowid
        # retention: keep the newest _KEEP_BINARIES retained rows per key;
        # older ones rotate to metadata-only.
        stale = conn.execute(
            "SELECT id, retained_path FROM theme_revisions "
            " WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
            "   AND COALESCE(edition_key, '') = ? AND retained_path IS NOT NULL "
            " ORDER BY id DESC LIMIT -1 OFFSET ?",
            (media_type, tmdb_id, section_id, edition_key or "",
             _KEEP_BINARIES)).fetchall()
        for old in stale:
            conn.execute("UPDATE theme_revisions SET retained_path = NULL "
                         "WHERE id = ?", (old["id"],))
        # v0.51.292 (holistic review): the sha-keyed dedupe above lets SEVERAL
        # rows share one retained file (content recurrence A→B→A′ reuses the
        # sha path). Unlinking by the rotated row's path destroyed the binary
        # a NEWER row still referenced — and still reported restorable=1.
        # Collect the paths that remain referenced inside the same txn.
        _still_referenced = {r["retained_path"] for r in conn.execute(
            "SELECT DISTINCT retained_path FROM theme_revisions "
            " WHERE retained_path IS NOT NULL")}
    for old in stale:
        try:
            # v1.18.10 amplifier rule: only ever delete files THIS module put
            # in .revisions/ — never anything outside it.
            if old["retained_path"] in _still_referenced:
                continue  # v0.51.292: shared with a newer retained row
            if old["retained_path"] and old["retained_path"].startswith(_REV_DIR):
                (themes_dir / old["retained_path"]).unlink(missing_ok=True)
        except OSError as e:
            log.warning("revisions: rotation could not unlink %s: %s",
                        old["retained_path"], e)
    return rev_id


def list_revisions(db_path: Path, *, media_type: str, tmdb_id: int,
                   section_id: str | None = None,
                   edition_key: str | None = None) -> list[dict]:
    q = ("SELECT id, section_id, edition_key, created_at, source_kind, "
         "       source_video_id, content_sha256, file_size, reason, actor, "
         "       retained_path IS NOT NULL AS restorable "
         "  FROM theme_revisions WHERE media_type = ? AND tmdb_id = ?")
    args: list = [media_type, tmdb_id]
    if section_id is not None:
        q += " AND section_id = ?"; args.append(section_id)
    if edition_key is not None:
        q += " AND COALESCE(edition_key, '') = ?"; args.append(edition_key)
    q += " ORDER BY id DESC LIMIT 100"
    with get_conn(db_path) as conn:
        return [dict(r) for r in conn.execute(q, args)]


def restore_revision(db_path: Path, themes_dir: Path, *, revision_id: int,
                     actor: str = "admin") -> dict:
    """Make a retained revision the active canonical again.

    Raises ValueError with an operator-readable reason on every refusal path
    (no such revision, metadata-only, already active, no local_files row).
    The outgoing current is captured first — restore is a transition — and the
    place pipe re-serves the survivor (the v0.51.272 lesson: a carry that
    stops at the DB row leaves Plex playing nothing)."""
    with get_conn(db_path) as conn:
        rev = conn.execute("SELECT * FROM theme_revisions WHERE id = ?",
                           (revision_id,)).fetchone()
    if rev is None:
        raise ValueError("no such revision")
    if not rev["retained_path"]:
        raise ValueError("metadata-only revision — its binary was rotated out; "
                         "re-download or SET URL instead")
    src = themes_dir / rev["retained_path"]
    if not src.exists():
        raise ValueError("retained file is missing on disk")
    key = dict(media_type=rev["media_type"], tmdb_id=rev["tmdb_id"],
               section_id=rev["section_id"],
               edition_key=rev["edition_key"] or "")
    with get_conn(db_path) as conn:
        cur_row = conn.execute(
            "SELECT file_path, file_sha256 FROM local_files "
            " WHERE media_type = :media_type AND tmdb_id = :tmdb_id "
            "   AND section_id = :section_id "
            "   AND COALESCE(edition_key, '') = :edition_key", key).fetchone()
    if cur_row is None:
        raise ValueError("the row no longer has a local file to restore over")
    if cur_row["file_sha256"] and cur_row["file_sha256"] == rev["content_sha256"]:
        raise ValueError("this revision is already the active content")
    target = themes_dir / cur_row["file_path"]
    tmp = target.with_suffix(".rev-restore-tmp")
    import shutil
    target.parent.mkdir(parents=True, exist_ok=True)
    # v0.51.292 (holistic review): secure the restore-target's bytes BEFORE the
    # capture below — its keep-last-2 rotation can NULL + unlink THIS revision's
    # file (restoring the OLDER of the two retained revisions always did: the
    # new capture made it 3rd-newest), which crashed the copy at this line AND
    # permanently destroyed the binary being restored.
    shutil.copy2(src, tmp)                # COPY: the retained binary stays retained
    try:
        capture_revision(db_path, themes_dir, media_type=key["media_type"],
                         tmdb_id=key["tmdb_id"], section_id=key["section_id"],
                         edition_key=key["edition_key"],
                         reason="replaced_by_restore", actor=actor)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
    tmp.replace(target)
    size = target.stat().st_size
    import json
    # v0.51.292: restore is a byte-replacement writer like save_edit — the 11
    # loudness/norm columns must clear (the restored bytes' loudness is
    # unknown; stale norm anchors would let // UNDO LEVELING run mp3gain -u
    # against the wrong file), plus norm_plex_entry_uri (the v0.51.185 anchor).
    from .worker import _cond_columns
    _lc = _cond_columns(None, rev["content_sha256"])
    with get_conn(db_path) as conn, transaction(conn):
        conn.execute(
            "UPDATE local_files SET file_sha256 = ?, file_size = ?, "
            "       source_kind = COALESCE(?, source_kind), "
            "       source_video_id = COALESCE(?, source_video_id), "
            "       downloaded_at = ?, canonical_present = 1, "
            "       loudness_i=?, loudness_tp=?, loudness_lra=?, "
            "       loudness_measured_at=?, loudness_measured_sha256=?, "
            "       norm_state=?, norm_gain_db=?, norm_target=?, norm_at=?, "
            "       norm_orig_sha256=?, norm_orig_pcm_sha256=?, "
            "       norm_plex_entry_uri = NULL "
            " WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
            "   AND COALESCE(edition_key, '') = ?",
            (rev["content_sha256"], size, rev["source_kind"],
             rev["source_video_id"], now_iso(), *_lc, key["media_type"],
             key["tmdb_id"], key["section_id"], key["edition_key"]))
        conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, "
            "                  payload, status, created_at, next_run_at) "
            "VALUES ('place', ?, ?, ?, ?, 'pending', ?, ?)",
            (key["media_type"], key["tmdb_id"], key["section_id"],
             json.dumps({"edition_key": key["edition_key"]})
             if key["edition_key"] else "{}", now_iso(), now_iso()))
    summary = {"revision_id": revision_id, **key,
               "restored_sha256": rev["content_sha256"], "file_size": size}
    log_event(db_path, level="INFO", component="revisions",
              media_type=key["media_type"], tmdb_id=key["tmdb_id"],
              section_id=key["section_id"],
              message=(f"Revision {revision_id} restored for "
                       f"{key['media_type']}/{key['tmdb_id']} — re-place queued"),
              detail=summary)
    return summary
