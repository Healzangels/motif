"""v0.51.335: the backup bundle (feature D, docs/specs/BACKUP_BUNDLE_SPEC.md).

One archive, motif-bundle-YYYYMMDD-HHMMSS.tar.gz in <config_dir>/backups,
holding everything a rebuilt or moved install needs except the theme
bytes: a VACUUM INTO snapshot of motif.db, motif.yaml (secrets as-is —
the operator's decision 2: a bundle that cannot restore without re-typing
them does not serve a new host), cookies.txt when present, and a
manifest with versions, row counts, member checksums and a census of
every local_files row (path, size, sha256, source, placement kind) so a
lost themes directory becomes a re-download list.

Clock-free like db_backup: the caller supplies the UTC stamp. The tar
is written to a temp name in the backups dir and renamed into place, so a
mid-write failure never leaves a half bundle that list_backups would
count toward retention.
"""
from __future__ import annotations

import hashlib
import io
import json
import logging
import shutil
import sqlite3
import tarfile
import tempfile
import time
from pathlib import Path

from . import db_backup

log = logging.getLogger(__name__)

BUNDLE_FORMAT = 1
MEMBER_DB = "motif.db"
MEMBER_CONFIG = "motif.yaml"
MEMBER_COOKIES = "cookies.txt"
MEMBER_MANIFEST = "manifest.json"
# Every member a bundle can carry — the restore side (tag 2) extracts these
# names only, never the archived paths.
MEMBERS = (MEMBER_DB, MEMBER_CONFIG, MEMBER_COOKIES, MEMBER_MANIFEST)
CENSUS_TABLES = ("plex_items", "themes", "local_files", "placements",
                 "user_overrides", "saved_filters", "previous_urls")


def bundle_name(now_stamp: str) -> str:
    return f"motif-bundle-{now_stamp}.tar.gz"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def table_counts(db_path: Path) -> dict[str, int]:
    """Row counts for the tables the manifest reports. A table missing on an
    older schema counts as absent, not as an error."""
    out: dict[str, int] = {}
    conn = sqlite3.connect(str(db_path))
    try:
        for t in CENSUS_TABLES:
            try:
                out[t] = int(conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0])
            except sqlite3.OperationalError:
                continue
    finally:
        conn.close()
    return out


def themes_census(db_path: Path) -> list[dict]:
    """Every local_files row, joined to its placement (same media_type /
    tmdb_id / section_id / edition), as the fields a rebuild needs: where
    the file was, how big, its hash, where it came from, how it was placed.
    The hash comes from the DB (never recomputed here — a bundle must not
    walk 13 GB of themes)."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT lf.media_type, lf.tmdb_id, lf.section_id, lf.edition_key, "
            "       lf.file_path, lf.file_size, lf.file_sha256, lf.source_kind, "
            "       lf.source_video_id, lf.provenance, p.placement_kind "
            "FROM local_files lf "
            "LEFT JOIN placements p "
            "  ON p.media_type = lf.media_type AND p.tmdb_id = lf.tmdb_id "
            " AND p.section_id = lf.section_id "
            " AND COALESCE(p.edition_key, '') = COALESCE(lf.edition_key, '') "
            "ORDER BY lf.media_type, lf.tmdb_id, lf.section_id"
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "media_type": r["media_type"], "tmdb_id": r["tmdb_id"],
            "section_id": r["section_id"], "edition_key": r["edition_key"] or "",
            "path": r["file_path"], "size": r["file_size"],
            "sha256": r["file_sha256"] or None,
            "source_kind": r["source_kind"], "source_video_id": r["source_video_id"],
            "provenance": r["provenance"], "placement_kind": r["placement_kind"],
        }
        for r in rows
    ]


def build_manifest(*, created_at: str, motif_version: str, schema_version: int,
                   members: dict[str, dict], config_dir: Path,
                   themes_dir: Path | None, counts: dict[str, int],
                   census: list[dict]) -> dict:
    return {
        "kind": "motif-bundle",
        "format": BUNDLE_FORMAT,
        "created_at": created_at,
        "motif_version": motif_version,
        "schema_version": schema_version,
        "members": members,
        "paths": {"config_dir": str(config_dir),
                  "themes_dir": str(themes_dir) if themes_dir else None},
        "counts": counts,
        "themes_census": census,
    }


def create_bundle(db_path: Path, config_dir: Path, *,
                  config_file: Path | None, cookies_file: Path | None,
                  themes_dir: Path | None, now_stamp: str,
                  motif_version: str, schema_version: int) -> db_backup.BackupFile:
    """Write motif-bundle-<stamp>.tar.gz into the backups dir. Raises
    FileNotFoundError if the DB is missing, FileExistsError on a same-second
    collision, ValueError on a malformed stamp; sqlite / OS errors propagate
    after the temp dir is cleaned."""
    if not db_path.exists():
        raise FileNotFoundError(f"database not found: {db_path}")
    name = bundle_name(now_stamp)
    if not db_backup.is_backup_name(name):
        raise ValueError(f"invalid backup stamp: {now_stamp!r}")
    bdir = db_backup.backups_dir(config_dir)
    bdir.mkdir(parents=True, exist_ok=True)
    dest = bdir / name
    if dest.exists():
        raise FileExistsError(f"backup already exists: {name}")
    created_at = db_backup._iso_from_stamp(now_stamp)
    # Same filesystem as the destination, so the final rename is atomic.
    tmp = Path(tempfile.mkdtemp(prefix=".bundle-", dir=bdir))
    try:
        db_member = tmp / MEMBER_DB
        db_backup.vacuum_into(db_path, db_member)
        members: dict[str, dict] = {
            MEMBER_DB: {"size": db_member.stat().st_size, "sha256": _sha256_file(db_member)},
        }
        parts: list[tuple[str, Path]] = [(MEMBER_DB, db_member)]
        if config_file is not None and config_file.is_file():
            members[MEMBER_CONFIG] = {"size": config_file.stat().st_size,
                                      "sha256": _sha256_file(config_file),
                                      "secrets": "as-is"}
            parts.append((MEMBER_CONFIG, config_file))
        if cookies_file is not None and cookies_file.is_file():
            members[MEMBER_COOKIES] = {"size": cookies_file.stat().st_size,
                                       "sha256": _sha256_file(cookies_file)}
            parts.append((MEMBER_COOKIES, cookies_file))
        manifest = build_manifest(
            created_at=created_at, motif_version=motif_version,
            schema_version=schema_version, members=members,
            config_dir=config_dir, themes_dir=themes_dir,
            counts=table_counts(db_member), census=themes_census(db_member),
        )
        payload = json.dumps(manifest, indent=1, sort_keys=True).encode("utf-8")
        part = tmp / (name + ".part")
        with tarfile.open(part, "w:gz") as tar:
            for arcname, src in parts:
                tar.add(str(src), arcname=arcname, recursive=False)
            ti = tarfile.TarInfo(MEMBER_MANIFEST)
            ti.size = len(payload)
            ti.mtime = int(time.time())
            tar.addfile(ti, io.BytesIO(payload))
        part.replace(dest)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    st = dest.stat()
    log.info("backup bundle written: %s (%d bytes, %d census rows)",
             name, st.st_size, len(manifest["themes_census"]))
    return db_backup.BackupFile(name=name, size=st.st_size, created_at=created_at,
                                kind="bundle")


def read_manifest(path: Path) -> dict:
    """The manifest of a bundle on disk, by member name only (no archived
    paths are honoured)."""
    with tarfile.open(path, "r:gz") as tar:
        try:
            member = tar.getmember(MEMBER_MANIFEST)
        except KeyError:
            raise ValueError("not a motif bundle: no manifest.json")
        f = tar.extractfile(member)
        if f is None:
            raise ValueError("not a motif bundle: manifest.json is not a file")
        return json.loads(f.read().decode("utf-8"))
