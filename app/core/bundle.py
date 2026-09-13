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


# ── restore from a bundle (v0.51.336, tag 2) ──────────────────────────
# The DB member stages through db_backup.stage_restore exactly as a bare
# snapshot does; motif.yaml and cookies.txt stage next to it as
# <name>.restore-pending and are swapped at boot by apply_pending_config
# — BEFORE get_settings() reads the YAML — after a .prerestore-<stamp> copy
# of what they replace. A preview (manifest line, DB check, the config keys
# that differ with secrets masked, the cookies verdict) is computed before
# anything is staged, and the operator may keep the live config.

import os as _os
import re as _re
from dataclasses import dataclass as _dataclass, field as _field

CONFIG_PENDING = "motif.yaml" + db_backup.RESTORE_PENDING_SUFFIX
COOKIES_PENDING = "cookies.txt" + db_backup.RESTORE_PENDING_SUFFIX
# Keys whose values never leave the box in a preview: masked on BOTH sides.
_SECRET_KEY_RE = _re.compile(
    r"token|secret|password|passwd|api_key|apikey|cookie|auth|apprise|webhook",
    _re.I)
MASK = "••••"


@_dataclass
class BundleCheck:
    ok: bool
    error: str | None = None
    manifest: dict | None = None
    db: "db_backup.RestoreCheck | None" = None
    has_config: bool = False
    has_cookies: bool = False
    staged: list[str] = _field(default_factory=list)


def flatten_config(text: str) -> dict[str, object]:
    """motif.yaml text → {dotted.key: scalar-or-list}. Malformed YAML is an
    empty mapping (the diff then shows every bundle key as new)."""
    import yaml
    try:
        raw = yaml.safe_load(text) or {}
    except Exception:
        raw = {}
    if not isinstance(raw, dict):
        return {}
    flat: dict[str, object] = {}

    def walk(node, path):
        if isinstance(node, dict):
            if not node:
                if path:          # an empty section is a leaf; an empty root is nothing
                    flat[path] = {}
                return
            for k, v in node.items():
                walk(v, f"{path}.{k}" if path else str(k))
        else:
            flat[path] = node
    walk(raw, "")
    return flat


def _render(v) -> str:
    if v is None:
        return "(unset)"
    if isinstance(v, (list, dict)):
        return json.dumps(v, sort_keys=True)
    return str(v)


def config_diff(live_text: str, bundle_text: str) -> list[dict]:
    """The keys whose values differ between the live motif.yaml and the
    bundle's, as [{key, live, bundle, secret}] sorted by key. A secret key
    reads MASK on both sides — still listed, never shown."""
    live, other = flatten_config(live_text), flatten_config(bundle_text)
    out = []
    for key in sorted(set(live) | set(other)):
        if live.get(key) == other.get(key):
            continue
        secret = bool(_SECRET_KEY_RE.search(key))
        out.append({
            "key": key, "secret": secret,
            "live": MASK if secret else _render(live.get(key)),
            "bundle": MASK if secret else _render(other.get(key)),
        })
    return out


def _extract_members(path: Path, into: Path) -> dict[str, Path]:
    """Extract only the known member NAMES into `into` (never the archived
    paths; anything else in the archive is a refusal). Returns name → file."""
    got: dict[str, Path] = {}
    with tarfile.open(path, "r:gz") as tar:
        for m in tar.getmembers():
            if m.name not in MEMBERS:
                raise ValueError(f"not a motif bundle: unexpected member {m.name!r}")
            if not m.isfile():
                raise ValueError(f"not a motif bundle: {m.name} is not a plain file")
            f = tar.extractfile(m)
            if f is None:
                raise ValueError(f"not a motif bundle: {m.name} unreadable")
            dest = into / m.name
            with dest.open("wb") as out:
                shutil.copyfileobj(f, out)
            got[m.name] = dest
    return got


def inspect_bundle(path: Path) -> BundleCheck:
    """Validate a bundle before anything is staged: a gzip tar whose members
    are only the known names, a manifest of a format this build reads, every
    member's sha256 matching the manifest, and a DB member that passes the
    snapshot restore checks. Never raises."""
    tmp = Path(tempfile.mkdtemp(prefix=".bundle-inspect-", dir=path.parent))
    try:
        try:
            got = _extract_members(path, tmp)
        except (tarfile.TarError, OSError, ValueError, EOFError) as e:
            return BundleCheck(False, f"not a motif bundle: {e}")
        if MEMBER_MANIFEST not in got or MEMBER_DB not in got:
            return BundleCheck(False, "not a motif bundle: manifest.json or motif.db missing")
        try:
            manifest = json.loads(got[MEMBER_MANIFEST].read_text("utf-8"))
        except Exception as e:
            return BundleCheck(False, f"not a motif bundle: manifest unreadable ({e})")
        if manifest.get("kind") != "motif-bundle":
            return BundleCheck(False, "not a motif bundle: manifest kind mismatch")
        fmt = manifest.get("format")
        if not isinstance(fmt, int) or fmt > BUNDLE_FORMAT:
            return BundleCheck(False, f"bundle format {fmt!r} is newer than this build reads "
                                      f"({BUNDLE_FORMAT}) — upgrade motif before restoring")
        members = manifest.get("members") or {}
        for name, meta in members.items():
            if name not in got:
                return BundleCheck(False, f"bundle is missing {name} the manifest lists")
            if _sha256_file(got[name]) != (meta or {}).get("sha256"):
                return BundleCheck(False, f"{name} does not match the manifest checksum")
        for name in got:
            if name != MEMBER_MANIFEST and name not in members:
                return BundleCheck(False, f"bundle carries {name} the manifest does not list")
        dbc = db_backup.inspect_restore_source(got[MEMBER_DB])
        if not dbc.ok:
            return BundleCheck(False, dbc.error, manifest=manifest, db=dbc)
        return BundleCheck(True, None, manifest=manifest, db=dbc,
                           has_config=MEMBER_CONFIG in got, has_cookies=MEMBER_COOKIES in got)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def bundle_config_text(path: Path) -> str | None:
    """The bundle's motif.yaml text, or None when it carries none."""
    with tarfile.open(path, "r:gz") as tar:
        try:
            m = tar.getmember(MEMBER_CONFIG)
        except KeyError:
            return None
        f = tar.extractfile(m)
        return f.read().decode("utf-8", "replace") if f else None


def preview(path: Path, live_config: Path | None) -> dict:
    """What a restore from this bundle would do — for the settings card's
    preview, before anything is staged. Secrets are masked."""
    check = inspect_bundle(path)
    if not check.ok:
        raise ValueError(check.error or "invalid bundle")
    m = check.manifest or {}
    live_text = live_config.read_text("utf-8", "replace") if (live_config and live_config.is_file()) else ""
    diff = config_diff(live_text, bundle_config_text(path) or "") if check.has_config else []
    return {
        "name": path.name,
        "manifest": {
            "format": m.get("format"), "motif_version": m.get("motif_version"),
            "schema_version": m.get("schema_version"), "created_at": m.get("created_at"),
            "census_rows": len(m.get("themes_census") or []),
            "counts": m.get("counts") or {},
        },
        "db": {"ok": check.db.ok if check.db else False,
               "schema_version": check.db.schema_version if check.db else None},
        "config_in_bundle": check.has_config,
        "config_diff": diff,
        "cookies": "in bundle" if check.has_cookies else "not in bundle",
    }


def _stage_file(src: Path, pending: Path) -> None:
    tmp = pending.with_name(pending.name + ".tmp")
    shutil.copy2(src, tmp)
    _os.replace(tmp, pending)


def stage_bundle_restore(db_path: Path, config_dir: Path, bundle_path: Path, *,
                         keep_config: bool) -> BundleCheck:
    """Stage the DB member through db_backup.stage_restore, and — unless the
    operator keeps the live config — motif.yaml and cookies.txt as
    <name>.restore-pending in config_dir. Raises ValueError when the bundle
    fails inspection. Nothing live changes until the next boot."""
    check = inspect_bundle(bundle_path)
    if not check.ok:
        raise ValueError(check.error or "invalid bundle")
    tmp = Path(tempfile.mkdtemp(prefix=".bundle-stage-", dir=config_dir))
    try:
        got = _extract_members(bundle_path, tmp)
        db_backup.stage_restore(db_path, got[MEMBER_DB])
        check.staged.append("database")
        cfg_pending = config_dir / CONFIG_PENDING
        ck_pending = config_dir / COOKIES_PENDING
        if keep_config:
            for p in (cfg_pending, ck_pending):
                if p.exists():
                    p.unlink()
        else:
            if MEMBER_CONFIG in got:
                _stage_file(got[MEMBER_CONFIG], cfg_pending)
                check.staged.append("config")
            if MEMBER_COOKIES in got:
                _stage_file(got[MEMBER_COOKIES], ck_pending)
                check.staged.append("cookies")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    log.info("bundle restore staged from %s: %s; applies on next restart",
             bundle_path.name, ", ".join(check.staged))
    return check


def pending_members(db_path: Path, config_dir: Path) -> list[str]:
    out = []
    if db_backup.restore_pending_path(db_path).exists():
        out.append("database")
    if (config_dir / CONFIG_PENDING).exists():
        out.append("config")
    if (config_dir / COOKIES_PENDING).exists():
        out.append("cookies")
    return out


def cancel_pending(db_path: Path, config_dir: Path) -> bool:
    """Drop everything staged — the DB (db_backup) and the config members."""
    any_ = db_backup.cancel_pending_restore(db_path)
    for name in (CONFIG_PENDING, COOKIES_PENDING):
        p = config_dir / name
        if p.exists():
            p.unlink()
            any_ = True
    return any_


def apply_pending_config(config_dir: Path, *, now_stamp: str) -> dict | None:
    """BOOT hook, before get_settings() reads motif.yaml. Swap a staged
    motif.yaml / cookies.txt into place, each after a .prerestore-<stamp>
    copy of the file it replaces. Never raises; a failure leaves the live
    file and the pending file in place and logs WHY (cold-path rule)."""
    targets = ((CONFIG_PENDING, "motif.yaml"), (COOKIES_PENDING, "cookies.txt"))
    if not any((config_dir / p).exists() for p, _ in targets):
        return None
    applied: list[str] = []
    safety: dict[str, str] = {}
    errors: dict[str, str] = {}
    for pending_name, live_name in targets:
        pending = config_dir / pending_name
        if not pending.exists():
            continue
        live = config_dir / live_name
        try:
            if live.exists():
                keep = config_dir / f"{live_name}.prerestore-{now_stamp}"
                shutil.copy2(live, keep)
                safety[live_name] = keep.name
            _os.replace(pending, live)
            applied.append(live_name)
            log.warning("%s RESTORED from a staged bundle (pre-restore copy: %s)",
                        live_name, safety.get(live_name, "(none)"))
        except OSError as e:
            errors[live_name] = str(e)
            log.error("apply_pending_config: %s not swapped (%s) — live file kept, "
                      "pending file kept for a retry", live_name, e)
    return {"applied": applied, "safety": safety, "errors": errors}
