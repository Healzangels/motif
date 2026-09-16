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

import errno
import gzip
import hashlib
import io
import json
import logging
import shutil
import sqlite3
import tarfile
import tempfile
import threading
import time
import zlib
from datetime import datetime
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
# v0.51.342: the spec's "size-capped", judged on each header before a byte is written — nothing capped a member
# v0.51.342: ONE table — create_bundle writes no member over it, and _inspect_into judges every header by it
_MEMBER_CAP = {MEMBER_DB: 4 << 30, MEMBER_MANIFEST: 64 << 20, MEMBER_CONFIG: 1 << 20, MEMBER_COOKIES: 16 << 20}
CENSUS_TABLES = ("plex_items", "themes", "local_files", "placements",
                 "user_overrides", "saved_filters", "previous_urls")


def bundle_name(now_stamp: str, *, partial: bool = False) -> str:
    return f"motif-bundle-{'partial-' if partial else ''}{now_stamp}.tar.gz"  # v0.51.344: partial = a member left out over its cap (db_backup._PARTIAL_RE)


def uploaded_bundle_name(now_stamp: str, n: int = 1) -> str:
    return f"motif-bundle-upload-{now_stamp}{f'-{n}' if n > 1 else ''}.tar.gz"  # v0.51.343: db_backup's retained-False shape — an upload never takes a retention slot


def file_upload(tmp: Path, bdir: Path, now_stamp: str) -> Path:
    """v0.51.344: rename a checked upload into bdir under a name no other upload holds — each candidate gated, claimed O_EXCL, then renamed onto."""
    import os
    for n in range(1, 100):
        name = uploaded_bundle_name(now_stamp, n)
        if not db_backup.is_backup_name(name):  # v0.51.344: create_bundle's gate — a name outside the table was os.replace'd in, invisible to the API
            raise ValueError(f"invalid backup stamp in {name!r}")
        dest = bdir / name
        try:
            os.close(os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600))  # v0.51.344: exists() then os.replace let a same-second upload overwrite one already previewed
        except FileExistsError:
            continue
        try:
            os.replace(tmp, dest)
        except BaseException:
            try:
                dest.unlink(missing_ok=True)
            except OSError as e:
                log.warning("bundle upload: could not remove the empty claim %s (%s) — delete it from the backup list", name, e)
            raise
        return dest
    raise FileExistsError(f"every upload name for {now_stamp} is taken")


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
            except sqlite3.OperationalError as e:
                if "no such table" not in str(e):
                    raise  # v0.51.339: only an older schema's missing table is "absent"; I/O, malformed, locked propagate
                log.info("bundle counts: %s is absent on this schema (%s)", t, e)
    finally:
        conn.close()
    return out


def themes_census(db_path: Path) -> list[dict]:
    """Every local_files row, with ONE of its placements (same media_type /
    tmdb_id / section_id / edition; a present one first, then the newest),
    as the fields a rebuild needs: where the file was, how big, its hash,
    where it came from, how it was placed. The hash comes from the DB (never
    recomputed here — a bundle must not walk 13 GB of themes)."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT lf.media_type, lf.tmdb_id, lf.section_id, lf.edition_key, "
            "       lf.file_path, lf.file_size, lf.file_sha256, lf.source_kind, "
            "       lf.source_video_id, lf.provenance, "
            # v0.51.339: a subquery, not a join — placements' PK includes media_folder, so two folders fanned one file into two rows.
            "       (SELECT p.placement_kind FROM placements p "
            "         WHERE p.media_type = lf.media_type AND p.tmdb_id = lf.tmdb_id "
            "           AND p.section_id = lf.section_id "
            "           AND COALESCE(p.edition_key, '') = COALESCE(lf.edition_key, '') "
            # v0.51.341: DESC sorts NULL last — a verified-missing (0) placement outranked an unverified one.
            "         ORDER BY CASE WHEN p.theme_present = 1 THEN 0 "
            "                       WHEN p.theme_present IS NULL THEN 1 ELSE 2 END, "
            "                  p.placed_at DESC, p.media_folder "
            "         LIMIT 1) AS placement_kind "
            "FROM local_files lf "
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
                   census: list[dict], left_out: dict[str, dict] | None = None) -> dict:
    out = {
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
    if left_out:
        out["left_out"] = left_out  # v0.51.342: the note a member over its cap leaves in place of the member
    return out


class BundleOverCap(ValueError):
    """v0.51.344: a bundle refused for a member over its cap — a plain snapshot can still be taken."""


def create_bundle(db_path: Path, config_dir: Path, *,
                  config_file: Path | None, cookies_file: Path | None,
                  themes_dir: Path | None, now_stamp: str,
                  motif_version: str, schema_version: int) -> db_backup.BackupFile:
    """Write bundle_name(now_stamp) into the backups dir, its partial shape when
    a member is left out over its cap. Raises FileNotFoundError if the DB is
    missing, FileExistsError on a same-second collision with either shape,
    BundleOverCap (a ValueError) when the database or manifest is over its cap,
    ValueError on a malformed stamp; sqlite / OS errors propagate after the
    temp dir is cleaned."""
    if not db_path.exists():
        raise FileNotFoundError(f"database not found: {db_path}")
    name = bundle_name(now_stamp)
    if not db_backup.is_backup_name(name):
        raise ValueError(f"invalid backup stamp: {now_stamp!r}")
    bdir = db_backup.backups_dir(config_dir)
    bdir.mkdir(parents=True, exist_ok=True)
    dest = bdir / name
    if dest.exists() or (bdir / bundle_name(now_stamp, partial=True)).exists():  # v0.51.344: one bundle a second, whichever shape it took
        raise FileExistsError(f"backup already exists: {name}")
    created_at = db_backup._iso_from_stamp(now_stamp)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        about = ((conn.execute("PRAGMA page_count").fetchone()[0] - conn.execute("PRAGMA freelist_count").fetchone()[0])
                 * conn.execute("PRAGMA page_size").fetchone()[0])
    finally:
        conn.close()
    if about > _MEMBER_CAP[MEMBER_DB] * 21 // 20:  # v0.51.344: the whole VACUUM (time + disk) was paid before this refusal; 5% over the estimate's measured 4% error
        raise BundleOverCap(f"the database is about {about} bytes, over the {_MEMBER_CAP[MEMBER_DB]}-byte cap a bundle's "
                            "database may be — no bundle was written; take a plain snapshot instead")
    # Same filesystem as the destination, so the final rename is atomic.
    tmp = Path(tempfile.mkdtemp(prefix=".bundle-", dir=bdir))
    try:
        db_member = tmp / MEMBER_DB
        db_backup.vacuum_into(db_path, db_member)
        db_size = db_member.stat().st_size
        if db_size > _MEMBER_CAP[MEMBER_DB]:  # v0.51.342: written without complaint, then every restore refused it
            raise BundleOverCap(f"the database snapshot is {db_size} bytes, over the {_MEMBER_CAP[MEMBER_DB]}-byte cap a "
                                "bundle's database may be — no bundle was written; take a plain snapshot instead")
        members: dict[str, dict] = {
            MEMBER_DB: {"size": db_size, "sha256": _sha256_file(db_member)},
        }
        parts: list[tuple[str, Path]] = [(MEMBER_DB, db_member)]
        left_out: dict[str, dict] = {}
        for arcname, src in ((MEMBER_CONFIG, config_file), (MEMBER_COOKIES, cookies_file)):
            if src is None or not src.is_file():
                continue
            size = src.stat().st_size
            if size > _MEMBER_CAP[arcname]:  # v0.51.342: a 17 MiB cookies.txt made every restore refuse the bundle, KEEP MY CURRENT CONFIG too
                left_out[arcname] = {"size": size, "cap": _MEMBER_CAP[arcname]}
                continue
            members[arcname] = {"size": size, "sha256": _sha256_file(src)}
            if arcname == MEMBER_CONFIG:
                members[arcname]["secrets"] = "as-is"
            parts.append((arcname, src))
        if left_out:  # v0.51.344: its own name — the manifest is the LAST tar member, so prune could not tell partial from complete without inflating
            name = bundle_name(now_stamp, partial=True)
            dest = bdir / name
        manifest = build_manifest(
            created_at=created_at, motif_version=motif_version,
            schema_version=schema_version, members=members,
            config_dir=config_dir, themes_dir=themes_dir,
            counts=table_counts(db_member), census=themes_census(db_member),
            left_out=left_out,
        )
        payload = json.dumps(manifest, indent=1, sort_keys=True).encode("utf-8")
        if len(payload) > _MEMBER_CAP[MEMBER_MANIFEST]:  # v0.51.342: the census grows with the library, and inspect refuses a manifest over its cap
            raise BundleOverCap(f"the bundle manifest is {len(payload)} bytes, over the {_MEMBER_CAP[MEMBER_MANIFEST]}-byte "
                                "cap a manifest may be — no bundle was written; take a plain snapshot instead")
        part = tmp / (name + ".part")
        # v0.51.339: dereference — a symlinked motif.yaml/cookies.txt was archived as a 0-byte SYMTYPE (its target path leaked) and every restore refused it.
        with tarfile.open(part, "w:gz", dereference=True) as tar:
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
    if left_out:
        from .events import log_event
        said = "; ".join(f"{n} ({m['size']} bytes, over its {m['cap']}-byte cap)" for n, m in left_out.items())
        msg = (f"Backup bundle {name} left out {said} — a restore from it leaves that file as it is; "
               "shrink it, then take a new bundle")
        log.warning("backup bundle: %s", msg)  # v0.51.342: the scheduled job's other misses are WARNING events too
        log_event(db_path, level="WARNING", component="backup", message=msg,  # v0.51.342: no member name as a key — the scrubber redacts a "cookies.txt" key's value
                  detail={"name": name, "left_out": [{"member": n, **m} for n, m in left_out.items()]})
    return db_backup.BackupFile(name=name, size=st.st_size, created_at=created_at,
                                kind="bundle", partial=bool(left_out))


def create_bundle_for(settings, now_stamp: str) -> db_backup.BackupFile:
    """v0.51.344: one spelling of a live install's bundle — the scheduler and the API each hand-kept eight kwargs."""
    from .. import __version__
    from .db import CURRENT_SCHEMA_VERSION
    return create_bundle(settings.db_path, settings.config_dir, config_file=settings.config_file.path,
                         cookies_file=settings.cookies_file, themes_dir=settings.themes_dir, now_stamp=now_stamp,
                         motif_version=__version__, schema_version=CURRENT_SCHEMA_VERSION)


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
# v0.51.339: boot applies the DB first; apply_pending_config then apply_pending_cookies (settings.cookies_file) follow only a DB that applied.

import os as _os
from dataclasses import dataclass as _dataclass, field as _field, replace as _replace

from .config_file import SECRET_MASK, is_secret_config_key, mask_config_value

CONFIG_PENDING = "motif.yaml" + db_backup.RESTORE_PENDING_SUFFIX
COOKIES_PENDING = "cookies.txt" + db_backup.RESTORE_PENDING_SUFFIX
# v0.51.339: the preview masks through GET /api/config's rule (config_file.mask_config_value), never a second regex.
MASK = SECRET_MASK


@_dataclass
class BundleCheck:
    ok: bool
    error: str | None = None
    manifest: dict | None = None
    db: "db_backup.RestoreCheck | None" = None
    has_config: bool = False
    has_cookies: bool = False
    staged: list[str] = _field(default_factory=list)
    # v0.51.342: what the one pass verified, handed on instead of re-read — repr=False: the config carries the Plex token
    config_bytes: bytes | None = _field(default=None, repr=False)
    cookies_bytes: bytes | None = _field(default=None, repr=False)
    db_source: db_backup.VerifiedSource | None = _field(default=None, repr=False)
    oversize: dict[str, int] = _field(default_factory=dict)  # v0.51.342: config/cookies over their cap — hashed, never held
    left_as_is: dict[str, str] = _field(default_factory=dict)  # v0.51.342: a member this staging leaves live, and why


def _parse_error_summary(e: Exception) -> str:
    # v0.51.339: type + position only — a YAML error's own text quotes the offending line, which can hold a secret.
    mark = getattr(e, "problem_mark", None)
    return type(e).__name__ + (f" at line {mark.line + 1}, column {mark.column + 1}" if mark is not None else "")


_UNREADABLE_WORDS = {"tag:yaml.org,2002:int": "an integer too long to read", "tag:yaml.org,2002:timestamp": "a date that does not exist"}


def _unreadable_scalar(text: str) -> str | None:
    """The dotted key of the first scalar YAML's safe loader cannot build, in words — never its value — or None."""
    import yaml
    loader, builder = yaml.SafeLoader(text), yaml.SafeLoader("")
    try:
        root = loader.get_single_node()
    except yaml.YAMLError as e:
        log.info("bundle restore: the motif.yaml does not compose (%s) — its parse error is named by type", type(e).__name__)
        return None
    finally:
        loader.dispose()
    seen: set[int] = set()

    def walk(node, path: str) -> str | None:
        if id(node) in seen:
            return None
        seen.add(id(node))
        if isinstance(node, yaml.MappingNode):
            for k, v in node.value:
                name = k.value if isinstance(k, yaml.ScalarNode) else "?"
                if found := walk(v, f"{path}.{name}" if path else str(name)):
                    return found
            return None
        if isinstance(node, yaml.SequenceNode):
            return next((found for v in node.value if (found := walk(v, path))), None)
        try:
            builder.construct_object(node)
        except ValueError:
            return f"{path or 'its top level'} holds {_UNREADABLE_WORDS.get(node.tag, 'a value YAML cannot read')}"
        except Exception as e:  # v0.51.344: a tag the safe loader refuses is not the ValueError being named — look on
            log.info("bundle restore: %s does not build either (%s)", path or "its top level", type(e).__name__)
        return None
    try:
        return walk(root, "") if root is not None else None
    finally:
        builder.dispose()


def flatten_config(text: str | bytes, *, side: str) -> tuple[dict[str, object], str | None]:
    """motif.yaml text (bytes decode as strict UTF-8, as the boot read does) →
    ({dotted.key: scalar-or-list}, None), or ({}, why) when it does not parse to
    the mapping ConfigFile.load() needs — logged at WARNING naming the side."""
    import yaml
    try:
        raw = yaml.safe_load(text.decode("utf-8") if isinstance(text, bytes) else text) or {}
    except Exception as e:  # v0.51.339: was a silent {} — a full false diff, then a staged file that crashed the next boot
        # v0.51.344: a bare ValueError (an integer past Python's text limit, a date that does not exist) read only "ValueError"; a UnicodeDecodeError keeps its summary
        err = (_unreadable_scalar(text.decode("utf-8") if isinstance(text, bytes) else text) if type(e) is ValueError else None) or _parse_error_summary(e)
        log.warning("bundle restore: the %s motif.yaml does not parse (%s) — no config diff", side, err)
        return {}, err
    if not isinstance(raw, dict):
        err = f"its top level is a {type(raw).__name__}, not a mapping"
        log.warning("bundle restore: the %s motif.yaml does not parse (%s) — no config diff", side, err)
        return {}, err
    err = _loader_contract_error(raw)
    if err:  # v0.51.341: "plex: 5" hydrated without raising, then the boot that swapped it in died reading cfg.plex.url
        log.warning("bundle restore: the %s motif.yaml does not parse (%s) — no config diff", side, err)
        return {}, err
    import dataclasses
    from . import config_file as cf
    flat: dict[str, object] = {}

    def walk(node, path, declared):
        if isinstance(node, dict):
            if not node:
                if path:          # an empty section is a leaf; an empty root is nothing
                    flat[path] = {}
                return
            names = {f.name for f in dataclasses.fields(declared)} if dataclasses.is_dataclass(declared) else set()
            for k, v in node.items():
                walk(v, f"{path}.{k}" if path else str(k), getattr(declared, k) if k in names else None)
        else:
            # v0.51.342: the value the loader hydrates — a live `movie_section: 1` and a bundle's '1' are one setting
            flat[path] = node if declared is None else cf._coerce_leaf(declared, node)
    walk(raw, "", cf.MotifConfig())
    return flat, None


_LEAF_KINDS = ((bool, "true or false"), (int, "an integer"), (float, "a number"),
               (str, "a string"), (list, "a list"), (dict, "a mapping"))


def _leaf_type_error(default, value) -> str | None:
    """Why `value` cannot stand in for a leaf whose dataclass default is `default` — by the type the loader hydrates it to, never the value — or None."""
    from . import config_file as cf
    loaded = cf._coerce_leaf(default, value)  # v0.51.342: the loader's own coercion — stricter than it, the rule refused a `movie_section: 1` that loaded and ran
    for kind, word in _LEAF_KINDS:  # bool first: a bool is an int to isinstance
        if isinstance(default, kind):
            if isinstance(loaded, kind) and (kind is bool or not isinstance(loaded, bool)):
                return None
            return f"must be {word}, not {type(value).__name__}"
    return None  # a None default takes null or anything; a type this rule does not know is the loader's


def _loader_contract_error(raw: dict) -> str | None:
    """How `raw` breaks ConfigFile.load()'s contract — by dotted key or exception type, never a value — or None."""
    import dataclasses
    from . import config_file as cf

    def walk(target, node: dict, path: str) -> str | None:
        for f in dataclasses.fields(target):
            cur = getattr(target, f.name)
            if f.name not in node:
                continue
            if not dataclasses.is_dataclass(cur):
                err = _leaf_type_error(cur, node[f.name])  # v0.51.342: "plex: {url: 5}" hydrated, then plex.py's cfg.url.rstrip crashed the boot that swapped it in
                if err:
                    return f"{path}{f.name} {err}"
                continue
            if not isinstance(node[f.name], dict):  # v0.51.341: a dataclass section, at any depth, must be a mapping
                return f"{path}{f.name} must be a mapping, not {type(node[f.name]).__name__}"
            err = walk(cur, node[f.name], f"{path}{f.name}.")
            if err:
                return err
        return None

    err = walk(cf.MotifConfig(), raw, "")
    if err:
        return err
    try:
        cf._hydrate_dataclass(cf.MotifConfig(), raw)  # v0.51.341: the real loader's hydration, in memory — the live file is never touched
    except Exception as e:
        log.info("bundle restore: the config loader refuses this motif.yaml (%s)", type(e).__name__)
        return f"the config loader refuses it ({type(e).__name__})"
    return None


def _too_long_to_show(v) -> str | None:
    if isinstance(v, int) and not isinstance(v, bool):
        try:
            str(v)
        except ValueError:  # v0.51.344: a hex literal past Python's int-to-text limit loads, then config_diff's str() 422'd the preview in Python's words
            log.info("bundle restore: a %d-bit integer is too long to show in the config diff", v.bit_length())
            return f"(a {v.bit_length()}-bit integer, too long to show)"
    return None


def _plain(v):
    # v0.51.341: a YAML date or a non-string key inside a list was a json.dumps TypeError — a preview 500
    if isinstance(v, dict):
        return {str(k): _plain(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_plain(x) for x in v]
    return (_too_long_to_show(v) or v) if v is None or isinstance(v, (str, int, float, bool)) else str(v)


def _render(v) -> str:
    if v is None:
        return "(unset)"
    if isinstance(v, (list, dict)):
        return json.dumps(_plain(v), sort_keys=True)
    return _too_long_to_show(v) or str(v)


def _diff_rows(live: dict, other: dict) -> list[dict]:
    out = []
    for key in sorted(set(live) | set(other)):
        if live.get(key) == other.get(key):  # raw values: two secrets that mask alike still differ
            continue
        out.append({
            "key": key, "secret": is_secret_config_key(key),
            "live": _render(mask_config_value(key, live[key]) if live.get(key) is not None else None),
            "bundle": _render(mask_config_value(key, other[key]) if other.get(key) is not None else None),
        })
    return out


def config_diff(live_text: str, bundle_text: str) -> list[dict]:
    """The keys whose values differ between the live motif.yaml and the
    bundle's, as [{key, live, bundle, secret}] sorted by key, each side shown
    as GET /api/config shows it. Empty when either side does not parse."""
    live, live_err = flatten_config(live_text, side="live")
    other, bundle_err = flatten_config(bundle_text, side="bundle")
    return [] if (live_err or bundle_err) else _diff_rows(live, other)


_STREAM_BUF = 1 << 20
# v0.51.342: one gz.read past end-of-archive scans empty gzip members / zero padding unbounded (seconds under STAGING_LOCK); both sit above a 1 MiB fetch + gzip's 128 KiB readahead
_TAIL_RAW_BUDGET = 2 << 20
_TAIL_INFLATE_BUDGET = 4 << 20


class _BundleTail(Exception):
    """v0.51.342: over _TAIL_RAW_BUDGET raw bytes read since tarfile last fetched an inflated chunk."""


class _TailGuard:
    """v0.51.342: the raw file gzip reads, refusing once it pulls more than _TAIL_RAW_BUDGET bytes between reset()s."""
    def __init__(self, f):
        self._f = f
        self._n = 0

    def reset(self) -> None:
        self._n = 0

    def read(self, size=-1):
        data = self._f.read(size)
        self._n += len(data)
        if self._n > _TAIL_RAW_BUDGET:  # v0.51.344: the allowance spans the whole stream, so a trip inside the archive read "after the archive's end"
            raise _BundleTail(f"not a motif bundle: over {_TAIL_RAW_BUDGET} bytes of its gzip stream were read for too little data — "
                              "padding or empty gzip members")
        return data

    def readable(self) -> bool:
        return True

    def __getattr__(self, name):  # seek/tell/seekable/mode/name fall through to the real file, exactly as the bare fileobj did
        return getattr(self._f, name)


_EXT_HEADER_CAP = 64 << 10
_EXT_TYPES = (tarfile.GNUTYPE_LONGNAME, tarfile.GNUTYPE_LONGLINK, tarfile.XHDTYPE, tarfile.XGLTYPE, tarfile.SOLARIS_XHDTYPE)


class _BoundedTarInfo(tarfile.TarInfo):
    def _proc_member(self, tar):
        if self.type == tarfile.XGLTYPE:  # v0.51.344: a 'g' header's keys ride every later header and member as a copy — create_bundle never writes one
            raise ValueError("not a motif bundle: a global pax header, which motif never writes")
        if self.type == tarfile.GNUTYPE_SPARSE:  # v0.51.344: its extended blocks are read whole before the member gate refuses it — a 437 KiB file held 300 MiB
            raise ValueError("not a motif bundle: a GNU sparse header, which motif never writes")
        if self.type in _EXT_TYPES:  # v0.51.344: tarfile holds an L/K/x payload whole before any member gate — a 522 KiB file declaring 512 MiB took 1.3 GiB
            tar.motif_ext_bytes = getattr(tar, "motif_ext_bytes", 0) + tarfile.BLOCKSIZE + self._block(self.size)
            if tar.motif_ext_bytes > _EXT_HEADER_CAP:  # v0.51.344: one budget for the whole stream — 250 headers under a per-header cap still held 383 MiB
                raise ValueError(f"not a motif bundle: over {_EXT_HEADER_CAP} bytes of extension headers")
        return super()._proc_member(tar)

    def _proc_gnusparse_10(self, *_):  # v0.51.344: a pax sparse 1.0 map is read from the member's data, unbounded, before any member gate — a 142 KiB upload held 392 MiB and STAGING_LOCK for 15 s
        raise ValueError("not a motif bundle: a GNU sparse member, which motif never writes")


class _TarFeed:
    """v0.51.342: what tarfile reads — every 1 MiB it fetches gets a fresh raw allowance, so a real database never trips it."""
    def __init__(self, gz, guard):
        self._gz, self._guard = gz, guard

    def read(self, size=-1):
        self._guard.reset()
        return self._gz.read(size)


class _HashingReader:
    def __init__(self, f, h):
        self._f, self._h = f, h

    def read(self, n=-1):
        data = self._f.read(n)
        self._h.update(data)
        return data


class _DiskRefused(Exception):
    """v0.51.342: an OSError writing the extracted database — the disk's, kept apart from the archive's read faults."""
    def __init__(self, err: OSError):
        super().__init__(str(err))
        self.err = err


class ExtractionWriteError(Exception):
    """The disk refused the extraction of a bundle's database — never a verdict on the bundle; `out_of_space` for ENOSPC / EDQUOT."""
    def __init__(self, message: str, *, out_of_space: bool):
        super().__init__(message)
        self.out_of_space = out_of_space


def _write_refused(e: OSError, name: str, beside: str) -> ExtractionWriteError:
    return ExtractionWriteError(f"could not write the extraction beside {beside} ({e.strerror or type(e).__name__}) — "
                                f"{name} was not judged; free space or fix permissions there, then try again",
                                out_of_space=e.errno in (errno.ENOSPC, errno.EDQUOT))


def _extract_database(src, dest: Path) -> None:
    try:
        # v0.51.342: born owner-only — the database holds the admin bcrypt hash and the session / API-token hashes, and took the umask's 0644 / 0664
        fd = _os.open(dest, _os.O_WRONLY | _os.O_CREAT | _os.O_EXCL, 0o600)
    except OSError as e:
        raise _DiskRefused(e) from e
    try:
        while chunk := src.read(_STREAM_BUF):  # v0.51.342: a fault reading the archive raises as itself — still a refusal
            view = memoryview(chunk)
            while view:
                try:
                    n = _os.write(fd, view)
                    if n == 0:  # v0.51.344: a 0-byte write never advanced the view — the loop spun forever under STAGING_LOCK
                        raise OSError(errno.EIO, "the write made no progress")
                    view = view[n:]
                except OSError as e:
                    raise _DiskRefused(e) from e
    except BaseException:
        try:
            _os.close(fd)
        except OSError as e:  # v0.51.344: raised, a close fault replaced the read fault in flight — a corrupt bundle read as a disk fault
            log.warning("bundle check: could not close the extraction %s (%s) — the fault already in flight is the one reported", dest.name, e)
        raise
    try:
        _os.close(fd)
    except OSError as e:
        raise _DiskRefused(e) from e


def _over_cap(name: str, size: int) -> str:
    return f"{name} is {size} bytes, over its {_MEMBER_CAP[name]}-byte cap"


def _member_refusal(m: tarfile.TarInfo, seen: dict) -> str | None:
    """Why this header is refused — judged as the stream meets it, before any of its bytes are read — or None."""
    if m.name not in MEMBERS:
        return f"not a motif bundle: unexpected member {m.name!r}"
    if m.name in seen:  # v0.51.342: the old extractor silently kept the LAST copy of a repeated name
        return f"not a motif bundle: {m.name} appears twice"
    if m.type not in (tarfile.REGTYPE, tarfile.AREGTYPE):  # v0.51.342: regular files only — isfile() also passed CONTTYPE and sparse members
        return f"not a motif bundle: {m.name} is not a plain file"
    if m.sparse is not None or any(k.startswith("GNU.sparse.") for k in m.pax_headers):  # v0.51.342: a pax sparse member rides a REGTYPE header, and its realsize replaces the size capped below
        return f"not a motif bundle: {m.name} is not a plain file"
    # v0.51.342: a config/cookies over its own cap is hashed, not held, and refused only by a staging that writes it — never read past a database's cap
    hard = _MEMBER_CAP[m.name] if m.name in (MEMBER_DB, MEMBER_MANIFEST) else _MEMBER_CAP[MEMBER_DB]
    if not 0 <= m.size <= hard:
        return f"not a motif bundle: {m.name} is {m.size} bytes, over its {hard}-byte cap"
    return None


def _inspect_into(path: Path, workdir: Path, *, beside: str = "the bundle") -> BundleCheck:
    """inspect_bundle's one pass, into an empty directory the caller owns and removes. Raises only ExtractionWriteError; an ok check's db_source lives only as long as workdir."""
    shas: dict[str, str] = {}
    sizes: dict[str, int] = {}
    small: dict[str, bytes] = {}
    oversize: dict[str, int] = {}
    try:
        # v0.51.342: ONE forward pass — getmembers() then extractfile() inflated the archive twice per open, and a flow opened it 2-5 times
        with open(path, "rb") as raw, gzip.GzipFile(fileobj=(guard := _TailGuard(raw)), mode="rb") as gz:
            with tarfile.open(fileobj=_TarFeed(gz, guard), mode="r|", bufsize=_STREAM_BUF, tarinfo=_BoundedTarInfo) as tar:
                for m in tar:
                    why = _member_refusal(m, shas)
                    if why:
                        return BundleCheck(False, why)
                    if tar.offset != m.offset_data + -(-m.size // tarfile.BLOCKSIZE) * tarfile.BLOCKSIZE:  # v0.51.342: a pax size capped here while tarfile skipped the raw base-256 one, and "r|" seeks that skip one read at a time
                        return BundleCheck(False, f"not a motif bundle: {m.name} is not a plain file")
                    h = hashlib.sha256()
                    src = _HashingReader(tar.extractfile(m), h)
                    if m.name == MEMBER_DB:
                        _extract_database(src, workdir / MEMBER_DB)
                    elif m.size > _MEMBER_CAP[m.name]:
                        while src.read(_STREAM_BUF):  # v0.51.342: the whole-bundle refusal left even KEEP MY CURRENT CONFIG no way to its database
                            pass
                        oversize[m.name] = m.size
                    else:
                        small[m.name] = src.read()  # capped above; never on disk before it is staged
                    shas[m.name], sizes[m.name] = h.hexdigest(), m.size
            drained = 0  # v0.51.342: on to the gzip trailer (CRC32 + length, never checked before) straight from gz — no fetch resets the guard, so the whole tail shares the last fetch's raw allowance
            while chunk := gz.read(_STREAM_BUF):
                drained += len(chunk)
                if drained > _TAIL_INFLATE_BUDGET:
                    words = f"not a motif bundle: over {_TAIL_INFLATE_BUDGET} bytes of data after the archive's end"
                    log.warning("bundle check: %s refused after the archive's end (%s)", path.name, words)  # v0.51.344: the one read-time refusal that left no breadcrumb (class 9)
                    return BundleCheck(False, words)
    except _DiskRefused as refused:  # v0.51.342: was caught below — a 422 "not a motif bundle: [Errno 28] No space left on device" for a good bundle
        e = refused.err
        log.error("bundle check: could not write the extracted database into %s (%s) — the disk refused it; %s was not judged",
                  workdir, e, path.name)
        raise _write_refused(e, path.name, beside) from e
    except (tarfile.TarError, OSError, ValueError, EOFError, zlib.error, IndexError, RecursionError, _BundleTail) as e:  # v0.51.342: zlib.error — a deflate fault past the first 1 MiB is read through extractfile, where tarfile does not wrap it; IndexError (a GNU sparse 'S' extended header at end of stream) + RecursionError (a ~400-deep chain of 'g'/'x'/'L' headers) crash tarfile's parser before any gate; _BundleTail bounds the archive's tail
        # v0.51.344: an OSError's str names the absolute path, which reached the browser; BadGzipFile has no strerror and keeps its words
        msg = f"{type(e).__name__}: {e.strerror}" if getattr(e, "strerror", None) else str(e)  # v0.51.339: refusals carry the prefix once — never "not a motif bundle: not a motif bundle:"
        log.warning("bundle check: %s refused while reading the archive (%s: %s)", path.name, type(e).__name__, e)
        return BundleCheck(False, msg if msg.startswith("not a motif bundle:") else f"not a motif bundle: {msg}")
    if MEMBER_MANIFEST not in shas or MEMBER_DB not in shas:
        return BundleCheck(False, "not a motif bundle: manifest.json or motif.db missing")
    try:
        manifest = json.loads(small[MEMBER_MANIFEST].decode("utf-8"))
    except Exception as e:
        return BundleCheck(False, f"not a motif bundle: manifest unreadable ({e})")
    if not isinstance(manifest, dict):  # v0.51.339: a foreign manifest's shapes are guarded, not trusted (was a 500)
        return BundleCheck(False, "not a motif bundle: manifest.json is not an object")
    if manifest.get("kind") != "motif-bundle":
        return BundleCheck(False, "not a motif bundle: manifest kind mismatch")
    fmt = manifest.get("format")
    if not isinstance(fmt, int) or fmt > BUNDLE_FORMAT:
        return BundleCheck(False, f"bundle format {fmt!r} is newer than this build reads "
                                  f"({BUNDLE_FORMAT}) — upgrade motif before restoring")
    members = manifest.get("members") or {}
    if not isinstance(members, dict):
        return BundleCheck(False, "not a motif bundle: manifest members is not an object")
    for name, meta in members.items():
        if name not in shas:
            return BundleCheck(False, f"bundle is missing {name} the manifest lists")
        if not isinstance(meta, dict):
            return BundleCheck(False, f"not a motif bundle: the manifest entry for {name} is not an object")
        if shas[name] != meta.get("sha256"):
            return BundleCheck(False, f"{name} does not match the manifest checksum")
    for name in shas:
        if name != MEMBER_MANIFEST and name not in members:
            return BundleCheck(False, f"bundle carries {name} the manifest does not list")
    dbc = db_backup.inspect_restore_source(workdir / MEMBER_DB)
    if not dbc.ok:
        return BundleCheck(False, dbc.error, manifest=manifest, db=dbc)
    if oversize:
        log.warning("bundle check: %s carries %s — %s", path.name, "; ".join(_over_cap(n, s) for n, s in oversize.items()),
                    "only its database can be restored (KEEP MY CURRENT CONFIG)" if MEMBER_CONFIG in oversize  # v0.51.342: cookies alone no longer block the config
                    else "a restore leaves the live cookies file as it is")
    return BundleCheck(True, None, manifest=manifest, db=dbc,
                       has_config=MEMBER_CONFIG in shas, has_cookies=MEMBER_COOKIES in shas,
                       config_bytes=small.get(MEMBER_CONFIG), cookies_bytes=small.get(MEMBER_COOKIES),
                       db_source=db_backup.VerifiedSource(workdir / MEMBER_DB, sizes[MEMBER_DB], shas[MEMBER_DB], dbc),
                       oversize=oversize)


def _remove_tree(p: Path) -> None:
    def warn(fn, path, exc):
        log.warning("bundle restore: could not remove %s (%s) — delete it by hand", path, exc)
    shutil.rmtree(p, onexc=warn)  # v0.51.342: was ignore_errors — an extraction left on /config went unlogged


def inspect_bundle(path: Path) -> BundleCheck:
    """Validate a bundle before anything is staged: a gzip tar whose members
    are only the known names, a manifest of a format this build reads, every
    member's sha256 matching the manifest, and a DB member that passes the
    snapshot restore checks. Raises only ExtractionWriteError (the disk
    refused the extraction)."""
    try:
        tmp = Path(tempfile.mkdtemp(prefix=".bundle-inspect-", dir=path.parent))
    except OSError as e:  # v0.51.344: mkdtemp sat outside the mapping — an ENOSPC was a wordless 500 the page blamed on a proxy
        log.error("bundle check: could not make the extraction directory beside %s (%s) — %s was not judged", path, e, path.name)
        raise _write_refused(e, path.name, "the bundle") from e
    try:
        return _replace(_inspect_into(path, tmp), db_source=None)  # v0.51.342: the extraction dies with tmp — never hand out a token to a deleted file
    finally:
        _remove_tree(tmp)


def preview(path: Path, live_config: Path | None, *, cookies_target: Path | None = None,
            check: BundleCheck | None = None) -> dict:
    """What a restore from this bundle would do — for the settings card's
    preview, before anything is staged. Secrets are masked. `check`: an
    inspection of these same bytes the caller already holds."""
    if check is None:
        check = inspect_bundle(path)
    if not check.ok:
        raise ValueError(check.error or "invalid bundle")
    m = check.manifest or {}
    # v0.51.341: bytes, decoded strictly like the bundle side — a non-UTF-8 live file is named, never diffed through U+FFFD
    live_bytes = live_config.read_bytes() if (live_config and live_config.is_file()) else b""
    diff: list[dict] = []
    # v0.51.339: a side that does not parse is named, and there is no diff — never a false "every key differs".
    parse_error: dict[str, str | None] = {"live": None, "bundle": None}
    cookies_at = str(cookies_target) if cookies_target else None
    if check.has_config:
        live, parse_error["live"] = flatten_config(live_bytes, side="live")
        bundle_bytes = check.config_bytes or b""  # v0.51.342: from the one pass — bundle_config_bytes re-opened the archive and inflated it twice for a few hundred bytes
        if MEMBER_CONFIG in check.oversize:  # v0.51.342: never read in — its database alone restores, and the page keeps the config
            other, parse_error["bundle"] = {}, _over_cap(MEMBER_CONFIG, check.oversize[MEMBER_CONFIG])
        else:
            other, parse_error["bundle"] = flatten_config(bundle_bytes, side="bundle")
        if not (parse_error["live"] or parse_error["bundle"]):
            diff = _diff_rows(live, other)
        # v0.51.341: boot reads settings.cookies_file from the config this bundle swaps in — the live path only when it carries none
        cookies_at = None if parse_error["bundle"] else _cookies_file_after_swap(bundle_bytes)
    census, counts = m.get("themes_census"), m.get("counts")
    created = m.get("created_at")
    try:
        datetime.fromisoformat(created)
    except (TypeError, ValueError):
        log.info("bundle preview: %s has no usable created_at (a %s) — shown as unknown", path.name, type(created).__name__)
        created = None
    left_out = _left_out(m, check)
    cookies_why = left_out.get(MEMBER_COOKIES)
    return {
        "name": path.name,
        "manifest": {
            "format": m.get("format"), "motif_version": m.get("motif_version"),
            "schema_version": m.get("schema_version"),
            "created_at": created,  # v0.51.344: a foreign manifest's stamp is guarded like census_rows — the upload's name no longer reads it
            "census_rows": len(census) if isinstance(census, list) else 0,  # v0.51.339: a non-list census was a TypeError 500
            "counts": counts if isinstance(counts, dict) else {},
        },
        "db": {"ok": check.db.ok if check.db else False,
               "schema_version": check.db.schema_version if check.db else None},
        "config_in_bundle": check.has_config,
        "config_diff": diff,
        "config_parse_error": parse_error,
        # v0.51.342: an over-cap cookies.txt is left out of the staging, not fatal to it — the config still restores
        "cookies": ("in bundle" if check.has_cookies and not cookies_why
                    else f"in bundle, but {cookies_why} — it is not restored, and your cookies file stays as it is" if check.has_cookies
                    else f"not in bundle — {cookies_why}; your cookies file stays as it is" if cookies_why else "not in bundle"),
        "cookies_target": cookies_at,  # v0.51.341: restored cookies land on the boot's settings.cookies_file
        "left_out": left_out,
    }


def _left_out(manifest: dict, check: BundleCheck) -> dict[str, str]:
    """{member: why a restore from this bundle leaves it as it is} — over its cap here, or noted left out when the bundle was made."""
    out = {n: _over_cap(n, s) for n, s in check.oversize.items()}
    noted = manifest.get("left_out")
    for name, present in ((MEMBER_CONFIG, check.has_config), (MEMBER_COOKIES, check.has_cookies)):
        meta = noted.get(name) if isinstance(noted, dict) else None  # v0.51.342: a foreign manifest's shapes are guarded, never trusted
        if present or not isinstance(meta, dict):
            continue
        size, cap = meta.get("size"), meta.get("cap")
        if all(isinstance(v, int) and not isinstance(v, bool) for v in (size, cap)):
            out[name] = f"{name} was left out when the bundle was made ({size} bytes, over its {cap}-byte cap)"
    return out


def _cookies_file_after_swap(config_bytes: bytes) -> str | None:
    """settings.cookies_file as the boot that swaps this parsed motif.yaml in reads it."""
    from . import config_file as cf
    v = cf.load_config_text(config_bytes.decode("utf-8")).paths.cookies_file  # v0.51.344: load()'s own pipeline, env overrides included — a hand-kept copy of its order drifts when load() gains a step
    if not isinstance(v, str):
        log.warning("bundle restore: the bundle's paths.cookies_file is a %s, not a path — no cookies target named",
                    type(v).__name__)
        return None
    return str(Path(v))


_IN_PLACE_ERRNOS = frozenset({errno.EBUSY, errno.EXDEV, errno.EPERM})
_FSYNC_UNSUPPORTED = frozenset({errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP})
# v0.51.341: every stage and cancel runs whole under this — interleaved, one staging's database sat beside another's config
STAGING_LOCK = threading.Lock()


class StagingError(Exception):
    """A stage or cancel that stopped short; the message says what is still staged and what to do."""


class _InPlaceWriteFailed(OSError):
    """The in-place write into a mounted file failed after truncating it; `restored` when its original bytes went back."""
    restored = False


def _stage_file(data: bytes, pending: Path) -> None:
    tmp = pending.with_name(pending.name + ".tmp")
    try:
        if not tmp.is_dir():  # v0.51.344: O_TRUNC kept a crashed staging's 0644 tmp at 0644 while the token went in; a directory keeps open's EISDIR (unlink says EPERM on macOS)
            tmp.unlink(missing_ok=True)
        # v0.51.342: the checksum-verified bytes, never re-read from disk; born 0600, so a chmod-refusing share never leaves the token group-readable
        fd = _os.open(tmp, _os.O_WRONLY | _os.O_CREAT | _os.O_TRUNC, 0o600)
        with open(fd, "wb") as f:
            f.write(data)
        _owner_only(tmp)
        _os.replace(tmp, pending)
    except BaseException:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
        except OSError as e:
            log.warning("bundle restore: could not remove %s (%s)", tmp.name, e)
        raise


def _cause(err: BaseException) -> str:
    # v0.51.342: a StagingError reaches the browser — str(err) names absolute config_dir paths, which stay in log.error
    words = getattr(err, "strerror", None)
    return f"{type(err).__name__}: {words}" if words else type(err).__name__


def _named_failures(failed: dict[str, str]) -> str:
    return "; ".join(f"{name}: {err}" for name, err in failed.items())


_MEMBER_WORD = {CONFIG_PENDING: "config", COOKIES_PENDING: "cookies"}
_LIVE_WORD = {CONFIG_PENDING: "motif.yaml", COOKIES_PENDING: "cookies file"}


def _partial_drop(removed: list[str], failed: dict[str, str], *, db_staged: bool) -> str:
    """What a partial clear dropped, and what a restart now applies in its place ('' when nothing went)."""
    if not removed:
        return ""
    staged = " + ".join((["database"] if db_staged else []) + [_MEMBER_WORD[n] for n in failed])
    live = " and ".join(f"your live {_LIVE_WORD[n]}" for n in removed)
    return f"; {', '.join(removed)} WAS dropped — a restart now applies the staged {staged} with {live}"


def _refuse_beside_stale_config(db_path: Path, config_dir: Path) -> list[str]:
    """stage_restore's before_swap: drop an earlier staging's config/cookies (returns the names dropped), or refuse before its database is replaced."""
    removed, failed = clear_pending_config(config_dir)
    if failed:
        db_staged = db_backup.restore_pending_path(db_path).exists()
        raise StagingError(  # v0.51.341: a partial clear named only what stayed — the earlier staging lost a member unsaid
            f"not staged: {', '.join(failed)} from an earlier restore could not be removed "
            f"({_named_failures(failed)}) and would apply beside this database at restart — "
            f"{'the staged database was not replaced' if db_staged else 'nothing was staged'}"
            f"{_partial_drop(removed, failed, db_staged=db_staged)}; remove {', '.join(failed)} from the config "
            "directory, then stage again")
    return removed


def _unstage_after_swap(db_path: Path, config_dir: Path, staged: list[str], member: str, err: BaseException, *,
                        dropped: list[str], replaced_db: bool) -> StagingError:
    """A bundle member failed after its database swapped in: remove what this call staged, and say what that leaves."""
    log.error("bundle restore: %s could not be staged after the database was (%s) — unstaging this bundle", member, err)
    paths = {"database": db_backup.restore_pending_path(db_path), "config": config_dir / CONFIG_PENDING,
             "cookies": config_dir / COOKIES_PENDING}
    stays: dict[str, str] = {}
    for word in staged:
        try:
            paths[word].unlink()
        except FileNotFoundError:
            pass
        except OSError as e:
            log.error("bundle restore: could not unstage %s (%s) — it still applies at restart", paths[word].name, e)
            stays[paths[word].name] = _cause(e)
            if word == "database":  # v0.51.344: its partners stay with it — unlinked, the database applied at boot beside the live motif.yaml
                break
    earlier = []
    if dropped:
        earlier.append(f"{' and '.join(dropped)} {'was' if len(dropped) == 1 else 'were'} already dropped")
    if replaced_db:
        earlier.append("the database it staged was already replaced")
    said = f"; from the earlier restore, {', and '.join(earlier)}" if earlier else ""
    if paths["database"].name in stays:
        live = "your live motif.yaml" if member == MEMBER_CONFIG else "your live cookies file"
        kept = [w for w in staged if w != "database"]
        beside = " and ".join([f"the staged {w}" for w in kept] + [live])
        # v0.51.344: the remedy names every pending the break kept — the database removed alone, its config applied at boot beside the live database
        alone = f" — removing {paths['database'].name} alone lets {' and '.join(f'the staged {w}' for w in kept)} apply by itself" if kept else ""
        return StagingError(
            f"not staged: {member} could not be staged ({_cause(err)}), and {paths['database'].name} could not be removed "
            f"({_named_failures(stays)}) — a restart applies it with {beside}{said}; "
            f"remove {' and '.join(paths[w].name for w in ['database', *kept])} by hand, then stage again{alone}")
    if stays:
        return StagingError(
            f"not staged: {member} could not be staged ({_cause(err)}), and {', '.join(stays)} could not be removed "
            f"({_named_failures(stays)}) — {'it applies' if len(stays) == 1 else 'they apply'} at restart{said}; "
            f"remove {', '.join(stays)} by hand, then stage again")
    return StagingError(f"not staged: {member} could not be staged ({_cause(err)}) — nothing from this bundle is staged{said}; "
                        "stage again once the cause is fixed")


def stage_bundle_restore(db_path: Path, config_dir: Path, bundle_path: Path, *,
                         keep_config: bool) -> BundleCheck:
    """Stage the DB member through db_backup.stage_restore, and — unless the
    operator keeps the live config — motif.yaml and cookies.txt as
    <name>.restore-pending in config_dir. Raises ValueError when the bundle
    fails inspection, StagingError when an earlier staging's config cannot
    be dropped or a member fails after the database swap (this bundle is
    then unstaged whole), ExtractionWriteError when the disk refuses the
    extraction. Nothing live changes until the next boot."""
    with STAGING_LOCK:
        try:
            tmp = Path(tempfile.mkdtemp(prefix=".bundle-stage-", dir=db_path.parent))  # v0.51.342: beside the pending file, so the checked database MOVES into place
        except OSError as e:  # v0.51.344: mkdtemp sat outside the mapping — an ENOSPC was a wordless 500 the page blamed on a proxy
            log.error("bundle restore: could not make the extraction directory beside %s (%s) — %s was not judged", db_path, e, bundle_path.name)
            raise _write_refused(e, bundle_path.name, db_path.name) from e
        try:
            check = _inspect_into(bundle_path, tmp, beside=db_path.name)  # v0.51.342: one pass + one integrity_check — was inspect_bundle, a second extraction, then stage_restore's own re-check
            if not check.ok:
                raise ValueError(check.error or "invalid bundle")
            if not keep_config and MEMBER_CONFIG in check.oversize:  # v0.51.342: only an over-cap motif.yaml refuses — an over-cap cookies.txt is left as it is, below
                raise ValueError(f"the bundle's {'; '.join(_over_cap(n, s) for n, s in check.oversize.items())} — "
                                 "restore with KEEP MY CURRENT CONFIG to restore its database")
            if not keep_config and check.has_config:
                _, cfg_err = flatten_config(check.config_bytes, side="bundle")
                if cfg_err:  # v0.51.339: staged, it swaps in at boot and ConfigFile.load() raises — the next boot would crash
                    raise ValueError(f"the bundle's motif.yaml does not parse ({cfg_err}) — "
                                     "restore with KEEP MY CURRENT CONFIG, or fix the bundle")
            dropped: list[str] = []
            replaced_db = db_backup.restore_pending_path(db_path).exists()
            # v0.51.339: a bundle stages exactly its own members — an earlier staging's cookies never ride along
            db_backup.stage_restore(db_path, check.db_source.path, verified=check.db_source,  # v0.51.341: dropped BEFORE the swap — a refused drop leaves the earlier database staged
                                    before_swap=lambda: dropped.extend(_refuse_beside_stale_config(db_path, config_dir)))
            check.staged.append("database")
            if not keep_config:
                member = MEMBER_CONFIG
                try:
                    if check.has_config:
                        _stage_file(check.config_bytes, config_dir / CONFIG_PENDING)
                        check.staged.append("config")
                    member = MEMBER_COOKIES
                    if check.has_cookies and MEMBER_COOKIES not in check.oversize:  # v0.51.342: never read in — the database and config restore without it
                        _stage_file(check.cookies_bytes, config_dir / COOKIES_PENDING)
                        check.staged.append("cookies")
                except BaseException as e:  # v0.51.342: an ENOSPC here left the new database staged with no config — it applied at boot beside the live motif.yaml
                    undone = _unstage_after_swap(db_path, config_dir, check.staged, member, e,
                                                 dropped=dropped, replaced_db=replaced_db)
                    if not isinstance(e, Exception):
                        raise
                    raise undone from e
                for member, why in _left_out(check.manifest or {}, check).items():  # v0.51.344: left out when made, too — a partial bundle answered that nothing was left as it is
                    live = "motif.yaml" if member == MEMBER_CONFIG else "cookies file"
                    check.left_as_is[member] = f"{why} — it is not restored, and your {live} is left as it is"
        finally:
            _remove_tree(tmp)
        log.info("bundle restore staged from %s: %s; applies on next restart",
                 bundle_path.name, ", ".join(check.staged))
        for words in check.left_as_is.values():
            log.warning("bundle restore from %s: %s", bundle_path.name, words)
        return _replace(check, db_source=None, config_bytes=None, cookies_bytes=None)  # v0.51.342: the token's file is gone and the caller never needs the bytes


def stage_snapshot_restore(db_path: Path, config_dir: Path, source_path: Path) -> db_backup.RestoreCheck:
    """Both snapshot endpoints: stage the database alone, an earlier bundle's config/cookies dropped before the swap."""
    with STAGING_LOCK:
        return db_backup.stage_restore(db_path, source_path,
                                       before_swap=lambda: _refuse_beside_stale_config(db_path, config_dir))


def pending_members(db_path: Path, config_dir: Path) -> list[str]:
    out = []
    if db_backup.restore_pending_path(db_path).exists():
        out.append("database")
    if (config_dir / CONFIG_PENDING).exists():
        out.append("config")
    if (config_dir / COOKIES_PENDING).exists():
        out.append("cookies")
    return out


def clear_pending_config(config_dir: Path) -> tuple[list[str], dict[str, str]]:
    """Unlink the staged motif.yaml / cookies.txt. Never raises; returns (names removed, {name: error} still staged)."""
    removed: list[str] = []
    failed: dict[str, str] = {}
    for name in (CONFIG_PENDING, COOKIES_PENDING):
        p = config_dir / name
        try:
            if not p.exists():
                continue
            p.unlink()
        except OSError as e:  # v0.51.341: raised, it 500'd a staging whose new database was already pending, and crashed boot before logging
            log.error("staged config restore: could not remove %s (%s) — it is still staged", name, e)
            failed[name] = _cause(e)  # v0.51.342: these words reach the browser
            continue
        removed.append(name)
    if removed:
        log.info("staged config restore dropped: %s", ", ".join(removed))
    return removed, failed


def cancel_pending(db_path: Path, config_dir: Path) -> bool:
    """Drop everything staged — the config members first, then the DB (db_backup). Raises StagingError when a file stays."""
    with STAGING_LOCK:  # v0.51.341: a cancel interleaved with a staging could keep that staging's config without its database
        removed, failed = clear_pending_config(config_dir)
        if failed:  # v0.51.341: the database stays staged — cancelled alone, a config left behind would go live without it
            db_staged = db_backup.restore_pending_path(db_path).exists()
            held = " — the staged database is kept" if db_staged else ""
            if db_staged and not removed:  # v0.51.341: a dropped member means a restart applies the database without it
                held += " so nothing applies without it"
            raise StagingError(
                f"not cancelled: {', '.join(failed)} could not be removed ({_named_failures(failed)}){held}"
                f"{_partial_drop(removed, failed, db_staged=db_staged)}; remove {', '.join(failed)} from the config "
                "directory, then cancel again")
        try:
            db_cancelled = db_backup.cancel_pending_restore(db_path)
        except OSError as e:
            log.error("cancel_pending: the staged database could not be removed (%s)", e)
            raise StagingError(
                f"not cancelled: the staged database could not be removed ({_cause(e)}) — it still applies at restart, "
                f"with the live config; remove {db_backup.restore_pending_path(db_path).name} beside motif.db, "
                "then cancel again") from e
        return bool(removed) or db_cancelled


def _owner_only(path: Path) -> None:
    try:
        path.chmod(0o600)
    except OSError as e:  # v0.51.339: a share that refuses chmod keeps the restore — the mode is a belt, not the swap
        log.warning("bundle restore: could not chmod 0600 %s (%s) — it keeps its current mode", path, e)


def _prerestore_copy(live: Path, pending: Path, now_stamp: str) -> Path | None:
    """The one undo copy of `live` for this pending: None when live already holds its bytes, a retry's identical copy when one exists."""
    data = live.read_bytes()
    if data == pending.read_bytes():
        return None  # v0.51.341: a boot retrying a finished swap — nothing is replaced, so no second copy
    prefix = f"{live.name}.prerestore-"
    for p in sorted(live.parent.iterdir()):  # v0.51.341: every boot retrying a refused swap wrote another <live>.prerestore-<stamp>
        if not p.name.startswith(prefix) or not p.is_file():
            continue
        try:
            if p.stat().st_size == len(data) and p.read_bytes() == data:
                return p
        except OSError as e:
            log.warning("bundle restore: could not compare %s (%s) — writing a fresh pre-restore copy", p.name, e)
    keep = live.with_name(f"{prefix}{now_stamp}")
    shutil.copyfile(live, keep)  # v0.51.339: copyfile + a chmod that may fail — copy2's copystat raised on a chmod-refusing share
    _owner_only(keep)
    return keep


def _replace_or_write_in_place(src: Path, dest: Path) -> bool:
    """os.replace(src, dest), or — when dest is a mount point a rename cannot land on — src's bytes written into it. True when in place."""
    try:
        _os.replace(src, dest)
        return False
    except OSError as e:
        if e.errno not in _IN_PLACE_ERRNOS or not dest.is_file():
            raise
        # v0.51.341: a single-file bind mount (-v host/cookies.txt:/config/cookies.txt) refused the rename with EBUSY on every boot
        log.warning("bundle restore: %s cannot be replaced by a rename (%s) — writing the restored bytes into it in place",
                    dest, e)
    data = src.read_bytes()
    fd = _os.open(dest, _os.O_RDWR)  # never truncates on open: a refusal here leaves the live file whole
    try:
        with open(fd, "rb", closefd=False) as f:
            original = f.read()  # v0.51.342: what the truncate destroys — a partial file was left, and the next boot's undo copy was of it
        try:
            _rewrite_fd(fd, data)
        except OSError as e:
            try:
                _rewrite_fd(fd, original)
            except OSError as back:
                log.error("bundle restore: %s was left partly written (%s), and its original bytes could not be "
                          "written back (%s)", dest, e, back)
                raise _InPlaceWriteFailed(f"{e} while writing {dest} in place — it may be partly written") from e
            log.error("bundle restore: writing %s in place failed (%s) — its original bytes were written back", dest, e)
            failed = _InPlaceWriteFailed(f"{e} while writing {dest} in place — its original bytes were written back")
            failed.restored = True
            raise failed from e
    finally:
        _os.close(fd)
    return True


def _rewrite_fd(fd: int, data: bytes) -> None:
    _os.lseek(fd, 0, _os.SEEK_SET)
    _os.ftruncate(fd, 0)
    view = memoryview(data)
    while view:
        n = _os.write(fd, view)
        if n == 0:  # v0.51.344: a 0-byte write never advanced the view — the loop spun forever at boot
            raise OSError(errno.EIO, "the write made no progress")
        view = view[n:]
    try:
        _os.fsync(fd)
    except OSError as e:
        if e.errno not in _FSYNC_UNSUPPORTED:
            raise
        log.warning("bundle restore: this mount cannot fsync the file (%s) — its %d bytes are written, not synced", e, len(data))  # v0.51.344: was "partly written" over whole bytes, and every boot retried and failed


def apply_pending_config(config_dir: Path, *, now_stamp: str) -> dict | None:
    """BOOT hook, before get_settings() reads motif.yaml, once the staged
    database applied (or none was staged). Swap a staged motif.yaml into
    place after a .prerestore-<stamp> copy. Never raises; a failure keeps
    the live and pending files and logs WHY (cold-path rule)."""
    pending = config_dir / CONFIG_PENDING
    if not pending.exists():
        return None
    live = config_dir / MEMBER_CONFIG
    safety: dict[str, str] = {}
    try:
        if live.exists():
            keep = _prerestore_copy(live, pending, now_stamp)  # v0.51.341: copy2 here raised on a chmod-refusing share, and every retrying boot added a copy
            if keep is not None:
                safety[MEMBER_CONFIG] = keep.name
        in_place = _replace_or_write_in_place(pending, live)
    except OSError as e:
        kept = "restore it from its pre-restore copy" if isinstance(e, _InPlaceWriteFailed) and not e.restored else "live file kept"
        log.error("apply_pending_config: motif.yaml not swapped (%s) — %s, pending file kept for a retry", e, kept)
        return {"applied": [], "safety": safety, "errors": {MEMBER_CONFIG: str(e)}}
    if in_place:
        try:
            pending.unlink()
        except OSError as e:
            log.error("apply_pending_config: motif.yaml is restored but %s could not be removed (%s) — "
                      "it applies again on the next restart", pending.name, e)
    _owner_only(live)  # v0.51.339: a pending staged by .336-.338 is 0644 — the live config holds the Plex token
    log.warning("motif.yaml RESTORED from a staged bundle (pre-restore copy: %s)",
                safety.get(MEMBER_CONFIG, "(none)"))
    return {"applied": [MEMBER_CONFIG], "safety": safety, "errors": {}}


def apply_pending_cookies(config_dir: Path, live: Path, *, now_stamp: str) -> dict | None:
    """BOOT hook, after get_settings(): copy a staged cookies.txt onto the
    cookies file yt-dlp reads (settings.cookies_file, any mount) after a
    <live>.prerestore-<stamp> copy. Never raises; a failure keeps the live
    and pending files and logs WHY (cold-path rule)."""
    pending = config_dir / COOKIES_PENDING
    if not pending.exists():
        return None
    safety: dict[str, str] = {}
    dest: Path | None = None
    tmp: Path | None = None
    in_place = False
    try:
        dest = Path(_os.path.realpath(live))  # v0.51.339: a symlinked cookies file is restored through its link, never replaced by a plain file
        tmp = dest.with_name(dest.name + ".restore-tmp")
        if dest.exists():
            keep = _prerestore_copy(dest, pending, now_stamp)  # v0.51.341: one undo copy per staged pending, however many boots retry
            if keep is not None:
                safety[str(dest)] = keep.name
        shutil.copyfile(pending, tmp)
        _owner_only(tmp)
        in_place = _replace_or_write_in_place(tmp, dest)  # v0.51.339: the temp sits beside the target, so the swap is atomic on the target's own mount
    except (OSError, ValueError) as e:  # ValueError: a cookies path with no file name (e.g. "/") — never-raises holds at boot
        kept = "restore it from its pre-restore copy" if isinstance(e, _InPlaceWriteFailed) and not e.restored else "live file kept"
        log.error("apply_pending_cookies: %s not restored (%s) — %s, "
                  "pending file kept for a retry", dest or live, e, kept)
        if tmp is not None:
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass
            except OSError as ce:
                log.warning("apply_pending_cookies: could not remove %s (%s)", tmp, ce)
        return {"applied": [], "safety": safety, "errors": {str(dest or live): str(e)}}
    if in_place:
        # v0.51.342: no chmod — 0600 on a host's bind-mounted file locked out every other container that reads it
        log.info("apply_pending_cookies: %s was written in place — its mode is left as the host set it", dest)
        try:
            tmp.unlink()
        except OSError as e:
            log.warning("apply_pending_cookies: could not remove %s (%s)", tmp, e)
    log.warning("cookies RESTORED from a staged bundle to %s (pre-restore copy: %s)",
                dest, safety.get(str(dest), "(none)"))
    try:
        pending.unlink()
    except OSError as e:
        log.error("apply_pending_cookies: %s is restored but %s could not be removed (%s) — "
                  "it applies again on the next restart", dest, pending.name, e)
    return {"applied": [str(dest)], "safety": safety, "errors": {}}
