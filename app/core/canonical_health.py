"""v0.51.167: CANONICAL HEALTH — find + repair local_files rows whose canonical
theme.mp3 is gone (or 0-byte) from motif's storage, and decide how each is fixed.

The loudness audit (loudness_audit.py) surfaced a real cohort: ~14 library items
whose canonical file_path pointed at a missing file — ffmpeg measure returned
rc=254 "No such file or directory". A vanished / corrupt canonical is separate
from the loudness feature; it's a storage-health problem (an external sweep,
a failed download that left a 0-byte stub, a moved file, a lost mount that never
came back). motif already STAMPS the condition — `verify_canonical_health`
(plex_enum.py) writes `local_files.canonical_present = 0` for a row whose
`themes_dir/file_path` is missing/0-byte, and the library paints a red DL dot +
`dl_pills=broken` filter for it. What was missing is the OPERATOR-facing repair:
an aggregated list of the broken rows, split by how each can be fixed.

Repair decision (per the download worker's own URL resolution, worker.py:1672 —
`override.youtube_url or theme.youtube_url`):

  • RE-DOWNLOADABLE — the row's recorded source IS a re-fetchable URL, so a fresh
    download restores it byte-for-byte:
      - source_kind='url'  → a user_overrides URL is the source (always present +
        NOT NULL for a U row).
      - source_kind='themerrdb' / NULL(legacy) → the ThemerrDB `youtube_url` is the
        source; re-downloadable iff it's non-empty AND the item is genuinely
        TDB-tracked (upstream_source != 'plex_orphan').

  • CANONICAL MISSING — no re-fetchable URL; the operator must re-place manually
    (SET URL / UPLOAD MP3 / RESTORE FROM PLEX from the INFO card):
      - source_kind IN ('upload','adopt','plex_cloud') → the bytes came from a
        direct upload / adopted sidecar / Plex-cloud backup, NOT a URL motif can
        re-fetch. Re-downloading TDB over these would REPLACE the operator's chosen
        content with something different — so they are never auto-repaired here.
      - any re-download-eligible row whose resolved URL is empty.

Everything is edition-scoped: local_files' PK is
(media_type, tmdb_id, section_id, edition_key) and every query + the re-download
enqueue carry edition_key, so one edition's broken canonical never re-downloads
into a sibling edition's folder (v1.21.x edition-isolation rule).
"""
from __future__ import annotations

import logging
from pathlib import Path

log = logging.getLogger("motif.canonical_health")

# The download pipeline records no re-fetchable URL for these — their bytes are a
# direct upload / adopted sidecar / Plex-cloud backup. A broken canonical for one
# of these is surfaced for manual re-place, never auto-re-downloaded (a TDB URL on
# the shared themes row is NOT this row's source — re-downloading would swap the
# operator's content). Mirrors the api.py canonical-danger grouping.
_NO_URL_SOURCE_KINDS = ("upload", "adopt", "plex_cloud")


def _override_url(conn, r) -> str | None:
    """The user_overrides URL the download worker would resolve for this row —
    section-scoped first, then the '' global fallback (mirrors worker.py:1646)."""
    row = conn.execute(
        "SELECT youtube_url FROM user_overrides "
        "WHERE media_type = ? AND tmdb_id = ? AND section_id = ? AND edition_key = ?",
        (r["media_type"], r["tmdb_id"], r["section_id"], r["edition_key"]),
    ).fetchone()
    if row is None:
        row = conn.execute(
            "SELECT youtube_url FROM user_overrides "
            "WHERE media_type = ? AND tmdb_id = ? AND section_id = '' AND edition_key = ?",
            (r["media_type"], r["tmdb_id"], r["edition_key"]),
        ).fetchone()
    return row["youtube_url"] if row else None


def classify_repair(conn, r) -> str:
    """'redownload' if the row's recorded source is a re-fetchable URL, else
    'canonical_missing' (surface for manual re-place). See the module docstring
    for the full rule; keyed off source_kind + the worker's URL resolution so it
    agrees with what a re-download would actually do."""
    if r["source_kind"] in _NO_URL_SOURCE_KINDS:
        return "canonical_missing"
    # source_kind is 'url', 'themerrdb', or NULL(legacy). A live override URL is
    # always re-fetchable (a U row); otherwise fall back to the TDB URL, but only
    # for a genuinely TDB-tracked item (a plex_orphan carries no meaningful TDB URL).
    if _override_url(conn, r):
        return "redownload"
    if r["upstream_source"] != "plex_orphan" and (r["tdb_url"] or "").strip():
        return "redownload"
    return "canonical_missing"


def _broken_rows(conn) -> list:
    """local_files rows CONFIRMED broken (canonical_present = 0, stamped by
    verify_canonical_health). NULL (never-verified) rows are excluded — we only
    surface a confirmed-missing canonical, never an unverified one. Carries the
    themes fields the classifier needs + a has_live_placement hint (a surviving
    Plex-folder copy → RESTORE FROM PLEX is available for a manual row)."""
    return conn.execute(
        "SELECT lf.media_type, lf.tmdb_id, lf.section_id, lf.edition_key, "
        "       lf.file_path, lf.source_kind, "
        "       t.title, t.year, t.youtube_url AS tdb_url, t.upstream_source, "
        "       EXISTS(SELECT 1 FROM placements p "
        "              WHERE p.media_type = lf.media_type AND p.tmdb_id = lf.tmdb_id "
        "                AND p.section_id = lf.section_id "
        "                AND p.edition_key = lf.edition_key "
        "                AND p.theme_present = 1) AS has_live_placement, "
        "       COALESCE(ps.is_anime, 0) AS is_anime "
        "FROM local_files lf "
        "LEFT JOIN themes t "
        "  ON t.media_type = lf.media_type AND t.tmdb_id = lf.tmdb_id "
        "LEFT JOIN plex_sections ps ON ps.section_id = lf.section_id "
        "WHERE lf.canonical_present = 0 "
        "ORDER BY lf.media_type, t.title, lf.tmdb_id, lf.section_id, lf.edition_key"
    ).fetchall()


def _entry(r) -> dict:
    """The report payload for one broken row — identity + the INFO-card deep-link
    fields (media_type/tmdb_id/section_id) the operator clicks to re-place."""
    return {
        "media_type": r["media_type"],
        "tmdb_id": r["tmdb_id"],
        "section_id": r["section_id"],
        "edition_key": r["edition_key"],
        "title": r["title"] or f'{r["media_type"]}/{r["tmdb_id"]}',
        "year": r["year"],
        "source_kind": r["source_kind"] or "",
        "file_path": r["file_path"],
        "is_anime": bool(r["is_anime"]),
        # v0.51.311 (review): anime rows deep-link to /anime, not /tv — the
        # .308/.309 routing fix reached the inbox + /queue producers only.
    }


def _sidecar_survives(media_folder) -> bool:
    """Stat, not the stored flag: a database restored onto a new box carries the
    OLD box's theme_present stamps, and a fresh install has none — the bulk
    restores from what is actually there, so the report must count the same."""
    if not media_folder:
        return False
    from .plex_enum import _candidate_local_paths
    for cand in _candidate_local_paths(media_folder):
        try:
            p = cand / "theme.mp3"
            if p.is_file() and p.stat().st_size > 0:
                return True
        except OSError:
            continue
    return False


def broken_canonical_report(conn, themes_dir: "Path | None" = None) -> dict:
    """Read-only: every confirmed-broken canonical, split into the re-downloadable
    set (REPAIR ALL can fix these) and the canonical-missing set (manual re-place).
    Writes nothing — the /admin/canonical-health page renders this.
    v0.51.337: every broken row also says whether Plex still holds a copy
    (restorable_from_plex — a surviving sidecar or a plex_upload placement), and
    with a themes_dir the report adds the CHANGED rows (present, wrong size)."""
    rows = _broken_rows(conn)
    redownloadable: list[dict] = []
    canonical_missing: list[dict] = []
    restorable = 0
    for r in rows:
        entry = _entry(r)
        p = _placement_for(conn, r)
        entry["has_live_placement"] = bool(r["has_live_placement"])
        # a pushed theme lives in Plex's store (no folder to copy from); any other
        # live placement is a sidecar still in the Plex folder.
        entry["plex_copy"] = (
            "store" if (p and p["placement_kind"] == "plex_upload")
            else "sidecar" if (r["has_live_placement"] or _sidecar_survives(p["media_folder"] if p else None))
            else None)
        if entry["plex_copy"]:
            restorable += 1
        if classify_repair(conn, r) == "redownload":
            redownloadable.append(entry)
        else:
            canonical_missing.append(entry)
    changed = changed_canonicals(conn, themes_dir) if themes_dir else []
    return {
        "broken": len(rows),
        "redownloadable": redownloadable,
        "canonical_missing": canonical_missing,
        "changed": changed,
        "counts": {
            "broken": len(rows),
            "redownloadable": len(redownloadable),
            "canonical_missing": len(canonical_missing),
            "restorable_from_plex": restorable,
            "changed": len(changed),
        },
    }


def enqueue_canonical_repairs(conn) -> dict:
    """Re-download every re-downloadable broken canonical (auto-place + force so
    the fresh bytes overwrite whatever remains — same shape as the /redownload
    endpoint). Edition-scoped: each row re-downloads only into its own section +
    edition. Canonical-missing (no-URL) rows are counted as `surfaced`, never
    touched. Returns a summary. Caller wraps this in a transaction."""
    from .sync import _enqueue_download

    rows = _broken_rows(conn)
    repaired_rows = 0
    enqueued_sections = 0
    surfaced = 0
    no_op = 0
    for r in rows:
        if classify_repair(conn, r) != "redownload":
            surfaced += 1
            continue
        n = _enqueue_download(
            conn,
            media_type=r["media_type"],
            tmdb_id=r["tmdb_id"],
            reason="manual",
            auto_place=True,
            force_place=True,
            only_section_id=r["section_id"],
            edition_key=r["edition_key"],
        )
        if n:
            repaired_rows += 1
            enqueued_sections += n
        else:
            # _enqueue_download returned 0 — item isn't in an included Plex section
            # yet, or an identical download is already queued (dedup). Neither is an
            # error; the operator just sees it stay broken until Plex/enum catches up.
            no_op += 1
    log.info(
        "canonical repair: %d re-downloaded (%d sections), %d surfaced (no URL), "
        "%d no-op of %d broken",
        repaired_rows, enqueued_sections, surfaced, no_op, len(rows),
    )
    return {
        "broken": len(rows),
        "repaired_rows": repaired_rows,
        "enqueued_sections": enqueued_sections,
        "surfaced": surfaced,
        "no_op": no_op,
    }


# ── v0.51.337: RESTORE FROM PLEX + CHANGED (feature D tag 3, the themes check) ──
# docs/specs/BACKUP_BUNDLE_SPEC.md § 3: after a restore (or any time), the
# themes directory is checked against what the database expects. The missing
# rows already split into re-downloadable vs canonical-missing above; this
# adds the other way a canonical can come back without a source URL — from
# Plex: the sidecar still in the Plex folder (a hardlink/copy placement
# survives motif's copy being unlinked), or Plex's own store for a
# plex_upload placement (the v0.51.171 fetch_theme_bytes path). And the
# third state a bundle's census implies: a file that is present but no
# longer the size the database recorded.

_PLACEMENT_SQL = (
    "SELECT media_folder, placement_kind, plex_rating_key, theme_present "
    "FROM placements WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
    "AND COALESCE(edition_key, '') = COALESCE(?, '')"
)


def _placement_for(conn, r):
    return conn.execute(
        _PLACEMENT_SQL, (r["media_type"], r["tmdb_id"], r["section_id"], r["edition_key"]),
    ).fetchone()


def _stamp_restored(db_path: Path, r, canonical: Path, *, prior_size, prior_sha,
                    placement_kind: str | None) -> None:
    """After bytes landed at the canonical path: re-hash, stamp size / sha /
    downloaded_at / canonical_present=1, and record the placement kind when the
    restore changed it (a hardlink that had to fall back to a copy)."""
    import hashlib
    from .db import get_conn, transaction
    from .events import now_iso
    try:
        size = canonical.stat().st_size
        h = hashlib.sha256()
        with canonical.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        sha = h.hexdigest()
    except OSError as e:
        log.warning("restore: re-hash failed for %s/%s section=%s: %s — keeping prior size/sha",
                    r["media_type"], r["tmdb_id"], r["section_id"], e)
        size, sha = prior_size, prior_sha
    with get_conn(db_path) as conn, transaction(conn):
        conn.execute(
            "UPDATE local_files SET file_size = ?, file_sha256 = ?, downloaded_at = ?, "
            "canonical_present = 1 "
            "WHERE media_type = ? AND tmdb_id = ? AND section_id = ? AND edition_key = ?",
            (size, sha, now_iso(), r["media_type"], r["tmdb_id"], r["section_id"], r["edition_key"]),
        )
        if placement_kind is not None:
            conn.execute(
                "UPDATE placements SET placement_kind = ? WHERE media_type = ? AND tmdb_id = ? "
                "AND section_id = ? AND edition_key = ?",
                (placement_kind, r["media_type"], r["tmdb_id"], r["section_id"], r["edition_key"]),
            )


def restore_from_placement(db_path: Path, themes_dir: Path, r) -> dict:
    """Re-create ONE row's canonical from the theme.mp3 still in its Plex folder
    (hardlink first, copy across filesystems). `r` carries media_type, tmdb_id,
    section_id, edition_key, file_path, file_size, file_sha256, media_folder,
    placement_kind. Returns {ok, kind} or {ok: False, reason} with the reasons
    the per-item endpoint has always reported."""
    import os
    import shutil
    from .plex_enum import _candidate_local_paths
    canonical = themes_dir / r["file_path"]
    try:
        if canonical.is_file() and canonical.stat().st_size > 0:
            return {"ok": False, "reason": "canonical_already_present"}
    except OSError:
        pass
    if not r["media_folder"]:
        return {"ok": False, "reason": "no_placement"}
    src: Path | None = None
    for cand in _candidate_local_paths(r["media_folder"]):
        p = cand / "theme.mp3"
        try:
            if p.is_file() and p.stat().st_size > 0:
                src = p
                break
        except OSError:
            continue
    if src is None:
        return {"ok": False, "reason": "placement_file_missing"}
    try:
        canonical.parent.mkdir(parents=True, exist_ok=True)
        if canonical.exists():
            canonical.unlink()  # a 0-byte stub
        kind = "hardlink"
        try:
            os.link(src, canonical)
        except OSError as e:
            if e.errno != 18:  # EXDEV — cross-device, fall back to a copy
                raise
            shutil.copy2(src, canonical)
            kind = "copy"
    except OSError as e:
        log.warning("restore-canonical: %s/%s section=%s failed: %s",
                    r["media_type"], r["tmdb_id"], r["section_id"], e)
        return {"ok": False, "reason": f"link_failed:{e}"}
    _stamp_restored(db_path, r, canonical, prior_size=r["file_size"], prior_sha=r["file_sha256"],
                    placement_kind=kind if r["placement_kind"] != kind else None)
    return {"ok": True, "kind": kind}


def _selected_entry_uri(themes_body) -> str | None:
    """The entry Plex serves for an item: the one flagged selected, else the only
    entry. None when the store holds nothing usable."""
    if not isinstance(themes_body, dict):
        return None
    metadata = (themes_body.get("MediaContainer") or {}).get("Metadata") or []
    entries = [e for e in metadata if isinstance(e, dict) and isinstance(e.get("ratingKey"), str)]
    if not entries:
        return None
    for e in entries:
        if e.get("selected") is True:
            return e["ratingKey"]
    return entries[0]["ratingKey"] if len(entries) == 1 else None


def refetch_from_plex_store(db_path: Path, themes_dir: Path, plex_client, r) -> dict:
    """Re-create ONE row's canonical from the bytes Plex itself serves for the
    item (a plex_upload placement lives in Plex's metadata store, not a folder).
    Returns {ok, bytes, entry_uri} or {ok: False, reason}."""
    import os
    rk = r["plex_rating_key"]
    if not rk or not str(rk).isdigit():
        return {"ok": False, "reason": "no_rating_key"}
    if plex_client is None:
        return {"ok": False, "reason": "plex_unavailable"}
    themes = plex_client.get_themes(rating_key=str(rk))
    if not themes.get("ok"):
        return {"ok": False, "reason": f"plex_themes:{themes.get('http_status') or themes.get('error')}"}
    uri = _selected_entry_uri(themes.get("body"))
    if not uri:
        return {"ok": False, "reason": "no_theme_entry"}
    got = plex_client.fetch_theme_bytes(item_rating_key=str(rk), entry_uri=uri)
    data = got.get("bytes") if got.get("ok") else None
    if not data:
        return {"ok": False, "reason": f"plex_fetch:{got.get('http_status') or got.get('error')}"}
    canonical = themes_dir / r["file_path"]
    try:
        canonical.parent.mkdir(parents=True, exist_ok=True)
        tmp = canonical.with_name(canonical.name + ".part")
        with tmp.open("wb") as f:
            f.write(data)
        os.replace(tmp, canonical)
    except OSError as e:
        log.warning("restore-from-plex-store: %s/%s section=%s write failed: %s",
                    r["media_type"], r["tmdb_id"], r["section_id"], e)
        return {"ok": False, "reason": f"write_failed:{e}"}
    _stamp_restored(db_path, r, canonical, prior_size=r["file_size"], prior_sha=r["file_sha256"],
                    placement_kind=None)
    return {"ok": True, "bytes": len(data), "entry_uri": uri}


def _broken_rows_with_placement(conn) -> list[dict]:
    """The broken rows as dicts carrying the fields both restore paths need."""
    out = []
    for r in _broken_rows(conn):
        lf = conn.execute(
            "SELECT file_size, file_sha256 FROM local_files WHERE media_type = ? AND tmdb_id = ? "
            "AND section_id = ? AND edition_key = ?",
            (r["media_type"], r["tmdb_id"], r["section_id"], r["edition_key"]),
        ).fetchone()
        p = _placement_for(conn, r)
        d = {k: r[k] for k in r.keys()}
        d["file_size"] = lf["file_size"] if lf else None
        d["file_sha256"] = lf["file_sha256"] if lf else None
        d["media_folder"] = p["media_folder"] if p else None
        d["placement_kind"] = p["placement_kind"] if p else None
        d["plex_rating_key"] = p["plex_rating_key"] if p else None
        out.append(d)
    return out


def restore_from_plex(db_path: Path, themes_dir: Path, plex_client) -> dict:
    """The bulk: every broken canonical that Plex still holds a copy of — the
    sidecar in its Plex folder first (no network), else Plex's own store for a
    plex_upload placement. Rows with neither are skipped with a reason; nothing
    is re-downloaded here (that is REPAIR ALL). Never overwrites a present file."""
    from .db import get_conn
    with get_conn(db_path) as conn:
        rows = _broken_rows_with_placement(conn)
    restored_sidecar = 0
    restored_store = 0
    skipped: list[dict] = []
    for r in rows:
        res = None
        if r["media_folder"]:
            res = restore_from_placement(db_path, themes_dir, r)
            if res["ok"]:
                restored_sidecar += 1
                continue
        if r["placement_kind"] == "plex_upload" or (r["media_folder"] in (None, "") and r["plex_rating_key"]):
            res2 = refetch_from_plex_store(db_path, themes_dir, plex_client, r)
            if res2["ok"]:
                restored_store += 1
                continue
            res = res2
        skipped.append({"title": r["title"] or f'{r["media_type"]}/{r["tmdb_id"]}',
                        "media_type": r["media_type"], "tmdb_id": r["tmdb_id"],
                        "section_id": r["section_id"],
                        "reason": (res or {}).get("reason") or "no_plex_copy"})
    log.info("restore from plex: %d from sidecars, %d from Plex's store, %d skipped of %d broken",
             restored_sidecar, restored_store, len(skipped), len(rows))
    return {"broken": len(rows), "restored_sidecar": restored_sidecar,
            "restored_store": restored_store, "restored": restored_sidecar + restored_store,
            "skipped": skipped}


def changed_canonicals(conn, themes_dir: Path) -> list[dict]:
    """Rows whose canonical IS on disk but no longer the size the database
    recorded — a truncated write, an external edit, a file swapped underneath
    motif. A size the database never recorded (NULL / 0) cannot be judged and
    is left alone. Read-only: stats, never stamps."""
    rows = conn.execute(
        "SELECT lf.media_type, lf.tmdb_id, lf.section_id, lf.edition_key, lf.file_path, "
        "       lf.file_size, lf.source_kind, t.title, t.year, COALESCE(ps.is_anime, 0) AS is_anime "
        "FROM local_files lf "
        "LEFT JOIN themes t ON t.media_type = lf.media_type AND t.tmdb_id = lf.tmdb_id "
        "LEFT JOIN plex_sections ps ON ps.section_id = lf.section_id "
        "WHERE COALESCE(lf.canonical_present, 1) = 1 AND COALESCE(lf.file_size, 0) > 0 "
        "ORDER BY lf.media_type, t.title, lf.tmdb_id, lf.section_id, lf.edition_key"
    ).fetchall()
    out = []
    for r in rows:
        try:
            st = (themes_dir / r["file_path"]).stat()
        except OSError:
            continue  # absent rows are the broken set's business
        if st.st_size != r["file_size"] and st.st_size > 0:
            out.append({**_entry(r), "recorded": r["file_size"], "on_disk": st.st_size})
    return out
