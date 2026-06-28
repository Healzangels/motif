"""v1.18.37: Plex-side drift scanner for plex_upload placements.

Walks every `placement_kind='plex_upload'` row in motif's DB,
queries Plex's `/library/metadata/{rk}/themes` for each, and
classifies whether motif's tracking matches Plex's reality.

Read-only — does NOT apply fixes. The classifications guide
operator follow-up (RE-PUSH, LET PLEX SERVE, etc. via motif's
existing UI actions).

## Drift types

  - `ok` — motif's hash is present and selected. Healthy.
  - `rk_lookup_failed` — couldn't find a plex_items.rating_key
    for this (mt, tmdb_id, section). Likely orphaned placement
    from a Plex item that was removed.
  - `plex_fetch_failed` — GET /themes returned non-2xx or
    transport error. Plex unreachable or rk doesn't exist.
  - `no_plex_entries` — Plex has zero theme entries for the
    item. Motif's upload silently failed at some point.
  - `motif_hash_unknown` — couldn't hash motif's canonical
    (file missing / IO error). Selection state can't be
    verified.
  - `motif_entry_missing` — motif's hash is NOT in Plex's
    entries. Upload silently failed or was wiped externally.
  - `motif_not_selected` — motif's entry is in /themes but
    NOT selected. Plex is serving something else (e.g., user
    selected an alternate via Plex Web's hidden mechanism, or
    a prior buggy SWITCH-orphan upload is winning).
  - `nothing_selected` — motif's entry present but no entry
    is selected:true. Row plays no theme. Likely post-DELETE
    state without re-selection.

## Use

  scanner = scan_plex_upload_placements(db, plex_client)
  for f in scanner:
      print(f"{f['drift_type']}: {f['media_type']}/{f['tmdb_id']}")
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def _hash_file(path: Path) -> str | None:
    """SHA-1 of file contents (matches Plex's theme-store hash
    function), or None if file unreadable / missing."""
    if not path.is_file():
        return None
    try:
        h = hashlib.sha1()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError as e:
        log.debug("_hash_file: %s unreadable: %s", path, e)
        return None


def _resolve_canonical(themes_dir: Path | None, rel_or_abs: str) -> Path:
    """v1.18.39: local_files.file_path is stored RELATIVE to
    settings.themes_dir. Join the two unless the stored value is
    already absolute (legacy rows). Mirrors worker.py:1812 +
    placement.py expectations."""
    p = Path(rel_or_abs)
    if p.is_absolute() or themes_dir is None:
        return p
    return themes_dir / rel_or_abs


def _plex_mt(motif_mt: str) -> str:
    """Map motif's media_type to Plex's media_type string for
    plex_items lookups."""
    if motif_mt == "tv":
        return "show"
    if motif_mt == "collection":
        return "collection"
    return "movie"


def scan_plex_upload_placements(
    db_path: Path | str, plex: Any,
    themes_dir: Path | None = None,
    progress_cb=None,
) -> list[dict]:
    """Walk every placement_kind='plex_upload' row in motif's DB
    and cross-check against Plex's /themes state.

    Args:
      db_path: motif's SQLite file
      plex: a connected PlexClient instance
      themes_dir: v1.18.39 — settings.themes_dir for resolving
        local_files.file_path (stored relative to this dir).
        Optional for test fixtures that use absolute paths.

    Returns: list of per-placement finding dicts. See module
    docstring for drift_type classifications.
    """
    # Late import to avoid circular dep with the rest of app.core.
    from .db import get_conn

    findings: list[dict] = []
    with get_conn(db_path) as conn:
        # v1.22.81: carry edition_key — it joined the placements PK in
        # v1.21.51 PRECISELY to disambiguate plex_upload rows, but the
        # scan kept keying findings on the 3-tuple: a two-edition
        # plex_upload title produced identical-keyed findings (PROBE
        # patched the wrong row; RE-PUSH re-placed every edition).
        placements = conn.execute(
            "SELECT media_type, tmdb_id, section_id, edition_key "
            "FROM placements "
            "WHERE placement_kind = 'plex_upload' "
            "ORDER BY media_type, tmdb_id, section_id, edition_key"
        ).fetchall()

    # v1.21.24: report progress to the optional callback so the
    # background-op runner can surface "X / N checked" on the orphans
    # page while the per-row Plex /themes probes run.
    total = len(placements)
    for i, p in enumerate(placements):
        if progress_cb is not None:
            # v1.22.33 (audit): a progress-callback error must not abort the
            # whole sweep (it only surfaces "X / N checked" cosmetically).
            try:
                progress_cb(i, total)
            except Exception as _e:
                log.debug("orphan_scan: progress_cb raised: %s", _e)
        findings.append(scan_one_placement(
            db_path, plex,
            p["media_type"], p["tmdb_id"], p["section_id"],
            edition_key=p["edition_key"] or "",
            themes_dir=themes_dir,
        ))
    if progress_cb is not None:
        progress_cb(total, total)
    return findings


def scan_one_placement(
    db_path: Path | str, plex: Any,
    media_type: str, tmdb_id: int, section_id: str,
    # v1.22.81: the placement's edition — scopes the rk + local_files
    # lookups and stamps the finding's identity. None = legacy
    # unscoped (pre-edition callers / old fixtures).
    edition_key: str | None = None,
    themes_dir: Path | None = None,
) -> dict:
    """v1.22.59: classify ONE plex_upload placement — the extracted body
    of scan_plex_upload_placements' loop, so the orphans page per-row
    PROBE can re-check a single item without re-walking the whole
    library (the user: "is it possible that reprobe only checks the
    specific item being reprobed"). One classification implementation,
    two entry points — the full scan calls this per row.
    """
    from .db import get_conn

    mt = media_type
    tid = tmdb_id
    sec = section_id
    plex_mt = _plex_mt(mt)

    # ── Look up rating_key + local_files + title in one txn ──
    # v1.18.43: also fetch plex_items.folder_path for sidecar-
    # disk drift detection.
    with get_conn(db_path) as conn:
        tid_row = conn.execute(
            "SELECT id, title, year FROM themes "
            "WHERE media_type = ? AND tmdb_id = ?",
            (mt, tid),
        ).fetchone()
        tid_pk = tid_row["id"] if tid_row else None
        title = tid_row["title"] if tid_row else None
        year = tid_row["year"] if tid_row else None
        # v1.22.81: edition-scope the lookups when the caller names one.
        _ek_sql = (" AND COALESCE(edition_key, '') = ?"
                   if edition_key is not None else "")
        _ek_args = (edition_key,) if edition_key is not None else ()
        rk_row = None
        if tid_pk is not None:
            rk_row = conn.execute(
                "SELECT rating_key, folder_path FROM plex_items "
                "WHERE theme_id = ? AND section_id = ?" + _ek_sql,
                (tid_pk, sec, *_ek_args),
            ).fetchone()
        if rk_row is None:
            rk_row = conn.execute(
                "SELECT rating_key, folder_path FROM plex_items "
                "WHERE guid_tmdb = ? AND media_type = ? "
                "  AND section_id = ?" + _ek_sql,
                (str(tid), plex_mt, sec, *_ek_args),
            ).fetchone()
        # v1.24.3: probe the placement's OWN plex_rating_key — that's where
        # motif actually uploaded. Pre-fix the rk was RE-RESOLVED from
        # plex_items by the placement's edition_key, which reported FALSE
        # drift on multi-edition titles whose '' placement targets a NAMED
        # edition's rk (Amadeus Director's Cut, the Star Wars cuts, LotR/Hobbit
        # Theatrical): it probed the literal '' plex_items row — the wrong
        # edition, or none in that section → rk_lookup_failed — instead of the
        # rk the placement used (verified-serving, ok=1). The plex_items
        # resolution stays as the folder_path source + the legacy fallback for
        # placements with no stored rk.
        placement_rk = None
        placement_rk_folder = None
        if edition_key is not None:
            # v1.24.5: pull THIS rk's folder in the same query (LEFT JOIN) so
            # folder_path below comes from the rk we actually probe — for a
            # mislabeled placement the edition-resolved rk_row points at a
            # DIFFERENT edition's folder, which mis-targeted the orphan_sidecar
            # check + DELETE SIDECAR. media_folder='' asserts the plex_upload PK
            # invariant so the lookup is provably single-row.
            _pl = conn.execute(
                "SELECT p.plex_rating_key, pi.folder_path AS prk_folder "
                "FROM placements p "
                "LEFT JOIN plex_items pi ON pi.rating_key = p.plex_rating_key "
                "WHERE p.media_type = ? AND p.tmdb_id = ? AND p.section_id = ? "
                "  AND COALESCE(p.edition_key, '') = ? AND p.media_folder = '' "
                "  AND p.placement_kind = 'plex_upload'",
                (mt, tid, sec, edition_key),
            ).fetchone()
            if _pl is not None and _pl["plex_rating_key"]:
                placement_rk = _pl["plex_rating_key"]
                placement_rk_folder = _pl["prk_folder"]
            # Prefer this edition's row, fall back to the shared ''.
            lf_row = conn.execute(
                "SELECT file_path FROM local_files "
                "WHERE media_type = ? AND tmdb_id = ? "
                "  AND section_id = ? "
                "  AND edition_key IN (?, '') "
                "ORDER BY (edition_key = ?) DESC LIMIT 1",
                (mt, tid, sec, edition_key, edition_key),
            ).fetchone()
        else:
            lf_row = conn.execute(
                "SELECT file_path FROM local_files "
                "WHERE media_type = ? AND tmdb_id = ? "
                "  AND section_id = ?",
                (mt, tid, sec),
            ).fetchone()
        if lf_row is None:
            lf_row = conn.execute(
                "SELECT file_path FROM local_files "
                "WHERE media_type = ? AND tmdb_id = ? LIMIT 1",
                (mt, tid),
            ).fetchone()

    # ── Build per-placement finding ──
    # v1.18.40: title + year added so the operator-facing UI
    # can render human-readable rows without a follow-up
    # lookup. Falls back to "<mt>/<tid>" if themes row gone.
    base = {
        "media_type": mt, "tmdb_id": tid, "section_id": sec,
        # v1.22.81: part of the finding's identity (multi-edition
        # plex_upload rows were indistinguishable without it).
        "edition_key": edition_key if edition_key is not None else "",
        "title": title or f"{mt}/{tid}",
        "year": year,
    }
    if placement_rk is None and rk_row is None:
        return {
            **base, "rk": None,
            "drift_type": "rk_lookup_failed",
            "details": (
                f"no rating_key for ({mt}/{tid}/{sec}) — the placement "
                f"stores none and plex_items can't resolve one"
            ),
            "motif_hash": None,
            "motif_canonical": None,
            "entries_count": 0,
            "motif_entry_present": False,
            "motif_entry_selected": False,
            "any_selected": False,
        }
    # v1.24.3: the placement's stored rk wins (where motif uploaded); the
    # plex_items rk is only the fallback for legacy rk-less placements.
    rk = placement_rk or rk_row["rating_key"]
    # v1.18.43: sidecar-disk drift detection. plex_upload
    # placements shouldn't have a theme.<ext> at the Plex
    # media folder — pre-v1.18.36 SWITCH file→api operations
    # left these behind. find_theme_sidecar_path walks the
    # host→container path translations + returns the actual
    # Path if found.
    # v1.24.5: folder from the SAME rk we probe — placement_rk's own plex_items
    # folder when it won, NOT the edition-resolved rk_row (which diverges for a
    # mislabeled placement and mis-targeted the sidecar check + DELETE SIDECAR).
    if placement_rk is not None:
        folder_path = placement_rk_folder or ""
    else:
        folder_path = (rk_row["folder_path"] if rk_row is not None else "") or ""
    orphan_sidecar_path: str | None = None
    if folder_path:
        from .plex_enum import find_theme_sidecar_path
        sp = find_theme_sidecar_path(folder_path)
        if sp is not None:
            orphan_sidecar_path = str(sp)

    # Hash motif's canonical (may be None). v1.18.39: resolve
    # the relative file_path against themes_dir before hashing.
    motif_canonical: str | None = None
    motif_hash: str | None = None
    if lf_row and lf_row["file_path"]:
        resolved = _resolve_canonical(
            themes_dir, lf_row["file_path"],
        )
        motif_canonical = str(resolved)
        motif_hash = _hash_file(resolved)

    # GET themes from Plex.
    # v1.22.33 (audit): per-row guard so one rk's unexpected error (a Plex
    # client hiccup, a raise get_themes doesn't catch) records a
    # fetch-failed finding + continues instead of aborting the whole
    # diagnostic sweep partway — every later placement would otherwise go
    # unscanned with no signal to the operator.
    try:
        themes_resp = plex.get_themes(rating_key=rk)
    except Exception as _e:
        themes_resp = {"ok": False,
                       "error": f"{type(_e).__name__}: {_e}"}
    if not themes_resp.get("ok"):
        return {
            **base, "rk": rk,
            "drift_type": "plex_fetch_failed",
            "details": (
                themes_resp.get("error")
                or f"HTTP {themes_resp.get('http_status')}"
            ),
            "motif_hash": motif_hash,
            "motif_canonical": motif_canonical,
            "entries_count": 0,
            "motif_entry_present": False,
            "motif_entry_selected": False,
            "any_selected": False,
        }

    body = themes_resp.get("body") or {}
    entries = []
    if isinstance(body, dict):
        entries = (
            body.get("MediaContainer", {}).get("Metadata", [])
        )

    # Analyze entries against motif's hash.
    motif_entry_present = False
    motif_entry_selected = False
    any_selected = False
    for e in entries:
        ratingKey = str(e.get("ratingKey", ""))
        sel = bool(e.get("selected"))
        if sel:
            any_selected = True
        if motif_hash and ratingKey.endswith(f"/{motif_hash}"):
            motif_entry_present = True
            if sel:
                motif_entry_selected = True

    # Classify drift. v1.18.43: orphan_sidecar_on_disk takes
    # priority over `ok` because a sidecar shouldn't exist on
    # a plex_upload row even when Plex's /themes state is
    # otherwise healthy. Other drift types take priority over
    # the sidecar check because they're more serious — if
    # Plex's state is broken, the sidecar is secondary noise.
    if not entries:
        drift_type = "no_plex_entries"
        details = (
            "Plex has no theme entries — motif's upload "
            "missing or wiped"
        )
    elif motif_hash is None:
        drift_type = "motif_hash_unknown"
        details = (
            "canonical file missing/unreadable; "
            "selection state can't be verified"
        )
    elif not motif_entry_present:
        drift_type = "motif_entry_missing"
        details = (
            f"motif's hash {motif_hash[:8]}… not present "
            f"in Plex's {len(entries)} entries"
        )
    elif not motif_entry_selected and any_selected:
        drift_type = "motif_not_selected"
        details = (
            "motif's entry present but Plex selected a "
            "different one"
        )
    elif not any_selected:
        drift_type = "nothing_selected"
        details = (
            "motif's entry present but nothing is "
            "selected:true — row plays no theme"
        )
    elif orphan_sidecar_path is not None:
        drift_type = "orphan_sidecar_on_disk"
        details = (
            f"plex_upload placement has an orphan "
            f"theme.<ext> at {orphan_sidecar_path} "
            f"(likely from a pre-v1.18.36 SWITCH file→api "
            f"that didn't clean up)"
        )
    else:
        drift_type = "ok"
        details = (
            f"motif's entry selected (of {len(entries)} "
            f"entries)"
        )

    return {
        **base, "rk": rk,
        "drift_type": drift_type,
        "details": details,
        "motif_hash": motif_hash,
        "motif_canonical": motif_canonical,
        "entries_count": len(entries),
        "motif_entry_present": motif_entry_present,
        "motif_entry_selected": motif_entry_selected,
        "any_selected": any_selected,
        # v1.18.43: surface the orphan-sidecar path (if any)
        # so the dashboard can show a DELETE SIDECAR button
        # with the actual path the action will target.
        "orphan_sidecar_path": orphan_sidecar_path,
        "plex_folder_path": folder_path or None,
    }



def summarize(findings: list[dict]) -> dict:
    """Group findings by drift_type with counts. Convenience for
    the endpoint response so the operator sees the shape at a
    glance."""
    out: dict[str, int] = {}
    for f in findings:
        dt = f["drift_type"]
        out[dt] = out.get(dt, 0) + 1
    return out


def rekey_mislabeled_placements(db_path, *, dry_run: bool = True) -> dict:
    """v1.24.4: re-key plex_upload placements whose edition_key disagrees with
    the edition of their OWN plex_rating_key.

    The ''-as-falsy place-path bug (the worker resolves an arbitrary sibling rk
    when the payload edition is the standard '') wrote placements keyed
    edition_key='' whose plex_rating_key actually targets a NAMED edition
    (Amadeus Director's Cut etc.) — the v1.24.3 orphan scan surfaced these. This
    re-labels each such placement to the edition its rk genuinely belongs to,
    per plex_items (the source of truth for an rk's edition).

    Tracking-only + non-destructive: it never touches Plex or the on-disk theme,
    and leaves the canonical local_files row at '' (reached via the standard
    `edition_key IN (edition, '')` fallback). Skips a re-key when the target PK
    slot is already occupied (reported as a conflict) or when the rk isn't in
    plex_items (can't determine the true edition). dry_run=True (default)
    returns the plan without writing.
    """
    from .db import get_conn, transaction
    from .events import log_event

    rekeyed: list[dict] = []
    conflicts: list[dict] = []
    rk_unresolved: list[dict] = []
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT media_type, tmdb_id, section_id, media_folder, "
            "       edition_key, plex_rating_key "
            "FROM placements "
            "WHERE placement_kind = 'plex_upload' "
            "  AND plex_rating_key IS NOT NULL AND plex_rating_key != ''"
        ).fetchall()
        for p in rows:
            pi = conn.execute(
                "SELECT edition_key FROM plex_items WHERE rating_key = ?",
                (p["plex_rating_key"],),
            ).fetchone()
            cur_ed = p["edition_key"] or ""
            rec = {
                "media_type": p["media_type"], "tmdb_id": p["tmdb_id"],
                "section_id": p["section_id"], "media_folder": p["media_folder"],
                "rating_key": p["plex_rating_key"], "from_edition": cur_ed,
            }
            if pi is None:
                rk_unresolved.append(rec)
                continue
            true_ed = pi["edition_key"] or ""
            if true_ed == cur_ed:
                continue  # already correctly labelled
            rec["to_edition"] = true_ed
            clash = conn.execute(
                "SELECT 1 FROM placements "
                "WHERE media_type=? AND tmdb_id=? AND section_id=? "
                "  AND media_folder=? AND edition_key=?",
                (p["media_type"], p["tmdb_id"], p["section_id"],
                 p["media_folder"], true_ed),
            ).fetchone()
            (conflicts if clash is not None else rekeyed).append(rec)
        # v1.24.5: the clash check above only sees PRE-EXISTING rows. Two re-keys
        # in this batch that target the SAME final PK would collide on the second
        # UPDATE → IntegrityError aborts the whole transaction. Dedup intra-batch
        # target PKs into conflicts so a single bad pair can't sink the run.
        seen_targets: set = set()
        deduped: list[dict] = []
        for r in rekeyed:
            tgt = (r["media_type"], r["tmdb_id"], r["section_id"],
                   r["media_folder"], r["to_edition"])
            if tgt in seen_targets:
                conflicts.append(r)
            else:
                seen_targets.add(tgt)
                deduped.append(r)
        rekeyed = deduped
        if not dry_run and rekeyed:
            # Single transaction — atomic (nothing half-applies on an error).
            with transaction(conn):
                for r in rekeyed:
                    conn.execute(
                        "UPDATE placements SET edition_key=? "
                        "WHERE media_type=? AND tmdb_id=? AND section_id=? "
                        "  AND media_folder=? AND edition_key=?",
                        (r["to_edition"], r["media_type"], r["tmdb_id"],
                         r["section_id"], r["media_folder"], r["from_edition"]),
                    )
            # v1.24.5: log AFTER the write txn commits. log_event's flusher
            # writes on its OWN connection + the DB has no WAL (single writer),
            # so logging inside the open BEGIN IMMEDIATE could starve/drop the
            # audit batch on a large run.
            for r in rekeyed:
                log_event(
                    db_path, level="INFO", component="placements",
                    media_type=r["media_type"], tmdb_id=r["tmdb_id"],
                    message=(
                        f"Re-keyed mislabeled plex_upload placement "
                        f"{r['media_type']}/{r['tmdb_id']} sec={r['section_id']}"
                        f" edition '{r['from_edition']}'→'{r['to_edition']}'"
                        f" (rk {r['rating_key']})"),
                    detail=r,
                )
    return {
        "dry_run": dry_run,
        "rekeyed": rekeyed, "conflicts": conflicts,
        "rk_not_in_plex_items": rk_unresolved,
        "rekeyed_count": len(rekeyed),
        "conflict_count": len(conflicts),
        "rk_unresolved_count": len(rk_unresolved),
    }
