"""
Adoption operations — apply user decisions to scan_findings.

Each scan finding has a `decision` ∈ {pending, adopt, replace, keep_existing}.
The UI surfaces `keep_existing` as "LOCK" — same enum value, clearer label.

  adopt          — hardlink the existing theme.mp3 into motif's themes_dir
                   (or copy if cross-FS), record local_files + placements rows.
                   For orphans, also INSERT a new themes row (with negative
                   tmdb_id if no real ID was resolved).
  replace        — enqueue a download job for this theme's youtube_url, which
                   will overwrite the existing file when placement happens.
                   Only valid for non-orphan findings.
  keep_existing  — (UI label: LOCK) record a user_overrides row marking
                   'manual' provenance, so future placements won't overwrite
                   this theme.

Legacy: pre-v1.6.1 also accepted 'ignore' (a soft "dismiss this finding"
that left no DB side-effects). The CHECK constraint on scan_findings still
permits the value so historical rows stay valid, but the API and UI no
longer expose it — the same effect is implicit by leaving findings in
the 'pending' state.

All operations are recorded as completed by stamping the scan_findings row's
adopt_outcome and adopted_at fields.

v1.10.9: split out `adopt_folder()` — a finding-less primitive that takes
a folder_path + plex_item context and runs the same adopt logic without
requiring a scan to have produced a scan_findings row. Used by the inline
ADOPT button on /movies, /tv, /anime row actions.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
from pathlib import Path

from .canonical import canonical_theme_subdir
from .db import get_conn, transaction
from .editions import edition_key_for_folder
from .events import log_event, now_iso

log = logging.getLogger(__name__)


class AdoptError(Exception):
    """Failures during adoption — caller decides whether to retry or skip."""


# ---------------------------------------------------------------------------
# v1.10.9: finding-less primitives
#
# adopt_folder / replace_with_themerrdb let the API call adopt and replace
# semantics directly from a Plex row, without first running a scan to
# populate scan_findings. The scan workflow still works as before — these
# primitives are an additional code path for the inline row buttons.
# ---------------------------------------------------------------------------

_SHA_BUFSIZE = 1024 * 1024  # 1 MiB

def _hash_file(path: Path) -> tuple[str, int]:
    """Return (sha256_hex, file_size). Streams to keep memory bounded."""
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        while True:
            chunk = f.read(_SHA_BUFSIZE)
            if not chunk:
                break
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def adopt_folder(
    db_path: Path, *,
    settings,
    folder_path: str,
    media_type: str,
    title: str,
    year: str,
    section_id: str | None = None,
    imdb_id: str | None = None,
    tmdb_id: int | None = None,
    decided_by: str,
) -> dict:
    """Adopt the theme.mp3 sitting at folder_path/theme.mp3 as a motif-managed
    theme. Builds a synthetic finding-shaped dict and reuses _do_adopt so the
    file-placement and DB-write logic is shared with the /scans flow.

    v1.11.0: section_id determines which Plex section the adoption belongs
    to (the staging file lands under that section's themes_subdir, and the
    local_files / placements rows are keyed by it). When the caller doesn't
    supply one, we resolve via plex_items.folder_path → section_id; if that
    lookup fails too, the adopt is rejected so we never write a row keyed
    to the wrong section.
    """
    if media_type not in ("movie", "tv"):
        raise AdoptError(f"unknown media_type: {media_type}")
    # v0.51.247: was a raw `Path(folder_path) / "theme.mp3"`. api_adopt_sidecar
    # gates the button on plex_items.local_theme_file, which plex_enum sets via
    # the TRANSLATED stat — so on an install needing host->container translation
    # the UI offered ADOPT FROM PLEX and this raised 409 every time, on exactly
    # the installs the translation table exists for. Same helper as the gate.
    from .plex_enum import find_theme_sidecar_path  # lazy: avoids a cycle
    sidecar = find_theme_sidecar_path(folder_path)
    if sidecar is None:
        raise AdoptError(f"no theme sidecar at {folder_path}")
    sha256, size = _hash_file(sidecar)

    if not section_id:
        plex_type = "show" if media_type == "tv" else "movie"
        with get_conn(db_path) as conn:
            row = conn.execute(
                "SELECT section_id FROM plex_items "
                "WHERE folder_path = ? AND media_type = ? "
                "ORDER BY last_seen_at DESC LIMIT 1",
                (str(folder_path), plex_type),
            ).fetchone()
        if row is None:
            raise AdoptError(
                f"no plex_items row for {folder_path} — cannot infer section_id; "
                "re-run plex_enum or pass section_id explicitly"
            )
        section_id = row["section_id"]

    metadata: dict = {"title": title, "year": year}
    if imdb_id:
        metadata["imdb_id"] = imdb_id
    if tmdb_id is not None:
        metadata["tmdb_id"] = int(tmdb_id)

    section_type = "movie" if media_type == "movie" else "show"
    finding_kind = ("orphan_resolvable"
                    if (imdb_id or tmdb_id is not None)
                    else "orphan_unresolved")

    finding = {
        "section_id": section_id,
        "section_type": section_type,
        "finding_kind": finding_kind,
        "theme_id": None,
        "file_path": str(sidecar),
        "file_sha256": sha256,
        "file_size": size,
        "media_folder": str(folder_path),
        "resolved_metadata": json.dumps(metadata),
    }
    outcome = _do_adopt(db_path, finding, settings, decided_by)
    # v1.10.57: if a previous unmanage / forget recorded a URL for
    # this exact file content (sha256 match), restore it. The newly-
    # adopted row promotes from A → U with the original youtube_url
    # back in user_overrides + themes, source_kind='url'.
    restored = _maybe_restore_url_history(
        db_path,
        media_type=outcome.get("media_type"),
        tmdb_id=outcome.get("tmdb_id"),
        # v1.12.77: pass section_id so the local_files flip is
        # scoped — pre-fix the UPDATE spanned every section's
        # local_files row, silently flipping sibling-edition
        # source_kind based on a single section's adoption.
        section_id=section_id,
        # v1.22.63: + the adopting edition (same derivation _do_adopt
        # uses) so the flip can't bleed to a sibling edition's row.
        edition_key=edition_key_for_folder(finding["media_folder"]),
        sha256=sha256,
        decided_by=decided_by,
    )
    # v1.11.42: read back the just-written rows so the event log records
    # what's actually in the DB (theme_id, source_kind, provenance,
    # placement_kind, file_path, media_folder, theme_id_in_pi). The
    # 'no action was taken' bug report on Matilda 4K showed the success
    # log but no visible state change — we couldn't tell whether the
    # rows were missing, the badge logic was wrong, or the UI was
    # caching. With this log line we can match what's on disk to what
    # the lib renderer should see.
    verify = _verify_adopt_state(
        db_path,
        media_type=outcome.get("media_type"),
        tmdb_id=outcome.get("tmdb_id"),
        section_id=section_id,
        edition_key=edition_key_for_folder(finding["media_folder"]),
    )
    log_event(db_path, level="INFO", component="adopt",
              media_type=outcome.get("media_type"),
              tmdb_id=outcome.get("tmdb_id"),
              message=f"Inline adopt of sidecar at {folder_path}",
              detail={"folder_path": str(folder_path),
                      "sha256": sha256,
                      "kind": finding_kind,
                      "section_id": section_id,
                      "decided_by": decided_by,
                      "restored_from_history": restored,
                      "outcome": outcome,
                      "verify": verify})
    if restored:
        outcome["restored_from_history"] = restored
    return outcome


def _verify_adopt_state(
    db_path: Path, *,
    media_type: str | None, tmdb_id: int | None,
    section_id: str | None,
    edition_key: str = "",
) -> dict:
    """Read back local_files / placements / plex_items.theme_id for the
    just-adopted (item, section, edition) so the event log captures the
    actual row state. Lets us debug 'badge didn't change' reports without DB
    access.

    v0.50.89: added edition_key scoping — pre-fix these 3 reads matched by
    (media_type, tmdb_id, section_id) alone with no deterministic ORDER BY,
    so on a multi-edition title the diagnostic could report a SIBLING
    edition's row instead of the one just adopted, misleading debugging."""
    if media_type is None or tmdb_id is None or not section_id:
        return {"ok": False, "reason": "missing keys"}
    with get_conn(db_path) as conn:
        lf = conn.execute(
            """SELECT theme_id, file_path, source_kind, provenance
               FROM local_files
               WHERE media_type = ? AND tmdb_id = ? AND section_id = ?
                 AND edition_key = ?""",
            (media_type, tmdb_id, section_id, edition_key),
        ).fetchone()
        p = conn.execute(
            """SELECT theme_id, media_folder, placement_kind, provenance
               FROM placements
               WHERE media_type = ? AND tmdb_id = ? AND section_id = ?
                 AND edition_key = ?""",
            (media_type, tmdb_id, section_id, edition_key),
        ).fetchone()
        pi = conn.execute(
            """SELECT rating_key, theme_id, has_theme, local_theme_file
               FROM plex_items
               WHERE section_id = ? AND guid_tmdb = ?
                 AND media_type = (CASE ? WHEN 'tv' THEN 'show' ELSE ? END)
                 AND edition_key = ?
               LIMIT 1""",
            (section_id, tmdb_id, media_type, media_type, edition_key),
        ).fetchone()
    return {
        "local_files": dict(lf) if lf else None,
        "placements": dict(p) if p else None,
        "plex_items": dict(pi) if pi else None,
    }


def _maybe_restore_url_history(
    db_path: Path, *,
    media_type: str | None, tmdb_id: int | None,
    sha256: str, decided_by: str,
    section_id: str | None = None,
    edition_key: str | None = None,
) -> dict | None:
    """v1.10.57: look up local_files_history by sha256 and, if a prior
    URL-sourced entry matches, restore the youtube_url onto the
    just-adopted row. Promotes the row from A back to U because the
    file content is byte-identical to what was previously
    URL-downloaded.

    Returns a small dict describing what was restored (for logging),
    or None when there's no usable history match.

    Only acts on history rows whose source_kind was 'url' — for
    'upload' the URL was empty anyway, and for 'themerrdb'/'adopt'
    there's no original-URL semantic to preserve. Picks the most
    recent saved_at when multiple matches exist.

    v1.12.77: section_id scopes the local_files UPDATE so the flip
    only affects the section being adopted into. Pre-fix the UPDATE
    spanned every section's local_files row, silently flipping
    sibling-edition source_kind on a single-section adopt — the
    user adopted the 4K theme.mp3 and the standard edition's row
    classified differently as a side effect.
    """
    if not sha256 or media_type is None or tmdb_id is None:
        return None
    # v0.51.236: same autocommit exposure as replace_with_themerrdb — the A→U
    # promotion is a local_files source_kind flip PLUS a user_overrides INSERT plus
    # a themes.youtube_url UPDATE. Partially applied, the row's SRC letter disagrees
    # with its override state (source_kind='url' with no override to justify it, or
    # the reverse), which no walker reconciles. All-or-nothing.
    with get_conn(db_path) as conn, transaction(conn):
        hist = conn.execute(
            """SELECT source_kind, source_video_id, youtube_url, saved_at
               FROM local_files_history
               WHERE file_sha256 = ?
                 AND source_kind = 'url'
                 AND youtube_url IS NOT NULL
                 AND youtube_url != ''
               ORDER BY saved_at DESC
               LIMIT 1""",
            (sha256,),
        ).fetchone()
        if hist is None:
            return None
        url = hist["youtube_url"]
        # v1.12.73: branch on URL match. If the historical URL is
        # IDENTICAL to themes.youtube_url, the file is content-
        # identical to what TDB would download right now, so the
        # row is conceptually T-managed — flip local_files to
        # source_kind='themerrdb' and skip writing user_overrides
        # entirely. Pre-fix the row landed at U with a global
        # override URL that just shadowed TDB's URL, then the next
        # sync surfaced a synthetic urls_match prompt asking the
        # user to "convert U → T" — extra UI for what was already
        # a T-equivalent state.
        theme_yt_row = conn.execute(
            "SELECT youtube_url FROM themes "
            "WHERE media_type = ? AND tmdb_id = ?",
            (media_type, tmdb_id),
        ).fetchone()
        theme_yt = (theme_yt_row and theme_yt_row["youtube_url"]) or ""
        url_matches_tdb = bool(theme_yt and url and theme_yt.strip() == url.strip())
        if url_matches_tdb:
            # Adopt-as-T path: keep themes.youtube_url, no override,
            # flip the local_files.source_kind to 'themerrdb' for
            # this section only (v1.12.77 — was global pre-fix and
            # silently flipped sibling sections). Row classifies as
            # T immediately.
            # v1.22.63 (audit round 2 #5): edition-scope the flip — the
            # one edition straggler of the v1.21.66→v1.22.25 campaign.
            # Pre-fix a section-only WHERE rewrote source_kind +
            # source_video_id on EVERY edition's row in the section
            # (post-v63 PK a title legitimately holds '' + 'Extended'
            # sibling rows), so adopting the Extended sidecar corrupted
            # the standard edition's tracking.
            _ed_clause = "" if edition_key is None else " AND edition_key = ?"
            _ed_args = () if edition_key is None else (edition_key,)
            if section_id:
                conn.execute(
                    "UPDATE local_files SET source_kind = 'themerrdb', "
                    "                       source_video_id = ? "
                    "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?"
                    + _ed_clause,
                    (hist["source_video_id"] or "", media_type, tmdb_id,
                     section_id, *_ed_args),
                )
            else:
                conn.execute(
                    "UPDATE local_files SET source_kind = 'themerrdb', "
                    "                       source_video_id = ? "
                    "WHERE media_type = ? AND tmdb_id = ?" + _ed_clause,
                    (hist["source_video_id"] or "", media_type, tmdb_id,
                     *_ed_args),
                )
            return {
                "youtube_url": url,
                "source_kind": "themerrdb",
                "url_matches_tdb": True,
                "history_saved_at": hist["saved_at"],
            }
        # Adopt-as-U path (URL differs from TDB): write user_overrides
        # + flip local_files to source_kind='url'. The override is
        # section-scoped to '' (cross-section by default; per-section
        # SET URL still wins via the worker fall-back chain, v1.12.72).
        # v1.23.97 (audit): scope it to the ADOPTING edition too. The
        # worker's override resolution (worker.py:1522-1534) is edition-
        # keyed — it tries (section, edition) then ('', edition), with NO
        # edition='' fallback (v1.21.92). Writing the restored override at
        # edition='' while the local_files flip below targets the adopting
        # edition (Extended, …) left that edition's U-row with no resolvable
        # override → the worker found no URL on the next download. Match the
        # local_files flip: edition_key = the adopting edition (or '' when
        # the adopt wasn't edition-scoped).
        conn.execute(
            """INSERT INTO user_overrides
                 (media_type, tmdb_id, youtube_url, set_at, set_by, note,
                  section_id, edition_key)
               VALUES (?, ?, ?, ?, ?,
                       'restored from local_files_history on adopt', '', ?)
               ON CONFLICT(media_type, tmdb_id, section_id, edition_key) DO UPDATE SET
                   youtube_url = excluded.youtube_url,
                   set_at = excluded.set_at,
                   set_by = excluded.set_by,
                   note = excluded.note""",
            (media_type, tmdb_id, url, now_iso(), decided_by, edition_key or ''),
        )
        # v0.51.265: this puts a NON-TDB url in themes.youtube_url (see the
        # column comment) — the second of the two writers that make the column
        # "last committed" rather than "upstream's answer". youtube_video_id is
        # deliberately left alone: sync owns the pair and rewrites both.
        conn.execute(
            "UPDATE themes SET youtube_url = ? "
            "WHERE media_type = ? AND tmdb_id = ? "
            "  AND (youtube_url IS NULL OR youtube_url = '')",
            (url, media_type, tmdb_id),
        )
        # v1.12.77: scope local_files flip to the adopting section.
        # v1.22.63: + the adopting edition (same straggler as the
        # adopt-as-T branch above).
        _ed_clause = "" if edition_key is None else " AND edition_key = ?"
        _ed_args = () if edition_key is None else (edition_key,)
        if section_id:
            conn.execute(
                "UPDATE local_files SET source_kind = 'url', source_video_id = ? "
                "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?"
                + _ed_clause,
                (hist["source_video_id"] or "", media_type, tmdb_id,
                 section_id, *_ed_args),
            )
        else:
            conn.execute(
                "UPDATE local_files SET source_kind = 'url', source_video_id = ? "
                "WHERE media_type = ? AND tmdb_id = ?" + _ed_clause,
                (hist["source_video_id"] or "", media_type, tmdb_id,
                 *_ed_args),
            )
    return {
        "youtube_url": url,
        "source_kind": "url",
        "history_saved_at": hist["saved_at"],
    }


def replace_with_themerrdb(
    db_path: Path, *,
    media_type: str,
    tmdb_id: int,
    decided_by: str,
    section_id: str | None = None,
    edition_key: str = "",
) -> dict:
    """Sidecar is currently in place but ThemerrDB has the title — fetch
    motif's authoritative download and overwrite. Cancels any in-flight
    download/place jobs for this item, enqueues a fresh download with
    force_place=true so the worker overwrites the sidecar without
    plex_has_theme guarding it.

    v1.11.0: when section_id is provided the replace targets that one
    section (via a single per-section download job). When omitted we
    enqueue per-section jobs for every included section that owns
    this item — same fan-out semantics as a sync-driven download.

    v1.21.66: edition_key (resolved at the endpoint from the triggering
    rating_key's folder) scopes the replace to ONE edition — its download
    payload carries edition_key so the chained place job lands in THIS
    edition's folder/rk, and the override capture+delete only drops this
    edition's own override. '' = the standard edition = pre-edition
    behavior. Only meaningful when section_id is also given (the endpoint
    always pairs them); the section_id=None fan-out path keeps '' since a
    single rating_key's edition can't speak for every section.
    """
    if media_type not in ("movie", "tv"):
        raise AdoptError(f"unknown media_type: {media_type}")
    # v0.51.236: get_conn is isolation_level=None (autocommit), so these four
    # writes — cancel in-flight jobs, capture previous_urls, DELETE the override,
    # INSERT the download jobs — each committed on their own. If the final INSERT
    # failed (lock timeout, ENOSPC) the destructive half had ALREADY landed: the
    # user's override was gone and their in-flight download cancelled, with no
    # replacement enqueued. That is the destroy-then-fail ordering class v1.22.40
    # fixed for the filesystem; this is its DB twin. One transaction, so a failure
    # anywhere leaves the row exactly as the user left it.
    with get_conn(db_path) as conn, transaction(conn):
        theme = conn.execute(
            "SELECT youtube_url, upstream_source FROM themes "
            "WHERE media_type = ? AND tmdb_id = ?",
            (media_type, tmdb_id),
        ).fetchone()
        if theme is None:
            raise AdoptError(f"no theme row for {media_type}/{tmdb_id}")
        if theme["upstream_source"] == "plex_orphan":
            raise AdoptError("no ThemerrDB record to replace from")
        if not theme["youtube_url"]:
            raise AdoptError("ThemerrDB record has no youtube_url")

        # Cancel any in-flight downloads/places for this item across the
        # affected sections — about to enqueue replacements with force_place.
        # v1.21.82: edition-scoped (matching the edition-keyed replace below)
        # so REPLACE TDB on one edition doesn't cancel a sibling edition's
        # in-flight download/place. jobs has no edition_key column — the
        # download + chained place payloads carry it (v74 json_valid guard).
        _edition_clause = (
            " AND COALESCE(CASE WHEN json_valid(payload)"
            " THEN json_extract(payload, '$.edition_key') END, '') = ?")
        if section_id:
            conn.execute(
                """UPDATE jobs SET status = 'cancelled', finished_at = ?
                   WHERE job_type IN ('download','place')
                     AND media_type = ? AND tmdb_id = ? AND section_id = ?"""
                + _edition_clause +
                """ AND status IN ('pending','running')""",
                (now_iso(), media_type, tmdb_id, section_id, edition_key),
            )
            target_sections = [section_id]
        else:
            conn.execute(
                """UPDATE jobs SET status = 'cancelled', finished_at = ?
                   WHERE job_type IN ('download','place')
                     AND media_type = ? AND tmdb_id = ?"""
                + _edition_clause +
                """ AND status IN ('pending','running')""",
                (now_iso(), media_type, tmdb_id, edition_key),
            )
            plex_type = "show" if media_type == "tv" else "movie"
            target_sections = [
                r["section_id"] for r in conn.execute(
                    """SELECT DISTINCT pi.section_id FROM plex_items pi
                       JOIN plex_sections ps ON ps.section_id = pi.section_id
                       WHERE pi.guid_tmdb = ? AND pi.media_type = ?
                         AND ps.included = 1""",
                    (tmdb_id, plex_type),
                ).fetchall()
            ]
        if not target_sections:
            raise AdoptError(
                "no managed Plex sections own this item — enable a section in /settings"
            )
        # v1.12.62: REPLACE TDB on a U row — delete user_overrides
        # so the worker doesn't keep using the override URL and
        # re-classifying the new download as U. Pre-fix the user
        # would click REPLACE TDB on a U row, the download would
        # run, and the row would stay U because the worker still
        # saw user_overrides at line 563 of worker.py and stamped
        # source_kind='url'. The action's intent ("replace your
        # current theme with ThemerrDB's") implies dropping the
        # override too. Capture previous URL before the delete so
        # REVERT can restore the U state if the user changes their
        # mind. SQL inlined here (rather than calling
        # web.api._capture_previous_url) to avoid a core → web
        # circular import.
        # v1.12.72: section-aware override fetch + delete. REPLACE TDB
        # was given a section_id parameter; scope override ops to that
        # section so sibling sections' per-edition overrides aren't
        # collateral damage. Falls back to '' global when no
        # per-section row exists.
        # v1.12.86: previous-URL snapshot is per-section now via the
        # previous_urls table (was the title-global
        # themes.previous_youtube_url column). The capture below
        # writes to the row's section so RESTORE/REVERT in section A
        # doesn't disturb section B.
        # v1.21.66: scope the override capture+delete to THIS edition so a
        # REPLACE TDB on one edition doesn't drop a sibling edition's
        # override. The override SET URL wrote lives at
        # (mt, tmdb, section, edition_key); delete exactly that. edition_key
        # = '' reproduces the pre-edition (section-only) behavior.
        ovr_section_for_replace = section_id or ""
        ovr_row = conn.execute(
            "SELECT youtube_url, section_id FROM user_overrides "
            "WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
            "  AND edition_key = ?",
            (media_type, tmdb_id, ovr_section_for_replace, edition_key),
        ).fetchone()
        if ovr_row is None and ovr_section_for_replace:
            ovr_row = conn.execute(
                "SELECT youtube_url, section_id FROM user_overrides "
                "WHERE media_type = ? AND tmdb_id = ? AND section_id = '' "
                "  AND edition_key = ?",
                (media_type, tmdb_id, edition_key),
            ).fetchone()
            if ovr_row is not None:
                ovr_section_for_replace = ""
        if ovr_row and ovr_row["youtube_url"]:
            # previous_urls keeps its 3-col PK (mt, tmdb, section) — not
            # widened for editions — so the snapshot is per-section. REVERT
            # restores at section granularity (a known, pre-existing limit).
            conn.execute(
                """INSERT INTO previous_urls
                     (media_type, tmdb_id, section_id, youtube_url, kind, captured_at)
                   VALUES (?, ?, ?, ?, 'user', ?)
                   ON CONFLICT(media_type, tmdb_id, section_id) DO UPDATE SET
                       youtube_url = excluded.youtube_url,
                       kind = excluded.kind,
                       captured_at = excluded.captured_at""",
                (media_type, tmdb_id, ovr_section_for_replace,
                 ovr_row["youtube_url"], now_iso()),
            )
            conn.execute(
                "DELETE FROM user_overrides "
                "WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
                "  AND edition_key = ?",
                (media_type, tmdb_id, ovr_section_for_replace, edition_key),
            )
        # v1.21.66: carry edition_key into the download payload so the
        # worker's _record_local_file writes the edition's local_files row
        # and the chained place job lands in THIS edition's folder/rk.
        # Only the section-scoped call (the endpoint) passes a non-'' value;
        # the fan-out path leaves it '' (one rk can't speak for all sections).
        _dl_payload = {"reason": "replace_with_themerrdb", "force_place": True}
        if edition_key and section_id:
            _dl_payload["edition_key"] = edition_key
        job_ids: list[int] = []
        for sid in target_sections:
            cur = conn.execute(
                """INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,
                                     payload, status, created_at, next_run_at)
                   VALUES ('download', ?, ?, ?, ?, 'pending', ?, ?)""",
                (media_type, tmdb_id, sid, json.dumps(_dl_payload),
                 now_iso(), now_iso()),
            )
            job_ids.append(cur.lastrowid)
    log_event(db_path, level="INFO", component="adopt",
              media_type=media_type, tmdb_id=tmdb_id,
              message="Replace-with-ThemerrDB enqueued",
              detail={"job_ids": job_ids, "section_ids": target_sections,
                      "decided_by": decided_by})
    return {"action": "replace_with_themerrdb", "job_ids": job_ids,
            "section_ids": target_sections,
            "media_type": media_type, "tmdb_id": tmdb_id}


def adopt_finding(db_path: Path, finding_id: int, decision: str,
                  *, decided_by: str, settings) -> dict:
    """Apply a user decision to a scan_finding. Returns the outcome dict
    that gets stored in adopt_outcome and surfaced to the UI."""
    if decision not in ("adopt", "replace", "keep_existing"):
        raise AdoptError(f"unknown decision: {decision}")

    with get_conn(db_path) as conn:
        finding = conn.execute(
            "SELECT * FROM scan_findings WHERE id = ?", (finding_id,)
        ).fetchone()
    if not finding:
        raise AdoptError(f"finding {finding_id} not found")

    # Idempotence: if already adopted, no-op.
    if finding["adopted_at"]:
        return json.loads(finding["adopt_outcome"]) if finding["adopt_outcome"] else {}

    outcome: dict
    try:
        if decision == "adopt":
            outcome = _do_adopt(db_path, finding, settings, decided_by)
        elif decision == "replace":
            outcome = _do_replace(db_path, finding, settings, decided_by)
        elif decision == "keep_existing":
            outcome = _do_keep(db_path, finding, decided_by)
        else:
            raise AdoptError(f"unknown decision: {decision}")
    except Exception:
        # v1.24.1: re-raise — do NOT swallow + stamp a terminal outcome.
        # Pre-fix the broad catch built {action:failed}, then the UPDATE below
        # UNCONDITIONALLY set adopted_at, so a TRANSIENT failure (database is
        # locked, source sidecar momentarily gone, ENOSPC) became a permanent
        # fake-"adopted": the worker (which ignores this return) marked the job
        # DONE, and the idempotence guard above + the API's adopted_at/jobs
        # dedup then blocked every retry. Re-raising routes to the worker's
        # bounded _mark_failed path (FAIL dot lights, retries, then terminal)
        # and leaves adopted_at NULL so the finding stays re-decidable. Class 9
        # + the v1.21.38 partial-outcome-treated-as-final sibling.
        log.exception("Adopt finding %d failed", finding_id)
        raise

    with get_conn(db_path) as conn:
        conn.execute(
            """UPDATE scan_findings SET
                 decision = ?, decision_at = ?, decision_by = ?,
                 adopt_outcome = ?, adopted_at = ?
               WHERE id = ?""",
            (decision, now_iso(), decided_by,
             json.dumps(outcome), now_iso(), finding_id),
        )

    log_event(db_path, level="INFO", component="adopt",
              media_type=None, tmdb_id=None,
              message=f"Finding {finding_id} resolved: {decision}",
              detail={"finding_id": finding_id, "decision": decision,
                      "outcome": outcome})

    return outcome


def _do_adopt(db_path: Path, finding, settings, decided_by: str) -> dict:
    """Adopt the existing theme.mp3 — hardlink it (or copy if cross-FS) into
    motif's per-section staging dir and record DB rows.

    v1.11.0 layout:
        themes_dir/<plex_sections.themes_subdir>/<Title (Year)>/theme.mp3

    The owning section comes from the scan_findings.section_id (the scanner
    walked exactly that section's location_paths to find this sidecar).
    """
    section_id = finding["section_id"]
    if not section_id:
        raise AdoptError("scan finding missing section_id")
    media_type = "movie" if finding["section_type"] == "movie" else "tv"
    finding_kind = finding["finding_kind"]
    theme_id = finding["theme_id"]

    if not settings.is_paths_ready():
        raise AdoptError("themes_dir not configured")
    with get_conn(db_path) as conn:
        sec = conn.execute(
            "SELECT themes_subdir FROM plex_sections WHERE section_id = ?",
            (section_id,),
        ).fetchone()
    if sec is None or not sec["themes_subdir"]:
        raise AdoptError(
            f"section {section_id} has no themes_subdir — re-discover sections in /settings"
        )
    media_root = settings.section_themes_dir_by_subdir(sec["themes_subdir"])
    if not media_root:
        raise AdoptError("themes_dir not configured")

    source_path = Path(finding["file_path"])
    if not source_path.is_file():
        raise AdoptError(f"source no longer exists: {source_path}")

    # For orphans, allocate a themes row first
    if finding_kind in ("orphan_resolvable", "orphan_unresolved"):
        theme_id, allocated_tmdb_id = _create_orphan_theme(
            db_path, finding, decided_by,
        )
        media_type = "movie" if finding["section_type"] == "movie" else "tv"
        tmdb_id = allocated_tmdb_id
    else:
        with get_conn(db_path) as conn:
            row = conn.execute(
                "SELECT media_type, tmdb_id FROM themes WHERE id = ?",
                (theme_id,),
            ).fetchone()
            if not row:
                raise AdoptError(f"theme_id {theme_id} not found")
            media_type = row["media_type"]
            tmdb_id = row["tmdb_id"]

    # Resolve title+year for the canonical subdir from the themes row.
    with get_conn(db_path) as conn:
        meta = conn.execute(
            "SELECT title, year FROM themes WHERE id = ?", (theme_id,),
        ).fetchone()
    title = (meta and meta["title"]) or ""
    year = (meta and meta["year"]) or ""
    # v1.21.86: derive the edition from the Plex folder the sidecar lives in
    # ({edition-X} tag) so ADOPT stages into THIS edition's subdir and keys
    # its local_files/placements rows per edition — pre-fix _do_adopt always
    # wrote edition_key='' + the standard folder, so adopting the Extended
    # sidecar landed on the standard edition's state.
    edition_key = edition_key_for_folder(finding["media_folder"])
    out_dir = media_root / canonical_theme_subdir(title, year, edition_key)
    out_dir.mkdir(parents=True, exist_ok=True)
    canonical_path = out_dir / "theme.mp3"

    # Place the canonical file. If canonical_path already exists with the same
    # inode as source, nothing to do. Otherwise hardlink (or copy on EXDEV).
    placement_kind = "hardlink"
    try:
        if canonical_path.exists():
            if canonical_path.stat().st_ino == source_path.stat().st_ino:
                placement_kind = "hardlink"  # already linked
            else:
                # v0.51.228 (audit): stage into a sibling tmp + atomic os.replace.
                # Pre-fix this ran `canonical_path.unlink()` FIRST and only then
                # os.link — so if the link raised (EXDEV) AND the copy2 fallback
                # also raised (ENOSPC/EACCES/EMLINK), the outer handler raised
                # AdoptError with the live canonical already DELETED while its
                # local_files row still pointed at it: the row kept rendering as
                # tracked/themed and every later place failed "source file
                # missing". Destroy-then-fail (the v1.22.40 class). Both siblings
                # already stage — worker.py's sibling-hardlink (theme.mp3.sib.tmp
                # + os.replace) and placement.py's os.replace(dst_tmp, dst).
                _tmp = canonical_path.with_name(canonical_path.name + ".adopt.tmp")
                _tmp.unlink(missing_ok=True)
                try:
                    try:
                        os.link(source_path, _tmp)
                    except OSError as e:
                        # v1.20.42: hardlink failed — copy IS the functional
                        # fallback, but a non-EXDEV errno (EMLINK max-links,
                        # EPERM, ENOSPC) is a silent disk-bloat trap, so surface
                        # it (class 9). EXDEV (cross-FS) is expected, stays quiet.
                        if e.errno != 18:
                            log.warning("adopt: hardlink failed for %s (errno=%s), "
                                        "fell back to copy", canonical_path, e.errno)
                        _tmp.unlink(missing_ok=True)
                        shutil.copy2(source_path, _tmp)
                        placement_kind = "copy"
                    os.replace(_tmp, canonical_path)
                except OSError:
                    _tmp.unlink(missing_ok=True)  # never strand the staged temp
                    raise
        else:
            try:
                os.link(source_path, canonical_path)
            except OSError as e:
                if e.errno != 18:  # EXDEV (cross-device)
                    raise
                shutil.copy2(source_path, canonical_path)
                placement_kind = "copy"
    except OSError as e:
        raise AdoptError(f"placement failed: {e}") from e

    # Provenance: hash/exact matches share content with motif's canonical
    # (same sha as what ThemerrDB would produce), so they earn the 'auto'
    # provenance — T badge in the UI. Everything else (orphans, content
    # mismatches) is user-curated and gets 'manual'.
    provenance = "auto" if finding_kind in ("hash_match", "exact_match") else "manual"
    # source_kind drives the T/U/A badge unambiguously (v1.10.12). Hash
    # and exact matches are byte-identical with what ThemerrDB would
    # download, so 'themerrdb'. Everything else (orphan_* findings,
    # content_mismatch) is the user adopting a local file as the source
    # of truth — 'adopt'.
    source_kind = ("themerrdb"
                   if finding_kind in ("hash_match", "exact_match")
                   else "adopt")

    # source_video_id: keep the previous convention (yt id when known,
    # else hash-based) for back-compat with row badge heuristics, but
    # decouple from the on-disk filename which is now always theme.mp3.
    source_vid = _canonical_filename(
        db_path, theme_id, finding["file_sha256"],
    ).replace(".mp3", "")
    # file_path stored relative to themes_dir, matching the worker
    # download path. Pre-1.10.13 stored absolute, which broke the
    # canonical-layout migration on existing installs.
    rel_path = str(canonical_path.relative_to(settings.themes_dir))
    # v1.24.13 (holistic review): wrap the 4-statement write cluster in ONE
    # transaction. Pre-fix this ran on the autocommit connection, so a crash
    # between the local_files upsert and the placements upsert left a tracked
    # canonical (SRC=A/T renders) with no placement row — and the inline
    # adopt-folder path has no worker retry to self-heal it. Atomic now.
    with get_conn(db_path) as conn, transaction(conn):
        # v1.24.1: ON CONFLICT DO UPDATE, not INSERT OR REPLACE. OR REPLACE
        # delete+reinserts on a re-adopt → every column NOT listed here resets
        # to its schema default (local_files mismatch_state/canonical_present/
        # canonical_health_checked_at; placements plex_rating_key/theme_present/
        # placement_health_checked_at), silently zeroing the health state that
        # drives the NEEDS WORK / DL / PL sorts until the next health sweep.
        # Same class as the v1.23.99 _do_keep fix; aligns adopt with the
        # worker's ON CONFLICT writers (worker.py:2106/2637).
        conn.execute(
            """INSERT INTO local_files
                 (media_type, tmdb_id, section_id, edition_key, theme_id,
                  file_path, file_sha256, file_size, downloaded_at,
                  source_video_id, provenance, source_kind)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(media_type, tmdb_id, section_id, edition_key) DO UPDATE SET
                   theme_id = excluded.theme_id,
                   file_path = excluded.file_path,
                   file_sha256 = excluded.file_sha256,
                   file_size = excluded.file_size,
                   downloaded_at = excluded.downloaded_at,
                   source_video_id = excluded.source_video_id,
                   provenance = excluded.provenance,
                   source_kind = excluded.source_kind""",
            (media_type, tmdb_id, section_id, edition_key, theme_id,
             rel_path, finding["file_sha256"], finding["file_size"],
             now_iso(), source_vid, provenance, source_kind),
        )
        conn.execute(
            """INSERT INTO placements
                 (media_type, tmdb_id, section_id, edition_key, theme_id,
                  media_folder, placed_at, placement_kind, plex_refreshed,
                  provenance)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
               ON CONFLICT(media_type, tmdb_id, section_id, media_folder, edition_key) DO UPDATE SET
                   theme_id = excluded.theme_id,
                   placed_at = excluded.placed_at,
                   placement_kind = excluded.placement_kind,
                   plex_refreshed = excluded.plex_refreshed,
                   provenance = excluded.provenance""",
            (media_type, tmdb_id, section_id, edition_key, theme_id,
             finding["media_folder"], now_iso(), placement_kind, provenance),
        )
        # v1.10.50: implicit ack — adopting a sidecar is the user
        # routing around whatever was broken on the TDB side. Keep
        # failure_kind so the TDB pill stays red but stop alerting.
        # No-op when there's no failure to ack.
        # v1.14.34: per-section sfa write instead of title-global
        # `themes.failure_acked_at`. Pre-fix the title-global ack
        # instantly dismissed the FAIL glyph + N FAIL count for
        # EVERY section that owns the title — even though only this
        # section got an adopted sidecar. Same cross-section bleed
        # pattern (CLAUDE.md class K) v1.14.24 closed for SET URL +
        # UPLOAD MP3. Adopt was the third / final user-resolves-
        # locally surface that needed the same conversion. The
        # section_id at the top of _do_adopt is the row-scoped
        # write target; ON CONFLICT DO NOTHING preserves any
        # earlier explicit ack the user may have set.
        if section_id:
            conn.execute(
                """INSERT INTO section_failure_acks
                   (media_type, tmdb_id, section_id, acked_at, acked_by)
                   SELECT ?, ?, ?, ?, 'auto:adopt'
                   WHERE EXISTS (
                       SELECT 1 FROM themes t
                       WHERE t.media_type = ?
                         AND t.tmdb_id = ?
                         AND t.failure_kind IS NOT NULL
                   )
                   ON CONFLICT(media_type, tmdb_id, section_id) DO NOTHING""",
                (media_type, tmdb_id, section_id, now_iso(),
                 media_type, tmdb_id),
            )
        # v1.11.43: keep the denormalized plex_items.theme_id column in
        # sync with the just-written themes row, so the library renderer
        # picks up the new state on the very next /api/library refresh
        # without waiting for plex_enum's resolve_theme_ids to catch up.
        # Pre-fix the row appeared to "do nothing" after adopt — the
        # lib query joins themes via pi.theme_id (v1.11.26 cache), so
        # if it was NULL or pointing at an old plex_orphan row the
        # adopted lf+p rows weren't visible until the next enum
        # (~tens of seconds to minutes on large libraries). Scoped to
        # the section being adopted so a per-section adopt doesn't
        # touch sibling sections that may legitimately resolve
        # differently.
        # v0.50.89: added "AND edition_key = ?" — pre-fix this UPDATE matched
        # by (section_id, media_type, guid_tmdb) alone, so adopting ONE
        # edition's sidecar re-linked EVERY sibling edition's plex_items row
        # in the section to the newly-adopted theme_id, even though the
        # local_files/placements writes above are correctly edition-scoped.
        plex_media_type = "show" if media_type == "tv" else "movie"
        conn.execute(
            """UPDATE plex_items SET theme_id = ?
               WHERE section_id = ?
                 AND media_type = ?
                 AND edition_key = ?
                 AND (guid_tmdb = ? OR (guid_tmdb IS NULL AND folder_path = ?))""",
            (theme_id, section_id, plex_media_type, edition_key, tmdb_id,
             finding["media_folder"]),
        )

    return {
        "action": "adopt",
        "media_type": media_type,
        "tmdb_id": tmdb_id,
        "theme_id": theme_id,
        "canonical_path": str(canonical_path),
        "placement_kind": placement_kind,
    }


def _do_replace(db_path: Path, finding, settings, decided_by: str) -> dict:
    """Enqueue a fresh download job for this theme. The existing theme.mp3
    will be overwritten when the worker places the new file."""
    theme_id = finding["theme_id"]
    if theme_id is None:
        raise AdoptError("cannot replace orphan — no upstream record exists")

    with get_conn(db_path) as conn:
        theme = conn.execute(
            "SELECT media_type, tmdb_id, youtube_url FROM themes WHERE id = ?",
            (theme_id,),
        ).fetchone()
        if not theme:
            raise AdoptError(f"theme_id {theme_id} not found")
        if not theme["youtube_url"]:
            raise AdoptError("theme has no youtube_url to download from")

        # v1.11.0: scoped to the finding's section. Pre-fix this
        # inserted a section-less download job which the worker rejects
        # with _JobPermanentFailure.
        section_id = finding["section_id"]
        if not section_id:
            raise AdoptError("scan finding missing section_id")
        conn.execute(
            """INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,
                                 payload, status, created_at, next_run_at)
               VALUES ('download', ?, ?, ?, ?, 'pending', ?, ?)""",
            (theme["media_type"], theme["tmdb_id"], section_id,
             json.dumps({"reason": "scan_replace"}),
             now_iso(), now_iso()),
        )
    return {
        "action": "replace",
        "theme_id": theme_id,
        "tmdb_id": theme["tmdb_id"],
        "youtube_url": theme["youtube_url"],
        "status": "download enqueued",
    }


def _do_keep(db_path: Path, finding, decided_by: str) -> dict:
    """Mark this theme as a manual override so future placements won't
    overwrite it. The user's existing theme.mp3 stays exactly where it is."""
    theme_id = finding["theme_id"]
    if theme_id is None:
        return {"action": "keep_existing", "note": "orphan kept in place"}

    with get_conn(db_path) as conn:
        theme = conn.execute(
            "SELECT media_type, tmdb_id, youtube_url FROM themes WHERE id = ?",
            (theme_id,),
        ).fetchone()
        if not theme:
            raise AdoptError(f"theme_id {theme_id} not found")
        # An override row keyed on (media_type, tmdb_id) signals manual
        # provenance. We store the existing youtube_url (or empty) so this
        # serves as a "do not auto-replace" marker.
        # v1.12.72: scan/adopt's "keep existing" lock writes the
        # global ('') override row since the keep-decision applies
        # across all sections. Per-section preferences set later
        # via SET URL still take precedence per worker fall-back.
        # v1.23.99 (audit): ON CONFLICT DO UPDATE the named columns instead of
        # INSERT OR REPLACE. The OR REPLACE deleted + re-inserted the row, so any
        # column NOT listed here (notably `intent`) silently reset to its schema
        # default ('replace') — a scan KEEP on a row previously set intent='backup'
        # (via a PROMOTE / backup-only flow) flipped it back to active-replace,
        # inverting the user's backup-vs-active choice. DO UPDATE preserves intent
        # (and any other unlisted column) on an existing override.
        conn.execute(
            """INSERT INTO user_overrides
                 (media_type, tmdb_id, theme_id, youtube_url, set_at,
                  set_by, note, section_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, '')
               ON CONFLICT(media_type, tmdb_id, section_id, edition_key) DO UPDATE SET
                   theme_id = excluded.theme_id,
                   youtube_url = excluded.youtube_url,
                   set_at = excluded.set_at,
                   set_by = excluded.set_by,
                   note = excluded.note""",
            (theme["media_type"], theme["tmdb_id"], theme_id,
             theme["youtube_url"] or "",
             now_iso(), decided_by, "kept existing theme.mp3 from scan"),
        )
    return {
        "action": "keep_existing",
        "theme_id": theme_id,
    }


def _create_orphan_theme(db_path: Path, finding,
                         decided_by: str) -> tuple[int, int]:
    """Insert a new themes row for an orphan finding. Returns (theme_id,
    tmdb_id). For unresolved orphans, allocates a synthetic negative
    tmdb_id starting from -1 going down."""
    metadata = json.loads(finding["resolved_metadata"] or "{}")
    media_type = "movie" if finding["section_type"] == "movie" else "tv"
    title = metadata.get("title") or "Unknown"
    year = metadata.get("year") or ""
    real_tmdb = metadata.get("tmdb_id")
    imdb_id = metadata.get("imdb_id")
    tvdb_id = metadata.get("tvdb_id")

    # v1.22.52 (forward-fix): resolve imdb→tmdb via TMDB BEFORE minting a
    # synthetic negative id. the user's prod diagnostic: 549/555 imdb-bearing
    # orphans resolved to a real tmdb — they were real titles minted synthetic
    # only because no resolver ran here. Resolving now keys the row to its real
    # identity from birth (no de-orphan walker needed); failures fall through to
    # the synthetic mint exactly as before. Network BEFORE get_conn (no lock
    # held). `..config` not `.config` (the v1.18.55 import trap).
    if not real_tmdb and imdb_id:
        try:
            from ..config import Settings
            from .tmdb import TMDBClient
            _settings = Settings()
            if _settings.tmdb_api_key:
                _hit = TMDBClient(
                    _settings.tmdb_api_key, db_path).lookup_by_imdb(imdb_id)
                _cand = (_hit or {}).get("tmdb_id")
                _kind = (_hit or {}).get("kind")
                if _cand and ((_kind == "tv") == (media_type == "tv")):
                    real_tmdb = _cand
                    log.info(
                        "orphan-create: resolved imdb=%s → tmdb=%s for '%s' "
                        "(%s) — keying to real identity instead of synthetic",
                        imdb_id, _cand, title, media_type)
        except Exception as e:  # noqa: BLE001
            log.warning(
                "orphan-create: imdb→tmdb resolve failed for %s ('%s'): %s — "
                "falling back to synthetic id", imdb_id, title, e)

    with get_conn(db_path) as conn:
        if real_tmdb:
            # Check if a row already exists for this real tmdb_id (paranoia
            # — orphan_resolvable said no row, but races happen)
            existing = conn.execute(
                "SELECT id FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, real_tmdb),
            ).fetchone()
            if existing:
                return existing["id"], real_tmdb
            # v1.22.78: tolerate losing the check→INSERT race (a concurrent
            # adopt inserted between the SELECT above and here) — DO NOTHING
            # + re-read instead of 500ing on the UNIQUE.
            cur = conn.execute(
                """INSERT INTO themes
                     (media_type, tmdb_id, tvdb_id, imdb_id, title, year,
                      upstream_source, last_seen_sync_at, first_seen_sync_at)
                   VALUES (?, ?, ?, ?, ?, ?, 'plex_orphan', ?, ?)
                   ON CONFLICT(media_type, tmdb_id) DO NOTHING""",
                (media_type, real_tmdb, tvdb_id, imdb_id, title, year,
                 now_iso(), now_iso()),
            )
            if cur.rowcount:
                return cur.lastrowid, real_tmdb
            raced = conn.execute(
                "SELECT id FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, real_tmdb),
            ).fetchone()
            return raced["id"], real_tmdb
        # v1.22.78: mint the synthetic id INSIDE the INSERT — one statement
        # is atomic even on this autocommit conn. The old SELECT-MIN-then-
        # INSERT pair ran as two independent autocommit statements (the
        # prior "atomic via single transaction" comment was false): a
        # concurrent inline ADOPT + worker adopt could read the same MIN,
        # and the loser 500'd on the UNIQUE.
        cur = conn.execute(
            """INSERT INTO themes
                 (media_type, tmdb_id, tvdb_id, imdb_id, title, year,
                  upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES (?, COALESCE((SELECT MIN(tmdb_id) FROM themes
                                    WHERE media_type = ?
                                      AND tmdb_id < 0), 0) - 1,
                       ?, ?, ?, ?, 'plex_orphan', ?, ?)""",
            (media_type, media_type, tvdb_id, imdb_id, title, year,
             now_iso(), now_iso()),
        )
        minted = conn.execute(
            "SELECT tmdb_id FROM themes WHERE id = ?", (cur.lastrowid,),
        ).fetchone()
        return cur.lastrowid, minted["tmdb_id"]


def _canonical_filename(db_path: Path, theme_id: int, file_sha256: str) -> str:
    """Pick a filename for the canonical themes_dir copy.

    Preference: the theme's youtube_video_id if known (consistent with
    auto-downloaded themes). Otherwise: 'orphan_<8-hex>.mp3' from the file's
    sha256."""
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT youtube_video_id FROM themes WHERE id = ?", (theme_id,),
        ).fetchone()
    vid = row and row["youtube_video_id"]
    if vid:
        return f"{vid}.mp3"
    return f"orphan_{file_sha256[:8]}.mp3"
