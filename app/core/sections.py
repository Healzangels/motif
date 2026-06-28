"""
Plex section discovery cache.

On startup and after each sync, motif enumerates Plex library sections
via /library/sections, applies the include/exclude rules from settings,
and caches the result in the plex_sections table. The table is the
single source of truth for "which sections do we manage" — the cached
metadata also drives the /libraries page in the UI.

Sections that disappear from Plex (deleted libraries) are kept in the
cache with stale last_seen_at; the UI shows them dimmed. The user can
manually purge them.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from .canonical import (
    canonical_theme_subdir,
    compute_section_themes_subdir,
    legacy_canonical_theme_subdir_pre_v1_14_94,
)
from .db import get_conn
from .plex import PlexClient, PlexSection

log = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _allocate_themes_subdir(
    conn, *, title: str, type_: str, is_anime: bool, is_4k: bool,
    location_paths: list[str] | None = None,
    own_section_id: str | None = None,
) -> str:
    """v1.11.0: pick a unique themes_subdir slug for a section.

    Computes the candidate slug via compute_section_themes_subdir,
    then checks plex_sections for collisions (excluding own_section_id
    when updating). On collision appends `-2`, `-3`, ... until unique.

    v1.11.58: now takes location_paths so the slug derives from the
    on-disk library folder basename (matching the user's mental
    model 'themes folder mirrors media folder').
    """
    base = compute_section_themes_subdir(
        title, type_=type_, is_anime=is_anime, is_4k=is_4k,
        location_paths=location_paths,
    )
    candidate = base
    suffix = 2
    while True:
        if own_section_id is None:
            existing = conn.execute(
                "SELECT 1 FROM plex_sections WHERE themes_subdir = ?",
                (candidate,),
            ).fetchone()
        else:
            existing = conn.execute(
                "SELECT 1 FROM plex_sections "
                "WHERE themes_subdir = ? AND section_id != ?",
                (candidate, own_section_id),
            ).fetchone()
        if existing is None:
            return candidate
        candidate = f"{base}-{suffix}"
        suffix += 1


def reassign_themes_subdir(
    db_path: Path, section_id: str,
) -> str | None:
    """v1.11.0: recompute themes_subdir for a section after the user
    toggles is_anime / is_4k. Returns the new subdir, or None when the
    section doesn't exist. The on-disk staging tree under the OLD
    subdir is left in place — callers (worker / API) that move files
    are responsible for re-staging if the rename actually shifts the
    storage location.
    """
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT title, type, is_anime, is_4k, location_paths "
            "FROM plex_sections WHERE section_id = ?",
            (section_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            locs = json.loads(row["location_paths"] or "[]")
        except (json.JSONDecodeError, TypeError) as e:
            # v1.15.35: log corrupted location_paths so the operator
            # can manually repair via API. Pre-fix the silent-empty
            # fallback meant the section's themes_subdir allocation
            # used title-based slug only — risking collision with
            # another section that had the same title.
            log.warning(
                "reassign_themes_subdir: section %s has corrupt "
                "location_paths JSON (%s) — falling back to empty "
                "list for subdir allocation",
                section_id, e,
            )
            locs = []
        new_subdir = _allocate_themes_subdir(
            conn, title=row["title"], type_=row["type"],
            is_anime=bool(row["is_anime"]), is_4k=bool(row["is_4k"]),
            location_paths=locs,
            own_section_id=section_id,
        )
        conn.execute(
            "UPDATE plex_sections SET themes_subdir = ? WHERE section_id = ?",
            (new_subdir, section_id),
        )
    return new_subdir


def refresh_sections(
    db_path: Path,
    plex: PlexClient,
    *,
    excluded_titles: set[str],
    included_titles: set[str],
) -> list[dict]:
    """Discover sections from Plex, apply include/exclude rules, persist
    to plex_sections, and return the current set of managed sections.

    Rules:
    - If included_titles is non-empty, only sections with matching titles are
      included; everything else is excluded.
    - Else if excluded_titles is non-empty, all sections are included EXCEPT
      those in the exclude list (legacy v1.4.x behavior).
    - Otherwise, sections are unchecked by default and the user opts in via
      the Libraries page.
    - Sections seen in this sync get last_seen_at updated.
    - Sections not seen are left untouched (stale rows kept for UI).
    """
    discovered: list[PlexSection] = plex.discover_sections()
    log.info("Plex returned %d sections", len(discovered))

    now = _now()
    sections_with_state: list[dict] = []

    with get_conn(db_path) as conn:
        for s in discovered:
            if included_titles:
                included = s.title in included_titles
            elif excluded_titles:
                included = s.title not in excluded_titles
            else:
                included = False

            existing = conn.execute(
                "SELECT included, themes_subdir, is_anime, is_4k "
                "FROM plex_sections WHERE section_id = ?",
                (s.section_id,),
            ).fetchone()

            if existing is None:
                # v1.11.0: compute the section's themes_subdir slug here
                # so it's stable across title renames. is_anime/is_4k
                # default to 0 at first discovery; if the user later
                # toggles a flag we recompute via api flag-update.
                # v1.11.58: pass location_paths so the slug derives
                # from the on-disk library folder basename instead of
                # the section title.
                subdir = _allocate_themes_subdir(
                    conn, title=s.title, type_=s.type,
                    is_anime=False, is_4k=False,
                    location_paths=s.location_paths,
                )
                conn.execute(
                    """INSERT INTO plex_sections
                       (section_id, uuid, title, type, agent, language,
                        location_paths, included, themes_subdir,
                        discovered_at, last_seen_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (s.section_id, s.uuid, s.title, s.type, s.agent, s.language,
                     json.dumps(s.location_paths), 1 if included else 0,
                     subdir, now, now),
                )
            else:
                # Preserve any user-driven include/exclude override that may
                # have been set in the UI; only update metadata that came
                # from Plex. The env-var rules apply only on FIRST DISCOVERY.
                # If the section title changed we leave themes_subdir alone
                # (rewriting it would orphan the on-disk staging files).
                conn.execute(
                    """UPDATE plex_sections SET
                          uuid = ?, title = ?, type = ?, agent = ?, language = ?,
                          location_paths = ?, last_seen_at = ?
                       WHERE section_id = ?""",
                    (s.uuid, s.title, s.type, s.agent, s.language,
                     json.dumps(s.location_paths), now, s.section_id),
                )

        # Read everything back for return
        rows = conn.execute(
            "SELECT * FROM plex_sections ORDER BY title COLLATE NOCASE"
        ).fetchall()
        sections_with_state = [dict(r) for r in rows]

    return sections_with_state


# v1.17.9: get_managed_section_ids deleted — was imported by
# api.py but never called. Callers query `plex_sections.included=1`
# directly with their own joins. Hygiene audit removal.


def list_sections(db_path: Path) -> list[dict]:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM plex_sections ORDER BY type, title COLLATE NOCASE"
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["location_paths"] = json.loads(d.get("location_paths") or "[]")
        except (TypeError, ValueError) as e:
            # v1.15.35: log corrupt location_paths in list_sections
            # too — read-only path, but the operator should know
            # if a section's stored JSON has rotted (could
            # explain placement mis-routing reports).
            log.warning(
                "list_sections: section %s has corrupt "
                "location_paths JSON: %s",
                d.get("section_id"), e,
            )
            d["location_paths"] = []
        out.append(d)
    return out


def set_section_inclusion(
    db_path: Path, section_id: str, included: bool, *, updated_by: str = "user"
) -> bool:
    with get_conn(db_path) as conn:
        cur = conn.execute(
            "UPDATE plex_sections SET included = ? WHERE section_id = ?",
            (1 if included else 0, section_id),
        )
    return cur.rowcount > 0


def migrate_themes_subdirs_inplace(db_path: Path, themes_dir: Path) -> int:
    """v1.11.58: one-shot recompute of every section's themes_subdir
    against the latest naming rule (basename of location_paths[0]).
    For each section whose subdir changed:
      1. If <themes_dir>/<old> exists on disk, rename to <themes_dir>/<new>.
         If <new> already exists we skip the move (would clobber); the
         user can resolve manually. Failure to rename aborts this
         section's migration to avoid leaving the DB pointing at a
         folder that doesn't exist.
      2. UPDATE plex_sections.themes_subdir.
      3. UPDATE local_files.file_path (any row whose path begins with
         '<old>/' is rewritten to '<new>/').

    Idempotent: a section already on the new naming becomes a no-op.
    Returns the count of sections actually migrated.
    """
    import os
    migrated = 0
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT section_id, title, type, is_anime, is_4k, "
            "       location_paths, themes_subdir "
            "FROM plex_sections"
        ).fetchall()
    for r in rows:
        try:
            locs = json.loads(r["location_paths"] or "[]")
        except (json.JSONDecodeError, TypeError):
            locs = []
        # Compute the desired slug (without collision suffix — we'll
        # apply that via _allocate_themes_subdir below)
        from .canonical import compute_section_themes_subdir
        desired_base = compute_section_themes_subdir(
            r["title"], type_=r["type"],
            is_anime=bool(r["is_anime"]), is_4k=bool(r["is_4k"]),
            location_paths=locs,
        )
        old_subdir = r["themes_subdir"] or ""
        if not old_subdir or desired_base == old_subdir:
            continue
        # Re-allocate (with collision check) to land on the final slug
        with get_conn(db_path) as conn:
            new_subdir = _allocate_themes_subdir(
                conn, title=r["title"], type_=r["type"],
                is_anime=bool(r["is_anime"]), is_4k=bool(r["is_4k"]),
                location_paths=locs,
                own_section_id=r["section_id"],
            )
        if new_subdir == old_subdir:
            continue  # collision pushed us back to the same name

        old_dir = themes_dir / old_subdir
        new_dir = themes_dir / new_subdir
        # Move the on-disk folder if the old one exists
        if old_dir.exists():
            if new_dir.exists():
                log.warning(
                    "Themes subdir migration: would move %s → %s but "
                    "the destination already exists; skipping section %s",
                    old_dir, new_dir, r["section_id"],
                )
                continue
            try:
                new_dir.parent.mkdir(parents=True, exist_ok=True)
                os.rename(str(old_dir), str(new_dir))
                log.info("Themes subdir migrated on disk: %s → %s",
                         old_dir, new_dir)
            except OSError as e:
                log.warning(
                    "Themes subdir migration: rename %s → %s failed: %s; "
                    "leaving section %s on the old subdir",
                    old_dir, new_dir, e, r["section_id"],
                )
                continue

        # DB updates: section row + every local_files.file_path with
        # the old prefix
        with get_conn(db_path) as conn:
            conn.execute(
                "UPDATE plex_sections SET themes_subdir = ? WHERE section_id = ?",
                (new_subdir, r["section_id"]),
            )
            conn.execute(
                "UPDATE local_files SET file_path = ? || substr(file_path, ?) "
                "WHERE file_path LIKE ?",
                (new_subdir + "/", len(old_subdir) + 1, old_subdir + "/%"),
            )
        migrated += 1
        log.info("Themes subdir migrated: section %s '%s' → '%s'",
                 r["section_id"], old_subdir, new_subdir)
    return migrated


def migrate_v1_14_94_colon_folders(
    db_path: Path, themes_dir: Path,
) -> dict:
    """v1.14.97: one-shot rename sweep for the v1.14.94 colon-handling
    fix. v1.14.94 changed canonical_theme_subdir so titles like
    `Terminator 2: Judgment Day` lay down as
    `Terminator 2 - Judgment Day (1991)` instead of
    `Terminator 2- Judgment Day (1991)`. Pre-v1.14.97 only NEW
    downloads got the corrected name; existing folders kept the
    legacy hyphen-no-space shape.

    the user: "1.14.94 folder name but can we make it so it updates
    existing?"

    Approach: for each local_files row, recompute both the v1.14.94
    canonical AND the pre-v1.14.94 (legacy) canonical. If the row's
    on-disk folder matches the legacy form AND the new form differs,
    rename + update DB. If it matches the new form, already migrated
    — skip. If it matches NEITHER, leave alone (title drifted
    independently and renaming would be wrong).

    Idempotent — re-running after success does nothing because every
    row's folder now matches the new canonical.

    Crash recovery: if a previous run renamed the folder but crashed
    before updating the DB, the old folder is gone and the new one
    exists. The sweep detects this (old missing, new present) and
    just updates the DB pointer.

    Returns a stats dict so the caller can log what happened:
      {scanned, renamed, db_repaired, conflict_skipped,
       canonical_missing, drift_skipped}
    """
    import os

    stats = {
        "scanned": 0,
        "renamed": 0,
        "db_repaired": 0,
        "conflict_skipped": 0,
        "canonical_missing": 0,
        "drift_skipped": 0,
    }
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT lf.media_type, lf.tmdb_id, lf.section_id, "
            "       lf.file_path, t.title, t.year "
            "FROM local_files lf "
            "JOIN themes t "
            "  ON t.media_type = lf.media_type AND t.tmdb_id = lf.tmdb_id "
            "WHERE lf.file_path IS NOT NULL"
        ).fetchall()

    for r in rows:
        stats["scanned"] += 1
        title = r["title"]
        year = str(r["year"]) if r["year"] is not None else None
        file_path = r["file_path"] or ""
        # file_path shape: <themes_subdir>/<canonical_dir>/theme.mp3
        # Need 3 parts; if it doesn't fit, skip (legacy shapes from
        # pre-v1.11.0 staging would have already been pruned).
        parts = file_path.split("/")
        if len(parts) < 3:
            continue
        themes_subdir = parts[0]
        current_dir = parts[1]
        # rest of the path after the canonical dir (typically just
        # 'theme.mp3' but tolerate extra segments defensively)
        tail = "/".join(parts[2:])

        new_canonical = canonical_theme_subdir(title, year)
        legacy_canonical = legacy_canonical_theme_subdir_pre_v1_14_94(
            title, year,
        )
        # No colon in title → both canonicals match → nothing to migrate.
        if new_canonical == legacy_canonical:
            continue

        old_full_path = themes_dir / themes_subdir / current_dir
        new_full_path = themes_dir / themes_subdir / new_canonical
        new_relative = f"{themes_subdir}/{new_canonical}/{tail}"

        if current_dir == new_canonical:
            # Already on the new canonical — nothing to do.
            continue

        if current_dir != legacy_canonical:
            # Folder name doesn't match either expected shape — title
            # drifted independently of the v1.14.94 fix. Don't touch.
            stats["drift_skipped"] += 1
            continue

        # Row's folder matches the legacy shape. Plan to rename to
        # the new canonical.
        if old_full_path.exists():
            if new_full_path.exists():
                log.warning(
                    "v1.14.94 colon migration: would rename %s → %s "
                    "but the destination already exists; skipping",
                    old_full_path, new_full_path,
                )
                stats["conflict_skipped"] += 1
                continue
            try:
                new_full_path.parent.mkdir(parents=True, exist_ok=True)
                os.rename(str(old_full_path), str(new_full_path))
                log.info(
                    "v1.14.94 colon migration: renamed %s → %s",
                    old_full_path, new_full_path,
                )
            except OSError as e:
                log.warning(
                    "v1.14.94 colon migration: rename %s → %s failed: %s",
                    old_full_path, new_full_path, e,
                )
                continue
            stats["renamed"] += 1
        elif new_full_path.exists():
            # Crash-recovery case: a previous run renamed the folder
            # but crashed before the DB update. Just fix the pointer.
            log.info(
                "v1.14.94 colon migration: DB pointer repair (folder "
                "already at new path): %s",
                new_full_path,
            )
            stats["db_repaired"] += 1
        else:
            # Neither old nor new exists on disk — canonical_missing.
            # The next stat-driven UI render will mark this row's DL
            # as broken; renaming the DB pointer would just shift the
            # broken state. Leave alone.
            stats["canonical_missing"] += 1
            continue

        # Update local_files.file_path to the new canonical path.
        with get_conn(db_path) as conn:
            conn.execute(
                "UPDATE local_files SET file_path = ? "
                "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?",
                (new_relative, r["media_type"], r["tmdb_id"], r["section_id"]),
            )
    return stats
