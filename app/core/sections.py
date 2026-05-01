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

from .canonical import compute_section_themes_subdir
from .db import get_conn
from .plex import PlexClient, PlexSection

log = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _allocate_themes_subdir(
    conn, *, title: str, type_: str, is_anime: bool, is_4k: bool,
    own_section_id: str | None = None,
) -> str:
    """v1.11.0: pick a unique themes_subdir slug for a section.

    Computes the candidate slug via compute_section_themes_subdir,
    then checks plex_sections for collisions (excluding own_section_id
    when updating). On collision appends `-2`, `-3`, ... until unique.
    """
    base = compute_section_themes_subdir(
        title, type_=type_, is_anime=is_anime, is_4k=is_4k,
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
            "SELECT title, type, is_anime, is_4k FROM plex_sections "
            "WHERE section_id = ?",
            (section_id,),
        ).fetchone()
        if row is None:
            return None
        new_subdir = _allocate_themes_subdir(
            conn, title=row["title"], type_=row["type"],
            is_anime=bool(row["is_anime"]), is_4k=bool(row["is_4k"]),
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
                subdir = _allocate_themes_subdir(
                    conn, title=s.title, type_=s.type,
                    is_anime=False, is_4k=False,
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


def get_managed_section_ids(db_path: Path, *, media_type: str | None = None) -> list[str]:
    """Return the section IDs motif should currently manage.
    If media_type is given, filters to that type."""
    with get_conn(db_path) as conn:
        if media_type is None:
            rows = conn.execute(
                "SELECT section_id FROM plex_sections WHERE included = 1"
            ).fetchall()
        else:
            plex_type = "movie" if media_type == "movie" else "show"
            rows = conn.execute(
                "SELECT section_id FROM plex_sections WHERE included = 1 AND type = ?",
                (plex_type,),
            ).fetchall()
    return [r["section_id"] for r in rows]


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
        except (TypeError, ValueError):
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
